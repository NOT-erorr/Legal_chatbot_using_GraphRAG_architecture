"""
orchestrator/graph.py — LangGraph workflow cho Vietnamese Legal GraphRAG.

Luồng multi-intent:

    ┌─────────┐
    │  START  │
    └────┬────┘
         │
    ┌────▼─────┐   Phân loại ý định (hybrid: heuristic → LLM fallback)
    │ CLASSIFY │   + trích xuất tham chiếu tường minh (Điều/Khoản/văn bản)
    └────┬─────┘
         │  (conditional)
    ┌────┴───────────────────────────┐
    │                                │
general_qa                       structured
    │                                │  EXPLAIN_ARTICLE / SUMMARIZE_DOC
┌───▼────┐  Qdrant + Neo4j      ┌────▼──────┐  COMPARE / LIST_RELATED
│RETRIEVE│  (hybrid + RRF)      │STRUCTURED │  đọc Neo4j theo tham chiếu
└───┬────┘                      └────┬──────┘  (fallback hybrid nếu trống)
    └──────────────┬─────────────────┘
            ┌──────▼──────┐  Gemini Flash, prompt theo intent
            │ SYNTHESIZE  │
            └──────┬──────┘
                   │
              ┌────▼────┐
              │   END   │
              └─────────┘
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from orchestrator.config import OrchestratorConfig
from orchestrator.retrievers import HybridRetriever
from orchestrator.intent import Intent, Reference, classify_intent

class GraphRAGState(TypedDict, total=False):
    """State được truyền qua các node trong LangGraph."""

    # Input
    question: str
    top_k_vector: int
    top_k_graph: int

    # Intent routing
    intent: str
    references: List[Dict[str, Any]]           # Reference.to_dict()

    # Retrieval results
    vector_chunks: List[Any]                   # Hybrid / structured chunks
    graph_related_docs: List[Any]
    graph_related_chunks: List[Any]
    related_docs: List[Dict[str, Any]]         # LIST_RELATED output
    merged_context: str                        # Context tổng hợp cho LLM

    #  Synthesis results
    answer: str
    citations: List[Dict[str, str]]

    #  Metadata
    trace: Dict[str, Any]
    error: Optional[str]


# Node Functions

def _make_retrieve_node(
    retriever: HybridRetriever,
    cfg: OrchestratorConfig,
):
    """Factory cho retrieve node — đóng gói HybridRetriever."""

    def retrieve(state: GraphRAGState) -> GraphRAGState:
        """Bước 1: Hybrid Search (Vector + Keyword + Graph)."""
        question = state["question"]
        top_k = state.get("top_k_vector", cfg.qdrant_top_k)
        t0 = time.perf_counter()

        try:
            result = retriever.search(query=question, top_k=top_k)
            chunks = result.chunks
        except Exception as exc:
            print(f"[retrieve] HybridRetriever error: {exc}")
            import traceback
            traceback.print_exc()
            chunks = []
            result = None

        # ── Merge context ───────────────────────────────────────────
        merged_context = _build_context(chunks)
        citations = _build_citations(chunks)
        total_ms = round((time.perf_counter() - t0) * 1000, 1)

        v_hits = result.vector_count if result else 0
        k_hits = result.keyword_count if result else 0
        g_hits = result.graph_count if result else 0

        return {
            **state,
            "vector_chunks": chunks,
            "graph_related_docs": [],
            "graph_related_chunks": [],
            "merged_context": merged_context,
            "citations": citations,
            "trace": {
                **(state.get("trace") or {}),
                "retrieve_ms": total_ms,
                "vector_hits": v_hits,
                "keyword_hits": k_hits,
                "graph_hits": g_hits,
                "hybrid_ms": result.elapsed_ms if result else 0,
            },
        }

    return retrieve


def _make_gemini_caller(cfg: OrchestratorConfig):
    """Tạo một hàm call(prompt)->str dùng chung cho classify + synthesize.

    Lazy-init + cache Gemini client (tái dùng cho mọi lần gọi)."""
    _cache: Dict[str, Any] = {}

    def _get_client():
        if "client" not in _cache:
            try:
                from google import genai
                _cache["client"] = genai.Client(api_key=cfg.gemini_api_key)
                _cache["provider"] = "google_genai"
            except ImportError:
                import google.generativeai as legacy
                legacy.configure(api_key=cfg.gemini_api_key)
                _cache["client"] = legacy
                _cache["provider"] = "legacy"
        return _cache["client"], _cache["provider"]

    def call(prompt: str, thinking_budget: Optional[int] = None) -> str:
        client, provider = _get_client()
        return _call_gemini(client, provider, cfg, prompt, thinking_budget)

    return call


def _make_classify_node(cfg: OrchestratorConfig, gemini_call):
    """Factory cho classify node — phân loại intent + trích tham chiếu."""

    def classify(state: GraphRAGState) -> GraphRAGState:
        question = state["question"]
        t0 = time.perf_counter()
        try:
            # Phân loại intent không cần suy luận → thinking=0 cho nhanh.
            result = classify_intent(
                question, llm_caller=lambda p: gemini_call(p, thinking_budget=0)
            )
        except Exception as exc:
            print(f"[classify] error: {exc} → fallback general_qa")
            result = None

        if result is None:
            intent, refs, used_llm = Intent.GENERAL_QA, [], False
        else:
            intent, refs, used_llm = result.intent, result.references, result.used_llm

        ms = round((time.perf_counter() - t0) * 1000, 1)
        return {
            **state,
            "intent": intent,
            "references": [r.to_dict() for r in refs],
            "trace": {
                **(state.get("trace") or {}),
                "classify_ms": ms,
                "intent": intent,
                "classify_used_llm": used_llm,
            },
        }

    return classify


def _make_structured_node(retriever: HybridRetriever, cfg: OrchestratorConfig):
    """Factory cho structured retrieval — EXPLAIN/SUMMARIZE/COMPARE/LIST_RELATED.

    Đọc trực tiếp Neo4j theo tham chiếu tường minh. Nếu không lấy được gì,
    fallback về hybrid search (đảm bảo không bao giờ trả ngữ cảnh trống)."""
    neo4j = retriever.neo4j

    def structured(state: GraphRAGState) -> GraphRAGState:
        intent = state.get("intent", Intent.GENERAL_QA)
        refs = [Reference.from_dict(r) for r in state.get("references", [])]
        t0 = time.perf_counter()

        chunks: List[Any] = []
        related: List[Dict[str, Any]] = []

        try:
            if intent == Intent.LIST_RELATED:
                ref = refs[0] if refs else None
                if ref:
                    related = neo4j.fetch_related_documents(ref.doc_number, ref.doc_name)
            elif intent == Intent.SUMMARIZE_DOC:
                ref = refs[0] if refs else None
                if ref:
                    doc_chunks = neo4j.fetch_document_chunks(ref.doc_number, ref.doc_name)
                    chunks = _cap_chunks(doc_chunks, _MAX_CONTEXT_CHARS)
            elif intent == Intent.COMPARE:
                for ref in refs[:2]:
                    doc_chunks = neo4j.fetch_document_chunks(ref.doc_number, ref.doc_name)
                    if ref.article:
                        doc_chunks = _select_article_chunks(doc_chunks, ref.article, ref.clause)
                    chunks.extend(doc_chunks[:8])
            else:  # EXPLAIN_ARTICLE
                ref = refs[0] if refs else None
                if ref:
                    doc_chunks = neo4j.fetch_document_chunks(ref.doc_number, ref.doc_name)
                    if ref.article:
                        doc_chunks = _select_article_chunks(doc_chunks, ref.article, ref.clause)
                    chunks = doc_chunks
        except Exception as exc:
            print(f"[structured] error: {exc}")
            import traceback
            traceback.print_exc()

        # Fallback hybrid nếu không có dữ liệu cấu trúc.
        used_fallback = False
        if not chunks and not related:
            used_fallback = True
            try:
                res = retriever.search(query=state["question"], top_k=cfg.qdrant_top_k)
                chunks = res.chunks
            except Exception as exc:
                print(f"[structured] hybrid fallback failed: {exc}")

        if related:
            merged_context = _build_related_context(related)
            citations = _build_related_citations(related)
        else:
            merged_context = _build_context(chunks)
            citations = _build_citations(chunks)

        ms = round((time.perf_counter() - t0) * 1000, 1)
        return {
            **state,
            "vector_chunks": chunks,
            "related_docs": related,
            "merged_context": merged_context,
            "citations": citations,
            "trace": {
                **(state.get("trace") or {}),
                "structured_ms": ms,
                "structured_chunks": len(chunks),
                "related_count": len(related),
                "structured_fallback": used_fallback,
            },
        }

    return structured


def _make_synthesize_node(cfg: OrchestratorConfig, gemini_call):
    """Factory cho synthesize node — chọn prompt theo intent rồi gọi Gemini."""

    def synthesize(state: GraphRAGState) -> GraphRAGState:
        question = state["question"]
        context = state.get("merged_context", "")
        intent = state.get("intent", Intent.GENERAL_QA)
        t0 = time.perf_counter()

        prompt = _build_prompt_for_intent(intent, question, context)
        budget = _thinking_budget_for_intent(cfg, intent)

        try:
            answer = gemini_call(prompt, thinking_budget=budget)
        except Exception as exc:
            answer = (
                f"[Lỗi khi gọi LLM: {exc}]\n\n"
                f"Dưới đây là ngữ cảnh pháp luật đã thu thập:\n\n{context}"
            )

        synth_ms = round((time.perf_counter() - t0) * 1000, 1)
        prev = state.get("trace") or {}
        stage_ms = (
            prev.get("classify_ms", 0)
            + prev.get("retrieve_ms", 0)
            + prev.get("structured_ms", 0)
        )

        return {
            **state,
            "answer": answer,
            "trace": {
                **prev,
                "synthesize_ms": synth_ms,
                "model": cfg.gemini_chat_model,
                "thinking_budget": budget,
                "total_ms": round(stage_ms + synth_ms, 1),
            },
        }

    return synthesize


# Helper Functions

_SYSTEM_PROMPT = """Bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam.

QUY TẮC BẮT BUỘC:
1. PHÂN LOẠI CÂU HỎI: 
   - Nếu người dùng chào hỏi, hoặc hỏi những vấn đề KHÔNG thuộc lĩnh vực pháp lý: Hãy chủ động giới thiệu bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam, được phát triển dựa trên hệ thống Agentic GraphRAG. Bạn có thể giúp tra cứu, giải đáp các quy định của pháp luật Việt Nam. Tuyệt đối KHÔNG trả lời hay tư vấn những vấn đề ngoài lề.
   - Nếu là câu hỏi pháp luật, tiếp tục thực hiện các quy tắc dưới.

2. TRẢ LỜI CÂU HỎI PHÁP LUẬT:
   - CHỈ sử dụng thông tin từ phần "NGỮ CẢNH PHÁP LUẬT" được cung cấp. KHÔNG bịa đặt.
   - Trích dẫn rõ ràng số hiệu văn bản, Điều, Khoản, Điểm cho mỗi thông tin.
   - Nếu phần ngữ cảnh trống hoặc không có thông tin nào liên quan, hãy nói rõ: "Xin lỗi, thông tin hiện tại trong cơ sở dữ liệu chưa đủ để tôi trả lời câu hỏi này." 

3. CÁCH TRÌNH BÀY:
   - Trả lời trực tiếp, ngôn ngữ chuyên nghiệp dễ hiểu.
   - Luôn kết thúc câu trả lời pháp luật bằng: "⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*" 
"""


_EXPLAIN_PROMPT = """Bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam, chuyên GIẢI THÍCH CHI TIẾT điều luật.

QUY TẮC:
- CHỈ dựa trên "NGỮ CẢNH PHÁP LUẬT" được cung cấp (là toàn văn Điều/Khoản người dùng hỏi). KHÔNG bịa.
- Trình bày theo cấu trúc:
  1. **Tóm lược**: điều luật này quy định về vấn đề gì.
  2. **Phân tích từng Khoản/Điểm**: diễn giải bằng ngôn ngữ dễ hiểu, kèm ví dụ minh hoạ thực tế nếu phù hợp.
  3. **Lưu ý áp dụng**: đối tượng áp dụng, điều kiện, ngoại lệ (nếu có trong ngữ cảnh).
- Trích dẫn rõ số hiệu văn bản, Điều, Khoản, Điểm cho mỗi nội dung.
- Nếu ngữ cảnh trống/không khớp: "Xin lỗi, tôi chưa tìm thấy nội dung điều luật này trong cơ sở dữ liệu."
- Kết thúc bằng: "⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*"
"""

_SUMMARIZE_PROMPT = """Bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam, chuyên TÓM TẮT văn bản pháp luật.

QUY TẮC:
- CHỈ dựa trên "NGỮ CẢNH PHÁP LUẬT" (các phần nội dung của văn bản). KHÔNG bịa.
- Tóm tắt có cấu trúc:
  1. **Phạm vi điều chỉnh & đối tượng áp dụng**.
  2. **Các nội dung chính** (gạch đầu dòng theo nhóm vấn đề, kèm Điều tương ứng).
  3. **Điểm đáng chú ý** (nếu có).
- Ngắn gọn, chính xác, giữ trích dẫn Điều/Khoản quan trọng.
- Nếu ngữ cảnh trống: "Xin lỗi, tôi chưa tìm thấy nội dung văn bản này trong cơ sở dữ liệu."
- Kết thúc bằng: "⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*"
"""

_COMPARE_PROMPT = """Bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam, chuyên SO SÁNH/ĐỐI CHIẾU quy định.

QUY TẮC:
- CHỈ dựa trên "NGỮ CẢNH PHÁP LUẬT" (chứa nội dung của hai đối tượng cần so sánh). KHÔNG bịa.
- Trình bày:
  1. **Điểm giống nhau**.
  2. **Điểm khác nhau** (nên dùng bảng: tiêu chí | đối tượng 1 | đối tượng 2).
  3. **Nhận xét** ngắn gọn.
- Trích dẫn số hiệu văn bản, Điều, Khoản cho mỗi luận điểm.
- Nếu thiếu một trong hai đối tượng trong ngữ cảnh: nói rõ phần nào thiếu.
- Kết thúc bằng: "⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*"
"""

_LIST_PROMPT = """Bạn là Trợ lý AI Tư vấn Pháp luật Việt Nam, chuyên LIỆT KÊ VĂN BẢN LIÊN QUAN.

QUY TẮC:
- "NGỮ CẢNH" là danh sách các văn bản liên quan kèm LOẠI QUAN HỆ và HƯỚNG (outgoing = văn bản gốc tác động tới văn bản kia; incoming = ngược lại).
- Trình bày thành danh sách rõ ràng, diễn giải quan hệ bằng tiếng Việt tự nhiên
  (vd: "Văn bản gốc ĐƯỢC SỬA ĐỔI bởi ..." / "Văn bản gốc SỬA ĐỔI ...").
- Nhóm theo loại quan hệ nếu hợp lý. Nêu số hiệu + tên văn bản.
- Nếu danh sách trống: "Hiện chưa có dữ liệu về văn bản liên quan trong cơ sở dữ liệu."
- Kết thúc bằng: "⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*"
"""

_INTENT_PROMPTS = {
    Intent.EXPLAIN_ARTICLE: _EXPLAIN_PROMPT,
    Intent.SUMMARIZE_DOC: _SUMMARIZE_PROMPT,
    Intent.COMPARE: _COMPARE_PROMPT,
    Intent.LIST_RELATED: _LIST_PROMPT,
}


def _build_prompt(question: str, context: str) -> str:
    return _build_prompt_for_intent(Intent.GENERAL_QA, question, context)


def _build_prompt_for_intent(intent: str, question: str, context: str) -> str:
    system = _INTENT_PROMPTS.get(intent, _SYSTEM_PROMPT)
    return f"""{system}

═══ NGỮ CẢNH PHÁP LUẬT ═══
{context}

═══ CÂU HỎI ═══
{question}

═══ TRẢ LỜI ═══"""


def _build_context(chunks: List[Any]) -> str:
    """Xây dựng context string tổng hợp từ hybrid results."""
    parts: List[str] = []

    if chunks:
        parts.append("── KẾT QUẢ TÌM KIẾM ──")
        for i, chunk in enumerate(chunks, 1):
            header = _chunk_header(chunk)
            score = getattr(chunk, 'rrf_score', 0.0)
            text = (getattr(chunk, 'chunk_text', '') or getattr(chunk, 'text', '')).strip()
            parts.append(f"\n[{i}] {header} (score: {score:.3f})\n{text}")

    return "\n".join(parts)


def _chunk_header(chunk: Any) -> str:
    """Tạo header mô tả cho chunk: [Số hiệu | Điều X | Khoản Y]"""
    parts = []
    if getattr(chunk, 'doc_number', None):
        parts.append(chunk.doc_number)
    elif getattr(chunk, 'doc_title', None):
        parts.append(chunk.doc_title[:60])
    if getattr(chunk, 'chapter', None):
        parts.append(chunk.chapter)
    if getattr(chunk, 'article', None):
        parts.append(chunk.article)
    if getattr(chunk, 'clause', None):
        parts.append(f"Khoản {chunk.clause}")
    return " | ".join(parts) if parts else "N/A"


def _build_citations(chunks: List[Any]) -> List[Dict[str, str]]:
    """Tạo danh sách trích dẫn nguồn (deduped by doc_id)."""
    seen = set()
    citations = []
    for chunk in chunks:
        doc_id = getattr(chunk, 'doc_id', "")
        if not doc_id or doc_id in seen:
            continue
        seen.add(doc_id)
        sources = getattr(chunk, 'sources', [])
        citations.append({
            "doc_id": doc_id,
            "doc_number": getattr(chunk, 'doc_number', ""),
            "doc_title": getattr(chunk, 'doc_title', ""),
            "source": ", ".join(sources) if sources else "hybrid",
        })
    return citations


# Structured-intent helpers

# Cap ngữ cảnh đưa vào LLM (~6k token) để tránh vượt context window khi tóm tắt
# văn bản dài. 1 token tiếng Việt ~ 4 ký tự → 24000 ký tự.
_MAX_CONTEXT_CHARS = 24000


def _norm_int(value: Any) -> Optional[int]:
    """Trích số nguyên đầu tiên từ chuỗi (vd 'Điều 8' → 8)."""
    if value is None:
        return None
    import re
    m = re.search(r"\d+", str(value))
    return int(m.group(0)) if m else None


def _select_article_chunks(
    chunks: List[Any], article: Optional[str], clause: Optional[str] = None
) -> List[Any]:
    """Lọc các chunk thuộc đúng Điều.

    Thực tế dữ liệu: field `c.article` gần như rỗng (chỉ ~27/86k chunk), nhưng
    nội dung Điều NẰM TRONG `chunk_text`. Chiến lược 2 lớp:
      1. Nếu có chunk khớp field `article` → dùng luôn.
      2. Fallback: quét theo span — bắt đầu ở chunk chứa 'Điều N', thu thập tới
         khi gặp 'Điều <khác>' (tức sang điều kế tiếp).
    Không khớp gì → trả [] để caller fallback hybrid."""
    target = _norm_int(article)
    if target is None:
        return chunks

    # Lớp 1 — theo field article (nếu chunking có tách)
    by_field = [c for c in chunks if _norm_int(getattr(c, "article", None)) == target]
    if by_field:
        return by_field

    # Lớp 2 — quét span trong chunk_text
    import re
    start_re = re.compile(rf"điều\s+0*{target}\b", re.IGNORECASE)
    any_re = re.compile(r"điều\s+0*(\d+)\b", re.IGNORECASE)

    selected: List[Any] = []
    collecting = False
    for c in chunks:
        text = getattr(c, "chunk_text", "") or ""
        if not collecting:
            if start_re.search(text):
                collecting = True
                selected.append(c)
        else:
            m = any_re.search(text)
            if m and int(m.group(1)) != target:
                break
            selected.append(c)
    return selected


def _cap_chunks(chunks: List[Any], max_chars: int) -> List[Any]:
    """Giữ chunk theo thứ tự cho tới khi đạt ngân sách ký tự."""
    out: List[Any] = []
    total = 0
    for c in chunks:
        text = getattr(c, "chunk_text", "") or ""
        if total + len(text) > max_chars and out:
            break
        out.append(c)
        total += len(text)
    return out


# Dịch relationship type (động) sang mô tả tiếng Việt dễ đọc cho LIST_RELATED.
_RELATION_VI = {
    "VĂN_BẢN_SỬA_ĐỔI": "sửa đổi",
    "VĂN_BẢN_ĐƯỢC_SỬA_ĐỔI": "được sửa đổi bởi",
    "VĂN_BẢN_BỔ_SUNG": "bổ sung",
    "VĂN_BẢN_ĐƯỢC_BỔ_SUNG": "được bổ sung bởi",
    "VĂN_BẢN_CĂN_CỨ": "căn cứ",
    "VĂN_BẢN_DẪN_CHIẾU": "dẫn chiếu",
    "VĂN_BẢN_HẾT_HIỆU_LỰC": "hết hiệu lực",
    "VĂN_BẢN_BỊ_HẾT_HIỆU_LỰC_1_PHẦN": "bị hết hiệu lực một phần",
    "VĂN_BẢN_QUY_ĐỊNH_HẾT_HIỆU_LỰC": "quy định hết hiệu lực",
    "VĂN_BẢN_QUY_ĐỊNH_HẾT_HIỆU_LỰC_1_PHẦN": "quy định hết hiệu lực một phần",
    "VĂN_BẢN_HD,_QĐ_CHI_TIẾT": "hướng dẫn/quy định chi tiết",
    "VĂN_BẢN_ĐƯỢC_HD,_QĐ_CHI_TIẾT": "được hướng dẫn/quy định chi tiết bởi",
    "VĂN_BẢN_LIÊN_QUAN_KHÁC": "liên quan khác",
    "MENTIONS": "nhắc đến",
    "REFERS_TO": "tham chiếu",
}


def _relation_label(rel_type: str) -> str:
    return _RELATION_VI.get(rel_type, (rel_type or "").replace("_", " ").lower())


def _build_related_context(related: List[Dict[str, Any]]) -> str:
    if not related:
        return ""
    parts = ["── VĂN BẢN LIÊN QUAN ──"]
    for i, r in enumerate(related, 1):
        label = _relation_label(r.get("relation", ""))
        direction = r.get("direction", "")
        num = r.get("doc_number") or ""
        title = r.get("title") or ""
        parts.append(
            f"[{i}] quan hệ: {label} ({direction}) | {num} — {title}"
        )
    return "\n".join(parts)


def _build_related_citations(related: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    seen = set()
    citations = []
    for r in related:
        num = r.get("doc_number") or ""
        if not num or num in seen:
            continue
        seen.add(num)
        citations.append({
            "doc_id": num,
            "doc_number": num,
            "doc_title": r.get("title") or "",
            "source": f"neo4j_related:{_relation_label(r.get('relation',''))}",
        })
    return citations


def _warn_if_truncated(response: Any) -> None:
    """Cảnh báo nếu câu trả lời bị cắt do chạm trần token (finish_reason=MAX_TOKENS).

    Trước đây lỗi này bị "im lặng" vì code chỉ đọc response.text."""
    try:
        candidates = getattr(response, "candidates", None) or []
        if not candidates:
            return
        finish_reason = getattr(candidates[0], "finish_reason", None)
        if finish_reason is not None and "MAX_TOKENS" in str(finish_reason):
            print(
                "[synthesize] CẢNH BÁO: câu trả lời bị cắt (finish_reason=MAX_TOKENS). "
                "Tăng GEMINI_MAX_OUTPUT_TOKENS hoặc giảm GEMINI_THINKING_BUDGET."
            )
    except Exception:
        pass


def _gen_config(
    cfg: OrchestratorConfig, thinking_budget: Optional[int] = None
) -> Dict[str, Any]:
    """Build generation config dùng chung. `thinking_budget` override cho phép đặt
    ngân sách suy luận theo từng intent (None = dùng cfg.gemini_thinking_budget)."""
    budget = cfg.gemini_thinking_budget if thinking_budget is None else thinking_budget
    config: Dict[str, Any] = {
        "temperature": cfg.gemini_temperature,
        "max_output_tokens": cfg.gemini_max_output_tokens,
    }
    # thinking_budget < 0 → để model tự quyết (không set, dùng dynamic mặc định).
    if budget >= 0:
        config["thinking_config"] = {"thinking_budget": budget}
    return config


# Ngân sách "thinking" theo intent: nhóm cần suy luận/cấu trúc (giải thích, tóm tắt,
# so sánh) giữ ngân sách đầy đủ để không giảm chất lượng/accuracy; nhóm tra cứu
# (general_qa) và chỉ-format (list_related) đặt 0 để cắt ~5-7s latency.
_REASONING_INTENTS = {Intent.EXPLAIN_ARTICLE, Intent.SUMMARIZE_DOC, Intent.COMPARE}


def _thinking_budget_for_intent(cfg: OrchestratorConfig, intent: str) -> int:
    return cfg.gemini_thinking_budget if intent in _REASONING_INTENTS else 0


def _call_gemini(
    client: Any,
    provider: str,
    cfg: OrchestratorConfig,
    prompt: str,
    thinking_budget: Optional[int] = None,
) -> str:
    """Gọi Gemini API để sinh câu trả lời."""
    fallback_models = ["gemini-2.5-flash"]
    model_candidates = [cfg.gemini_chat_model, *fallback_models]
    deduped_models: List[str] = []
    for m in model_candidates:
        if m and m not in deduped_models:
            deduped_models.append(m)

    gen_config = _gen_config(cfg, thinking_budget)

    if provider == "google_genai":
        last_exc: Optional[Exception] = None
        for model_name in deduped_models:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=gen_config,
                )
                _warn_if_truncated(response)
                return response.text
            except Exception as exc:
                last_exc = exc
                if "NOT_FOUND" not in str(exc):
                    raise
        assert last_exc is not None
        raise last_exc

    # Legacy SDK — thinking_config có thể không được hỗ trợ → bỏ nếu lỗi.
    last_exc = None
    for model_name in deduped_models:
        try:
            model = client.GenerativeModel(model_name)
            response = model.generate_content(
                prompt,
                generation_config={
                    "temperature": cfg.gemini_temperature,
                    "max_output_tokens": cfg.gemini_max_output_tokens,
                },
            )
            _warn_if_truncated(response)
            return response.text
        except Exception as exc:
            last_exc = exc
            if "NOT_FOUND" not in str(exc):
                raise
    assert last_exc is not None
    raise last_exc


# Graph Builder

def _route_by_intent(state: GraphRAGState) -> str:
    """Conditional edge: GENERAL_QA → hybrid; còn lại → structured fetch."""
    return "hybrid" if state.get("intent", Intent.GENERAL_QA) == Intent.GENERAL_QA else "structured"


def build_graph(cfg: OrchestratorConfig) -> Any:
    """Xây dựng LangGraph StateGraph cho GraphRAG orchestration.

    Luồng multi-intent:
        classify ─┬─ (general_qa)  → retrieve (hybrid) ─┐
                  └─ (structured)  → structured        ─┴─ synthesize → END
    """
    retriever = HybridRetriever(cfg)
    gemini_call = _make_gemini_caller(cfg)

    classify_fn = _make_classify_node(cfg, gemini_call)
    retrieve_fn = _make_retrieve_node(retriever, cfg)
    structured_fn = _make_structured_node(retriever, cfg)
    synthesize_fn = _make_synthesize_node(cfg, gemini_call)

    workflow = StateGraph(GraphRAGState)
    workflow.add_node("classify", classify_fn)
    workflow.add_node("retrieve", retrieve_fn)
    workflow.add_node("structured", structured_fn)
    workflow.add_node("synthesize", synthesize_fn)

    workflow.set_entry_point("classify")
    workflow.add_conditional_edges(
        "classify",
        _route_by_intent,
        {"hybrid": "retrieve", "structured": "structured"},
    )
    workflow.add_edge("retrieve", "synthesize")
    workflow.add_edge("structured", "synthesize")
    workflow.add_edge("synthesize", END)

    compiled = workflow.compile()

    # Attach cleanup references
    compiled._retriever = retriever  # type: ignore[attr-defined]

    return compiled

def invoke_graph(
    compiled_graph: Any,
    question: str,
    top_k_vector: int = 5,
    top_k_graph: int = 5,
    return_chunks: bool = False,
) -> Dict[str, Any]:
    """Chạy graph và trả về kết quả formatted."""
    t0 = time.perf_counter()

    initial_state: GraphRAGState = {
        "question": question,
        "top_k_vector": top_k_vector,
        "top_k_graph": top_k_graph,
        "intent": "",
        "references": [],
        "vector_chunks": [],
        "graph_related_docs": [],
        "graph_related_chunks": [],
        "related_docs": [],
        "merged_context": "",
        "answer": "",
        "citations": [],
        "trace": {},
        "error": None,
    }

    final_state = compiled_graph.invoke(initial_state)
    total_ms = round((time.perf_counter() - t0) * 1000, 1)

    # Update trace with wall-clock time
    trace = final_state.get("trace", {})
    trace["wall_clock_ms"] = total_ms

    result: Dict[str, Any] = {
        "answer": final_state.get("answer", ""),
        "intent": final_state.get("intent", ""),
        "citations": final_state.get("citations", []),
        "trace": trace,
        "disclaimer": "⚖️ Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.",
    }
    if return_chunks:
        result["vector_chunks"] = final_state.get("vector_chunks", [])
        result["related_docs"] = final_state.get("related_docs", [])
    return result
