"""
orchestrator/graph.py — LangGraph workflow cho Vietnamese Legal GraphRAG.

Luồng rút gọn tối ưu cho < 2 giây:

    ┌─────────┐
    │  START  │
    └────┬────┘
         │
    ┌────▼────┐    Embed query + search Qdrant
    │ RETRIEVE│    + parallel Neo4j graph expansion
    └────┬────┘
         │
    ┌────▼──────┐  Gọi Gemini 2.0 Flash với context
    │ SYNTHESIZE│
    └────┬──────┘
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

class GraphRAGState(TypedDict, total=False):
    """State được truyền qua các node trong LangGraph."""

    # ── Input ───────────────────────────────────────────────────────────
    question: str
    top_k_vector: int
    top_k_graph: int

    # ── Retrieval results ───────────────────────────────────────────────
    vector_chunks: List[Any]                   # Hybrid chunks
    graph_related_docs: List[Any]
    graph_related_chunks: List[Any]
    merged_context: str                        # Context tổng hợp cho LLM

    # ── Synthesis results ───────────────────────────────────────────────
    answer: str
    citations: List[Dict[str, str]]

    # ── Metadata ────────────────────────────────────────────────────────
    trace: Dict[str, Any]
    error: Optional[str]


# ═════════════════════════════════════════════════════════════════════════════
# Node Functions
# ═════════════════════════════════════════════════════════════════════════════

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


def _make_synthesize_node(cfg: OrchestratorConfig):
    """Factory cho synthesize node — gọi Gemini Flash để sinh câu trả lời."""

    # Lazy-init Gemini client
    _client_cache: Dict[str, Any] = {}

    def _get_client():
        if "client" not in _client_cache:
            try:
                from google import genai
                _client_cache["client"] = genai.Client(api_key=cfg.gemini_api_key)
                _client_cache["provider"] = "google_genai"
            except ImportError:
                import google.generativeai as legacy
                legacy.configure(api_key=cfg.gemini_api_key)
                _client_cache["client"] = legacy
                _client_cache["provider"] = "legacy"
        return _client_cache["client"], _client_cache["provider"]

    def synthesize(state: GraphRAGState) -> GraphRAGState:
        """Bước 2: Gọi Gemini Flash để tổng hợp câu trả lời."""
        question = state["question"]
        context = state.get("merged_context", "")
        t0 = time.perf_counter()

        prompt = _build_prompt(question, context)

        try:
            client, provider = _get_client()
            answer = _call_gemini(client, provider, cfg, prompt)
        except Exception as exc:
            answer = (
                f"[Lỗi khi gọi LLM: {exc}]\n\n"
                f"Dưới đây là ngữ cảnh pháp luật đã thu thập:\n\n{context}"
            )

        synth_ms = round((time.perf_counter() - t0) * 1000, 1)

        return {
            **state,
            "answer": answer,
            "trace": {
                **(state.get("trace") or {}),
                "synthesize_ms": synth_ms,
                "model": cfg.gemini_chat_model,
                "total_ms": round(
                    (state.get("trace") or {}).get("retrieve_ms", 0) + synth_ms, 1
                ),
            },
        }

    return synthesize


# ═════════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═════════════════════════════════════════════════════════════════════════════

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


def _build_prompt(question: str, context: str) -> str:
    return f"""{_SYSTEM_PROMPT}

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


def _call_gemini(client: Any, provider: str, cfg: OrchestratorConfig, prompt: str) -> str:
    """Gọi Gemini API để sinh câu trả lời."""
    fallback_models = ["gemini-2.5-flash"]
    model_candidates = [cfg.gemini_chat_model, *fallback_models]
    deduped_models: List[str] = []
    for m in model_candidates:
        if m and m not in deduped_models:
            deduped_models.append(m)

    if provider == "google_genai":
        last_exc: Optional[Exception] = None
        for model_name in deduped_models:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config={
                        "temperature": cfg.gemini_temperature,
                        "max_output_tokens": cfg.gemini_max_output_tokens,
                    },
                )
                return response.text
            except Exception as exc:
                last_exc = exc
                if "NOT_FOUND" not in str(exc):
                    raise
        assert last_exc is not None
        raise last_exc

    # Legacy SDK
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
            return response.text
        except Exception as exc:
            last_exc = exc
            if "NOT_FOUND" not in str(exc):
                raise
    assert last_exc is not None
    raise last_exc


# ═════════════════════════════════════════════════════════════════════════════
# Graph Builder
# ═════════════════════════════════════════════════════════════════════════════

def build_graph(cfg: OrchestratorConfig) -> Any:
    """Xây dựng LangGraph StateGraph cho GraphRAG orchestration.

    Luồng rút gọn 2 bước:
        retrieve → synthesize → END
    """
    retriever = HybridRetriever(cfg)

    retrieve_fn = _make_retrieve_node(retriever, cfg)
    synthesize_fn = _make_synthesize_node(cfg)

    # ── Build graph ─────────────────────────────────────────────────────
    workflow = StateGraph(GraphRAGState)

    workflow.add_node("retrieve", retrieve_fn)
    workflow.add_node("synthesize", synthesize_fn)

    workflow.set_entry_point("retrieve")
    workflow.add_edge("retrieve", "synthesize")
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
) -> Dict[str, Any]:
    """Chạy graph và trả về kết quả formatted."""
    t0 = time.perf_counter()

    initial_state: GraphRAGState = {
        "question": question,
        "top_k_vector": top_k_vector,
        "top_k_graph": top_k_graph,
        "vector_chunks": [],
        "graph_related_docs": [],
        "graph_related_chunks": [],
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

    return {
        "answer": final_state.get("answer", ""),
        "citations": final_state.get("citations", []),
        "trace": trace,
        "disclaimer": "⚖️ Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.",
    }
