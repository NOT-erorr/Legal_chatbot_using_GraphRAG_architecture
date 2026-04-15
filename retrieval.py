"""
retrieval.py  —  GraphRAG Retrieval Layer
==========================================
Pipeline 6 bước:
    1) Embed Query       →  Mã hoá câu hỏi bằng multilingual-e5
    2) Vector Search     →  FAISS top-K  →  chunk_ids
    3) Metadata Fetch    →  PostgreSQL   →  content + doc_ids
    4) Graph Expansion   →  Neo4j       →  related docs (AMENDS, REFERENCES, REPEALS, GUIDES)
    5) Synthesis         →  Gộp content + related docs  →  Final Context
    6) Generation        →  LLM (GPT-4o / Claude-3.5)   →  Final Answer

Sử dụng:
    python retrieval.py -q "Doanh nghiệp có bắt buộc đóng BHXH cho lao động thử việc không?"
    python retrieval.py --interactive
    python retrieval.py --search-only -q "hợp đồng lao động"

Xử lý conflict chunk_id – quan hệ chéo trong Neo4j:
    Một chunk_id thuộc một doc_id duy nhất (FK). Khi Graph Expansion trả về nhiều
    related docs cùng liên quan đến doc_id đó (VD: doc A —AMENDS→ doc B —REFERENCES→ doc C),
    hệ thống de-duplicate theo doc_pg_id, giữ nguyên chuỗi quan hệ (rel_chain) để
    LLM hiểu đường dẫn pháp lý. Nếu 2 chunks cùng doc dẫn đến cùng related doc,
    related doc chỉ xuất hiện 1 lần trong context nhưng rel_chain gộp lại.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# --- Local imports (cùng folder) ---
from ingestion import (
    VectorPipeline,
    GraphPipeline,
    FAISS_INDEX_DIR,
    POSTGRES_DSN,
)

# ---------------------------------------------------------------------------
# LLM clients  (hỗ trợ OpenAI  hoặc  Anthropic,  tuỳ API key có sẵn)
# ---------------------------------------------------------------------------
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")       # "openai" | "anthropic"
OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL      = os.getenv("OPENAI_MODEL", "gpt-4o")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL   = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")

TOP_K_VECTOR = int(os.getenv("TOP_K_VECTOR", "5"))
TOP_K_GRAPH  = int(os.getenv("TOP_K_GRAPH",  "5"))

# ---------------------------------------------------------------------------
# System prompt — Đóng vai luật sư tư vấn pháp luật Việt Nam
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = textwrap.dedent("""\
Bạn là một luật sư tư vấn pháp luật Việt Nam giàu kinh nghiệm. Nhiệm vụ của bạn là
trả lời câu hỏi pháp luật dựa **HOÀN TOÀN** vào các tài liệu pháp lý được cung cấp
trong phần NGỮ CẢNH bên dưới.

QUY TẮC BẮT BUỘC:
1. Chỉ sử dụng thông tin từ NGỮ CẢNH. TUYỆT ĐỐI KHÔNG bịa đặt hoặc suy luận ngoài tài liệu.
2. Với mỗi luận điểm, PHẢI trích dẫn rõ: tên văn bản, số ký hiệu, điều/khoản.
   Ví dụ: "Theo Điều 24 Bộ luật Lao động 2019 (45/2019/QH14), …"
3. Nếu tài liệu không đủ thông tin, nói rõ: "Dựa trên tài liệu được cung cấp, không đủ
   căn cứ để kết luận về vấn đề này."
4. Khi có nhiều văn bản liên quan (sửa đổi, hướng dẫn, thay thế), phân tích tính ưu tiên
   áp dụng và giải thích lý do.
5. Kết thúc bằng KHUYẾN NGHỊ và LƯU Ý rằng đây chỉ là tham khảo, không thay thế tư vấn
   pháp lý chuyên nghiệp.

ĐỊNH DẠNG TRẢ LỜI:
## Phân tích pháp lý
(Trình bày luận điểm kèm trích dẫn)

## Các văn bản liên quan
(Liệt kê văn bản sửa đổi / hướng dẫn / thay thế nếu có)

## Kết luận & Khuyến nghị
(Tóm tắt câu trả lời + khuyến nghị)

⚠️ *Lưu ý: Nội dung trên chỉ mang tính tham khảo, không phải tư vấn pháp lý ràng buộc.*
""")


# ============================================================================
# LLM Abstraction
# ============================================================================

def _call_llm(system: str, user: str) -> str:
    """
    Gọi LLM với system prompt + user message.
    Hỗ trợ: OpenAI (gpt-4o)  hoặc  Anthropic (claude-3.5-sonnet).
    Nếu không có API key  →  trả raw context.
    """
    # ----- OpenAI -----
    if LLM_PROVIDER == "openai" and OPENAI_API_KEY:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user},
                ],
                temperature=0.2,
                max_tokens=4096,
            )
            return resp.choices[0].message.content
        except Exception as e:
            return f"[OpenAI Error: {e}]"

    # ----- Anthropic -----
    if LLM_PROVIDER == "anthropic" and ANTHROPIC_API_KEY:
        try:
            from anthropic import Anthropic
            client = Anthropic(api_key=ANTHROPIC_API_KEY)
            resp = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
            return resp.content[0].text
        except Exception as e:
            return f"[Anthropic Error: {e}]"

    # ----- Fallback: No LLM -----
    return (
        "[⚠️  LLM chưa được cấu hình. Set OPENAI_API_KEY hoặc ANTHROPIC_API_KEY]\n\n"
        f"System Prompt:\n{system[:200]}…\n\n"
        f"User Query + Context (raw):\n{user}"
    )


# ============================================================================
# Core Retrieval Function
# ============================================================================

def retrieve_and_generate(
    user_query: str,
    top_k_vector: int = TOP_K_VECTOR,
    top_k_graph:  int = TOP_K_GRAPH,
    *,
    vector_pipeline: Optional[VectorPipeline] = None,
    graph_pipeline:  Optional[GraphPipeline] = None,
    search_only: bool = False,
) -> Dict[str, Any]:
    """
    Pipeline truy vấn GraphRAG 6 bước.

    Args:
        user_query:      Câu hỏi pháp luật từ người dùng.
        top_k_vector:    Số chunks trả về từ FAISS.
        top_k_graph:     Số related docs tối đa từ Neo4j.
        vector_pipeline: (tái sử dụng) VectorPipeline instance.
        graph_pipeline:  (tái sử dụng) GraphPipeline instance.
        search_only:     True = chỉ tìm kiếm, không gọi LLM.

    Returns:
        Dict { answer, citations, context, trace, disclaimer }
    """

    # === CREATE PIPELINE INSTANCES (nếu chưa truyền vào) ===
    own_vp = vector_pipeline is None
    own_gp = graph_pipeline is None
    if own_vp:
        vector_pipeline = VectorPipeline()
        vector_pipeline.load()
    if own_gp:
        graph_pipeline = GraphPipeline()

    # ------------------------------------------------------------------
    # STEP 1 — Embed Query
    # ------------------------------------------------------------------
    print(f"\n{'─'*60}")
    print(f"❓  Query: {user_query}")
    print(f"{'─'*60}")
    print("⚡  Step 1: Embed query …")
    # (embedding xảy ra bên trong vector_pipeline.search)

    # ------------------------------------------------------------------
    # STEP 2 — Vector Search  (FAISS top-K)
    # ------------------------------------------------------------------
    print(f"🔍  Step 2: FAISS vector search (top-{top_k_vector}) …")
    faiss_hits = vector_pipeline.search(user_query, top_k=top_k_vector)

    if not faiss_hits:
        return {
            "answer": "Không tìm thấy thông tin liên quan trong hệ thống.",
            "citations": [], "context": {}, "trace": {"steps_completed": 2},
            "disclaimer": "Không đủ dữ liệu.",
        }

    chunk_ids = [h["chunk_id"] for h in faiss_hits]
    for h in faiss_hits:
        print(f"     [{h['rank']}] {h['chunk_id']}  score={h['score']:.4f}")

    # ------------------------------------------------------------------
    # STEP 3 — Metadata & Content Fetch  (PostgreSQL)
    # ------------------------------------------------------------------
    print("📄  Step 3: Fetch content + metadata from PostgreSQL …")
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 3a) Lấy nội dung chunk + metadata văn bản
    cur.execute("""
        SELECT c.chunk_id, c.content, c.chapter, c.article, c.clause,
               c.doc_id,
               d.title, d.so_ky_hieu, d.loai_van_ban, d.co_quan_ban_hanh,
               d.ngay_ban_hanh, d.tinh_trang_hieu_luc
        FROM document_chunks c
        JOIN documents d ON c.doc_id = d.id
        WHERE c.chunk_id = ANY(%s);
    """, (chunk_ids,))
    chunks_data = cur.fetchall()

    # Sắp xếp theo thứ tự FAISS rank
    chunk_map = {c["chunk_id"]: c for c in chunks_data}
    ordered_chunks = [chunk_map[cid] for cid in chunk_ids if cid in chunk_map]

    # Thu thập unique doc_ids
    doc_ids = list({c["doc_id"] for c in ordered_chunks})
    print(f"     → {len(ordered_chunks)} chunks từ {len(doc_ids)} văn bản")

    # ------------------------------------------------------------------
    # STEP 4 — Graph Expansion  (Neo4j)
    # ------------------------------------------------------------------
    print(f"🕸️   Step 4: Neo4j graph expansion (depth=2) for {len(doc_ids)} doc_ids …")
    all_related: List[Dict[str, Any]] = []
    seen_related_ids: set = set()

    for did in doc_ids:
        related_docs = graph_pipeline.query_related_documents(did, depth=2)
        for rd in related_docs:
            rd_id = rd["doc_pg_id"]
            if rd_id not in seen_related_ids and rd_id not in doc_ids:
                seen_related_ids.add(rd_id)
                all_related.append(rd)

    # Giới hạn số lượng related docs
    all_related = all_related[:top_k_graph]

    if all_related:
        print(f"     → {len(all_related)} related docs found:")
        for rd in all_related:
            rels_str = " → ".join(rd.get("relationships", ["?"]))
            print(f"       📎 {rd['so_ky_hieu']}: {rd['title'][:55]}…  ({rels_str})")

        # Lấy thêm nội dung chunks của related docs để bổ sung context
        related_pg_ids = [rd["doc_pg_id"] for rd in all_related]
        cur.execute("""
            SELECT c.chunk_id, c.content, c.article,
                   d.title, d.so_ky_hieu, d.loai_van_ban
            FROM document_chunks c
            JOIN documents d ON c.doc_id = d.id
            WHERE c.doc_id = ANY(%s)
            ORDER BY c.chunk_index
            LIMIT 15;
        """, (related_pg_ids,))
        related_chunks = cur.fetchall()
    else:
        related_chunks = []
        print("     → Không tìm thấy văn bản liên quan qua graph")

    cur.close()
    conn.close()

    # ------------------------------------------------------------------
    # STEP 5 — Synthesis  (Build Final Context)
    # ------------------------------------------------------------------
    print("📋  Step 5: Synthesizing final context …")
    final_context = _build_final_context(ordered_chunks, faiss_hits, all_related, related_chunks)

    # ------------------------------------------------------------------
    # Search-only mode: dừng ở đây
    # ------------------------------------------------------------------
    if search_only:
        print("✅  Search-only mode — skipping LLM generation")
        result = {
            "faiss_results": faiss_hits,
            "chunks": [dict(c) for c in ordered_chunks],
            "related_docs": all_related,
            "final_context_preview": final_context[:2000],
        }
        if own_gp: graph_pipeline.close()
        return result

    # ------------------------------------------------------------------
    # STEP 6 — Generation  (LLM)
    # ------------------------------------------------------------------
    llm_name = f"{LLM_PROVIDER}/{OPENAI_MODEL if LLM_PROVIDER == 'openai' else ANTHROPIC_MODEL}"
    print(f"🤖  Step 6: Generating answer via {llm_name} …")

    user_message = (
        f"NGỮ CẢNH PHÁP LUẬT:\n{final_context}\n\n"
        f"CÂU HỎI:\n{user_query}"
    )
    answer = _call_llm(SYSTEM_PROMPT, user_message)

    # Build citations
    citations = _build_citations(ordered_chunks, all_related)

    # Cleanup
    if own_gp:
        graph_pipeline.close()

    return {
        "answer": answer,
        "citations": citations,
        "context": {
            "faiss_chunks": [
                {"chunk_id": c["chunk_id"], "doc_id": c["doc_id"],
                 "article": c.get("article"),
                 "score": next((h["score"] for h in faiss_hits if h["chunk_id"] == c["chunk_id"]), 0)}
                for c in ordered_chunks
            ],
            "graph_related": [
                {"doc_pg_id": r["doc_pg_id"], "title": r["title"],
                 "so_ky_hieu": r["so_ky_hieu"], "relationships": r.get("relationships", [])}
                for r in all_related
            ],
        },
        "trace": {
            "steps_completed": 6,
            "vector_hits": len(ordered_chunks),
            "graph_hits": len(all_related),
            "graph_chunks_added": len(related_chunks),
            "total_context_chars": len(final_context),
            "llm": llm_name,
        },
        "disclaimer": "Nội dung trên chỉ mang tính tham khảo, không phải tư vấn pháp lý ràng buộc.",
    }


# ============================================================================
# Context Builder
# ============================================================================

def _build_final_context(
    primary_chunks: List[Dict],
    faiss_hits: List[Dict],
    related_docs: List[Dict],
    related_chunks: List[Dict],
) -> str:
    """
    Bước 5 — Gộp content (FAISS) + related docs (Neo4j) thành Final Context.

    Xử lý conflict chunk_id có nhiều quan hệ chéo:
        • De-duplicate theo doc_pg_id khi gộp related docs
        • Gộp rel_chain nếu cùng target doc
        • Primary chunks (từ FAISS) luôn ưu tiên cao hơn related chunks
    """
    parts: List[str] = []

    # ---- Primary context (FAISS hits) ----
    parts.append("═══ KẾT QUẢ TÌM KIẾM NGỮ NGHĨA (FAISS) ═══")
    for chunk in primary_chunks:
        score = next(
            (h["score"] for h in faiss_hits if h["chunk_id"] == chunk["chunk_id"]), 0
        )
        header = (
            f"[{chunk.get('loai_van_ban', '')} | "
            f"{chunk.get('so_ky_hieu', '')} | "
            f"{chunk.get('title', '')[:60]} | "
            f"{chunk.get('article', '') or ''}"
            f"]  (relevance: {score:.4f})"
        )
        parts.append(f"\n{header}\n{chunk['content']}")

    # ---- Expanded context (Neo4j graph) ----
    if related_chunks:
        parts.append("\n\n═══ VĂN BẢN LIÊN QUAN (MỞ RỘNG TỪ ĐỒ THỊ TRI THỨC) ═══")

        # Group by doc for clarity
        doc_groups: Dict[str, List[Dict]] = {}
        for rc in related_chunks:
            key = rc.get("so_ky_hieu", "?")
            doc_groups.setdefault(key, []).append(rc)

        for sky, rc_list in doc_groups.items():
            rd_info = next((r for r in related_docs if r.get("so_ky_hieu") == sky), {})
            rel_str = " → ".join(rd_info.get("relationships", ["liên quan"]))
            parts.append(f"\n── {rd_info.get('title', sky)} ({sky}) ── [{rel_str}]")
            for rc in rc_list[:3]:  # Giới hạn 3 chunks/doc
                art = rc.get("article", "")
                parts.append(f"  [{art}] {rc['content'][:400]}")

    return "\n".join(parts)


def _build_citations(chunks: List[Dict], related: List[Dict]) -> List[Dict[str, str]]:
    """Tạo danh sách trích dẫn nguồn (de-duplicated)."""
    seen = set()
    citations = []
    for c in chunks:
        sky = c.get("so_ky_hieu", "")
        if sky and sky not in seen:
            seen.add(sky)
            citations.append({
                "so_ky_hieu": sky,
                "title": c.get("title", ""),
                "loai_van_ban": c.get("loai_van_ban", ""),
                "co_quan_ban_hanh": c.get("co_quan_ban_hanh", ""),
                "article": c.get("article", ""),
            })
    for r in related:
        sky = r.get("so_ky_hieu", "")
        if sky and sky not in seen:
            seen.add(sky)
            citations.append({
                "so_ky_hieu": sky,
                "title": r.get("title", ""),
                "loai_van_ban": r.get("type", ""),
                "relationships": r.get("relationships", []),
            })
    return citations


# ============================================================================
# Pretty Printer
# ============================================================================

def print_result(result: Dict[str, Any]):
    """In kết quả đẹp ra console."""
    print("\n" + "═" * 70)
    print("  📝  CÂU TRẢ LỜI")
    print("═" * 70)
    print(result.get("answer", "N/A"))

    citations = result.get("citations", [])
    if citations:
        print("\n" + "─" * 70)
        print("  📚  TRÍCH DẪN NGUỒN")
        print("─" * 70)
        for i, c in enumerate(citations, 1):
            rels = c.get("relationships", [])
            rel_str = f"  ({' → '.join(rels)})" if rels else ""
            print(f"  [{i}] {c.get('title', '')}  ({c.get('so_ky_hieu', '')}){rel_str}")

    trace = result.get("trace", {})
    if trace:
        print("\n" + "─" * 70)
        print("  🔍  TRACE")
        print("─" * 70)
        for k, v in trace.items():
            print(f"  {k:25s}: {v}")

    disclaimer = result.get("disclaimer", "")
    if disclaimer:
        print(f"\n  ⚠️  {disclaimer}")
    print("═" * 70)


# ============================================================================
# Interactive Mode
# ============================================================================

def interactive_mode(vp: VectorPipeline, gp: GraphPipeline):
    """Hỏi đáp tương tác qua terminal."""
    print("\n🤖  CHẾ ĐỘ TƯƠNG TÁC  —  GraphRAG Vietnamese Law")
    print("   Nhập câu hỏi pháp luật, hoặc 'quit' để thoát.\n")

    while True:
        try:
            q = input("❓  Câu hỏi: ").strip()
            if q.lower() in ("quit", "exit", "q", "thoát"):
                print("👋  Tạm biệt!")
                break
            if len(q) < 5:
                print("   ⚠️  Vui lòng đặt câu hỏi cụ thể hơn.\n")
                continue

            result = retrieve_and_generate(
                q, vector_pipeline=vp, graph_pipeline=gp,
            )
            print_result(result)
            print()

        except KeyboardInterrupt:
            print("\n👋  Tạm biệt!")
            break
        except Exception as e:
            print(f"   ❌  Lỗi: {e}\n")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="GraphRAG Retrieval  (6-step pipeline)")
    parser.add_argument("-q", "--question", type=str, default=None)
    parser.add_argument("-i", "--interactive", action="store_true")
    parser.add_argument("--search-only", action="store_true",
                        help="Chỉ tìm kiếm, không gọi LLM")
    parser.add_argument("--top-k-vector", type=int, default=TOP_K_VECTOR)
    parser.add_argument("--top-k-graph",  type=int, default=TOP_K_GRAPH)
    args = parser.parse_args()

    print("═" * 65)
    print("  GraphRAG Vietnamese Law  —  Retrieval Pipeline")
    print("═" * 65)

    # Load shared pipeline instances
    vp = VectorPipeline()
    vp.load()
    gp = GraphPipeline()

    try:
        if args.interactive:
            interactive_mode(vp, gp)
            return

        question = args.question or (
            "Doanh nghiệp có bắt buộc đóng bảo hiểm xã hội cho lao động "
            "thử việc không? Căn cứ pháp lý nào?"
        )

        result = retrieve_and_generate(
            question,
            top_k_vector=args.top_k_vector,
            top_k_graph=args.top_k_graph,
            vector_pipeline=vp,
            graph_pipeline=gp,
            search_only=args.search_only,
        )

        if args.search_only:
            print("\n🔍  Search-only results:")
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            print_result(result)

    except Exception as e:
        print(f"❌  Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        gp.close()


if __name__ == "__main__":
    main()
