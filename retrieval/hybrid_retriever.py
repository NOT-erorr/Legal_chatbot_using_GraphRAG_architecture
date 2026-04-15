"""
hybrid_retriever.py — Luồng truy vấn GraphRAG kết hợp FAISS + Neo4j + LLM.

Quy trình 3 bước:
    Bước 1: FAISS semantic search → tìm top-K chunks gần nhất
    Bước 2: Neo4j graph expansion → mở rộng ngữ cảnh qua quan hệ
    Bước 3: Context aggregation → gửi LLM để sinh câu trả lời
"""

from pathlib import Path
from typing import List, Dict, Any, Optional

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from ingestion.document_loader import DocumentLoader
from pipelines.vector_pipeline import VectorPipeline
from pipelines.graph_pipeline import GraphPipeline

# LLM integration
try:
    import google.generativeai as genai

    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False


class HybridRetriever:
    """
    Hybrid Retriever kết hợp:
        - FAISS: Tìm kiếm ngữ nghĩa (semantic search)
        - Neo4j: Mở rộng ngữ cảnh (graph expansion)
        - PostgreSQL: Truy xuất nội dung gốc (source of truth)
        - LLM (Gemini): Sinh câu trả lời dựa trên context
    """

    def __init__(self, load_index: bool = True):
        """
        Khởi tạo HybridRetriever.

        Args:
            load_index: Có load FAISS index từ disk không (True khi serve)
        """
        # Khởi tạo components
        self.doc_loader = DocumentLoader()
        self.vector_pipeline = VectorPipeline()
        self.graph_pipeline = GraphPipeline()

        # Load FAISS index nếu đã có
        if load_index:
            try:
                self.vector_pipeline.load_index()
            except FileNotFoundError:
                print("⚠️  FAISS index chưa tồn tại. Cần chạy build_vector_index.py trước.")

        # Khởi tạo LLM (Gemini)
        self.llm = None
        if GEMINI_AVAILABLE and settings.GEMINI_API_KEY:
            genai.configure(api_key=settings.GEMINI_API_KEY)
            self.llm = genai.GenerativeModel(settings.GEMINI_CHAT_MODEL)
            print(f"🤖 LLM đã sẵn sàng: {settings.GEMINI_CHAT_MODEL}")
        else:
            print("⚠️  LLM chưa được cấu hình. Set GEMINI_API_KEY trong .env")

    def retrieve(self, question: str, top_k_vector: int = None,
                 top_k_graph: int = None) -> Dict[str, Any]:
        """
        Truy vấn GraphRAG hoàn chỉnh: FAISS → Neo4j → LLM.

        Args:
            question: Câu hỏi pháp luật từ người dùng
            top_k_vector: Số chunks từ FAISS (mặc định từ settings)
            top_k_graph: Số docs mở rộng từ Neo4j (mặc định từ settings)

        Returns:
            Dict chứa:
                - answer: Câu trả lời từ LLM
                - citations: Danh sách trích dẫn nguồn
                - context: Chi tiết context đã sử dụng
                - trace: Metadata về quá trình retrieval
        """
        top_k_vector = top_k_vector or settings.RETRIEVAL_TOP_K_VECTOR
        top_k_graph = top_k_graph or settings.RETRIEVAL_TOP_K_GRAPH

        # ============================================================
        # BƯỚC 1: FAISS Semantic Search
        # ============================================================
        print(f"\n🔍 Bước 1: FAISS search (top-{top_k_vector})...")
        faiss_results = self.vector_pipeline.search(question, top_k=top_k_vector)

        if not faiss_results:
            return {
                "answer": "Không tìm thấy thông tin liên quan trong hệ thống.",
                "citations": [],
                "context": {},
                "trace": {"vector_hits": 0, "graph_hits": 0},
            }

        # Lấy chunk_ids và doc_ids từ kết quả FAISS
        faiss_chunk_ids = [r["chunk_id"] for r in faiss_results]
        faiss_chunks = self.doc_loader.get_chunks_by_ids(faiss_chunk_ids)

        # Lấy unique doc_ids
        faiss_doc_ids = list(set(c["doc_id"] for c in faiss_chunks))

        print(f"   → Tìm thấy {len(faiss_chunks)} chunks từ {len(faiss_doc_ids)} văn bản")
        for r in faiss_results:
            print(f"     [{r['rank']}] {r['chunk_id']} (score: {r['score']:.4f})")

        # ============================================================
        # BƯỚC 2: Neo4j Graph Expansion
        # ============================================================
        print(f"\n🕸️  Bước 2: Neo4j expansion từ {len(faiss_doc_ids)} doc_ids...")
        graph_related = []
        expanded_chunks = []

        for doc_id in faiss_doc_ids:
            related = self.graph_pipeline.query_related_docs(doc_id, depth=2)
            graph_related.extend(related)

        # Lấy unique related doc_ids (loại bỏ docs đã có từ FAISS)
        related_doc_ids = list(set(
            r["doc_id"] for r in graph_related
            if r["doc_id"] not in faiss_doc_ids
        ))[:top_k_graph]

        if related_doc_ids:
            expanded_chunks = self.doc_loader.get_chunks_by_doc_ids(related_doc_ids)
            print(f"   → Mở rộng thêm {len(expanded_chunks)} chunks từ {len(related_doc_ids)} văn bản liên quan")
            for r in graph_related[:5]:
                rels = " → ".join(r.get("relationships", [""]))
                print(f"     📎 {r['doc_id']}: {r['title'][:60]}... ({rels})")
        else:
            print("   → Không tìm thấy văn bản liên quan qua graph")

        # ============================================================
        # BƯỚC 3: Tổng hợp context & gọi LLM
        # ============================================================
        print(f"\n🧠 Bước 3: Tổng hợp context và gọi LLM...")

        # Chuẩn bị context
        context = self._build_context(faiss_chunks, expanded_chunks, faiss_results)

        # Tạo citations
        citations = self._build_citations(faiss_chunks, expanded_chunks)

        # Gọi LLM
        answer = self._generate_answer(question, context)

        result = {
            "answer": answer,
            "citations": citations,
            "context": {
                "faiss_chunks": [
                    {"chunk_id": c["chunk_id"], "doc_id": c["doc_id"],
                     "article": c.get("article"), "score": next(
                        (r["score"] for r in faiss_results if r["chunk_id"] == c["chunk_id"]), 0
                    )}
                    for c in faiss_chunks
                ],
                "graph_expansion": [
                    {"doc_id": r["doc_id"], "title": r["title"],
                     "relationships": r.get("relationships", [])}
                    for r in graph_related[:top_k_graph]
                ],
            },
            "trace": {
                "vector_hits": len(faiss_chunks),
                "graph_hits": len(expanded_chunks),
                "total_context_chunks": len(faiss_chunks) + len(expanded_chunks),
                "model": settings.GEMINI_CHAT_MODEL if self.llm else "none",
            },
            "disclaimer": "Nội dung chỉ mang tính tham khảo, không phải tư vấn pháp lý ràng buộc.",
        }

        return result

    def _build_context(
        self,
        faiss_chunks: List[Dict],
        expanded_chunks: List[Dict],
        faiss_results: List[Dict],
    ) -> str:
        """Xây dựng context string để gửi cho LLM."""
        parts = []

        # Context từ FAISS (primary)
        parts.append("=== KẾT QUẢ TÌM KIẾM NGỮ NGHĨA (FAISS) ===")
        for chunk in faiss_chunks:
            score = next(
                (r["score"] for r in faiss_results if r["chunk_id"] == chunk["chunk_id"]),
                0,
            )
            parts.append(
                f"\n[Nguồn: {chunk.get('doc_title', '')} | "
                f"{chunk.get('doc_number', '')} | "
                f"{chunk.get('article', '')}] "
                f"(Relevance: {score:.4f})\n"
                f"{chunk['content']}"
            )

        # Context từ Neo4j (expanded)
        if expanded_chunks:
            parts.append("\n\n=== VĂN BẢN LIÊN QUAN (MỞ RỘNG TỪ ĐỒ THỊ PHÁP LUẬT) ===")
            for chunk in expanded_chunks[:10]:  # Giới hạn context mở rộng
                parts.append(
                    f"\n[Nguồn: {chunk.get('doc_title', '')} | "
                    f"{chunk.get('doc_number', '')} | "
                    f"{chunk.get('article', '')}]\n"
                    f"{chunk['content']}"
                )

        return "\n".join(parts)

    def _build_citations(
        self,
        faiss_chunks: List[Dict],
        expanded_chunks: List[Dict],
    ) -> List[Dict[str, str]]:
        """Tạo danh sách trích dẫn nguồn."""
        seen = set()
        citations = []

        for chunk in faiss_chunks + expanded_chunks:
            doc_id = chunk["doc_id"]
            if doc_id not in seen:
                seen.add(doc_id)
                citations.append({
                    "source_id": doc_id,
                    "title": chunk.get("doc_title", ""),
                    "doc_number": chunk.get("doc_number", ""),
                    "doc_type": chunk.get("doc_type", ""),
                    "issuing_body": chunk.get("issuing_body", ""),
                    "article": chunk.get("article", ""),
                })

        return citations

    def _generate_answer(self, question: str, context: str) -> str:
        """
        Gọi LLM để sinh câu trả lời dựa trên context.
        Nếu không có LLM, trả về context trực tiếp.
        """
        if not self.llm:
            return (
                f"[LLM chưa được cấu hình — Hiển thị raw context]\n\n"
                f"Câu hỏi: {question}\n\n{context}"
            )

        system_prompt = """Bạn là trợ lý tư vấn pháp luật Việt Nam. Hãy trả lời câu hỏi dựa HOÀN TOÀN 
vào ngữ cảnh pháp luật được cung cấp bên dưới. 

QUY TẮC BẮT BUỘC:
1. Chỉ trả lời dựa trên thông tin trong context. KHÔNG được bịa đặt hay suy đoán.
2. Trích dẫn rõ ràng nguồn (tên văn bản, số hiệu, điều khoản) cho mỗi thông tin.
3. Nếu context không đủ thông tin để trả lời, hãy nói rõ.
4. Sử dụng ngôn ngữ chuyên nghiệp, dễ hiểu.
5. Kết thúc bằng disclaimer: thông tin chỉ mang tính tham khảo.

FORMAT TRẢ LỜI:
- Trả lời trực tiếp câu hỏi
- Phân tích các điều luật liên quan
- Trích dẫn nguồn cụ thể
- Lưu ý/Disclaimer"""

        prompt = f"""{system_prompt}

=== NGỮ CẢNH PHÁP LUẬT ===
{context}

=== CÂU HỎI ===
{question}

=== TRẢ LỜI ==="""

        try:
            response = self.llm.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"[Lỗi khi gọi LLM: {str(e)}]\n\nContext đã thu thập:\n{context}"

    def search_only(self, question: str, top_k: int = 5) -> Dict[str, Any]:
        """
        Chỉ tìm kiếm (không gọi LLM). Hữu ích cho testing/debugging.
        """
        faiss_results = self.vector_pipeline.search(question, top_k=top_k)
        faiss_chunk_ids = [r["chunk_id"] for r in faiss_results]
        faiss_chunks = self.doc_loader.get_chunks_by_ids(faiss_chunk_ids)

        return {
            "faiss_results": faiss_results,
            "chunks": faiss_chunks,
        }

    def close(self):
        """Đóng tất cả kết nối."""
        self.doc_loader.close()
        self.vector_pipeline.close()
        self.graph_pipeline.close()
