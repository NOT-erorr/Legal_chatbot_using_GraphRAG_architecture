"""
run_query.py — CLI để test truy vấn GraphRAG end-to-end.

Sử dụng:
    python scripts/run_query.py
    python scripts/run_query.py --question "Quy định về thử việc?"
    python scripts/run_query.py --interactive
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from retrieval.hybrid_retriever import HybridRetriever


def print_result(result: dict):
    """In kết quả truy vấn đẹp."""
    print("\n" + "=" * 70)
    print("📝 CÂU TRẢ LỜI")
    print("=" * 70)
    print(result.get("answer", "Không có câu trả lời"))

    # Citations
    citations = result.get("citations", [])
    if citations:
        print("\n" + "-" * 70)
        print("📚 TRÍCH DẪN NGUỒN")
        print("-" * 70)
        for i, cite in enumerate(citations, 1):
            print(f"  [{i}] {cite.get('title', '')} ({cite.get('doc_number', '')})")
            print(f"      Loại: {cite.get('doc_type', '')} | Cơ quan: {cite.get('issuing_body', '')}")

    # Trace
    trace = result.get("trace", {})
    if trace:
        print("\n" + "-" * 70)
        print("🔍 TRACE")
        print("-" * 70)
        print(f"  Vector hits:  {trace.get('vector_hits', 0)}")
        print(f"  Graph hits:   {trace.get('graph_hits', 0)}")
        print(f"  Total chunks: {trace.get('total_context_chunks', 0)}")
        print(f"  Model:        {trace.get('model', 'N/A')}")

    # Disclaimer
    disclaimer = result.get("disclaimer", "")
    if disclaimer:
        print(f"\n⚠️  {disclaimer}")

    print("=" * 70)


def interactive_mode(retriever: HybridRetriever):
    """Chế độ hỏi đáp tương tác."""
    print("\n🤖 CHẾ ĐỘ TƯƠNG TÁC — GraphRAG Vietnamese Law")
    print("   Nhập câu hỏi pháp luật, hoặc 'quit' để thoát.\n")

    while True:
        try:
            question = input("❓ Câu hỏi: ").strip()
            if question.lower() in ("quit", "exit", "q", "thoat"):
                print("👋 Tạm biệt!")
                break
            if len(question) < 5:
                print("   ⚠️  Câu hỏi quá ngắn. Vui lòng đặt câu hỏi cụ thể hơn.\n")
                continue

            result = retriever.retrieve(question)
            print_result(result)
            print()

        except KeyboardInterrupt:
            print("\n👋 Tạm biệt!")
            break
        except Exception as e:
            print(f"   ❌ Lỗi: {e}\n")


def main():
    parser = argparse.ArgumentParser(description="Test truy vấn GraphRAG")
    parser.add_argument(
        "--question", "-q",
        type=str,
        default=None,
        help="Câu hỏi pháp luật (nếu không có sẽ dùng câu hỏi mẫu)",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Chế độ hỏi đáp tương tác",
    )
    parser.add_argument(
        "--top-k-vector",
        type=int,
        default=5,
        help="Số chunks từ FAISS",
    )
    parser.add_argument(
        "--top-k-graph",
        type=int,
        default=3,
        help="Số docs mở rộng từ Neo4j",
    )
    parser.add_argument(
        "--search-only",
        action="store_true",
        help="Chỉ tìm kiếm (không gọi LLM)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("🚀 GraphRAG Vietnamese Law — Query Test")
    print("=" * 60)

    retriever = HybridRetriever(load_index=True)

    try:
        if args.interactive:
            interactive_mode(retriever)
            return

        # Single query mode
        question = args.question or "Doanh nghiệp có bắt buộc đóng bảo hiểm xã hội cho lao động thử việc không?"

        print(f"\n❓ Câu hỏi: {question}\n")

        if args.search_only:
            result = retriever.search_only(question, top_k=args.top_k_vector)
            print("🔍 Kết quả tìm kiếm (FAISS only):")
            for r in result.get("faiss_results", []):
                print(f"  [{r['rank']}] {r['chunk_id']} (score: {r['score']:.4f})")
            print("\n📄 Chi tiết chunks:")
            for chunk in result.get("chunks", []):
                print(f"\n  [{chunk['chunk_id']}]")
                print(f"  Văn bản: {chunk.get('doc_title', '')}")
                print(f"  Điều: {chunk.get('article', '')}")
                print(f"  Nội dung: {chunk['content'][:150]}...")
        else:
            result = retriever.retrieve(
                question,
                top_k_vector=args.top_k_vector,
                top_k_graph=args.top_k_graph,
            )
            print_result(result)

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        retriever.close()


if __name__ == "__main__":
    main()
