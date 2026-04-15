"""
build_vector_index.py — Xây dựng FAISS vector index từ PostgreSQL.

Sử dụng:
    python scripts/build_vector_index.py
    python scripts/build_vector_index.py --save-path ./data/faiss_index
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from pipelines.vector_pipeline import VectorPipeline


def main():
    parser = argparse.ArgumentParser(description="Xây dựng FAISS index từ PostgreSQL")
    parser.add_argument(
        "--save-path",
        type=str,
        default=settings.FAISS_INDEX_PATH,
        help="Thư mục lưu FAISS index",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("🔤 XÂY DỰNG FAISS VECTOR INDEX")
    print("=" * 60)
    print(f"  Embedding model: {settings.EMBEDDING_MODEL}")
    print(f"  Dimension:       {settings.EMBEDDING_DIMENSION}")
    print(f"  Save path:       {args.save_path}")
    print()

    pipeline = VectorPipeline()

    try:
        # Build index
        start_time = time.time()
        num_vectors = pipeline.build_index()
        build_time = time.time() - start_time

        if num_vectors == 0:
            print("⚠️  Không có dữ liệu! Hãy chạy init_db.py trước.")
            sys.exit(1)

        # Save index
        pipeline.save_index(args.save_path)

        # Test search
        print("\n🧪 Test search:")
        test_queries = [
            "quy định về hợp đồng lao động",
            "bảo hiểm xã hội bắt buộc",
            "thành lập doanh nghiệp",
        ]
        for query in test_queries:
            results = pipeline.search(query, top_k=3)
            print(f"\n   Q: \"{query}\"")
            for r in results:
                print(f"      [{r['rank']}] {r['chunk_id']} (score: {r['score']:.4f})")

        print(f"\n⏱️  Thời gian build: {build_time:.2f}s")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pipeline.close()

    print("\n" + "=" * 60)
    print("✅ FAISS INDEX ĐÃ SẴN SÀNG!")
    print("=" * 60)


if __name__ == "__main__":
    main()
