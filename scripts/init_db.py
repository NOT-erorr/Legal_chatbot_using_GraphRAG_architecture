"""
init_db.py — Khởi tạo database PostgreSQL với sample data.

Sử dụng:
    python scripts/init_db.py
    python scripts/init_db.py --json-path path/to/custom_data.json
"""

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings, BASE_DIR
from ingestion.document_loader import DocumentLoader


def main():
    parser = argparse.ArgumentParser(description="Khởi tạo PostgreSQL với dữ liệu văn bản luật")
    parser.add_argument(
        "--json-path",
        type=str,
        default=str(BASE_DIR / "data" / "sample_legal_docs.json"),
        help="Đường dẫn tới file JSON chứa dữ liệu (mặc định: data/sample_legal_docs.json)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("📦 KHỞI TẠO DATABASE — GraphRAG Vietnamese Law")
    print("=" * 60)
    print(f"  PostgreSQL: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
    print(f"  Data file:  {args.json_path}")
    print()

    loader = DocumentLoader()

    try:
        result = loader.load_from_json(args.json_path)
        print(f"\n📊 Kết quả:")
        print(f"   Documents: {result['documents']}")
        print(f"   Chunks:    {result['chunks']}")

        # Verify
        chunks = loader.get_all_chunks()
        print(f"\n✅ Verification: {len(chunks)} chunks trong database")

        # Hiển thị preview
        print("\n📋 Preview 3 chunks đầu tiên:")
        for chunk in chunks[:3]:
            print(f"   [{chunk['chunk_id']}] {chunk['content'][:80]}...")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        sys.exit(1)
    finally:
        loader.close()

    print("\n" + "=" * 60)
    print("✅ HOÀN TẤT! Hãy chạy tiếp:")
    print("   python scripts/build_vector_index.py")
    print("   python scripts/build_knowledge_graph.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
