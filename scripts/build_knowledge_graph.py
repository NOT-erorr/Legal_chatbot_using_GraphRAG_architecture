"""
build_knowledge_graph.py — Xây dựng Knowledge Graph pháp luật trong Neo4j.

Sử dụng:
    python scripts/build_knowledge_graph.py
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from pipelines.graph_pipeline import GraphPipeline


def main():
    print("=" * 60)
    print("🕸️  XÂY DỰNG KNOWLEDGE GRAPH PHÁP LUẬT")
    print("=" * 60)
    print(f"  Neo4j:      {settings.NEO4J_URI}")
    print(f"  PostgreSQL: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
    print()

    pipeline = GraphPipeline()

    try:
        # Build graph
        stats = pipeline.build_graph()

        # Verify
        print("\n🧪 Verification — Graph Summary:")
        summary = pipeline.get_graph_summary()
        for key, value in summary.items():
            print(f"   {key}: {value}")

        # Test query
        print("\n🧪 Test query — Văn bản liên quan đến Bộ luật Lao động:")
        related = pipeline.query_related_docs("LUAT-2019-45", depth=2)
        for r in related:
            rels = " → ".join(r.get("relationships", [""]))
            print(f"   📎 {r['doc_id']}: {r['title'][:60]}...")
            print(f"      Quan hệ: {rels}")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pipeline.close()

    print("\n" + "=" * 60)
    print("✅ KNOWLEDGE GRAPH ĐÃ SẴN SÀNG!")
    print("   Truy cập Neo4j Browser: http://localhost:7474")
    print("=" * 60)


if __name__ == "__main__":
    main()
