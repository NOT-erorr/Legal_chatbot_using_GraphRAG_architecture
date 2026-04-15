"""
ingestion.py  —  Data Ingestion Pipeline (ETL Layer)
=====================================================
Hai luồng đọc dữ liệu từ PostgreSQL:
  • Vector Pipeline : DOCUMENT_CHUNKS  →  Embedding (multilingual-e5)  →  FAISS
  • Graph  Pipeline : DOCUMENTS + metadata  →  Neo4j  (+ bảng audit neo4j_*)

Sử dụng:
    python ingestion.py                          # Chạy cả 2 pipeline
    python ingestion.py --vector-only            # Chỉ FAISS
    python ingestion.py --graph-only             # Chỉ Neo4j
    python ingestion.py --seed                   # Nạp sample data trước, rồi chạy pipeline
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import faiss
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# LangChain text splitter (dùng khi cần re-chunk nội dung)
# ---------------------------------------------------------------------------
try:
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration  (ưu tiên đọc từ .env hoặc biến môi trường)
# ---------------------------------------------------------------------------
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB", "law_graphrag")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")

NEO4J_URI      = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4j123")

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "intfloat/multilingual-e5-base")
EMBEDDING_DIM   = 768                                    # multilingual-e5-base
FAISS_INDEX_DIR = os.getenv("FAISS_INDEX_DIR", str(Path(__file__).parent / "data" / "faiss_index"))

CHUNK_SIZE    = int(os.getenv("CHUNK_SIZE", "800"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "150"))

SAMPLE_DATA_PATH = str(Path(__file__).parent / "data" / "sample_legal_docs.json")

POSTGRES_DSN = (
    f"host={POSTGRES_HOST} port={POSTGRES_PORT} "
    f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD}"
)


# ============================================================================
# Helpers
# ============================================================================

def get_pg_conn():
    """Trả về một kết nối psycopg2 mới."""
    return psycopg2.connect(POSTGRES_DSN)


def get_neo4j_driver():
    """Trả về Neo4j Driver."""
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def _json_serial(obj):
    """JSON serializer cho các kiểu date / datetime."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# ============================================================================
# SEED  —  Nạp dữ liệu mẫu vào PostgreSQL
# ============================================================================

def seed_sample_data(json_path: str = SAMPLE_DATA_PATH) -> Dict[str, int]:
    """
    Đọc file sample_legal_docs.json  →  INSERT vào 3 bảng PG:
        documents  →  document_content  →  document_chunks
    """
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    stats = {"documents": 0, "content": 0, "chunks": 0, "relations": 0}

    for doc in data.get("documents", []):
        # --- 1) INSERT documents ---
        cur.execute("""
            INSERT INTO documents
                (title, so_ky_hieu, ngay_ban_hanh, loai_van_ban,
                 nganh, linh_vuc, co_quan_ban_hanh, tinh_trang_hieu_luc)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (so_ky_hieu) DO UPDATE SET title=EXCLUDED.title
            RETURNING id;
        """, (
            doc["title"],
            doc["so_ky_hieu"],
            doc.get("ngay_ban_hanh"),
            doc.get("loai_van_ban"),
            doc.get("nganh"),
            doc.get("linh_vuc"),
            doc.get("co_quan_ban_hanh"),
            doc.get("tinh_trang_hieu_luc", "Còn hiệu lực"),
        ))
        doc_id = cur.fetchone()[0]
        stats["documents"] += 1

        # --- 2) INSERT document_content ---
        full_text = doc.get("content_text", "")
        if not full_text:
            # Nối tất cả chunks thành full_text
            full_text = "\n\n".join(c["content"] for c in doc.get("chunks", []))
        cur.execute("""
            INSERT INTO document_content (id, content_html, content_text)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET content_text=EXCLUDED.content_text;
        """, (doc_id, doc.get("content_html"), full_text))
        stats["content"] += 1

        # --- 3) INSERT document_chunks ---
        for i, chunk in enumerate(doc.get("chunks", [])):
            chunk_id = chunk.get("chunk_id", f"{doc['so_ky_hieu']}__chunk_{i:03d}")
            cur.execute("""
                INSERT INTO document_chunks
                    (chunk_id, doc_id, chunk_index, content, chapter, article, clause, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (chunk_id) DO UPDATE SET content=EXCLUDED.content;
            """, (
                chunk_id, doc_id, i, chunk["content"],
                chunk.get("chapter"), chunk.get("article"), chunk.get("clause"),
                json.dumps(chunk.get("metadata", {}), default=_json_serial, ensure_ascii=False),
            ))
            stats["chunks"] += 1

        # --- 4) INSERT neo4j_relation (nếu dữ liệu JSON định nghĩa relations) ---
        for rel in doc.get("relations", []):
            # Tìm target doc_id
            cur.execute("SELECT id FROM documents WHERE so_ky_hieu=%s", (rel["target_so_ky_hieu"],))
            row = cur.fetchone()
            if row:
                target_id = row[0]
                cur.execute("""
                    INSERT INTO neo4j_relation (source_doc_id, target_doc_id, relation_type, description)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (source_doc_id, target_doc_id, relation_type) DO NOTHING;
                """, (doc_id, target_id, rel["type"], rel.get("description")))
                stats["relations"] += 1

    cur.close()
    conn.close()
    print(f"✅  Seed hoàn tất: {stats}")
    return stats


# ============================================================================
# VECTOR PIPELINE  —  DOCUMENT_CHUNKS  →  Embedding  →  FAISS
# ============================================================================

class VectorPipeline:
    """
    Reader 1: PostgreSQL → Embedding → FAISS

    • Đọc bảng DOCUMENT_CHUNKS
    • Encode bằng multilingual-e5-base  (768-dim, hỗ trợ 100+ ngôn ngữ)
    • Normalize L2  →  IndexFlatIP  (cosine similarity)
    • Lưu mapping  FAISS-position  ↔  chunk_id
    """

    def __init__(self, model_name: str = EMBEDDING_MODEL):
        print(f"📦  Loading embedding model: {model_name} ...")
        self.model = SentenceTransformer(model_name)
        self.dimension = EMBEDDING_DIM
        self.index: Optional[faiss.IndexFlatIP] = None
        self.chunk_ids: List[str] = []

    # ------------------------------------------------------------------
    def build_index_from_postgres(self) -> int:
        """Đọc tất cả chunks từ PG  →  Build FAISS index."""
        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT c.chunk_id, c.content, c.doc_id,
                   d.title, d.so_ky_hieu, d.loai_van_ban
            FROM document_chunks c
            JOIN documents d ON c.doc_id = d.id
            ORDER BY c.doc_id, c.chunk_index;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            print("⚠️   Không có chunks trong PG. Hãy chạy --seed trước.")
            return 0

        print(f"📝  {len(rows)} chunks. Đang tạo embeddings …")

        # Multilingual-E5 yêu cầu prefix "query: " hoặc "passage: "
        texts = [f"passage: {r['content']}" for r in rows]
        self.chunk_ids = [r["chunk_id"] for r in rows]

        embeddings = self.model.encode(
            texts, show_progress_bar=True, convert_to_numpy=True, batch_size=32
        ).astype("float32")

        faiss.normalize_L2(embeddings)

        self.index = faiss.IndexFlatIP(self.dimension)
        self.index.add(embeddings)

        print(f"✅  FAISS index: {self.index.ntotal} vectors × {self.dimension}d")
        return self.index.ntotal

    # ------------------------------------------------------------------
    def save(self, save_dir: str = FAISS_INDEX_DIR):
        """Lưu FAISS index + chunk mapping ra disk."""
        if self.index is None:
            print("⚠️   Chưa có index.")
            return
        out = Path(save_dir)
        out.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self.index, str(out / "faiss.index"))
        with open(out / "chunk_ids.json", "w", encoding="utf-8") as f:
            json.dump(self.chunk_ids, f, ensure_ascii=False)
        print(f"💾  Saved to {out}")

    # ------------------------------------------------------------------
    def load(self, load_dir: str = FAISS_INDEX_DIR):
        """Load FAISS index + mapping từ disk."""
        d = Path(load_dir)
        self.index = faiss.read_index(str(d / "faiss.index"))
        with open(d / "chunk_ids.json", "r", encoding="utf-8") as f:
            self.chunk_ids = json.load(f)
        print(f"📂  Loaded FAISS: {self.index.ntotal} vectors, {len(self.chunk_ids)} ids")

    # ------------------------------------------------------------------
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Embed query  →  FAISS top-K search  →  [(chunk_id, score)]"""
        if self.index is None or self.index.ntotal == 0:
            return []
        q_emb = self.model.encode(
            [f"query: {query}"], convert_to_numpy=True
        ).astype("float32")
        faiss.normalize_L2(q_emb)

        scores, idxs = self.index.search(q_emb, min(top_k, self.index.ntotal))
        results = []
        for rank, (score, idx) in enumerate(zip(scores[0], idxs[0])):
            if idx < 0:
                continue
            results.append({
                "chunk_id": self.chunk_ids[idx],
                "score": float(score),
                "rank": rank + 1,
            })
        return results


# ============================================================================
# GRAPH PIPELINE  —  DOCUMENTS + relations  →  Neo4j Knowledge Graph
# ============================================================================

class GraphPipeline:
    """
    Reader 2: PostgreSQL → Trích xuất thực thể & quan hệ → Neo4j

    Node labels:
        (:Document { doc_pg_id, title, type, sector, so_ky_hieu, ... })

    Relationship types (edges):
        -[:AMENDS]->        Sửa đổi, bổ sung
        -[:REFERENCES]->    Căn cứ, dẫn chiếu
        -[:REPEALS]->       Thay thế, bãi bỏ
        -[:GUIDES]->        Hướng dẫn thi hành
    """

    CYPHER_CONSTRAINTS = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_pg_id IS UNIQUE",
    ]
    CYPHER_INDEXES = [
        "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.so_ky_hieu)",
        "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.type)",
    ]

    def __init__(self):
        self.driver = get_neo4j_driver()

    def close(self):
        self.driver.close()

    # ------------------------------------------------------------------
    def _ensure_schema(self):
        """Tạo constraints & indexes trong Neo4j."""
        with self.driver.session() as s:
            for cypher in self.CYPHER_CONSTRAINTS + self.CYPHER_INDEXES:
                s.run(cypher)
        print("🔒  Neo4j constraints/indexes OK")

    # ------------------------------------------------------------------
    def build_graph_from_postgres(self) -> Dict[str, int]:
        """Đọc PG  →  Tạo nodes + edges trong Neo4j."""
        self._ensure_schema()
        stats = {"nodes": 0, "amends": 0, "references": 0, "repeals": 0, "guides": 0}

        conn = get_pg_conn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # ----- 1) Tạo Document nodes -----
        cur.execute("""
            SELECT id, title, so_ky_hieu, loai_van_ban, nganh,
                   linh_vuc, co_quan_ban_hanh, ngay_ban_hanh,
                   tinh_trang_hieu_luc
            FROM documents;
        """)
        docs = cur.fetchall()

        with self.driver.session() as session:
            for doc in docs:
                session.run("""
                    MERGE (d:Document {doc_pg_id: $id})
                    SET d.title            = $title,
                        d.so_ky_hieu       = $so_ky_hieu,
                        d.type             = $loai_van_ban,
                        d.sector           = $nganh,
                        d.field            = $linh_vuc,
                        d.issuing_body     = $co_quan_ban_hanh,
                        d.issued_date      = toString($ngay_ban_hanh),
                        d.status           = $tinh_trang_hieu_luc
                """, {
                    "id": doc["id"],
                    "title": doc["title"],
                    "so_ky_hieu": doc["so_ky_hieu"],
                    "loai_van_ban": doc["loai_van_ban"],
                    "nganh": doc["nganh"],
                    "linh_vuc": doc["linh_vuc"],
                    "co_quan_ban_hanh": doc["co_quan_ban_hanh"],
                    "ngay_ban_hanh": str(doc["ngay_ban_hanh"]) if doc["ngay_ban_hanh"] else None,
                    "tinh_trang_hieu_luc": doc["tinh_trang_hieu_luc"],
                })
                stats["nodes"] += 1

                # Cập nhật audit table
                cur2 = conn.cursor()
                cur2.execute("""
                    INSERT INTO neo4j_document (doc_id) VALUES (%s)
                    ON CONFLICT (doc_id) DO UPDATE SET synced_at=NOW();
                """, (doc["id"],))
                conn.commit()
                cur2.close()

        print(f"📄  {stats['nodes']} Document nodes đã tạo trong Neo4j")

        # ----- 2) Tạo Edges từ bảng neo4j_relation -----
        cur.execute("""
            SELECT source_doc_id, target_doc_id, relation_type, description
            FROM neo4j_relation;
        """)
        rels = cur.fetchall()

        with self.driver.session() as session:
            for rel in rels:
                rtype = rel["relation_type"]       # AMENDS | REFERENCES | REPEALS | GUIDES
                cypher = f"""
                    MATCH (a:Document {{doc_pg_id: $src}})
                    MATCH (b:Document {{doc_pg_id: $tgt}})
                    MERGE (a)-[r:{rtype}]->(b)
                    SET r.description = $desc
                """
                session.run(cypher, {
                    "src": rel["source_doc_id"],
                    "tgt": rel["target_doc_id"],
                    "desc": rel["description"],
                })
                stats[rtype.lower()] = stats.get(rtype.lower(), 0) + 1

        cur.close()
        conn.close()

        print(f"🔗  Edges: AMENDS={stats['amends']}  REFERENCES={stats['references']}  "
              f"REPEALS={stats['repeals']}  GUIDES={stats['guides']}")
        return stats

    # ------------------------------------------------------------------
    def query_related_documents(
        self, doc_pg_id: int, depth: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Truy vấn Neo4j: tìm tất cả Document liên quan qua AMENDS | REFERENCES |
        REPEALS | GUIDES, trong phạm vi depth bước.
        """
        cypher = """
            MATCH (start:Document {doc_pg_id: $id})
                  -[r:AMENDS|REFERENCES|REPEALS|GUIDES*1..""" + str(depth) + """]-
                  (related:Document)
            WHERE related.doc_pg_id <> $id
            WITH DISTINCT related, [rel IN r | type(rel)] AS rel_chain
            RETURN related.doc_pg_id  AS doc_pg_id,
                   related.title       AS title,
                   related.so_ky_hieu  AS so_ky_hieu,
                   related.type        AS type,
                   related.sector      AS sector,
                   related.status      AS status,
                   rel_chain           AS relationships
            LIMIT 30
        """
        with self.driver.session() as session:
            result = session.run(cypher, id=doc_pg_id)
            return [dict(record) for record in result]

    # ------------------------------------------------------------------
    def get_summary(self) -> Dict[str, int]:
        """Đếm nodes + từng loại edges."""
        summary: Dict[str, int] = {}
        with self.driver.session() as s:
            summary["Document_nodes"] = s.run(
                "MATCH (d:Document) RETURN count(d) AS c"
            ).single()["c"]
            for rtype in ("AMENDS", "REFERENCES", "REPEALS", "GUIDES"):
                summary[rtype] = s.run(
                    f"MATCH ()-[r:{rtype}]->() RETURN count(r) AS c"
                ).single()["c"]
        return summary


# ============================================================================
# Utility: Chunking với LangChain (nếu cần re-chunk từ document_content)
# ============================================================================

def rechunk_from_content(doc_id: int, conn=None) -> int:
    """
    Đọc document_content.content_text  →  RecursiveCharacterTextSplitter
    →  INSERT vào document_chunks.
    Hữu ích khi dữ liệu mới chỉ có content_text chưa chunk.
    """
    if not LANGCHAIN_AVAILABLE:
        print("⚠️   LangChain chưa cài. pip install langchain")
        return 0

    own_conn = conn is None
    if own_conn:
        conn = get_pg_conn()
        conn.autocommit = True

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, content_text FROM document_content WHERE id=%s", (doc_id,))
    row = cur.fetchone()
    if not row or not row["content_text"]:
        cur.close()
        return 0

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", " ", ""],
    )
    chunks = splitter.split_text(row["content_text"])

    # Lấy so_ky_hieu để tạo chunk_id
    cur.execute("SELECT so_ky_hieu FROM documents WHERE id=%s", (doc_id,))
    so_ky_hieu = cur.fetchone()["so_ky_hieu"].replace("/", "_")

    for i, text in enumerate(chunks):
        chunk_id = f"{so_ky_hieu}__chunk_{i:03d}"
        cur.execute("""
            INSERT INTO document_chunks (chunk_id, doc_id, chunk_index, content)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chunk_id) DO UPDATE SET content=EXCLUDED.content;
        """, (chunk_id, doc_id, i, text))

    cur.close()
    if own_conn:
        conn.close()

    print(f"   ✂️  doc_id={doc_id}: {len(chunks)} chunks tạo mới")
    return len(chunks)


# ============================================================================
# MAIN  —  CLI Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="GraphRAG Ingestion Pipeline  (Vector + Graph)"
    )
    parser.add_argument("--seed",         action="store_true", help="Nạp sample data vào PG trước")
    parser.add_argument("--vector-only",  action="store_true", help="Chỉ chạy Vector Pipeline")
    parser.add_argument("--graph-only",   action="store_true", help="Chỉ chạy Graph Pipeline")
    parser.add_argument("--rechunk",      action="store_true", help="Re-chunk document_content trước khi build FAISS")
    args = parser.parse_args()

    run_vector = not args.graph_only
    run_graph  = not args.vector_only

    print("=" * 65)
    print("  GraphRAG Vietnamese Law  —  Ingestion Pipeline")
    print("=" * 65)
    print(f"  PG:    {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"  Neo4j: {NEO4J_URI}")
    print(f"  Model: {EMBEDDING_MODEL}")
    print(f"  FAISS: {FAISS_INDEX_DIR}")
    print()

    # ---- Seed ----
    if args.seed:
        print("── SEED ─────────────────────────────────────────────")
        seed_sample_data()
        print()

    # ---- Re-chunk (tuỳ chọn) ----
    if args.rechunk:
        print("── RE-CHUNK ─────────────────────────────────────────")
        conn = get_pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT id FROM documents")
        for (doc_id,) in cur.fetchall():
            rechunk_from_content(doc_id, conn)
        cur.close()
        conn.close()
        print()

    # ---- Vector Pipeline ----
    if run_vector:
        print("── VECTOR PIPELINE  (PG → Embedding → FAISS) ───────")
        t0 = time.time()
        vp = VectorPipeline()
        n = vp.build_index_from_postgres()
        if n > 0:
            vp.save()
            # Quick test
            print("\n🧪  Quick test:")
            for q in ["hợp đồng lao động", "bảo hiểm xã hội", "thành lập doanh nghiệp"]:
                hits = vp.search(q, top_k=3)
                print(f"   Q: \"{q}\"")
                for h in hits:
                    print(f"      [{h['rank']}] {h['chunk_id']}  (score={h['score']:.4f})")
        print(f"⏱️   Vector Pipeline: {time.time()-t0:.1f}s\n")

    # ---- Graph Pipeline ----
    if run_graph:
        print("── GRAPH PIPELINE  (PG → Neo4j) ────────────────────")
        gp = GraphPipeline()
        try:
            gp.build_graph_from_postgres()
            summary = gp.get_summary()
            print(f"📊  Neo4j summary: {summary}")
        finally:
            gp.close()
        print()

    print("=" * 65)
    print("✅  Ingestion Pipeline hoàn tất!")
    print("    Tiếp theo:  python retrieval.py --interactive")
    print("=" * 65)


if __name__ == "__main__":
    main()
