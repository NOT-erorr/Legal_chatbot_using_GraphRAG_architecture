"""
graph_pipeline.py — Reader 2: PostgreSQL → Neo4j
Module đọc dữ liệu từ PostgreSQL, trích xuất thực thể và quan hệ pháp lý,
rồi ánh xạ vào Neo4j để tạo Knowledge Graph.
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple
from neo4j import GraphDatabase

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from ingestion.document_loader import DocumentLoader


class GraphPipeline:
    """
    Reader 2: Đọc dữ liệu từ PostgreSQL → Trích xuất thực thể & quan hệ → Neo4j.
    
    Entities (Nodes):
        - VanBan: Văn bản luật (Luật, Nghị định, Thông tư...)
        - DieuKhoan: Điều khoản cụ thể (chunk)
        - CoQuanBanHanh: Cơ quan ban hành (Quốc hội, Chính phủ...)
        - LoaiVanBan: Loại văn bản (Luật, Nghị định, Thông tư...)

    Relationships:
        - CHUA_DIEU_KHOAN: VanBan → DieuKhoan
        - BAN_HANH_BOI: VanBan → CoQuanBanHanh
        - THUOC_LOAI: VanBan → LoaiVanBan
        - DAN_CHIEU_TOI: VanBan → VanBan (dẫn chiếu/tham chiếu)
        - THAY_THE_CHO: VanBan → VanBan (thay thế văn bản cũ)
        - HUONG_DAN_BOI: VanBan → VanBan (hướng dẫn bởi)
    """

    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )
        self.doc_loader = DocumentLoader()

    def close(self):
        """Đóng kết nối."""
        self.driver.close()
        self.doc_loader.close()

    def build_graph(self) -> Dict[str, int]:
        """
        Xây dựng Knowledge Graph pháp luật từ dữ liệu PostgreSQL.

        Returns:
            Dict thống kê: nodes và relationships đã tạo
        """
        stats = {
            "van_ban_nodes": 0,
            "dieu_khoan_nodes": 0,
            "co_quan_nodes": 0,
            "loai_van_ban_nodes": 0,
            "chua_dieu_khoan_rels": 0,
            "ban_hanh_boi_rels": 0,
            "thuoc_loai_rels": 0,
            "dan_chieu_toi_rels": 0,
            "thay_the_cho_rels": 0,
            "huong_dan_boi_rels": 0,
        }

        print("🔄 Đang đọc dữ liệu từ PostgreSQL...")

        # Bước 1: Tạo constraints & indexes trong Neo4j
        self._create_constraints()

        # Bước 2: Tạo VanBan nodes từ legal_documents
        stats["van_ban_nodes"] = self._create_document_nodes()

        # Bước 3: Tạo DieuKhoan nodes từ legal_chunks
        stats["dieu_khoan_nodes"] = self._create_chunk_nodes()

        # Bước 4: Tạo Entity nodes (CoQuanBanHanh, LoaiVanBan)
        co_quan, loai_vb = self._create_entity_nodes()
        stats["co_quan_nodes"] = co_quan
        stats["loai_van_ban_nodes"] = loai_vb

        # Bước 5: Tạo relationships
        rels = self._create_relationships()
        stats.update(rels)

        self._print_stats(stats)
        return stats

    def _create_constraints(self):
        """Tạo uniqueness constraints trong Neo4j."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (v:VanBan) REQUIRE v.doc_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:DieuKhoan) REQUIRE d.chunk_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:CoQuanBanHanh) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:LoaiVanBan) REQUIRE l.name IS UNIQUE",
        ]
        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)
        print("🔒 Neo4j constraints đã tạo")

    def _create_document_nodes(self) -> int:
        """Tạo VanBan nodes từ PostgreSQL legal_documents."""
        query_pg = """
            SELECT doc_id, doc_type, doc_number, title, issuing_body, 
                   issued_date, effective_date, status
            FROM legal_documents;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_pg)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        docs = [dict(zip(columns, row)) for row in rows]

        cypher = """
            UNWIND $docs AS doc
            MERGE (v:VanBan {doc_id: doc.doc_id})
            SET v.title = doc.title,
                v.doc_type = doc.doc_type,
                v.doc_number = doc.doc_number,
                v.issuing_body = doc.issuing_body,
                v.issued_date = toString(doc.issued_date),
                v.effective_date = toString(doc.effective_date),
                v.status = doc.status
        """
        # Chuyển date thành string để Neo4j xử lý
        for doc in docs:
            doc["issued_date"] = str(doc["issued_date"]) if doc["issued_date"] else None
            doc["effective_date"] = str(doc["effective_date"]) if doc["effective_date"] else None

        with self.driver.session() as session:
            session.run(cypher, docs=docs)

        print(f"📄 Tạo {len(docs)} VanBan nodes")
        return len(docs)

    def _create_chunk_nodes(self) -> int:
        """Tạo DieuKhoan nodes từ PostgreSQL legal_chunks."""
        chunks = self.doc_loader.get_all_chunks()

        cypher = """
            UNWIND $chunks AS chunk
            MERGE (d:DieuKhoan {chunk_id: chunk.chunk_id})
            SET d.doc_id = chunk.doc_id,
                d.chapter = chunk.chapter,
                d.article = chunk.article,
                d.clause = chunk.clause,
                d.content_preview = left(chunk.content, 200),
                d.doc_title = chunk.doc_title
        """
        chunk_data = [
            {
                "chunk_id": c["chunk_id"],
                "doc_id": c["doc_id"],
                "chapter": c.get("chapter"),
                "article": c.get("article"),
                "clause": c.get("clause"),
                "content": c["content"],
                "doc_title": c.get("doc_title", ""),
            }
            for c in chunks
        ]

        with self.driver.session() as session:
            session.run(cypher, chunks=chunk_data)

        print(f"📑 Tạo {len(chunk_data)} DieuKhoan nodes")
        return len(chunk_data)

    def _create_entity_nodes(self) -> Tuple[int, int]:
        """Tạo CoQuanBanHanh và LoaiVanBan nodes."""
        query_pg = """
            SELECT DISTINCT issuing_body FROM legal_documents WHERE issuing_body IS NOT NULL;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_pg)
            issuing_bodies = [row[0] for row in cur.fetchall()]

        # Tạo CoQuanBanHanh nodes
        cypher_cq = """
            UNWIND $names AS name
            MERGE (c:CoQuanBanHanh {name: name})
        """
        with self.driver.session() as session:
            session.run(cypher_cq, names=issuing_bodies)

        # Lấy danh sách loại văn bản
        query_type = """
            SELECT DISTINCT doc_type FROM legal_documents WHERE doc_type IS NOT NULL;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_type)
            doc_types = [row[0] for row in cur.fetchall()]

        # Tạo LoaiVanBan nodes
        cypher_loai = """
            UNWIND $names AS name
            MERGE (l:LoaiVanBan {name: name})
        """
        with self.driver.session() as session:
            session.run(cypher_loai, names=doc_types)

        print(f"🏛️  Tạo {len(issuing_bodies)} CoQuanBanHanh, {len(doc_types)} LoaiVanBan nodes")
        return len(issuing_bodies), len(doc_types)

    def _create_relationships(self) -> Dict[str, int]:
        """Tạo tất cả relationships trong Neo4j."""
        stats = {}

        # 1. CHUA_DIEU_KHOAN: VanBan → DieuKhoan
        cypher_chua = """
            MATCH (v:VanBan), (d:DieuKhoan)
            WHERE v.doc_id = d.doc_id
            MERGE (v)-[:CHUA_DIEU_KHOAN]->(d)
        """
        with self.driver.session() as session:
            result = session.run(cypher_chua)
            summary = result.consume()
            stats["chua_dieu_khoan_rels"] = summary.counters.relationships_created

        # 2. BAN_HANH_BOI: VanBan → CoQuanBanHanh
        cypher_bh = """
            MATCH (v:VanBan), (c:CoQuanBanHanh)
            WHERE v.issuing_body = c.name
            MERGE (v)-[:BAN_HANH_BOI]->(c)
        """
        with self.driver.session() as session:
            result = session.run(cypher_bh)
            summary = result.consume()
            stats["ban_hanh_boi_rels"] = summary.counters.relationships_created

        # 3. THUOC_LOAI: VanBan → LoaiVanBan
        cypher_loai = """
            MATCH (v:VanBan), (l:LoaiVanBan)
            WHERE v.doc_type = l.name
            MERGE (v)-[:THUOC_LOAI]->(l)
        """
        with self.driver.session() as session:
            result = session.run(cypher_loai)
            summary = result.consume()
            stats["thuoc_loai_rels"] = summary.counters.relationships_created

        # 4. DAN_CHIEU_TOI: Dựa trên cột references_to trong PostgreSQL
        query_refs = """
            SELECT doc_id, references_to FROM legal_documents 
            WHERE references_to IS NOT NULL AND array_length(references_to, 1) > 0;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_refs)
            rows = cur.fetchall()

        ref_count = 0
        for doc_id, refs in rows:
            if refs:
                for ref_id in refs:
                    cypher_ref = """
                        MATCH (v1:VanBan {doc_id: $doc_id})
                        MATCH (v2:VanBan {doc_id: $ref_id})
                        MERGE (v1)-[:DAN_CHIEU_TOI]->(v2)
                    """
                    with self.driver.session() as session:
                        result = session.run(cypher_ref, doc_id=doc_id, ref_id=ref_id)
                        summary = result.consume()
                        ref_count += summary.counters.relationships_created
        stats["dan_chieu_toi_rels"] = ref_count

        # 5. THAY_THE_CHO: Dựa trên cột replaced_by
        query_replace = """
            SELECT doc_id, replaced_by FROM legal_documents 
            WHERE replaced_by IS NOT NULL;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_replace)
            rows = cur.fetchall()

        replace_count = 0
        for doc_id, replaced_by in rows:
            cypher_replace = """
                MATCH (v1:VanBan {doc_id: $replaced_by})
                MATCH (v2:VanBan {doc_id: $doc_id})
                MERGE (v1)-[:THAY_THE_CHO]->(v2)
            """
            with self.driver.session() as session:
                result = session.run(cypher_replace, doc_id=doc_id, replaced_by=replaced_by)
                summary = result.consume()
                replace_count += summary.counters.relationships_created
        stats["thay_the_cho_rels"] = replace_count

        # 6. HUONG_DAN_BOI: Dựa trên cột guided_by
        query_guide = """
            SELECT doc_id, guided_by FROM legal_documents 
            WHERE guided_by IS NOT NULL AND array_length(guided_by, 1) > 0;
        """
        with self.doc_loader.conn.cursor() as cur:
            cur.execute(query_guide)
            rows = cur.fetchall()

        guide_count = 0
        for doc_id, guides in rows:
            if guides:
                for guide_id in guides:
                    cypher_guide = """
                        MATCH (v1:VanBan {doc_id: $doc_id})
                        MATCH (v2:VanBan {doc_id: $guide_id})
                        MERGE (v1)-[:HUONG_DAN_BOI]->(v2)
                    """
                    with self.driver.session() as session:
                        result = session.run(
                            cypher_guide, doc_id=doc_id, guide_id=guide_id
                        )
                        summary = result.consume()
                        guide_count += summary.counters.relationships_created
        stats["huong_dan_boi_rels"] = guide_count

        return stats

    def query_related_docs(self, doc_id: str, depth: int = 2) -> List[Dict[str, Any]]:
        """
        Truy vấn các văn bản liên quan đến doc_id qua quan hệ graph.

        Args:
            doc_id: ID văn bản cần tra cứu
            depth: Độ sâu duyệt graph (1-3)

        Returns:
            List các văn bản liên quan với thông tin quan hệ
        """
        cypher = """
            MATCH (v:VanBan {doc_id: $doc_id})-[r*1..""" + str(depth) + """]-(related:VanBan)
            WHERE related.doc_id <> $doc_id
            WITH DISTINCT related, 
                 [rel IN r | type(rel)] AS rel_types
            RETURN related.doc_id AS doc_id,
                   related.title AS title,
                   related.doc_type AS doc_type,
                   related.doc_number AS doc_number,
                   related.issuing_body AS issuing_body,
                   rel_types AS relationships
            LIMIT 20
        """
        with self.driver.session() as session:
            result = session.run(cypher, doc_id=doc_id)
            records = [dict(record) for record in result]

        return records

    def query_related_chunks(self, doc_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Lấy các DieuKhoan liên quan đến danh sách doc_ids.

        Args:
            doc_ids: Danh sách doc_id

        Returns:
            List chunk info từ graph
        """
        cypher = """
            UNWIND $doc_ids AS did
            MATCH (v:VanBan {doc_id: did})-[:CHUA_DIEU_KHOAN]->(d:DieuKhoan)
            RETURN d.chunk_id AS chunk_id,
                   d.doc_id AS doc_id,
                   d.article AS article,
                   d.chapter AS chapter,
                   d.content_preview AS content_preview,
                   d.doc_title AS doc_title
            LIMIT 50
        """
        with self.driver.session() as session:
            result = session.run(cypher, doc_ids=doc_ids)
            records = [dict(record) for record in result]

        return records

    def get_graph_summary(self) -> Dict[str, int]:
        """Lấy thống kê tổng quan của Knowledge Graph."""
        queries = {
            "VanBan": "MATCH (n:VanBan) RETURN count(n) AS count",
            "DieuKhoan": "MATCH (n:DieuKhoan) RETURN count(n) AS count",
            "CoQuanBanHanh": "MATCH (n:CoQuanBanHanh) RETURN count(n) AS count",
            "LoaiVanBan": "MATCH (n:LoaiVanBan) RETURN count(n) AS count",
            "Relationships": "MATCH ()-[r]->() RETURN count(r) AS count",
        }
        summary = {}
        with self.driver.session() as session:
            for label, cypher in queries.items():
                result = session.run(cypher)
                record = result.single()
                summary[label] = record["count"] if record else 0

        return summary

    def _print_stats(self, stats: Dict[str, int]):
        """In thống kê xây dựng graph."""
        print("\n" + "=" * 50)
        print("📊 KẾT QUẢ XÂY DỰNG KNOWLEDGE GRAPH")
        print("=" * 50)
        print(f"  Nodes:")
        print(f"    - VanBan:         {stats['van_ban_nodes']}")
        print(f"    - DieuKhoan:      {stats['dieu_khoan_nodes']}")
        print(f"    - CoQuanBanHanh:  {stats['co_quan_nodes']}")
        print(f"    - LoaiVanBan:     {stats['loai_van_ban_nodes']}")
        print(f"  Relationships:")
        print(f"    - CHUA_DIEU_KHOAN: {stats['chua_dieu_khoan_rels']}")
        print(f"    - BAN_HANH_BOI:    {stats['ban_hanh_boi_rels']}")
        print(f"    - THUOC_LOAI:      {stats['thuoc_loai_rels']}")
        print(f"    - DAN_CHIEU_TOI:   {stats['dan_chieu_toi_rels']}")
        print(f"    - THAY_THE_CHO:    {stats['thay_the_cho_rels']}")
        print(f"    - HUONG_DAN_BOI:   {stats['huong_dan_boi_rels']}")
        print("=" * 50)
