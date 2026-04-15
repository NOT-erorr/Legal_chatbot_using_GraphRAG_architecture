"""
document_loader.py — Load dữ liệu văn bản luật từ JSON và ghi vào PostgreSQL.
Đây là bước khởi đầu của pipeline: Raw Data → PostgreSQL (Source of Truth).
"""

import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from typing import List, Dict, Any

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings


class DocumentLoader:
    """Load văn bản luật từ JSON file và insert vào PostgreSQL."""

    def __init__(self):
        self.conn = psycopg2.connect(settings.POSTGRES_URL)
        self.conn.autocommit = True

    def close(self):
        """Đóng kết nối database."""
        if self.conn and not self.conn.closed:
            self.conn.close()

    def load_from_json(self, json_path: str) -> Dict[str, int]:
        """
        Đọc file JSON chứa văn bản luật và insert vào PostgreSQL.

        Args:
            json_path: Đường dẫn tới file JSON

        Returns:
            Dict với số lượng documents và chunks đã insert
        """
        json_path = Path(json_path)
        if not json_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file: {json_path}")

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        documents = data.get("documents", [])
        total_docs = 0
        total_chunks = 0

        for doc in documents:
            # Insert document metadata
            self._insert_document(doc)
            total_docs += 1

            # Insert chunks
            chunks = doc.get("chunks", [])
            for chunk in chunks:
                self._insert_chunk(doc["doc_id"], chunk)
                total_chunks += 1

        print(f"✅ Loaded {total_docs} documents, {total_chunks} chunks vào PostgreSQL")
        return {"documents": total_docs, "chunks": total_chunks}

    def _insert_document(self, doc: Dict[str, Any]):
        """Insert một văn bản luật vào bảng legal_documents."""
        query = """
            INSERT INTO legal_documents 
                (doc_id, doc_type, doc_number, title, issuing_body, 
                 issued_date, effective_date, status, references_to, 
                 replaced_by, guided_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (doc_id) DO UPDATE SET
                doc_type = EXCLUDED.doc_type,
                doc_number = EXCLUDED.doc_number,
                title = EXCLUDED.title,
                issuing_body = EXCLUDED.issuing_body,
                issued_date = EXCLUDED.issued_date,
                effective_date = EXCLUDED.effective_date,
                status = EXCLUDED.status,
                references_to = EXCLUDED.references_to,
                replaced_by = EXCLUDED.replaced_by,
                guided_by = EXCLUDED.guided_by;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (
                doc["doc_id"],
                doc.get("doc_type"),
                doc.get("doc_number"),
                doc["title"],
                doc.get("issuing_body"),
                doc.get("issued_date"),
                doc.get("effective_date"),
                doc.get("status", "active"),
                doc.get("references_to", []),
                doc.get("replaced_by"),
                doc.get("guided_by", []),
            ))

    def _insert_chunk(self, doc_id: str, chunk: Dict[str, Any]):
        """Insert một chunk vào bảng legal_chunks."""
        query = """
            INSERT INTO legal_chunks
                (doc_id, chunk_id, content, chapter, article, clause, section, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chunk_id) DO UPDATE SET
                content = EXCLUDED.content,
                chapter = EXCLUDED.chapter,
                article = EXCLUDED.article,
                clause = EXCLUDED.clause,
                section = EXCLUDED.section,
                metadata = EXCLUDED.metadata;
        """
        metadata = chunk.get("metadata", {})
        with self.conn.cursor() as cur:
            cur.execute(query, (
                doc_id,
                chunk["chunk_id"],
                chunk["content"],
                chunk.get("chapter"),
                chunk.get("article"),
                chunk.get("clause"),
                chunk.get("section"),
                json.dumps(metadata, ensure_ascii=False),
            ))

    def get_all_chunks(self) -> List[Dict[str, Any]]:
        """Lấy toàn bộ chunks từ PostgreSQL."""
        query = """
            SELECT c.chunk_id, c.doc_id, c.content, c.chapter, c.article, 
                   c.clause, c.section, c.metadata,
                   d.title as doc_title, d.doc_type, d.doc_number, d.issuing_body
            FROM legal_chunks c
            JOIN legal_documents d ON c.doc_id = d.doc_id
            ORDER BY c.id;
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def get_chunks_by_ids(self, chunk_ids: List[str]) -> List[Dict[str, Any]]:
        """Lấy chunks theo danh sách chunk_id."""
        if not chunk_ids:
            return []

        query = """
            SELECT c.chunk_id, c.doc_id, c.content, c.chapter, c.article,
                   c.clause, c.section, c.metadata,
                   d.title as doc_title, d.doc_type, d.doc_number, d.issuing_body
            FROM legal_chunks c
            JOIN legal_documents d ON c.doc_id = d.doc_id
            WHERE c.chunk_id = ANY(%s);
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (chunk_ids,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def get_chunks_by_doc_ids(self, doc_ids: List[str]) -> List[Dict[str, Any]]:
        """Lấy tất cả chunks thuộc các doc_id."""
        if not doc_ids:
            return []

        query = """
            SELECT c.chunk_id, c.doc_id, c.content, c.chapter, c.article,
                   c.clause, c.section, c.metadata,
                   d.title as doc_title, d.doc_type, d.doc_number, d.issuing_body
            FROM legal_chunks c
            JOIN legal_documents d ON c.doc_id = d.doc_id
            WHERE c.doc_id = ANY(%s)
            ORDER BY c.id;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (doc_ids,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def get_document_by_id(self, doc_id: str) -> Dict[str, Any]:
        """Lấy metadata của một văn bản theo doc_id."""
        query = """
            SELECT doc_id, doc_type, doc_number, title, issuing_body,
                   issued_date, effective_date, status, references_to,
                   replaced_by, guided_by
            FROM legal_documents
            WHERE doc_id = %s;
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (doc_id,))
            columns = [desc[0] for desc in cur.description]
            row = cur.fetchone()

        if row:
            return dict(zip(columns, row))
        return {}
