from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Iterable, List, Sequence

try:
    from neo4j import GraphDatabase  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - import guard
    GraphDatabase = None  # type: ignore[assignment]

try:
    from qdrant_client import QdrantClient  # type: ignore[import-not-found]
    from qdrant_client.http import models as qdrant_models  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - import guard
    QdrantClient = None  # type: ignore[assignment]
    qdrant_models = None  # type: ignore[assignment]


def _batch(items: Sequence[Dict[str, Any]], batch_size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for idx in range(0, len(items), batch_size):
        yield items[idx : idx + batch_size]


class QdrantSink:
    def __init__(
        self,
        url: str,
        api_key: str,
        collection_name: str,
        vector_size: int,
        distance: str = "Cosine",
        batch_size: int = 256,
    ) -> None:
        if QdrantClient is None or qdrant_models is None:
            raise ImportError(
                "qdrant-client is required. Install dependencies from requirements-databricks.txt"
            )

        self.collection_name = collection_name
        self.vector_size = vector_size
        self.batch_size = max(1, batch_size)

        self.client = QdrantClient(
            url=url,
            api_key=api_key or None,
            timeout=120,
        )

        distance_map = {
            "cosine": qdrant_models.Distance.COSINE,
            "dot": qdrant_models.Distance.DOT,
            "euclid": qdrant_models.Distance.EUCLID,
            "manhattan": qdrant_models.Distance.MANHATTAN,
        }
        self.distance = distance_map.get(distance.lower(), qdrant_models.Distance.COSINE)

    def ensure_collection(self) -> None:
        if qdrant_models is None:
            raise ImportError("qdrant-client is not available")

        exists = False
        collection_exists = getattr(self.client, "collection_exists", None)
        if callable(collection_exists):
            exists = bool(collection_exists(self.collection_name))
        else:
            try:
                self.client.get_collection(self.collection_name)
                exists = True
            except Exception:
                exists = False

        if exists:
            return

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=qdrant_models.VectorParams(
                size=self.vector_size,
                distance=self.distance,
            ),
        )

    def upsert_embeddings(self, rows: Sequence[Dict[str, Any]]) -> int:
        if not rows:
            return 0

        self.ensure_collection()

        total_points = 0
        for chunk in _batch(list(rows), self.batch_size):
            if qdrant_models is None:
                raise ImportError("qdrant-client is not available")

            points: List[Any] = []
            for row in chunk:
                vector = row["embedding"]
                if len(vector) != self.vector_size:
                    raise ValueError(
                        f"Vector size mismatch for {row['chunk_uid']}: "
                        f"expected {self.vector_size}, got {len(vector)}"
                    )

                point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, row["chunk_uid"]))
                payload = {
                    "doc_id": row.get("doc_id"),
                    "doc_number": row.get("doc_number"),
                    "doc_title": row.get("doc_title"),
                    "chunk_uid": row.get("chunk_uid"),
                    "chunk_order": row.get("chunk_order"),
                    "token_count": row.get("token_count"),
                    "chapter": row.get("chapter"),
                    "section": row.get("section"),
                    "article": row.get("article"),
                    "clause": row.get("clause"),
                    "point": row.get("point"),
                    "chunk_text": row.get("chunk_text"),
                    "metadata": row.get("metadata_json"),
                }
                points.append(
                    qdrant_models.PointStruct(
                        id=point_id,
                        vector=vector,
                        payload=payload,
                    )
                )

            self.client.upsert(collection_name=self.collection_name, points=points)
            total_points += len(points)

        return total_points


class Neo4jAuraSink:
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
        batch_size: int = 500,
    ) -> None:
        if GraphDatabase is None:
            raise ImportError(
                "neo4j python driver is required. Install dependencies from requirements-databricks.txt"
            )

        self.database = database
        self.batch_size = max(1, batch_size)
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def ensure_constraints(self) -> None:
        queries = [
            "CREATE CONSTRAINT doc_id_unique IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_id IS UNIQUE",
            "CREATE CONSTRAINT chunk_uid_unique IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunk_uid IS UNIQUE",
        ]
        with self.driver.session(database=self.database) as session:
            for query in queries:
                session.run(query)

    def upsert_documents(self, docs: Sequence[Dict[str, Any]]) -> int:
        if not docs:
            return 0

        query = """
            UNWIND $rows AS row
            MERGE (d:Document {doc_id: row.doc_id})
            SET d.doc_number = row.doc_number,
                d.title = row.title,
                d.doc_type = row.doc_type,
                d.issuing_body = row.issuing_body,
                d.issued_date = row.issued_date,
                d.status = row.status,
                d.updated_at = datetime()
        """

        total = 0
        with self.driver.session(database=self.database) as session:
            for chunk in _batch(list(docs), self.batch_size):
                session.run(query, rows=list(chunk))
                total += len(chunk)

        return total

    def upsert_chunks(self, chunks: Sequence[Dict[str, Any]]) -> int:
        if not chunks:
            return 0

        query = """
            UNWIND $rows AS row
            MERGE (c:Chunk {chunk_uid: row.chunk_uid})
            SET c.doc_id = row.doc_id,
                c.doc_number = row.doc_number,
                c.chunk_order = row.chunk_order,
                c.token_count = row.token_count,
                c.chapter = row.chapter,
                c.section = row.section,
                c.article = row.article,
                c.clause = row.clause,
                c.point = row.point,
                c.text = row.chunk_text,
                c.metadata_json = row.metadata_json,
                c.updated_at = datetime()
            WITH c, row
            MATCH (d:Document {doc_id: row.doc_id})
            MERGE (d)-[:HAS_CHUNK]->(c)
        """

        total = 0
        with self.driver.session(database=self.database) as session:
            for chunk in _batch(list(chunks), self.batch_size):
                session.run(query, rows=list(chunk))
                total += len(chunk)

        return total

    def upsert_relations(self, relations: Sequence[Dict[str, Any]]) -> int:
        if not relations:
            return 0

        query = """
            UNWIND $rows AS row
            MERGE (src:Document {doc_id: row.source_doc_id})
              ON CREATE SET src.doc_number = row.source_doc_number
            MERGE (dst:Document {doc_number: row.target_doc_number})
            MERGE (src)-[r:REFERS_TO]->(dst)
            SET r.relation_type = row.relation_type,
                r.updated_at = datetime()
        """

        total = 0
        with self.driver.session(database=self.database) as session:
            for chunk in _batch(list(relations), self.batch_size):
                session.run(query, rows=list(chunk))
                total += len(chunk)

        return total

    def sync(
        self,
        docs: Sequence[Dict[str, Any]],
        chunks: Sequence[Dict[str, Any]],
        relations: Sequence[Dict[str, Any]],
    ) -> Dict[str, int]:
        self.ensure_constraints()
        synced_docs = self.upsert_documents(docs)
        synced_chunks = self.upsert_chunks(chunks)
        synced_rels = self.upsert_relations(relations)

        return {
            "documents": synced_docs,
            "chunks": synced_chunks,
            "relations": synced_rels,
        }


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)
