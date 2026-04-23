from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from datasets import load_dataset
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from chunking import RuleBasedHierarchicalChunker
from config import PlatformConfig
from gemini_embedding import GeminiEmbeddingClient
from sinks import Neo4jAuraSink, QdrantSink


class LakehousePipeline:
    def __init__(self, spark: SparkSession, config: PlatformConfig) -> None:
        self.spark = spark
        self.config = config

    def run_bronze_ingestion(self) -> Dict[str, int]:
        self._ensure_schema()

        table_name = self.config.table("bronze_documents")
        dataset = load_dataset(
            self.config.hf_dataset_name,
            split=self.config.hf_split,
            streaming=True,
        )

        written = 0
        batch: List[Dict[str, Any]] = []
        for idx, source_row in enumerate(dataset):
            if self.config.max_source_records > 0 and written >= self.config.max_source_records:
                break

            mapped = self._map_source_record(source_row, idx)
            batch.append(mapped)
            written += 1

            if len(batch) >= self.config.source_batch_size:
                self._write_bronze_batch(table_name, batch)
                batch.clear()

        if batch:
            self._write_bronze_batch(table_name, batch)

        return {"bronze_documents": written}

    def run_silver_transform(self) -> Dict[str, int]:
        self._ensure_schema()

        bronze_df = self.spark.table(self.config.table("bronze_documents"))

        silver_docs_df = (
            bronze_df.select(
                "doc_id",
                "doc_number",
                F.col("title").alias("doc_title"),
                "doc_type",
                "issuing_body",
                "issued_date",
                "status",
                "content_text",
                "raw_chunks_json",
                "relations_json",
                "ingest_ts",
            )
            .withColumn("issued_date", F.to_date("issued_date"))
            .withColumn("status", F.coalesce(F.col("status"), F.lit("active")))
        )

        silver_docs_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_documents")
        )

        chunker = RuleBasedHierarchicalChunker(
            target_tokens=self.config.chunk_target_tokens,
            max_tokens=self.config.chunk_max_tokens,
            overlap_tokens=self.config.chunk_overlap_tokens,
        )

        chunk_rows: List[Dict[str, Any]] = []
        relation_rows: List[Dict[str, Any]] = []

        for doc in silver_docs_df.toLocalIterator():
            record = doc.asDict(recursive=True)
            doc_id = str(record.get("doc_id", "")).strip()
            doc_number = str(record.get("doc_number", "")).strip()
            doc_title = str(record.get("doc_title", "")).strip()

            raw_chunks = _safe_json_loads(record.get("raw_chunks_json"), fallback=[])
            relations = _safe_json_loads(record.get("relations_json"), fallback=[])
            document_text = self._build_document_text(
                fallback_text=str(record.get("content_text", "")),
                raw_chunks=raw_chunks,
            )

            chunks = chunker.chunk_document(
                doc_id=doc_id,
                doc_number=doc_number,
                doc_title=doc_title,
                raw_text=document_text,
                base_metadata={
                    "source_dataset": self.config.hf_dataset_name,
                    "source_split": self.config.hf_split,
                },
            )

            for chunk in chunks:
                chunk_rows.append(
                    {
                        "chunk_uid": chunk.chunk_uid,
                        "doc_id": chunk.doc_id,
                        "doc_number": chunk.doc_number,
                        "doc_title": chunk.doc_title,
                        "chunk_order": chunk.chunk_order,
                        "chunk_text": chunk.chunk_text,
                        "token_count": chunk.token_count,
                        "tokens": chunk.tokens,
                        "chapter": chunk.chapter,
                        "section": chunk.section,
                        "article": chunk.article,
                        "clause": chunk.clause,
                        "point": chunk.point,
                        "references_to": chunk.references_to,
                        "metadata_json": _json_dumps(chunk.metadata),
                        "transform_ts": datetime.now(timezone.utc),
                    }
                )

                for target_doc in chunk.references_to:
                    relation_rows.append(
                        {
                            "source_doc_id": doc_id,
                            "source_doc_number": doc_number,
                            "target_doc_number": target_doc,
                            "relation_type": "MENTIONS",
                            "relation_source": "chunk_reference",
                        }
                    )

            for relation in relations:
                target_doc = _pick_first(
                    relation,
                    ["target_so_ky_hieu", "target_doc_number", "target_doc_id"],
                    default="",
                )
                relation_type = _pick_first(
                    relation,
                    ["type", "relation_type"],
                    default="REFERS_TO",
                )
                if not target_doc:
                    continue
                relation_rows.append(
                    {
                        "source_doc_id": doc_id,
                        "source_doc_number": doc_number,
                        "target_doc_number": str(target_doc),
                        "relation_type": str(relation_type),
                        "relation_source": "explicit_relation",
                    }
                )

        silver_chunks_df = self.spark.createDataFrame(chunk_rows, schema=_chunk_schema())
        silver_chunks_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_chunks")
        )

        deduped_relations = _dedupe_relation_rows(relation_rows)
        silver_relations_df = self.spark.createDataFrame(
            deduped_relations,
            schema=_relation_schema(),
        )
        silver_relations_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_relations")
        )

        return {
            "silver_documents": silver_docs_df.count(),
            "silver_chunks": silver_chunks_df.count(),
            "silver_relations": silver_relations_df.count(),
        }

    def run_gold_embeddings_and_sync(self) -> Dict[str, int]:
        self.config.validate_for_gold()
        self._ensure_schema()

        silver_chunks_df = self.spark.table(self.config.table("silver_chunks")).orderBy(
            "doc_id", "chunk_order"
        )

        chunk_rows: List[Dict[str, Any]] = [
            row.asDict(recursive=True) for row in silver_chunks_df.toLocalIterator()
        ]

        texts = [str(row["chunk_text"]) for row in chunk_rows]
        token_counts = [int(row.get("token_count") or 1) for row in chunk_rows]

        embed_client = GeminiEmbeddingClient(
            api_key=self.config.gemini_api_key,
            model_name=self.config.gemini_embedding_model,
            rpm_limit=self.config.gemini_rpm_limit,
            tpm_limit=self.config.gemini_tpm_limit,
            safety_factor=self.config.rate_limit_headroom,
            batch_size=self.config.gemini_embed_batch_size,
        )
        vectors = embed_client.embed_texts(texts=texts, token_counts=token_counts)

        if vectors and self.config.qdrant_vector_size != len(vectors[0]):
            print(
                "[warn] QDRANT_VECTOR_SIZE does not match Gemini output size. "
                f"Using inferred size={len(vectors[0])}."
            )

        gold_rows: List[Dict[str, Any]] = []
        for chunk_row, vector in zip(chunk_rows, vectors):
            gold_rows.append(
                {
                    "chunk_uid": chunk_row["chunk_uid"],
                    "doc_id": chunk_row["doc_id"],
                    "doc_number": chunk_row["doc_number"],
                    "doc_title": chunk_row["doc_title"],
                    "chunk_order": int(chunk_row["chunk_order"]),
                    "chunk_text": chunk_row["chunk_text"],
                    "token_count": int(chunk_row["token_count"]),
                    "chapter": chunk_row.get("chapter"),
                    "section": chunk_row.get("section"),
                    "article": chunk_row.get("article"),
                    "clause": chunk_row.get("clause"),
                    "point": chunk_row.get("point"),
                    "metadata_json": chunk_row.get("metadata_json"),
                    "embedding": [float(value) for value in vector],
                    "embedded_at": datetime.now(timezone.utc),
                }
            )

        gold_df = self.spark.createDataFrame(gold_rows, schema=_gold_schema())
        gold_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("gold_embeddings")
        )

        sync_stats = {"qdrant_points": 0, "neo4j_documents": 0, "neo4j_chunks": 0, "neo4j_relations": 0}

        if self.config.sync_to_qdrant:
            vector_size = len(vectors[0]) if vectors else self.config.qdrant_vector_size
            qdrant_sink = QdrantSink(
                url=self.config.qdrant_url,
                api_key=self.config.qdrant_api_key,
                collection_name=self.config.qdrant_collection,
                vector_size=vector_size,
                distance=self.config.qdrant_distance,
                batch_size=self.config.qdrant_batch_size,
            )
            sync_stats["qdrant_points"] = qdrant_sink.upsert_embeddings(gold_rows)

        if self.config.sync_to_neo4j:
            docs_df = self.spark.table(self.config.table("silver_documents"))
            relations_df = self.spark.table(self.config.table("silver_relations"))

            docs_rows = [row.asDict(recursive=True) for row in docs_df.toLocalIterator()]
            relation_rows = [row.asDict(recursive=True) for row in relations_df.toLocalIterator()]

            neo4j_sink = Neo4jAuraSink(
                uri=self.config.neo4j_uri,
                user=self.config.neo4j_user,
                password=self.config.neo4j_password,
                database=self.config.neo4j_database,
                batch_size=self.config.neo4j_batch_size,
            )
            try:
                result = neo4j_sink.sync(
                    docs=[
                        {
                            "doc_id": row["doc_id"],
                            "doc_number": row.get("doc_number"),
                            "title": row.get("doc_title"),
                            "doc_type": row.get("doc_type"),
                            "issuing_body": row.get("issuing_body"),
                            "issued_date": str(row.get("issued_date")) if row.get("issued_date") else None,
                            "status": row.get("status"),
                        }
                        for row in docs_rows
                    ],
                    chunks=[
                        {
                            "chunk_uid": row["chunk_uid"],
                            "doc_id": row["doc_id"],
                            "doc_number": row.get("doc_number"),
                            "chunk_order": int(row["chunk_order"]),
                            "token_count": int(row["token_count"]),
                            "chapter": row.get("chapter"),
                            "section": row.get("section"),
                            "article": row.get("article"),
                            "clause": row.get("clause"),
                            "point": row.get("point"),
                            "chunk_text": row.get("chunk_text"),
                            "metadata_json": row.get("metadata_json"),
                        }
                        for row in gold_rows
                    ],
                    relations=relation_rows,
                )
            finally:
                neo4j_sink.close()

            sync_stats["neo4j_documents"] = result.get("documents", 0)
            sync_stats["neo4j_chunks"] = result.get("chunks", 0)
            sync_stats["neo4j_relations"] = result.get("relations", 0)

        return {
            "gold_embeddings": gold_df.count(),
            **sync_stats,
        }

    def run_all(self) -> Dict[str, int]:
        summary: Dict[str, int] = {}
        summary.update(self.run_bronze_ingestion())
        summary.update(self.run_silver_transform())
        summary.update(self.run_gold_embeddings_and_sync())
        return summary

    def _ensure_schema(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.catalog}.{self.config.schema}")

    def _write_bronze_batch(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        bronze_df = self.spark.createDataFrame(rows, schema=_bronze_schema())
        bronze_df.write.mode("append").format("delta").saveAsTable(table_name)

    def _map_source_record(self, row: Dict[str, Any], index: int) -> Dict[str, Any]:
        doc_number = _pick_first(row, ["so_ky_hieu", "doc_number", "so_hieu"], default="")
        title = _pick_first(row, ["title", "ten_van_ban"], default="")
        doc_type = _pick_first(row, ["loai_van_ban", "doc_type"], default="")
        issuing_body = _pick_first(row, ["co_quan_ban_hanh", "issuing_body"], default="")
        issued_date = _pick_first(row, ["ngay_ban_hanh", "issued_date"], default="")
        status = _pick_first(row, ["tinh_trang_hieu_luc", "status"], default="active")

        chunks = row.get("chunks") or []
        content_text = str(row.get("content_text") or "").strip()
        if not content_text and isinstance(chunks, list) and chunks:
            content_text = "\n".join(
                str(chunk.get("content") or "").strip() for chunk in chunks if chunk.get("content")
            )

        provided_doc_id = str(_pick_first(row, ["doc_id"], default="")).strip()
        doc_id = provided_doc_id or _generate_doc_id(doc_number=doc_number, title=title, index=index)

        return {
            "doc_id": doc_id,
            "doc_number": str(doc_number),
            "title": str(title),
            "doc_type": str(doc_type),
            "issuing_body": str(issuing_body),
            "issued_date": str(issued_date),
            "status": str(status),
            "content_text": str(content_text),
            "raw_chunks_json": _json_dumps(chunks),
            "relations_json": _json_dumps(row.get("relations") or []),
            "raw_record_json": _json_dumps(row),
            "source_dataset": self.config.hf_dataset_name,
            "ingest_ts": datetime.now(timezone.utc),
        }

    @staticmethod
    def _build_document_text(fallback_text: str, raw_chunks: Any) -> str:
        if not isinstance(raw_chunks, list) or not raw_chunks:
            return fallback_text

        lines: List[str] = []
        last_chapter = ""
        last_article = ""

        for chunk in raw_chunks:
            if not isinstance(chunk, dict):
                continue

            chapter = str(chunk.get("chapter") or "").strip()
            article = str(chunk.get("article") or "").strip()
            clause = str(chunk.get("clause") or "").strip()
            content = str(chunk.get("content") or "").strip()

            if chapter and chapter != last_chapter:
                lines.append(chapter)
                last_chapter = chapter

            if article and article != last_article:
                lines.append(article)
                last_article = article

            if clause:
                lines.append(clause)

            if content:
                lines.append(content)

        text = "\n".join(lines).strip()
        return text if text else fallback_text


def _pick_first(row: Dict[str, Any], keys: Iterable[str], default: Any = "") -> Any:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _safe_json_loads(raw: Any, fallback: Any) -> Any:
    if raw is None:
        return fallback
    if isinstance(raw, (list, dict)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return fallback


def _generate_doc_id(doc_number: str, title: str, index: int) -> str:
    base = (doc_number or title or f"document-{index}").strip()
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in base)[:40].strip("_")
    if not sanitized:
        sanitized = "document"
    return f"{sanitized}_{digest}".lower()


def _dedupe_relation_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    output = []
    for row in rows:
        key = (
            row.get("source_doc_id"),
            row.get("source_doc_number"),
            row.get("target_doc_number"),
            row.get("relation_type"),
            row.get("relation_source"),
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def _bronze_schema() -> StructType:
    return StructType(
        [
            StructField("doc_id", StringType(), False),
            StructField("doc_number", StringType(), True),
            StructField("title", StringType(), True),
            StructField("doc_type", StringType(), True),
            StructField("issuing_body", StringType(), True),
            StructField("issued_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("content_text", StringType(), True),
            StructField("raw_chunks_json", StringType(), True),
            StructField("relations_json", StringType(), True),
            StructField("raw_record_json", StringType(), True),
            StructField("source_dataset", StringType(), True),
            StructField("ingest_ts", TimestampType(), False),
        ]
    )


def _chunk_schema() -> StructType:
    return StructType(
        [
            StructField("chunk_uid", StringType(), False),
            StructField("doc_id", StringType(), False),
            StructField("doc_number", StringType(), True),
            StructField("doc_title", StringType(), True),
            StructField("chunk_order", IntegerType(), False),
            StructField("chunk_text", StringType(), False),
            StructField("token_count", IntegerType(), False),
            StructField("tokens", ArrayType(StringType(), containsNull=False), False),
            StructField("chapter", StringType(), True),
            StructField("section", StringType(), True),
            StructField("article", StringType(), True),
            StructField("clause", StringType(), True),
            StructField("point", StringType(), True),
            StructField("references_to", ArrayType(StringType(), containsNull=False), False),
            StructField("metadata_json", StringType(), True),
            StructField("transform_ts", TimestampType(), False),
        ]
    )


def _relation_schema() -> StructType:
    return StructType(
        [
            StructField("source_doc_id", StringType(), False),
            StructField("source_doc_number", StringType(), True),
            StructField("target_doc_number", StringType(), False),
            StructField("relation_type", StringType(), False),
            StructField("relation_source", StringType(), False),
        ]
    )


def _gold_schema() -> StructType:
    return StructType(
        [
            StructField("chunk_uid", StringType(), False),
            StructField("doc_id", StringType(), False),
            StructField("doc_number", StringType(), True),
            StructField("doc_title", StringType(), True),
            StructField("chunk_order", IntegerType(), False),
            StructField("chunk_text", StringType(), False),
            StructField("token_count", IntegerType(), False),
            StructField("chapter", StringType(), True),
            StructField("section", StringType(), True),
            StructField("article", StringType(), True),
            StructField("clause", StringType(), True),
            StructField("point", StringType(), True),
            StructField("metadata_json", StringType(), True),
            StructField("embedding", ArrayType(FloatType(), containsNull=False), False),
            StructField("embedded_at", TimestampType(), False),
        ]
    )
