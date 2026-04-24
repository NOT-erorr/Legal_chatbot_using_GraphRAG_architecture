from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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
from html_to_text import html_to_text
from sinks import Neo4jAuraSink, QdrantSink


_CHUNKER_CACHE: Dict[str, RuleBasedHierarchicalChunker] = {}


def _get_cached_chunker(
    target_tokens: int,
    max_tokens: int,
    overlap_tokens: int,
) -> RuleBasedHierarchicalChunker:
    cache_key = f"{target_tokens}:{max_tokens}:{overlap_tokens}"
    chunker = _CHUNKER_CACHE.get(cache_key)
    if chunker is None:
        chunker = RuleBasedHierarchicalChunker(
            target_tokens=target_tokens,
            max_tokens=max_tokens,
            overlap_tokens=overlap_tokens,
        )
        _CHUNKER_CACHE[cache_key] = chunker
    return chunker


class LakehousePipeline:
    def __init__(self, spark: SparkSession, config: PlatformConfig) -> None:
        self.spark = spark
        self.config = config

    def run_bronze_ingestion(self) -> Dict[str, int]:
        self._ensure_schema()

        table_name = self.config.table("bronze_documents")

        # ── Setup fresh crawl ───────────────────────────────────────────
        try:
            self.spark.sql(f"TRUNCATE TABLE {table_name}")
            print(f"[bronze] truncated {table_name} for a fresh crawl session.")
        except Exception:
            pass

        # ── Load auxiliary HF configs into memory for joining ───────────
        metadata_map, relations_map = _load_hf_auxiliary_configs(
            dataset_name=self.config.hf_dataset_name,
            cache_dir_raw=self.config.hf_cache_dir,
        )
        print(
            f"[bronze] loaded {len(metadata_map)} metadata rows, "
            f"{len(relations_map)} relation groups"
        )

        # ── Stream the content config and join ──────────────────────────
        source_rows = _iter_hf_source_rows(
            dataset_name=self.config.hf_dataset_name,
            dataset_config=self.config.hf_dataset_config,
            dataset_split=self.config.hf_split,
            cache_dir_raw=self.config.hf_cache_dir,
        )

        written = 0
        batch: List[Dict[str, Any]] = []
        for idx, source_row in enumerate(source_rows):
            if self.config.max_source_records > 0 and written >= self.config.max_source_records:
                break

            # Join metadata + relationships into the content row.
            joined_row = _join_hf_row(
                content_row=source_row,
                metadata_map=metadata_map,
                relations_map=relations_map,
            )

            mapped = self._map_source_record(joined_row, idx)

            # ── Restrict explicitly to Tax ("thuế") domain ──────────────────
            title = str(mapped.get("title") or "").lower()
            doc_type = str(mapped.get("doc_type") or "").lower()
            content_text = str(mapped.get("content_text") or "").lower()
            
            if "thuế" not in title and "thuế" not in doc_type and "thuế" not in content_text:
                continue

            batch.append(mapped)
            written += 1
            
            # Hard-cap 1.5-hour execution (~60,000 chunks = ~2500 docs)
            if written >= 2500:
                break

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
                "raw_record_json",
                "ingest_ts",
            )
            .withColumn(
                "issued_date",
                F.expr("try_cast(nullif(trim(issued_date), '') as date)"),
            )
            .withColumn("status", F.coalesce(F.col("status"), F.lit("active")))
            .withColumn(
                "silver_dedupe_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(F.col("doc_id"), F.lit("")),
                        F.coalesce(F.col("doc_number"), F.lit("")),
                        F.coalesce(F.col("doc_title"), F.lit("")),
                        F.coalesce(F.col("content_text"), F.lit("")),
                        F.coalesce(F.col("raw_chunks_json"), F.lit("")),
                        F.coalesce(F.col("raw_record_json"), F.lit("")),
                    ),
                    256,
                ),
            )
            .dropDuplicates(["silver_dedupe_key"])
            .drop("silver_dedupe_key")
        )

        silver_docs_output_df = silver_docs_df.drop("raw_record_json")
        silver_docs_output_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_documents")
        )

        chunk_source_df = silver_docs_df.select(
            "doc_id",
            "doc_number",
            "doc_title",
            "content_text",
            "raw_chunks_json",
            "relations_json",
            F.get_json_object(F.col("raw_record_json"), "$.content").alias("raw_content"),
            F.get_json_object(F.col("raw_record_json"), "$.text").alias("raw_text"),
            F.get_json_object(F.col("raw_record_json"), "$.body").alias("raw_body"),
            F.get_json_object(F.col("raw_record_json"), "$.noi_dung").alias("raw_noi_dung"),
            F.get_json_object(F.col("raw_record_json"), "$.full_text").alias("raw_full_text"),
            F.get_json_object(F.col("raw_record_json"), "$.content_html").alias("raw_content_html"),
        )

        chunk_udf_schema = ArrayType(_chunk_udf_item_schema())
        target_tokens = self.config.chunk_target_tokens
        max_tokens = self.config.chunk_max_tokens
        overlap_tokens = self.config.chunk_overlap_tokens
        source_dataset = self.config.hf_dataset_name
        source_split = self.config.hf_split

        def _chunk_document_rows(
            doc_id: Any,
            doc_number: Any,
            doc_title: Any,
            content_text: Any,
            raw_chunks_json: Any,
            raw_content: Any,
            raw_text: Any,
            raw_body: Any,
            raw_noi_dung: Any,
            raw_full_text: Any,
            raw_content_html: Any,
        ) -> List[Dict[str, Any]]:
            parsed_chunks = _safe_json_loads(raw_chunks_json, fallback=[])
            if not isinstance(parsed_chunks, list):
                parsed_chunks = []

            fallback_text = str(content_text or "").strip()
            if not fallback_text:
                for candidate in [raw_content, raw_text, raw_body, raw_noi_dung, raw_full_text]:
                    text = str(candidate or "").strip()
                    if text:
                        fallback_text = text
                        break
            if not fallback_text:
                html_value = str(raw_content_html or "").strip()
                if html_value:
                    fallback_text = html_to_text(html_value)

            document_text = LakehousePipeline._build_document_text(
                fallback_text=fallback_text,
                raw_chunks=parsed_chunks,
            )
            if not document_text:
                return []

            chunker = _get_cached_chunker(
                target_tokens=target_tokens,
                max_tokens=max_tokens,
                overlap_tokens=overlap_tokens,
            )

            generated_chunks = chunker.chunk_document(
                doc_id=str(doc_id or "").strip(),
                doc_number=str(doc_number or "").strip(),
                doc_title=str(doc_title or "").strip(),
                raw_text=document_text,
                base_metadata={
                    "source_dataset": source_dataset,
                    "source_split": source_split,
                },
            )

            rows: List[Dict[str, Any]] = []
            for chunk in generated_chunks:
                rows.append(
                    {
                        "chunk_uid": str(chunk.chunk_uid),
                        "doc_id": str(chunk.doc_id),
                        "doc_number": str(chunk.doc_number or ""),
                        "doc_title": str(chunk.doc_title or ""),
                        "chunk_order": int(chunk.chunk_order),
                        "chunk_text": str(chunk.chunk_text),
                        "token_count": int(chunk.token_count),
                        "tokens": [str(token) for token in (chunk.tokens or [])],
                        "chapter": str(chunk.chapter or "") or None,
                        "section": str(chunk.section or "") or None,
                        "article": str(chunk.article or "") or None,
                        "clause": str(chunk.clause or "") or None,
                        "point": str(chunk.point or "") or None,
                        "references_to": [
                            str(value).strip()
                            for value in (chunk.references_to or [])
                            if str(value or "").strip()
                        ],
                        "metadata_json": _json_dumps(chunk.metadata),
                    }
                )
            return rows

        chunk_rows_df = chunk_source_df.withColumn(
            "chunk_rows",
            F.udf(_chunk_document_rows, chunk_udf_schema)(
                F.col("doc_id"),
                F.col("doc_number"),
                F.col("doc_title"),
                F.col("content_text"),
                F.col("raw_chunks_json"),
                F.col("raw_content"),
                F.col("raw_text"),
                F.col("raw_body"),
                F.col("raw_noi_dung"),
                F.col("raw_full_text"),
                F.col("raw_content_html"),
            ),
        )

        silver_chunks_df = (
            chunk_rows_df.select(F.explode_outer(F.col("chunk_rows")).alias("chunk"))
            .where(F.col("chunk").isNotNull())
            .select(
                F.col("chunk.chunk_uid").alias("chunk_uid"),
                F.col("chunk.doc_id").alias("doc_id"),
                F.col("chunk.doc_number").alias("doc_number"),
                F.col("chunk.doc_title").alias("doc_title"),
                F.col("chunk.chunk_order").alias("chunk_order"),
                F.col("chunk.chunk_text").alias("chunk_text"),
                F.col("chunk.token_count").alias("token_count"),
                F.col("chunk.tokens").alias("tokens"),
                F.col("chunk.chapter").alias("chapter"),
                F.col("chunk.section").alias("section"),
                F.col("chunk.article").alias("article"),
                F.col("chunk.clause").alias("clause"),
                F.col("chunk.point").alias("point"),
                F.col("chunk.references_to").alias("references_to"),
                F.col("chunk.metadata_json").alias("metadata_json"),
                F.current_timestamp().alias("transform_ts"),
            )
        )
        silver_chunks_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_chunks")
        )

        explicit_relations_df = (
            chunk_source_df.select(
                F.col("doc_id").alias("source_doc_id"),
                F.col("doc_number").alias("source_doc_number"),
                F.from_json(F.col("relations_json"), ArrayType(_raw_relation_item_schema())).alias(
                    "relation_items"
                ),
            )
            .select(
                "source_doc_id",
                "source_doc_number",
                F.explode_outer(F.col("relation_items")).alias("relation"),
            )
            .where(F.col("relation").isNotNull())
            .withColumn(
                "target_doc_number",
                F.expr(
                    "coalesce(" 
                    "nullif(trim(relation.target_so_ky_hieu), ''), "
                    "nullif(trim(relation.target_doc_number), ''), "
                    "nullif(trim(relation.target_doc_id), '')"
                    ")"
                ),
            )
            .withColumn(
                "relation_type",
                F.expr(
                    "coalesce(" 
                    "nullif(trim(relation.type), ''), "
                    "nullif(trim(relation.relation_type), ''), "
                    "'REFERS_TO'"
                    ")"
                ),
            )
            .where(F.col("target_doc_number").isNotNull())
            .select(
                "source_doc_id",
                "source_doc_number",
                "target_doc_number",
                "relation_type",
                F.lit("explicit_relation").alias("relation_source"),
            )
        )

        chunk_reference_relations_df = (
            silver_chunks_df.select(
                F.col("doc_id").alias("source_doc_id"),
                F.col("doc_number").alias("source_doc_number"),
                F.explode_outer(F.col("references_to")).alias("target_doc_number"),
            )
            .where(F.length(F.trim(F.coalesce(F.col("target_doc_number"), F.lit("")))) > 0)
            .select(
                "source_doc_id",
                "source_doc_number",
                "target_doc_number",
                F.lit("MENTIONS").alias("relation_type"),
                F.lit("chunk_reference").alias("relation_source"),
            )
        )

        silver_relations_df = chunk_reference_relations_df.unionByName(explicit_relations_df).dropDuplicates(
            [
                "source_doc_id",
                "source_doc_number",
                "target_doc_number",
                "relation_type",
                "relation_source",
            ]
        )
        silver_relations_df.write.mode("overwrite").format("delta").saveAsTable(
            self.config.table("silver_relations")
        )

        return {
            "silver_documents": silver_docs_output_df.count(),
            "silver_chunks": silver_chunks_df.count(),
            "silver_relations": silver_relations_df.count(),
        }

    def run_gold_embeddings_and_sync(self) -> Dict[str, int]:
        self._ensure_schema()

        # ── Count total chunks (cheap operation) ────────────────────────
        silver_chunks_table = self.config.table("silver_chunks")
        total_chunks = self.spark.table(silver_chunks_table).count()
        print(f"[gold] total silver_chunks: {total_chunks}")

        if total_chunks == 0:
            empty_gold = self.spark.createDataFrame([], schema=_gold_schema())
            empty_gold.write.mode("overwrite").format("delta").saveAsTable(
                self.config.table("gold_embeddings")
            )
            return {
                "gold_embeddings": 0,
                "qdrant_points": 0,
                "neo4j_documents": 0,
                "neo4j_chunks": 0,
                "neo4j_relations": 0,
            }

        # ── Validate config ─────────────────────────────────────────────
        sync_to_qdrant = self.config.sync_to_qdrant
        if sync_to_qdrant and not self.config.qdrant_url:
            print("[warn] SYNC_TO_QDRANT=true but QDRANT_URL is empty. Skipping Qdrant sync.")
            sync_to_qdrant = False

        sync_to_neo4j = self.config.sync_to_neo4j
        if sync_to_neo4j and (not self.config.neo4j_uri or not self.config.neo4j_password):
            print(
                "[warn] SYNC_TO_NEO4J=true but NEO4J_URI/NEO4J_PASSWORD is missing. "
                "Skipping Neo4j sync."
            )
            sync_to_neo4j = False

        self.config.validate_for_gold(
            require_embedding_key=True,
            require_qdrant=sync_to_qdrant,
            require_neo4j=sync_to_neo4j,
        )

        # ── Initialize embedding client + sinks (once) ─────────────────
        embed_client = GeminiEmbeddingClient(
            api_key=self.config.gemini_api_key,
            model_name=self.config.gemini_embedding_model,
            rpm_limit=self.config.gemini_rpm_limit,
            tpm_limit=self.config.gemini_tpm_limit,
            safety_factor=self.config.rate_limit_headroom,
            batch_size=self.config.gemini_embed_batch_size,
        )

        qdrant_sink = None
        if sync_to_qdrant:
            qdrant_sink = QdrantSink(
                url=self.config.qdrant_url,
                api_key=self.config.qdrant_api_key,
                collection_name=self.config.qdrant_collection,
                vector_size=self.config.qdrant_vector_size,
                distance=self.config.qdrant_distance,
                batch_size=self.config.qdrant_batch_size,
            )

        # ── Process in batches ──────────────────────────────────────────
        BATCH_SIZE = 1000
        total_embedded = 0
        total_qdrant = 0
        first_batch = True

        silver_df = self.spark.table(silver_chunks_table)
        last_doc_id = ""
        last_chunk_order = -1
        batch_num = 1

        while True:
            if not last_doc_id:
                batch_df = silver_df.orderBy("doc_id", "chunk_order").limit(BATCH_SIZE)
            else:
                batch_df = (
                    silver_df.where(
                        (F.col("doc_id") > F.lit(last_doc_id)) |
                        ((F.col("doc_id") == F.lit(last_doc_id)) & (F.col("chunk_order") > F.lit(last_chunk_order)))
                    )
                    .orderBy("doc_id", "chunk_order")
                    .limit(BATCH_SIZE)
                )

            chunk_rows = [row.asDict(recursive=True) for row in batch_df.collect()]
            if not chunk_rows:
                break
            
            last_doc_id = chunk_rows[-1]["doc_id"]
            last_chunk_order = chunk_rows[-1]["chunk_order"]
            batch_start = (batch_num - 1) * BATCH_SIZE

            print(
                f"[gold] batch {batch_num}: {len(chunk_rows)} chunks "
                f"({batch_start + 1}–{batch_start + len(chunk_rows)} of {total_chunks})"
            )

            # ── Embed ───────────────────────────────────────────────────
            texts = [str(row["chunk_text"]) for row in chunk_rows]
            token_counts = [int(row.get("token_count") or 1) for row in chunk_rows]
            vectors = embed_client.embed_texts(texts=texts, token_counts=token_counts)

            if not vectors:
                print(f"[gold] batch {batch_num}: embedding failed, skipping")
                continue

            # ── Update qdrant_vector_size on first batch ────────────────
            if first_batch and vectors and qdrant_sink:
                actual_dim = len(vectors[0])
                if self.config.qdrant_vector_size != actual_dim:
                    print(
                        f"[warn] QDRANT_VECTOR_SIZE={self.config.qdrant_vector_size} "
                        f"!= Gemini output={actual_dim}. Re-creating sink."
                    )
                    qdrant_sink = QdrantSink(
                        url=self.config.qdrant_url,
                        api_key=self.config.qdrant_api_key,
                        collection_name=self.config.qdrant_collection,
                        vector_size=actual_dim,
                        distance=self.config.qdrant_distance,
                        batch_size=self.config.qdrant_batch_size,
                    )

            # ── Build gold rows ─────────────────────────────────────────
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
                        "embedding": [float(v) for v in vector],
                        "embedded_at": datetime.now(timezone.utc),
                    }
                )

            # ── Write gold batch to Delta ───────────────────────────────
            write_mode = "overwrite" if first_batch else "append"
            gold_df = self.spark.createDataFrame(gold_rows, schema=_gold_schema())
            gold_df.write.mode(write_mode).format("delta").saveAsTable(
                self.config.table("gold_embeddings")
            )
            total_embedded += len(gold_rows)

            # ── Sync to Qdrant ──────────────────────────────────────────
            if qdrant_sink and gold_rows:
                total_qdrant += qdrant_sink.upsert_embeddings(gold_rows)

            first_batch = False
            print(f"[gold] batch {batch_num}: done ({total_embedded} total embedded)")
            batch_num += 1

        # ── Neo4j sync (docs + relations are small, OK to collect) ──────
        sync_stats = {
            "qdrant_points": total_qdrant,
            "neo4j_documents": 0,
            "neo4j_chunks": 0,
            "neo4j_relations": 0,
        }

        if sync_to_neo4j:
            docs_df = self.spark.table(self.config.table("silver_documents"))
            relations_df = self.spark.table(self.config.table("silver_relations"))

            docs_rows = [row.asDict(recursive=True) for row in docs_df.toLocalIterator()]
            relation_rows = [row.asDict(recursive=True) for row in relations_df.toLocalIterator()]

            # Read gold chunk metadata (without embeddings) for Neo4j
            gold_meta_df = self.spark.table(self.config.table("gold_embeddings")).select(
                "chunk_uid", "doc_id", "doc_number", "chunk_order",
                "token_count", "chapter", "section", "article",
                "clause", "point", "chunk_text", "metadata_json",
            )
            gold_meta_rows = [row.asDict(recursive=True) for row in gold_meta_df.toLocalIterator()]

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
                        for row in gold_meta_rows
                    ],
                    relations=relation_rows,
                )
            finally:
                neo4j_sink.close()

            sync_stats["neo4j_documents"] = result.get("documents", 0)
            sync_stats["neo4j_chunks"] = result.get("chunks", 0)
            sync_stats["neo4j_relations"] = result.get("relations", 0)

        print(f"[gold] completed: {total_embedded} embeddings, {total_qdrant} qdrant points")

        return {
            "gold_embeddings": total_embedded,
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

    def _write_silver_chunk_batch(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        mode: str,
    ) -> None:
        if not rows:
            return
        chunk_df = self.spark.createDataFrame(rows, schema=_chunk_schema())
        chunk_df.write.mode(mode).format("delta").saveAsTable(table_name)

    def _write_silver_relation_batch(
        self,
        table_name: str,
        rows: List[Dict[str, Any]],
        mode: str,
    ) -> None:
        if not rows:
            return
        relation_df = self.spark.createDataFrame(_dedupe_relation_rows(rows), schema=_relation_schema())
        relation_df.write.mode(mode).format("delta").saveAsTable(table_name)

    def _map_source_record(self, row: Dict[str, Any], index: int) -> Dict[str, Any]:
        doc_number = _pick_first(row, ["so_ky_hieu", "doc_number", "so_hieu"], default="")
        title = _pick_first(row, ["title", "ten_van_ban"], default="")
        doc_type = _pick_first(row, ["loai_van_ban", "doc_type"], default="")
        issuing_body = _pick_first(row, ["co_quan_ban_hanh", "issuing_body"], default="")
        issued_date = _pick_first(row, ["ngay_ban_hanh", "issued_date"], default="")
        status = _pick_first(row, ["tinh_trang_hieu_luc", "status"], default="active")

        # ── Extract content text ────────────────────────────────────────
        # Priority: content_text > content/text/body > content_html > joining chunk contents
        content_text = str(row.get("content_text") or "").strip()

        if not content_text:
            content_text = str(_pick_first(row, ["content", "text", "body"], default="")).strip()

        if not content_text:
            raw_html = row.get("content_html")
            if raw_html:
                content_text = html_to_text(str(raw_html))

        chunks = row.get("chunks") or []
        if not content_text and isinstance(chunks, list) and chunks:
            content_text = "\n".join(
                str(
                    chunk.get("content")
                    or chunk.get("content_text")
                    or chunk.get("text")
                    or ""
                ).strip()
                for chunk in chunks
                if isinstance(chunk, dict)
                and (chunk.get("content") or chunk.get("content_text") or chunk.get("text"))
            )

        provided_doc_id = str(_pick_first(row, ["doc_id", "id"], default="")).strip()
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
            content = str(
                chunk.get("content") or chunk.get("content_text") or chunk.get("text") or ""
            ).strip()

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

    @staticmethod
    def _extract_fallback_text(record: Dict[str, Any]) -> str:
        direct_text = str(record.get("content_text") or "").strip()
        if direct_text:
            return direct_text

        payload_text = str(
            _pick_first(
                record,
                ["raw_content", "raw_text", "raw_body", "raw_noi_dung", "raw_full_text"],
                default="",
            )
            or ""
        ).strip()
        if payload_text:
            return payload_text

        payload_html = str(record.get("raw_content_html") or "").strip()
        if payload_html:
            return html_to_text(payload_html)

        return ""


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


def _is_writable_directory(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        probe = Path(path) / ".hf_write_probe"
        with probe.open("w", encoding="utf-8") as file:
            file.write("ok")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _prepare_hf_cache_dir(raw_cache_dir: str) -> Optional[str]:
    candidates = [
        str(raw_cache_dir or "").strip(),
        "/dbfs/tmp/.hf.data.cache",
        "/local_disk0/.hf.data.cache",
        "/local_disk0/tmp/.hf.data.cache",
        "/databricks/driver/.hf.data.cache",
        str(Path.cwd() / ".hf.data.cache"),
    ]

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)

        try:
            if not _is_writable_directory(candidate):
                continue
            return candidate
        except OSError:
            continue

    raise PermissionError(
        "Unable to find a writable HuggingFace cache directory. "
        "Set HF_DATASET_CACHE_DIR to a writable path (for Databricks, prefer /local_disk0/.hf.data.cache)."
    )


def _load_hf_streaming_dataset(
    dataset_name: str,
    dataset_config: str,
    dataset_split: str,
    cache_dir_raw: str,
    use_large_string_features: bool = False,
):
    cache_dir = _prepare_hf_cache_dir(cache_dir_raw)
    _configure_hf_runtime(cache_dir)

    split_candidates = _dedupe_candidates([dataset_split, "data", "train"])
    config_candidates = _dedupe_candidates([dataset_config, "content", ""])

    retryable_errors: List[str] = []
    base_kwargs: Dict[str, Any] = {
        "streaming": True,
    }
    if cache_dir:
        base_kwargs["cache_dir"] = cache_dir

    for config_name in config_candidates:
        for split_name in split_candidates:
            try:
                kwargs = dict(base_kwargs)
                kwargs["split"] = split_name
                if use_large_string_features:
                    large_features = _build_large_string_features(
                        dataset_name=dataset_name,
                        dataset_config=config_name,
                        cache_dir=cache_dir,
                    )
                    if large_features is not None:
                        kwargs["features"] = large_features

                if config_name:
                    return load_dataset(dataset_name, config_name, **kwargs)
                return load_dataset(dataset_name, **kwargs)
            except ValueError as error:
                if _is_hf_value_error_retryable(error):
                    retryable_errors.append(str(error))
                    continue
                raise

    if retryable_errors:
        raise ValueError(
            "Unable to resolve HuggingFace dataset config/split. "
            f"Attempted configs={config_candidates}, splits={split_candidates}. "
            f"Last error: {retryable_errors[-1]}"
        )

    raise RuntimeError(
        f"Unable to load dataset '{dataset_name}' with provided config and split candidates"
    )


def _iter_hf_source_rows(
    dataset_name: str,
    dataset_config: str,
    dataset_split: str,
    cache_dir_raw: str,
):
    dataset = _load_hf_streaming_dataset(
        dataset_name=dataset_name,
        dataset_config=dataset_config,
        dataset_split=dataset_split,
        cache_dir_raw=cache_dir_raw,
        use_large_string_features=True,
    )

    for row in dataset:
        if isinstance(row, dict):
            yield row


def _load_hf_auxiliary_configs(
    dataset_name: str,
    cache_dir_raw: str,
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Load ``metadata`` and ``relationships`` HF configs into memory dicts.

    Returns:
        metadata_map:  {str(id) -> {field: value, ...}}
        relations_map: {str(doc_id) -> [{other_doc_id, relationship}, ...]}
    """
    metadata_map: Dict[str, Dict[str, Any]] = {}
    relations_map: Dict[str, List[Dict[str, Any]]] = {}

    # ── metadata ────────────────────────────────────────────────────────
    try:
        meta_ds = _load_hf_streaming_dataset(
            dataset_name=dataset_name,
            dataset_config="metadata",
            dataset_split="data",
            cache_dir_raw=cache_dir_raw,
            use_large_string_features=True,
        )
        for row in meta_ds:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("id", "")).strip()
            if doc_id:
                metadata_map[doc_id] = row
    except Exception as exc:
        print(f"[warn] Failed to load metadata config: {exc}")

    # ── relationships ───────────────────────────────────────────────────
    try:
        rels_ds = _load_hf_streaming_dataset(
            dataset_name=dataset_name,
            dataset_config="relationships",
            dataset_split="data",
            cache_dir_raw=cache_dir_raw,
            use_large_string_features=False,
        )
        for row in rels_ds:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("doc_id", "")).strip()
            if doc_id:
                relations_map.setdefault(doc_id, []).append(row)
    except Exception as exc:
        print(f"[warn] Failed to load relationships config: {exc}")

    return metadata_map, relations_map


def _join_hf_row(
    content_row: Dict[str, Any],
    metadata_map: Dict[str, Dict[str, Any]],
    relations_map: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Merge a ``content`` row with matching ``metadata`` and ``relationships``."""
    doc_id = str(content_row.get("id", "")).strip()
    joined: Dict[str, Any] = dict(content_row)

    # Merge metadata fields (title, so_ky_hieu, co_quan_ban_hanh, etc.)
    meta = metadata_map.get(doc_id)
    if meta:
        for key, value in meta.items():
            if key == "id":
                continue  # already present as content_row["id"]
            if key not in joined or joined[key] is None:
                joined[key] = value

    # Attach relationships as "relations" list
    rels = relations_map.get(doc_id, [])
    joined_relations: List[Dict[str, Any]] = []
    for rel in rels:
        target = str(rel.get("other_doc_id", "")).strip()
        rel_type = str(rel.get("relationship", "REFERS_TO")).strip()
        if target:
            # Try to resolve target's so_ky_hieu from metadata
            target_meta = metadata_map.get(target)
            target_doc_number = (
                str(target_meta.get("so_ky_hieu", "")).strip()
                if target_meta
                else ""
            )
            joined_relations.append({
                "target_doc_id": target,
                "target_so_ky_hieu": target_doc_number,
                "type": rel_type,
            })
    if joined_relations:
        joined["relations"] = joined_relations

    return joined


def _configure_hf_runtime(cache_dir: Optional[str]) -> None:
    if cache_dir:
        os.environ["HF_DATASETS_CACHE"] = cache_dir
        os.environ["HF_HOME"] = cache_dir
        os.environ["XDG_CACHE_HOME"] = cache_dir
        os.environ["DATASETS_CACHE"] = cache_dir

        hub_cache = str(Path(cache_dir) / "hub")
        os.makedirs(hub_cache, exist_ok=True)
        os.environ["HF_HUB_CACHE"] = hub_cache
        os.environ["HUGGINGFACE_HUB_CACHE"] = hub_cache

        downloads_cache = str(Path(cache_dir) / "downloads")
        modules_cache = str(Path(cache_dir) / "modules")
        os.makedirs(downloads_cache, exist_ok=True)
        os.makedirs(modules_cache, exist_ok=True)

        # datasets reads some cache constants at import time, so override them at runtime too.
        try:
            import datasets as _datasets

            if hasattr(_datasets, "config"):
                if hasattr(_datasets.config, "HF_DATASETS_CACHE"):
                    _datasets.config.HF_DATASETS_CACHE = cache_dir
                if hasattr(_datasets.config, "DOWNLOADED_DATASETS_PATH"):
                    _datasets.config.DOWNLOADED_DATASETS_PATH = downloads_cache
                if hasattr(_datasets.config, "HF_DATASETS_DOWNLOADED_DATASETS_PATH"):
                    _datasets.config.HF_DATASETS_DOWNLOADED_DATASETS_PATH = downloads_cache
                if hasattr(_datasets.config, "HF_MODULES_CACHE"):
                    _datasets.config.HF_MODULES_CACHE = modules_cache
        except Exception:
            pass

        try:
            from huggingface_hub import constants as _hf_constants

            if hasattr(_hf_constants, "HF_HUB_CACHE"):
                _hf_constants.HF_HUB_CACHE = hub_cache
            if hasattr(_hf_constants, "HUGGINGFACE_HUB_CACHE"):
                _hf_constants.HUGGINGFACE_HUB_CACHE = hub_cache
        except Exception:
            pass

    temp_candidates = [
        str(Path(cache_dir) / "tmp") if cache_dir else "",
        "/local_disk0/tmp",
        "/databricks/driver/tmp",
        str(Path.cwd() / ".tmp"),
    ]

    for temp_dir in temp_candidates:
        if not temp_dir:
            continue
        try:
            if not _is_writable_directory(temp_dir):
                continue
            os.environ["TMPDIR"] = temp_dir
            os.environ["TMP"] = temp_dir
            os.environ["TEMP"] = temp_dir
            return
        except OSError:
            continue


def _dedupe_candidates(values: Iterable[str]) -> List[str]:
    output: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _is_hf_value_error_retryable(error: ValueError) -> bool:
    message = str(error)
    retry_markers = [
        "Config name is missing",
        "Please pick one among the available configs",
        "Bad split:",
        "Available splits:",
    ]
    return any(marker in message for marker in retry_markers)


def _is_hf_large_string_cast_error(error: Exception) -> bool:
    message = str(error)
    markers = [
        "large_string to string",
        "input array too large",
        "ArrowInvalid",
    ]
    return any(marker in message for marker in markers)


def _build_large_string_features(
    dataset_name: str,
    dataset_config: str,
    cache_dir: Optional[str],
):
    try:
        from datasets import Features, Sequence, Value, load_dataset_builder
    except Exception:
        return None

    builder_kwargs: Dict[str, Any] = {}
    if cache_dir:
        builder_kwargs["cache_dir"] = cache_dir

    try:
        if dataset_config:
            builder = load_dataset_builder(dataset_name, dataset_config, **builder_kwargs)
        else:
            builder = load_dataset_builder(dataset_name, **builder_kwargs)
    except Exception:
        return None

    source_features = getattr(builder.info, "features", None)
    if source_features is None:
        return None

    def promote(feature: Any) -> Any:
        if isinstance(feature, Value) and getattr(feature, "dtype", "") == "string":
            return Value("large_string")
        if isinstance(feature, Sequence):
            return Sequence(
                feature=promote(feature.feature),
                length=feature.length,
                id=feature.id,
            )
        if isinstance(feature, dict):
            return {key: promote(value) for key, value in feature.items()}
        if isinstance(feature, list):
            return [promote(value) for value in feature]
        return feature

    promoted_mapping = {
        key: promote(value)
        for key, value in source_features.items()
    }
    return Features(promoted_mapping)


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


def _chunk_udf_item_schema() -> StructType:
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


def _raw_relation_item_schema() -> StructType:
    return StructType(
        [
            StructField("target_so_ky_hieu", StringType(), True),
            StructField("target_doc_number", StringType(), True),
            StructField("target_doc_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("relation_type", StringType(), True),
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
