from __future__ import annotations

import os
from dataclasses import dataclass


def _read_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _read_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class PlatformConfig:
    # Lakehouse naming
    catalog: str = "main"
    schema: str = "law_lakehouse"
    bronze_documents_table: str = "bronze_legal_documents"
    silver_documents_table: str = "silver_legal_documents"
    silver_chunks_table: str = "silver_legal_chunks"
    silver_relations_table: str = "silver_legal_relations"
    gold_embeddings_table: str = "gold_legal_chunk_embeddings"

    # Source ingest (HuggingFace)
    hf_dataset_name: str = "th1nhng0/vietnamese-legal-documents"
    hf_split: str = "train"
    source_batch_size: int = 500
    max_source_records: int = 0

    # Transform
    chunk_target_tokens: int = 220
    chunk_max_tokens: int = 300
    chunk_overlap_tokens: int = 30

    # Embedding + rate limits
    gemini_api_key: str = ""
    gemini_embedding_model: str = "gemini-embedding-001"
    gemini_embed_batch_size: int = 32
    gemini_rpm_limit: int = 3000
    gemini_tpm_limit: int = 1_000_000
    rate_limit_headroom: float = 0.85

    # Qdrant sink
    sync_to_qdrant: bool = True
    qdrant_url: str = "http://localhost:6333"
    qdrant_api_key: str = ""
    qdrant_collection: str = "vn_legal_chunks"
    qdrant_vector_size: int = 3072
    qdrant_distance: str = "Cosine"
    qdrant_batch_size: int = 256

    # Neo4j AuraDB sink
    sync_to_neo4j: bool = True
    neo4j_uri: str = ""
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""
    neo4j_database: str = "neo4j"
    neo4j_batch_size: int = 500

    def table(self, logical_name: str) -> str:
        tables = {
            "bronze_documents": self.bronze_documents_table,
            "silver_documents": self.silver_documents_table,
            "silver_chunks": self.silver_chunks_table,
            "silver_relations": self.silver_relations_table,
            "gold_embeddings": self.gold_embeddings_table,
        }
        if logical_name not in tables:
            raise KeyError(f"Unknown logical table name: {logical_name}")
        return f"{self.catalog}.{self.schema}.{tables[logical_name]}"

    def validate_for_gold(self) -> None:
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY must be set for gold embedding stage.")

        if self.sync_to_qdrant and not self.qdrant_url:
            raise ValueError("QDRANT_URL must be set when SYNC_TO_QDRANT=true.")

        if self.sync_to_neo4j:
            if not self.neo4j_uri:
                raise ValueError("NEO4J_URI must be set when SYNC_TO_NEO4J=true.")
            if not self.neo4j_password:
                raise ValueError("NEO4J_PASSWORD must be set when SYNC_TO_NEO4J=true.")

    @classmethod
    def from_env(cls) -> "PlatformConfig":
        return cls(
            catalog=os.getenv("DBX_CATALOG", "main"),
            schema=os.getenv("DBX_SCHEMA", "law_lakehouse"),
            bronze_documents_table=os.getenv(
                "DBX_BRONZE_DOCUMENTS_TABLE", "bronze_legal_documents"
            ),
            silver_documents_table=os.getenv(
                "DBX_SILVER_DOCUMENTS_TABLE", "silver_legal_documents"
            ),
            silver_chunks_table=os.getenv(
                "DBX_SILVER_CHUNKS_TABLE", "silver_legal_chunks"
            ),
            silver_relations_table=os.getenv(
                "DBX_SILVER_RELATIONS_TABLE", "silver_legal_relations"
            ),
            gold_embeddings_table=os.getenv(
                "DBX_GOLD_EMBEDDINGS_TABLE", "gold_legal_chunk_embeddings"
            ),
            hf_dataset_name=os.getenv(
                "HF_DATASET_NAME", "th1nhng0/vietnamese-legal-documents"
            ),
            hf_split=os.getenv("HF_DATASET_SPLIT", "train"),
            source_batch_size=_read_int("SOURCE_BATCH_SIZE", 500),
            max_source_records=_read_int("MAX_SOURCE_RECORDS", 0),
            chunk_target_tokens=_read_int("CHUNK_TARGET_TOKENS", 220),
            chunk_max_tokens=_read_int("CHUNK_MAX_TOKENS", 300),
            chunk_overlap_tokens=_read_int("CHUNK_OVERLAP_TOKENS", 30),
            gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
            gemini_embedding_model=os.getenv(
                "GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"
            ),
            gemini_embed_batch_size=_read_int("GEMINI_EMBED_BATCH_SIZE", 32),
            gemini_rpm_limit=_read_int("GEMINI_RPM_LIMIT", 3000),
            gemini_tpm_limit=_read_int("GEMINI_TPM_LIMIT", 1_000_000),
            rate_limit_headroom=_read_float("RATE_LIMIT_HEADROOM", 0.85),
            sync_to_qdrant=_read_bool("SYNC_TO_QDRANT", True),
            qdrant_url=os.getenv("QDRANT_URL", "http://localhost:6333"),
            qdrant_api_key=os.getenv("QDRANT_API_KEY", ""),
            qdrant_collection=os.getenv("QDRANT_COLLECTION", "vn_legal_chunks"),
            qdrant_vector_size=_read_int("QDRANT_VECTOR_SIZE", 3072),
            qdrant_distance=os.getenv("QDRANT_DISTANCE", "Cosine"),
            qdrant_batch_size=_read_int("QDRANT_BATCH_SIZE", 256),
            sync_to_neo4j=_read_bool("SYNC_TO_NEO4J", True),
            neo4j_uri=os.getenv("NEO4J_URI", ""),
            neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
            neo4j_password=os.getenv("NEO4J_PASSWORD", ""),
            neo4j_database=os.getenv("NEO4J_DATABASE", "neo4j"),
            neo4j_batch_size=_read_int("NEO4J_BATCH_SIZE", 500),
        )
