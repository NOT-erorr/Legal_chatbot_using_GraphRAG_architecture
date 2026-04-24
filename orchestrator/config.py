
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Load .env nếu có
try:
    from dotenv import load_dotenv

    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw and raw.strip() else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw and raw.strip() else default


@dataclass(frozen=True)
class OrchestratorConfig:
    """Tất cả cấu hình cho Orchestrator Service."""

    # ── Gemini LLM ──────────────────────────────────────────────────────
    gemini_api_key: str = ""
    gemini_chat_model: str = "gemini-2.5-flash"
    gemini_temperature: float = 0.1
    gemini_max_output_tokens: int = 2048

    # ── Gemini Embedding (để tạo query vector) ──────────────────────────
    gemini_embedding_model: str = "gemini-embedding-001"

    # ── Qdrant ──────────────────────────────────────────────────────────
    qdrant_url: str = "http://localhost:6333"
    qdrant_api_key: str = ""
    qdrant_collection: str = "vn_legal_chunks"
    qdrant_top_k: int = 5
    qdrant_score_threshold: float = 0.3

    # ── Neo4j ───────────────────────────────────────────────────────────
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""
    neo4j_database: str = "neo4j"
    neo4j_max_depth: int = 2
    neo4j_max_related: int = 5

    # ── PostgreSQL ──────────────────────────────────────────────────────
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "legal_chatbot"
    pg_user: str = "postgres"
    pg_password: str = "postgres123"

    # ── Redis ───────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 300  # 5 phút

    # ── App ─────────────────────────────────────────────────────────────
    app_host: str = "0.0.0.0"
    app_port: int = 8001
    log_level: str = "info"

    @classmethod
    def from_env(cls) -> "OrchestratorConfig":
        return cls(
            gemini_api_key=_env("GEMINI_API_KEY"),
            gemini_chat_model=_env("GEMINI_CHAT_MODEL", "gemini-2.5-flash"),
            gemini_temperature=_env_float("GEMINI_TEMPERATURE", 0.1),
            gemini_max_output_tokens=_env_int("GEMINI_MAX_OUTPUT_TOKENS", 2048),
            gemini_embedding_model=_env("GEMINI_EMBEDDING_MODEL", "gemini-embedding-001"),
            qdrant_url=_env("QDRANT_URL", "http://localhost:6333"),
            qdrant_api_key=_env("QDRANT_API_KEY"),
            qdrant_collection=_env("QDRANT_COLLECTION", "vn_legal_chunks"),
            qdrant_top_k=_env_int("QDRANT_TOP_K", 5),
            qdrant_score_threshold=_env_float("QDRANT_SCORE_THRESHOLD", 0.3),
            neo4j_uri=_env("NEO4J_URI", "bolt://localhost:7687"),
            neo4j_user=_env("NEO4J_USER", "neo4j"),
            neo4j_password=_env("NEO4J_PASSWORD"),
            neo4j_database=_env("NEO4J_DATABASE", "neo4j"),
            neo4j_max_depth=_env_int("NEO4J_MAX_DEPTH", 2),
            neo4j_max_related=_env_int("NEO4J_MAX_RELATED", 5),
            pg_host=_env("POSTGRES_HOST", "localhost"),
            pg_port=_env_int("POSTGRES_PORT", 5432),
            pg_database=_env("POSTGRES_DB", "legal_chatbot"),
            pg_user=_env("POSTGRES_USER", "postgres"),
            pg_password=_env("POSTGRES_PASSWORD", "postgres123"),
            redis_url=_env("REDIS_URL", "redis://localhost:6379/0"),
            redis_cache_ttl=_env_int("REDIS_CACHE_TTL", 300),
            app_host=_env("ORCHESTRATOR_HOST", "0.0.0.0"),
            app_port=_env_int("ORCHESTRATOR_PORT", 8001),
            log_level=_env("LOG_LEVEL", "info"),
        )
