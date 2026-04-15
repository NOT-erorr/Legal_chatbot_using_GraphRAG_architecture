"""
GraphRAG Vietnamese Law — Centralized Configuration
Đọc cấu hình từ .env file sử dụng pydantic-settings.
"""

import os
from pathlib import Path
from pydantic_settings import BaseSettings

# Đường dẫn gốc của project
BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Cấu hình toàn bộ hệ thống GraphRAG."""

    # ---- PostgreSQL ----
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "law_graphrag"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres123"

    @property
    def POSTGRES_URL(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # ---- Neo4j ----
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "neo4j123"

    # ---- Redis ----
    REDIS_URL: str = "redis://localhost:6379/0"

    # ---- Embedding ----
    EMBEDDING_MODEL: str = "paraphrase-multilingual-MiniLM-L12-v2"
    EMBEDDING_DIMENSION: int = 384  # MiniLM-L12-v2 output dimension

    # ---- FAISS ----
    FAISS_INDEX_PATH: str = str(BASE_DIR / "data" / "faiss_index")

    # ---- LLM (Gemini) ----
    GEMINI_API_KEY: str = ""
    GEMINI_CHAT_MODEL: str = "gemini-1.5-pro"

    # ---- Retrieval ----
    RETRIEVAL_TOP_K_VECTOR: int = 5
    RETRIEVAL_TOP_K_GRAPH: int = 3

    # ---- App ----
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    APP_ENV: str = "development"

    class Config:
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"


# Singleton instance
settings = Settings()
