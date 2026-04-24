from __future__ import annotations

from orchestrator.config import OrchestratorConfig


def test_config_reads_environment(monkeypatch):
    monkeypatch.setenv("GEMINI_CHAT_MODEL", "gemini-test-model")
    monkeypatch.setenv("QDRANT_TOP_K", "9")
    monkeypatch.setenv("NEO4J_MAX_RELATED", "7")
    monkeypatch.setenv("POSTGRES_HOST", "postgres.internal")
    monkeypatch.setenv("ORCHESTRATOR_PORT", "8080")

    cfg = OrchestratorConfig.from_env()

    assert cfg.gemini_chat_model == "gemini-test-model"
    assert cfg.qdrant_top_k == 9
    assert cfg.neo4j_max_related == 7
    assert cfg.pg_host == "postgres.internal"
    assert cfg.app_port == 8080
