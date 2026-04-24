from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

# Ensure repo root is importable when pytest is launched
# from different working directories or shells.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def _base_env() -> None:
    os.environ.setdefault("GEMINI_API_KEY", "test-key")
    os.environ.setdefault("QDRANT_URL", "http://localhost:6333")
    os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    import orchestrator.app as app_module

    class _DummyNeo4j:
        def close(self) -> None:
            return None

    class _DummyGraph:
        _neo4j = _DummyNeo4j()

    def _mock_build_graph(_cfg: Any) -> Any:
        return _DummyGraph()

    def _mock_invoke_graph(**kwargs: Dict[str, Any]) -> Dict[str, Any]:
        question = kwargs.get("question", "")
        return {
            "answer": f"Mocked answer for: {question}",
            "citations": [{"doc_id": "doc-1", "doc_title": "Mock Law"}],
            "trace": {"wall_clock_ms": 123.4, "vector_hits": 1, "model": "mock-llm"},
            "disclaimer": "Mock disclaimer",
        }

    monkeypatch.setattr(app_module, "build_graph", _mock_build_graph)
    monkeypatch.setattr(app_module, "invoke_graph", _mock_invoke_graph)
    monkeypatch.setattr(app_module, "_init_persistence", lambda _cfg: None)
    monkeypatch.setattr(app_module, "_init_redis", lambda _cfg: None)

    with TestClient(app_module.app) as test_client:
        yield test_client
