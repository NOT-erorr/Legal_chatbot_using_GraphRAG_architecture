from __future__ import annotations


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "services" in payload
    assert payload["services"]["langgraph"] == "up"


def test_chat_happy_path(client):
    body = {
        "question": "Doanh nghiep co bat buoc dong BHXH cho lao dong thu viec khong?",
        "top_k_vector": 5,
        "top_k_graph": 3,
    }
    response = client.post("/api/v1/chat", json=body)
    assert response.status_code == 200

    payload = response.json()
    assert "answer" in payload
    assert payload["answer"].startswith("Mocked answer for:")
    assert isinstance(payload["citations"], list)
    assert "trace" in payload
