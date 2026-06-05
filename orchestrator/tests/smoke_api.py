"""
orchestrator/tests/smoke_api.py — Smoke-test toàn bộ API của Orchestrator (chạy thật).

Khác với test_api.py (unit test, mock mọi thứ), file này:
  1. SEED một conversation giả trực tiếp vào PostgreSQL qua ChatPersistence
     (user message + assistant message có citations + feedback).
  2. Gọi lần lượt các HTTP endpoint của server uvicorn ĐANG CHẠY và kiểm tra
     dữ liệu giả đó đọc ngược lại được.

Yêu cầu trước khi chạy:
  - Server đang chạy:   uvicorn orchestrator.app:app --reload
  - PostgreSQL + Redis đang chạy:  docker compose up -d postgres redis

Cách chạy (từ thư mục gốc D:\\GraphRAG_Legal):
  python -m orchestrator.tests.smoke_api
  python -m orchestrator.tests.smoke_api --url http://localhost:8001
  python -m orchestrator.tests.smoke_api --no-search   # bỏ qua test gọi Qdrant cloud
  python -m orchestrator.tests.smoke_api --cleanup     # xóa conversation giả sau khi test
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Optional

import httpx

from orchestrator.config import OrchestratorConfig

# ── Bộ đếm kết quả ───────────────────────────────────────────────────────────
_PASSED = 0
_FAILED = 0


def _ok(name: str, detail: str = "") -> None:
    global _PASSED
    _PASSED += 1
    print(f"  [PASS] {name}" + (f"  — {detail}" if detail else ""))


def _fail(name: str, detail: str = "") -> None:
    global _FAILED
    _FAILED += 1
    print(f"  [FAIL] {name}" + (f"  — {detail}" if detail else ""))


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


# ── Phase 1: Seed conversation giả vào PostgreSQL ────────────────────────────
def seed_fake_conversation(cfg: OrchestratorConfig) -> Optional[dict[str, str]]:
    """Tạo dữ liệu giả trực tiếp trong DB. Trả về dict các id đã tạo (hoặc None)."""
    _section("PHASE 1 — Seed conversation giả vào PostgreSQL")
    try:
        from orchestrator.persistence import ChatPersistence

        db = ChatPersistence(cfg)
        health = db.health_check()
        if health.get("status") != "up":
            _fail("Kết nối PostgreSQL", health.get("error", "unknown"))
            return None
        _ok("Kết nối PostgreSQL", f"stats={health.get('stats')}")

        user_id = db.get_or_create_guest_user()
        _ok("get_or_create_guest_user", user_id)

        conv_id = db.create_conversation(
            user_id=user_id,
            title="[SEED] Thử nghiệm hỏi đáp luật lao động",
            metadata={"seed": True, "source": "smoke_api"},
        )
        _ok("create_conversation", conv_id)

        user_msg_id = db.save_user_message(
            conversation_id=conv_id,
            content="Người lao động thử việc có được đóng BHXH không?",
        )
        _ok("save_user_message", user_msg_id)

        assistant_msg_id = db.save_assistant_message(
            conversation_id=conv_id,
            content=(
                "Theo Bộ luật Lao động 2019, trong thời gian thử việc, người lao động "
                "và người sử dụng lao động không bắt buộc tham gia BHXH bắt buộc nếu chỉ "
                "ký hợp đồng thử việc riêng (đây là câu trả lời giả phục vụ smoke-test)."
            ),
            citations=[
                {"doc_id": "blld-2019", "doc_title": "Bộ luật Lao động 2019", "article": "Điều 24"},
            ],
            sources=[{"chunk_uid": "seed-chunk-1", "score": 0.91}],
            confidence_score=0.88,
            latency_ms=1234,
        )
        _ok("save_assistant_message", assistant_msg_id)

        db.close()
        return {
            "user_id": user_id,
            "conversation_id": conv_id,
            "user_msg_id": user_msg_id,
            "assistant_msg_id": assistant_msg_id,
        }
    except Exception as exc:
        _fail("Seed conversation", repr(exc))
        return None


# ── Phase 2: Gọi các HTTP endpoint của server đang chạy ──────────────────────
def test_http_api(base_url: str, seed: Optional[dict[str, str]], do_search: bool) -> None:
    _section(f"PHASE 2 — Gọi HTTP API tại {base_url}")
    client = httpx.Client(base_url=base_url, timeout=60.0)

    # /health
    try:
        r = client.get("/health")
        body = r.json()
        if r.status_code == 200 and body.get("status") == "ok":
            svc = body.get("services", {})
            _ok("GET /health", f"langgraph={svc.get('langgraph')} pg={_status_of(svc.get('postgres'))}")
        else:
            _fail("GET /health", f"{r.status_code} {body}")
    except Exception as exc:
        _fail("GET /health", repr(exc))

    # /api/v1/graph/info
    try:
        r = client.get("/api/v1/graph/info")
        if r.status_code == 200 and "workflow" in r.json():
            _ok("GET /api/v1/graph/info", r.json()["workflow"])
        else:
            _fail("GET /api/v1/graph/info", f"{r.status_code}")
    except Exception as exc:
        _fail("GET /api/v1/graph/info", repr(exc))

    # ── Các endpoint cần conversation giả ──
    conv_id = seed.get("conversation_id") if seed else None
    assistant_msg_id = seed.get("assistant_msg_id") if seed else None

    # GET /api/v1/conversations  (conversation giả phải xuất hiện)
    try:
        r = client.get("/api/v1/conversations", params={"limit": 50})
        items = r.json().get("conversations", [])
        ids = {str(c.get("id")) for c in items}
        if r.status_code == 200 and (conv_id is None or conv_id in ids):
            _ok("GET /api/v1/conversations", f"total={len(items)}, seed có mặt={conv_id in ids if conv_id else 'n/a'}")
        else:
            _fail("GET /api/v1/conversations", f"seed id {conv_id} không có trong danh sách")
    except Exception as exc:
        _fail("GET /api/v1/conversations", repr(exc))

    if conv_id:
        # GET /api/v1/conversations/{id}
        try:
            r = client.get(f"/api/v1/conversations/{conv_id}")
            if r.status_code == 200 and str(r.json().get("id")) == conv_id:
                _ok("GET /conversations/{id}", r.json().get("title"))
            else:
                _fail("GET /conversations/{id}", f"{r.status_code}")
        except Exception as exc:
            _fail("GET /conversations/{id}", repr(exc))

        # GET /api/v1/conversations/{id}/messages  (phải có 2 message)
        try:
            r = client.get(f"/api/v1/conversations/{conv_id}/messages")
            msgs = r.json().get("messages", [])
            roles = [m.get("role") for m in msgs]
            if r.status_code == 200 and "user" in roles and "assistant" in roles:
                _ok("GET /conversations/{id}/messages", f"{len(msgs)} message, roles={roles}")
            else:
                _fail("GET /conversations/{id}/messages", f"roles={roles}")
        except Exception as exc:
            _fail("GET /conversations/{id}/messages", repr(exc))

    # POST /api/v1/messages/{id}/feedback
    if assistant_msg_id:
        try:
            r = client.post(
                f"/api/v1/messages/{assistant_msg_id}/feedback",
                json={"feedback_type": "thumbs_up", "rating": 5, "comment": "Hữu ích (seed)"},
            )
            if r.status_code == 200 and r.json().get("status") == "saved":
                _ok("POST /messages/{id}/feedback", r.json().get("feedback_id"))
            else:
                _fail("POST /messages/{id}/feedback", f"{r.status_code} {r.text}")
        except Exception as exc:
            _fail("POST /messages/{id}/feedback", repr(exc))

    # POST /api/v1/conversations  (tạo qua API) + DELETE (archive) để test CRUD
    try:
        r = client.post("/api/v1/conversations", json={"title": "[SEED] tạm để test DELETE"})
        if r.status_code == 200 and r.json().get("conversation_id"):
            tmp_id = r.json()["conversation_id"]
            _ok("POST /api/v1/conversations", tmp_id)
            rd = client.delete(f"/api/v1/conversations/{tmp_id}")
            if rd.status_code == 200 and rd.json().get("status") == "archived":
                _ok("DELETE /conversations/{id}", "archived")
            else:
                _fail("DELETE /conversations/{id}", f"{rd.status_code}")
        else:
            _fail("POST /api/v1/conversations", f"{r.status_code} {r.text}")
    except Exception as exc:
        _fail("POST/DELETE conversation", repr(exc))

    # POST /api/v1/search  (gọi Qdrant cloud thật — có thể bỏ qua bằng --no-search)
    if do_search:
        try:
            r = client.post("/api/v1/search", json={"query": "hợp đồng lao động thử việc", "top_k": 3})
            if r.status_code == 200 and "chunks" in r.json():
                _ok("POST /api/v1/search", f"total={r.json().get('total')}, {r.json().get('elapsed_ms')}ms")
            else:
                _fail("POST /api/v1/search", f"{r.status_code} {r.text[:200]}")
        except Exception as exc:
            _fail("POST /api/v1/search", repr(exc))
    else:
        print("  [SKIP] POST /api/v1/search (--no-search)")

    client.close()


def cleanup_fake_conversation(cfg: OrchestratorConfig, conversation_id: str) -> None:
    """Xóa hẳn conversation giả (CASCADE xóa messages, feedback, retrieval_logs)."""
    _section("CLEANUP — Xóa conversation giả")
    try:
        from orchestrator.persistence import ChatPersistence

        db = ChatPersistence(cfg)
        with db._cursor() as cur:
            cur.execute("DELETE FROM conversations WHERE id = %s", (conversation_id,))
            deleted = cur.rowcount
        db.close()
        if deleted:
            _ok("DELETE conversation giả", f"{conversation_id} (CASCADE)")
        else:
            _fail("DELETE conversation giả", "không tìm thấy id")
    except Exception as exc:
        _fail("Cleanup", repr(exc))


def _status_of(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("status"))
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test API Orchestrator (chạy thật).")
    parser.add_argument("--url", default="http://localhost:8001", help="Base URL của server")
    parser.add_argument("--no-search", action="store_true", help="Bỏ qua POST /search (Qdrant cloud)")
    parser.add_argument("--cleanup", action="store_true", help="Xóa conversation giả sau khi test xong")
    args = parser.parse_args()

    print("Smoke-test Orchestrator API")
    print(f"  Server: {args.url}")

    cfg = OrchestratorConfig.from_env()
    seed = seed_fake_conversation(cfg)
    test_http_api(args.url, seed, do_search=not args.no_search)

    if args.cleanup and seed and seed.get("conversation_id"):
        cleanup_fake_conversation(cfg, seed["conversation_id"])

    _section("KẾT QUẢ")
    total = _PASSED + _FAILED
    print(f"  PASS: {_PASSED}/{total}    FAIL: {_FAILED}/{total}")
    if seed and seed.get("conversation_id"):
        if args.cleanup:
            print("  Conversation giả đã được dọn sạch (--cleanup)")
        else:
            print(f"  Conversation giả đã seed: {seed['conversation_id']} (tiền tố [SEED])")
    return 1 if _FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
