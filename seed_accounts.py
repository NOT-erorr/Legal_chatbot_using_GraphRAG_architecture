"""
seed_accounts.py — Nạp sẵn 2 tài khoản vào PostgreSQL cho hệ thống GraphRAG Legal.

  1. ADMIN  — role='admin', question_limit = NULL  (KHÔNG giới hạn câu hỏi)
  2. USER   — role='user',  question_limit = 50    (giới hạn 50 câu hỏi)

Script idempotent: chạy nhiều lần không tạo trùng (upsert theo email).
Tự thêm cột `question_limit` vào bảng users nếu DB cũ chưa có (NULL = unlimited).

Yêu cầu:
  - PostgreSQL đang chạy:  docker compose up -d postgres
  - Cấu hình kết nối lấy từ orchestrator/.env (POSTGRES_HOST/PORT/DB/USER/PASSWORD)

Cách chạy (từ thư mục gốc D:\\GraphRAG_Legal):
  python seed_accounts.py
  python seed_accounts.py --admin-email boss@acme.vn --admin-password secret
  python seed_accounts.py --user-limit 100
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from typing import Optional

from passlib.context import CryptContext

from orchestrator.config import OrchestratorConfig
from orchestrator.persistence import ChatPersistence

# bcrypt — chuẩn công nghiệp, hash tương thích với mọi runtime có passlib
# (local venv + Docker image, xem orchestrator/requirements.txt).
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Hash mật khẩu bằng bcrypt (passlib). Trả về chuỗi '$2b$...' tự chứa salt."""
    return _pwd_context.hash(plain)


def ensure_question_limit_column(db: ChatPersistence) -> None:
    """Thêm cột question_limit vào bảng users nếu DB cũ chưa có (idempotent)."""
    with db._cursor() as cur:
        cur.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS question_limit INTEGER"
        )


def upsert_account(
    db: ChatPersistence,
    email: str,
    full_name: str,
    password: str,
    role: str,
    question_limit: Optional[int],
) -> str:
    """
    Tạo mới hoặc cập nhật 1 account theo email. Trả về user_id.
    question_limit = None nghĩa là không giới hạn (unlimited).
    """
    user_id = uuid.uuid4()
    with db._cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (id, email, full_name, hashed_password, role, question_limit)
            VALUES (%s, %s, %s, %s, %s::user_role, %s)
            ON CONFLICT (email) DO UPDATE SET
                full_name       = EXCLUDED.full_name,
                hashed_password = EXCLUDED.hashed_password,
                role            = EXCLUDED.role,
                question_limit  = EXCLUDED.question_limit,
                is_active       = TRUE
            RETURNING id
            """,
            (user_id, email, full_name, hash_password(password), role, question_limit),
        )
        row = cur.fetchone()
        return str(row["id"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed admin + user accounts cho GraphRAG Legal.")
    # Mật khẩu ưu tiên lấy từ env var (an toàn khi deploy — không lộ trong lệnh/CI log),
    # fallback về default cho môi trường dev.
    parser.add_argument("--admin-email", default=os.getenv("ADMIN_EMAIL", "admin@legal-chatbot.local"))
    parser.add_argument("--admin-password", default=os.getenv("ADMIN_PASSWORD", "admin123"))
    parser.add_argument("--user-email", default=os.getenv("USER_EMAIL", "user@legal-chatbot.local"))
    parser.add_argument("--user-password", default=os.getenv("USER_PASSWORD", "user123"))
    parser.add_argument("--user-limit", type=int, default=50, help="Giới hạn câu hỏi của user (mặc định 50)")
    args = parser.parse_args()

    cfg = OrchestratorConfig.from_env()
    print("Seed accounts → PostgreSQL")
    print(f"  Target: {cfg.pg_host}:{cfg.pg_port}/{cfg.pg_database}\n")

    try:
        db = ChatPersistence(cfg)
        health = db.health_check()
        if health.get("status") != "up":
            print(f"  [LỖI] Không kết nối được PostgreSQL: {health.get('error')}")
            print("        Hãy chạy: docker compose up -d postgres")
            return 1

        ensure_question_limit_column(db)

        admin_id = upsert_account(
            db,
            email=args.admin_email,
            full_name="Administrator",
            password=args.admin_password,
            role="admin",
            question_limit=None,  # unlimited
        )
        print(f"  [OK] ADMIN  {args.admin_email}")
        print(f"           id={admin_id}  role=admin  question_limit=UNLIMITED")
        print(f"           password={args.admin_password}\n")

        user_id = upsert_account(
            db,
            email=args.user_email,
            full_name="Standard User",
            password=args.user_password,
            role="user",
            question_limit=args.user_limit,
        )
        print(f"  [OK] USER   {args.user_email}")
        print(f"           id={user_id}  role=user  question_limit={args.user_limit}")
        print(f"           password={args.user_password}\n")

        db.close()
        print("Hoàn tất. (Chạy lại an toàn — upsert theo email, không tạo trùng.)")
        return 0
    except Exception as exc:
        print(f"  [LỖI] {exc!r}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
