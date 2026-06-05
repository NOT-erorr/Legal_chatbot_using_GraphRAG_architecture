"""
orchestrator/auth.py — Tiện ích xác thực: hash mật khẩu + xác minh Google ID token.

  - Hash/verify mật khẩu bằng bcrypt (passlib) — cùng scheme với seed_accounts.py,
    nên hash tạo ở đây tương thích với tài khoản seed sẵn.
  - verify_google_token(): xác minh ID token (JWT) do Google Identity Services phát hành
    ở phía frontend, trả về thông tin tài khoản (email, name, sub, ...).
"""

from __future__ import annotations

import time
from typing import Any, Dict

from passlib.context import CryptContext

# bcrypt — đồng bộ với seed_accounts.py (xem orchestrator/requirements.txt).
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    """Hash mật khẩu bằng bcrypt. Trả về chuỗi '$2b$...' tự chứa salt."""
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """So khớp mật khẩu thuần với hash đã lưu."""
    try:
        return _pwd_context.verify(plain, hashed)
    except Exception:
        return False


# ── JWT access token ───────────────────────────────────────────────────────

def create_access_token(
    claims: Dict[str, Any],
    secret: str,
    expire_minutes: int,
    algorithm: str = "HS256",
) -> str:
    """
    Phát access token (JWT). ``claims`` nên chứa 'sub' (user_id).
    Tự thêm 'iat' và 'exp'. Trả về chuỗi token.
    """
    import jwt  # PyJWT

    now = int(time.time())
    payload = {**claims, "iat": now, "exp": now + expire_minutes * 60}
    return jwt.encode(payload, secret, algorithm=algorithm)


def decode_access_token(
    token: str, secret: str, algorithm: str = "HS256"
) -> Dict[str, Any]:
    """
    Giải mã + xác minh access token. Raise jwt.PyJWTError nếu token sai/hết hạn.
    """
    import jwt  # PyJWT

    return jwt.decode(token, secret, algorithms=[algorithm])


def verify_google_token(credential: str, client_id: str) -> Dict[str, Any]:
    """
    Xác minh Google ID token (JWT) phát hành cho ứng dụng có ``client_id``.

    Trả về payload đã xác minh, gồm: email, email_verified, name, picture, sub, ...
    Raise ValueError nếu token không hợp lệ / sai audience / hết hạn.
    """
    try:
        from google.auth.transport import requests as google_requests
        from google.oauth2 import id_token
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "google-auth chưa được cài. Thêm 'google-auth' vào requirements.txt."
        ) from exc

    info = id_token.verify_oauth2_token(
        credential, google_requests.Request(), client_id
    )

    # verify_oauth2_token đã kiểm tra chữ ký, audience (client_id) và hạn dùng.
    # Chỉ chấp nhận issuer chính thức của Google.
    if info.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
        raise ValueError("Issuer không hợp lệ")

    return info
