"""
orchestrator/app.py — FastAPI server cho Orchestrator Service.

Endpoints:
    POST /api/v1/chat           — Hỏi đáp pháp luật (LangGraph full pipeline)
    POST /api/v1/search         — Chỉ tìm kiếm (không gọi LLM)
    GET  /health                — Health check (PG, Redis, Qdrant, Neo4j)
    GET  /api/v1/graph/info     — Thông tin LangGraph workflow

    Conversations:
    POST /api/v1/conversations                  — Tạo conversation mới
    GET  /api/v1/conversations                  — Liệt kê conversations
    GET  /api/v1/conversations/{id}             — Chi tiết conversation
    GET  /api/v1/conversations/{id}/messages    — Lịch sử messages
    DELETE /api/v1/conversations/{id}           — Archive conversation

    Feedback:
    POST /api/v1/messages/{id}/feedback         — Gửi feedback

    Auth:
    POST /api/v1/auth/register                  — Đăng ký bằng email + mật khẩu
    POST /api/v1/auth/login                      — Đăng nhập bằng email + mật khẩu
    POST /api/v1/auth/google                    — Đăng ký / đăng nhập bằng Google account
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from orchestrator.config import OrchestratorConfig
from orchestrator.graph import build_graph, invoke_graph

# Pydantic Models

class ChatRequest(BaseModel):
    """Request body cho /api/v1/chat."""
    question: str = Field(
        ..., description="Câu hỏi pháp luật Việt Nam", min_length=3, max_length=2000
    )
    conversation_id: Optional[str] = Field(None, description="ID cuộc hội thoại (tạo mới nếu None)")
    top_k_vector: int = Field(5, description="Số chunks từ Qdrant", ge=1, le=20)
    top_k_graph: int = Field(5, description="Số docs mở rộng từ Neo4j", ge=0, le=20)


class SearchRequest(BaseModel):
    """Request body cho /api/v1/search."""
    query: str = Field(..., description="Truy vấn tìm kiếm", min_length=3, max_length=2000)
    top_k: int = Field(5, description="Số kết quả trả về", ge=1, le=20)


class ChatResponse(BaseModel):
    """Response body cho /api/v1/chat."""
    answer: str
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    citations: List[Dict[str, str]]
    trace: Dict[str, Any]
    cached: bool = False
    disclaimer: str


class SearchResponse(BaseModel):
    """Response body cho /api/v1/search."""
    chunks: List[Dict[str, Any]]
    total: int
    elapsed_ms: float


class CreateConversationRequest(BaseModel):
    title: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class FeedbackRequest(BaseModel):
    feedback_type: str = Field("thumbs_up", description="thumbs_up | thumbs_down | flag | correction")
    rating: Optional[int] = Field(None, ge=1, le=5)
    comment: Optional[str] = None


class RegisterRequest(BaseModel):
    """Request body cho /api/v1/auth/register (đăng ký email + mật khẩu)."""
    email: str = Field(..., min_length=5, max_length=255, description="Email đăng nhập")
    password: str = Field(..., min_length=6, max_length=128, description="Mật khẩu")
    full_name: Optional[str] = Field(None, max_length=255)


class LoginRequest(BaseModel):
    """Request body cho /api/v1/auth/login (đăng nhập email + mật khẩu)."""
    email: str = Field(..., min_length=5, max_length=255)
    password: str = Field(..., min_length=1, max_length=128)


class GoogleAuthRequest(BaseModel):
    """Request body cho /api/v1/auth/google — ID token (JWT) từ Google Identity Services."""
    credential: str = Field(..., description="Google ID token (JWT) lấy ở frontend")


class AuthResponse(BaseModel):
    """Response chung cho đăng ký / đăng nhập."""
    user_id: str
    email: str
    full_name: Optional[str] = None
    role: str
    question_limit: Optional[int] = None
    is_new: bool = Field(..., description="True nếu tài khoản vừa được tạo")
    access_token: str = Field(..., description="JWT bearer token")
    token_type: str = "bearer"


# Application State

_compiled_graph: Any = None
_config: Optional[OrchestratorConfig] = None
_persistence: Any = None  # ChatPersistence
_mq: Any = None           # RedisMessageQueue
_guest_user_id: Optional[str] = None


def _init_persistence(cfg: OrchestratorConfig) -> Any:
    """Initialize PostgreSQL persistence (graceful if unavailable)."""
    try:
        from orchestrator.persistence import ChatPersistence
        db = ChatPersistence(cfg)
        health = db.health_check()
        if health["status"] == "up":
            print(f"    PostgreSQL: {cfg.pg_host}:{cfg.pg_port}/{cfg.pg_database} ✓")
            return db
        print(f"    PostgreSQL: {health.get('error', 'unknown error')}")
    except Exception as exc:
        print(f"    PostgreSQL unavailable: {exc}")
    return None


def _init_redis(cfg: OrchestratorConfig) -> Any:
    """Initialize Redis message queue (graceful if unavailable)."""
    try:
        from orchestrator.message_queue import RedisMessageQueue
        mq = RedisMessageQueue(cfg)
        health = mq.health_check()
        if health["status"] == "up":
            print(f"    Redis: {cfg.redis_url} ✓")
            return mq
        print(f"    Redis: {health.get('error', 'unknown error')}")
    except Exception as exc:
        print(f"    Redis unavailable: {exc}")
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Khởi tạo LangGraph workflow + PostgreSQL + Redis khi server start."""
    global _compiled_graph, _config, _persistence, _mq, _guest_user_id
    print(" Đang khởi tạo Orchestrator Service (LangGraph)...")

    _config = OrchestratorConfig.from_env()

    if not _config.gemini_api_key:
        print("    GEMINI_API_KEY chưa được set. LLM synthesis sẽ không hoạt động.")

    # ── Build LangGraph ─────────────────────────────────────────────
    _compiled_graph = build_graph(_config)
    print(f"    Qdrant: {_config.qdrant_url}/{_config.qdrant_collection}")
    print(f"    Neo4j:  {_config.neo4j_uri}")
    print(f"    LLM:    {_config.gemini_chat_model}")
    print(f"    LangGraph: retrieve → synthesize → END")

    # ── Init persistence + cache ────────────────────────────────────
    _persistence = _init_persistence(_config)
    _mq = _init_redis(_config)

    # ── Ensure guest user exists ────────────────────────────────────
    if _persistence:
        try:
            _guest_user_id = _persistence.get_or_create_guest_user()
            print(f"    Guest user: {_guest_user_id}")
        except Exception as exc:
            print(f"    Failed to create guest user: {exc}")

    print(" Orchestrator sẵn sàng!")

    yield

    # Cleanup
    if hasattr(_compiled_graph, "_neo4j"):
        _compiled_graph._neo4j.close()
    if _persistence:
        _persistence.close()
    if _mq:
        _mq.close()
    print(" Orchestrator Service đã đóng")


# FastAPI App

app = FastAPI(
    title="GraphRAG Orchestrator — Vietnamese Legal",
    description=(
        "Orchestrator Service sử dụng LangGraph cho hệ thống hỏi đáp pháp luật Việt Nam. "
        "Kết hợp tìm kiếm ngữ nghĩa (Qdrant) và đồ thị tri thức (Neo4j) "
        "với tổng hợp câu trả lời bằng Gemini Flash. "
        "Persistence: PostgreSQL. Message Queue: Redis Streams."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Security — JWT

# auto_error=False: tự xử lý để trả 401 (thay vì 403 mặc định) khi thiếu token.
_bearer_scheme = HTTPBearer(auto_error=False)


def _issue_token(user: Dict[str, Any]) -> str:
    """Phát JWT access token cho 1 user record (DB)."""
    from orchestrator.auth import create_access_token

    return create_access_token(
        {
            "sub": str(user["id"]),
            "email": user["email"],
            "role": str(user["role"]),
        },
        secret=_config.jwt_secret,
        expire_minutes=_config.jwt_expire_minutes,
        algorithm=_config.jwt_algorithm,
    )


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Dict[str, Any]:
    """
    Dependency xác thực: giải mã Bearer token → trả về thông tin user.
    Raise 401 nếu token thiếu / sai / hết hạn.
    """
    if not _config:
        raise HTTPException(status_code=503, detail="Server chưa sẵn sàng")
    if credentials is None:
        raise HTTPException(status_code=401, detail="Thiếu access token")

    from orchestrator.auth import decode_access_token

    try:
        claims = decode_access_token(
            credentials.credentials, _config.jwt_secret, _config.jwt_algorithm
        )
    except Exception:
        raise HTTPException(
            status_code=401, detail="Token không hợp lệ hoặc đã hết hạn"
        )

    user_id = claims.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token thiếu thông tin người dùng")

    return {
        "user_id": user_id,
        "email": claims.get("email"),
        "role": claims.get("role"),
    }


# Health Check

@app.get("/health", tags=["System"])
async def health_check():
    """Kiểm tra trạng thái tất cả services."""
    services = {
        "langgraph": "up" if _compiled_graph else "down",
        "qdrant": _config.qdrant_url if _config else None,
        "neo4j": _config.neo4j_uri if _config else None,
        "llm": _config.gemini_chat_model if _config else None,
    }

    if _persistence:
        services["postgres"] = _persistence.health_check()
    else:
        services["postgres"] = {"status": "not_configured"}

    if _mq:
        services["redis"] = _mq.health_check()
    else:
        services["redis"] = {"status": "not_configured"}

    return {"status": "ok", "services": services}


# Chat — Core endpoint

@app.post("/api/v1/chat", response_model=ChatResponse, tags=["Chat"])
async def chat(
    request: ChatRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """
    Hỏi đáp pháp luật — LangGraph full pipeline với persistence.

    Quy trình:
    1. Check Redis cache (nếu có)
    2. **Retrieve**: Qdrant semantic search + Neo4j graph expansion
    3. **Synthesize**: Gemini Flash sinh câu trả lời với trích dẫn nguồn
    4. Persist: Lưu messages vào PostgreSQL + log retrieval
    5. Cache: Lưu response vào Redis
    6. Publish: Analytics event vào Redis Stream
    """
    if _compiled_graph is None:
        raise HTTPException(status_code=503, detail="Orchestrator chưa sẵn sàng")

    # ── 1. Cache check ──────────────────────────────────────────────
    if _mq:
        cached = _mq.get_cached_response(request.question)
        if cached:
            cached["cached"] = True
            cached["trace"]["cache_hit"] = True
            return cached

    # ── 2. Rate limiting (nếu Redis available) ──────────────────────
    user_id = current_user["user_id"]
    if _mq and not _mq.check_rate_limit(user_id):
        raise HTTPException(status_code=429, detail="Quá nhiều yêu cầu. Vui lòng thử lại sau.")

    # ── 3. Create/get conversation ──────────────────────────────────
    conversation_id = request.conversation_id
    if _persistence:
        # Nếu client gửi conversation_id: chỉ chấp nhận khi hợp lệ và thuộc về user.
        if conversation_id:
            try:
                conv = _persistence.get_conversation(conversation_id)
            except Exception:
                conv = None  # id không phải UUID hợp lệ → coi như chưa có
            if not conv or str(conv.get("user_id")) != str(user_id):
                conversation_id = None  # bỏ qua id lạ/không sở hữu → tạo mới
        if not conversation_id:
            try:
                conversation_id = _persistence.create_conversation(
                    user_id=user_id,
                    title=request.question[:100],
                )
            except Exception as exc:
                print(f"[chat] Failed to create conversation: {exc}")

    # ── 4. Save user message ────────────────────────────────────────
    user_message_id = None
    if _persistence and conversation_id:
        try:
            user_message_id = _persistence.save_user_message(
                conversation_id=conversation_id,
                content=request.question,
            )
        except Exception as exc:
            print(f"[chat] Failed to save user message: {exc}")

    # ── 5. Run LangGraph pipeline ──────────────────────────────────
    try:
        result = invoke_graph(
            compiled_graph=_compiled_graph,
            question=request.question,
            top_k_vector=request.top_k_vector,
            top_k_graph=request.top_k_graph,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi xử lý: {str(exc)}")

    # ── 6. Save assistant message ──────────────────────────────────
    assistant_message_id = None
    trace = result.get("trace", {})
    if _persistence and conversation_id:
        try:
            assistant_message_id = _persistence.save_assistant_message(
                conversation_id=conversation_id,
                content=result["answer"],
                citations=result.get("citations"),
                latency_ms=int(trace.get("wall_clock_ms", 0)),
            )
        except Exception as exc:
            print(f"[chat] Failed to save assistant message: {exc}")

    # ── 7. Log retrieval ────────────────────────────────────────────
    if _persistence and assistant_message_id:
        try:
            _persistence.log_retrieval(
                message_id=assistant_message_id,
                retrieval_type="hybrid",
                query_used=request.question,
                results_meta=[
                    {"chunk_uid": c.get("chunk_uid"), "score": c.get("score"), "doc_id": c.get("doc_id")}
                    for c in (result.get("trace", {}).get("vector_chunks") or [])
                ][:10],
                latency_ms=int(trace.get("retrieve_ms", 0)),
            )
        except Exception as exc:
            print(f"[chat] Failed to log retrieval: {exc}")

    # ── 8. Cache response ──────────────────────────────────────────
    response = {
        "answer": result["answer"],
        "conversation_id": conversation_id,
        "message_id": assistant_message_id,
        "citations": result.get("citations", []),
        "trace": trace,
        "cached": False,
        "disclaimer": result.get("disclaimer", ""),
    }

    if _mq:
        _mq.cache_response(request.question, response, ttl=_config.redis_cache_ttl)

    # ── 9. Publish analytics event ─────────────────────────────────
    if _mq:
        try:
            _mq.publish_analytics_event("chat_completed", {
                "conversation_id": conversation_id,
                "latency_ms": trace.get("wall_clock_ms"),
                "vector_hits": trace.get("vector_hits"),
                "model": trace.get("model"),
            })
        except Exception:
            pass

    return response


# Search — Vector only

@app.post("/api/v1/search", response_model=SearchResponse, tags=["Search"])
async def search(request: SearchRequest):
    """
    Tìm kiếm thuần túy (chỉ Qdrant vector search, không gọi LLM).
    Hữu ích cho debug hoặc xem kết quả tìm kiếm.
    """
    if _config is None:
        raise HTTPException(status_code=503, detail="Orchestrator chưa sẵn sàng")

    try:
        from dataclasses import asdict, is_dataclass

        from orchestrator.retrievers import QdrantRetriever

        t0 = time.perf_counter()
        qdrant = QdrantRetriever(_config)
        chunks = qdrant.search(query=request.query, top_k=request.top_k)
        elapsed = round((time.perf_counter() - t0) * 1000, 1)

        # QdrantRetriever trả về list ChunkResult (dataclass) → convert sang dict
        # cho khớp response_model SearchResponse (chunks: List[Dict]).
        chunks_out = [asdict(c) if is_dataclass(c) else c for c in chunks]

        return {
            "chunks": chunks_out,
            "total": len(chunks_out),
            "elapsed_ms": elapsed,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi tìm kiếm: {str(exc)}")


# Conversations — CRUD

@app.post("/api/v1/conversations", tags=["Conversations"])
async def create_conversation(
    request: CreateConversationRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Tạo cuộc hội thoại mới."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = current_user["user_id"]
    try:
        conv_id = _persistence.create_conversation(
            user_id=user_id,
            title=request.title,
            metadata=request.metadata,
        )
        return {"conversation_id": conv_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi tạo conversation: {exc}")


@app.get("/api/v1/conversations", tags=["Conversations"])
async def list_conversations(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Liệt kê các cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = current_user["user_id"]
    try:
        conversations = _persistence.list_conversations(
            user_id=user_id, limit=limit, offset=offset
        )
        return {"conversations": conversations, "total": len(conversations)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi: {exc}")


def _get_owned_conversation(conversation_id: str, user_id: str) -> Dict[str, Any]:
    """Lấy conversation và đảm bảo nó thuộc về user. 404 nếu không tồn tại/không sở hữu."""
    conv = _persistence.get_conversation(conversation_id)
    # Trả 404 (không phải 403) để không lộ sự tồn tại của conversation người khác.
    if not conv or str(conv.get("user_id")) != str(user_id):
        raise HTTPException(status_code=404, detail="Conversation không tồn tại")
    return conv


@app.get("/api/v1/conversations/{conversation_id}", tags=["Conversations"])
async def get_conversation(
    conversation_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Chi tiết cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    return _get_owned_conversation(conversation_id, current_user["user_id"])


@app.get("/api/v1/conversations/{conversation_id}/messages", tags=["Conversations"])
async def get_conversation_messages(
    conversation_id: str,
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Lịch sử messages trong cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    _get_owned_conversation(conversation_id, current_user["user_id"])
    messages = _persistence.get_conversation_history(conversation_id, limit=limit)
    return {"messages": messages, "total": len(messages)}


@app.delete("/api/v1/conversations/{conversation_id}", tags=["Conversations"])
async def archive_conversation(
    conversation_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Archive (soft-delete) cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    _get_owned_conversation(conversation_id, current_user["user_id"])
    try:
        _persistence.archive_conversation(conversation_id)
        return {"status": "archived", "conversation_id": conversation_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi: {exc}")


# Auth — Đăng ký tài khoản

import re as _re

_EMAIL_RE = _re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _user_to_auth_response(user: Dict[str, Any], is_new: bool) -> Dict[str, Any]:
    """Chuẩn hóa record user trong DB → AuthResponse (không lộ hashed_password)."""
    return {
        "user_id": str(user["id"]),
        "email": user["email"],
        "full_name": user.get("full_name"),
        "role": str(user["role"]),
        "question_limit": user.get("question_limit"),
        "is_new": is_new,
        "access_token": _issue_token(user),
        "token_type": "bearer",
    }


@app.post("/api/v1/auth/register", response_model=AuthResponse, tags=["Auth"])
async def register(request: RegisterRequest):
    """Đăng ký tài khoản mới bằng email + mật khẩu."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    email = request.email.strip().lower()
    if not _EMAIL_RE.match(email):
        raise HTTPException(status_code=422, detail="Email không hợp lệ")

    if _persistence.get_user_by_email(email):
        raise HTTPException(status_code=409, detail="Email đã được đăng ký")

    from orchestrator.auth import hash_password

    full_name = (request.full_name or "").strip() or email.split("@")[0]
    try:
        user = _persistence.create_user(
            email=email,
            full_name=full_name,
            hashed_password=hash_password(request.password),
            role="user",
            question_limit=50,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi tạo tài khoản: {exc}")

    return _user_to_auth_response(user, is_new=True)


@app.post("/api/v1/auth/login", response_model=AuthResponse, tags=["Auth"])
async def login(request: LoginRequest):
    """Đăng nhập bằng email + mật khẩu."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    from orchestrator.auth import verify_password

    email = request.email.strip().lower()
    user = _persistence.get_user_by_email(email)
    # Thông báo chung cho cả email sai lẫn mật khẩu sai (tránh dò email tồn tại).
    if not user or not verify_password(request.password, user.get("hashed_password") or ""):
        raise HTTPException(status_code=401, detail="Email hoặc mật khẩu không đúng")
    if not user.get("is_active", True):
        raise HTTPException(status_code=403, detail="Tài khoản đã bị vô hiệu hóa")

    return _user_to_auth_response(user, is_new=False)


@app.post("/api/v1/auth/google", response_model=AuthResponse, tags=["Auth"])
async def google_auth(request: GoogleAuthRequest):
    """
    Đăng ký / đăng nhập bằng Google account.

    Frontend dùng Google Identity Services để lấy ID token (credential),
    gửi lên đây. Backend xác minh token với Google, rồi:
      - Nếu email chưa có → tạo tài khoản mới (is_new=True)
      - Nếu đã có        → trả về tài khoản hiện tại (is_new=False)
    """
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")
    if not _config or not _config.google_client_id:
        raise HTTPException(
            status_code=503,
            detail="Google OAuth chưa được cấu hình (thiếu GOOGLE_CLIENT_ID)",
        )

    from orchestrator.auth import hash_password, verify_google_token

    try:
        info = verify_google_token(request.credential, _config.google_client_id)
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Google token không hợp lệ: {exc}")

    email = (info.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Token Google thiếu email")
    if info.get("email_verified") is False:
        raise HTTPException(status_code=400, detail="Email Google chưa được xác minh")

    existing = _persistence.get_user_by_email(email)
    if existing:
        return _user_to_auth_response(existing, is_new=False)

    # Tài khoản Google không có mật khẩu → lưu hash ngẫu nhiên không dùng được
    # để vẫn thỏa ràng buộc NOT NULL của cột hashed_password.
    import uuid as _uuid

    try:
        user = _persistence.create_user(
            email=email,
            full_name=info.get("name") or email.split("@")[0],
            hashed_password=hash_password(_uuid.uuid4().hex),
            role="user",
            question_limit=50,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi tạo tài khoản: {exc}")

    return _user_to_auth_response(user, is_new=True)


# Feedback

@app.post("/api/v1/messages/{message_id}/feedback", tags=["Feedback"])
async def submit_feedback(
    message_id: str,
    request: FeedbackRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Gửi đánh giá cho một câu trả lời."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = current_user["user_id"]
    try:
        fb_id = _persistence.save_feedback(
            message_id=message_id,
            user_id=user_id,
            feedback_type=request.feedback_type,
            rating=request.rating,
            comment=request.comment,
        )

        # Publish feedback event
        if _mq:
            _mq.publish_analytics_event("feedback_submitted", {
                "message_id": message_id,
                "feedback_type": request.feedback_type,
                "rating": request.rating,
            })

        return {"feedback_id": fb_id, "status": "saved"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi: {exc}")


# System Info

@app.get("/api/v1/graph/info", tags=["System"])
async def graph_info():
    """Thông tin về LangGraph workflow."""
    return {
        "workflow": "retrieve → synthesize → END",
        "nodes": ["retrieve", "synthesize"],
        "edges": [
            {"from": "__start__", "to": "retrieve"},
            {"from": "retrieve", "to": "synthesize"},
            {"from": "synthesize", "to": "__end__"},
        ],
        "services": {
            "persistence": "PostgreSQL" if _persistence else "disabled",
            "cache": "Redis" if _mq else "disabled",
            "vector_db": "Qdrant",
            "graph_db": "Neo4j",
            "llm": _config.gemini_chat_model if _config else None,
        },
    }


# CLI Entry Point

def main():
    """Start the orchestrator server."""
    import uvicorn

    config = OrchestratorConfig.from_env()
    uvicorn.run(
        "orchestrator.app:app",
        host=config.app_host,
        port=config.app_port,
        reload=True,
        log_level=config.log_level,
    )


if __name__ == "__main__":
    main()
