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
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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
async def chat(request: ChatRequest):
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
    user_id = _guest_user_id or "anonymous"
    if _mq and not _mq.check_rate_limit(user_id):
        raise HTTPException(status_code=429, detail="Quá nhiều yêu cầu. Vui lòng thử lại sau.")

    # ── 3. Create/get conversation ──────────────────────────────────
    conversation_id = request.conversation_id
    if _persistence and not conversation_id:
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
        from orchestrator.retrievers import QdrantRetriever

        t0 = time.perf_counter()
        qdrant = QdrantRetriever(_config)
        chunks = qdrant.search(query=request.query, top_k=request.top_k)
        elapsed = round((time.perf_counter() - t0) * 1000, 1)

        return {
            "chunks": chunks,
            "total": len(chunks),
            "elapsed_ms": elapsed,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi tìm kiếm: {str(exc)}")


# Conversations — CRUD

@app.post("/api/v1/conversations", tags=["Conversations"])
async def create_conversation(request: CreateConversationRequest):
    """Tạo cuộc hội thoại mới."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = _guest_user_id or "anonymous"
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
):
    """Liệt kê các cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = _guest_user_id or "anonymous"
    try:
        conversations = _persistence.list_conversations(
            user_id=user_id, limit=limit, offset=offset
        )
        return {"conversations": conversations, "total": len(conversations)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi: {exc}")


@app.get("/api/v1/conversations/{conversation_id}", tags=["Conversations"])
async def get_conversation(conversation_id: str):
    """Chi tiết cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    conv = _persistence.get_conversation(conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation không tồn tại")
    return conv


@app.get("/api/v1/conversations/{conversation_id}/messages", tags=["Conversations"])
async def get_conversation_messages(
    conversation_id: str,
    limit: int = Query(50, ge=1, le=200),
):
    """Lịch sử messages trong cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    messages = _persistence.get_conversation_history(conversation_id, limit=limit)
    return {"messages": messages, "total": len(messages)}


@app.delete("/api/v1/conversations/{conversation_id}", tags=["Conversations"])
async def archive_conversation(conversation_id: str):
    """Archive (soft-delete) cuộc hội thoại."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    try:
        _persistence.archive_conversation(conversation_id)
        return {"status": "archived", "conversation_id": conversation_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Lỗi: {exc}")


# Feedback

@app.post("/api/v1/messages/{message_id}/feedback", tags=["Feedback"])
async def submit_feedback(message_id: str, request: FeedbackRequest):
    """Gửi đánh giá cho một câu trả lời."""
    if not _persistence:
        raise HTTPException(status_code=503, detail="PostgreSQL chưa sẵn sàng")

    user_id = _guest_user_id or "anonymous"
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
