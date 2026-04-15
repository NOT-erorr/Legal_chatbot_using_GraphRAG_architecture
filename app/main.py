"""
main.py — FastAPI application cho GraphRAG Vietnamese Law.
Cung cấp REST API để hỏi đáp pháp luật sử dụng hybrid retrieval.
"""

import sys
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from retrieval.hybrid_retriever import HybridRetriever

# ============================================================
# Pydantic Models
# ============================================================

class ChatRequest(BaseModel):
    """Request body cho endpoint /chat."""
    question: str = Field(..., description="Câu hỏi pháp luật", min_length=5)
    top_k_vector: Optional[int] = Field(None, description="Số chunks từ FAISS", ge=1, le=20)
    top_k_graph: Optional[int] = Field(None, description="Số docs mở rộng từ Neo4j", ge=0, le=10)


class SearchRequest(BaseModel):
    """Request body cho endpoint /search (chỉ tìm kiếm, không gọi LLM)."""
    query: str = Field(..., description="Truy vấn tìm kiếm", min_length=3)
    top_k: Optional[int] = Field(5, description="Số kết quả trả về", ge=1, le=20)


# ============================================================
# Application Lifespan
# ============================================================

retriever: Optional[HybridRetriever] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Khởi tạo và cleanup HybridRetriever."""
    global retriever
    print("🚀 Đang khởi tạo GraphRAG Retriever...")
    retriever = HybridRetriever(load_index=True)
    print("✅ GraphRAG Retriever sẵn sàng!")
    yield
    # Cleanup
    if retriever:
        retriever.close()
        print("🔒 GraphRAG Retriever đã đóng")


# ============================================================
# FastAPI App
# ============================================================

app = FastAPI(
    title="GraphRAG Vietnamese Law API",
    description=(
        "Hệ thống hỏi đáp pháp luật Việt Nam sử dụng GraphRAG. "
        "Kết hợp tìm kiếm ngữ nghĩa (FAISS) và đồ thị tri thức (Neo4j) "
        "để cung cấp câu trả lời chính xác với trích dẫn nguồn."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Endpoints
# ============================================================

@app.get("/health", tags=["System"])
async def health_check():
    """Kiểm tra trạng thái hệ thống."""
    status = {
        "status": "ok",
        "services": {
            "retriever": "up" if retriever else "down",
            "faiss_index": "loaded" if (retriever and retriever.vector_pipeline.index) else "not_loaded",
        },
    }

    # Check Neo4j
    try:
        if retriever:
            summary = retriever.graph_pipeline.get_graph_summary()
            status["services"]["neo4j"] = "up"
            status["neo4j_summary"] = summary
    except Exception:
        status["services"]["neo4j"] = "down"

    # Check PostgreSQL
    try:
        if retriever and not retriever.doc_loader.conn.closed:
            status["services"]["postgres"] = "up"
        else:
            status["services"]["postgres"] = "down"
    except Exception:
        status["services"]["postgres"] = "down"

    return status


@app.post("/api/v1/chat", tags=["Chat"])
async def chat(request: ChatRequest):
    """
    Hỏi đáp pháp luật — Luồng GraphRAG đầy đủ.

    Quy trình:
    1. FAISS semantic search tìm chunks liên quan
    2. Neo4j graph expansion mở rộng ngữ cảnh
    3. LLM sinh câu trả lời dựa trên context
    """
    if not retriever:
        raise HTTPException(status_code=503, detail="Retriever chưa sẵn sàng")

    try:
        result = retriever.retrieve(
            question=request.question,
            top_k_vector=request.top_k_vector,
            top_k_graph=request.top_k_graph,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi xử lý: {str(e)}")


@app.post("/api/v1/search", tags=["Search"])
async def search(request: SearchRequest):
    """
    Tìm kiếm thuần túy (chỉ FAISS, không gọi LLM).
    Hữu ích cho debug hoặc xem kết quả tìm kiếm trước khi chat.
    """
    if not retriever:
        raise HTTPException(status_code=503, detail="Retriever chưa sẵn sàng")

    try:
        result = retriever.search_only(
            question=request.query,
            top_k=request.top_k,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi tìm kiếm: {str(e)}")


@app.get("/api/v1/graph/summary", tags=["Graph"])
async def graph_summary():
    """Lấy thống kê tổng quan Knowledge Graph."""
    if not retriever:
        raise HTTPException(status_code=503, detail="Retriever chưa sẵn sàng")

    try:
        summary = retriever.graph_pipeline.get_graph_summary()
        return {"summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi: {str(e)}")


@app.get("/api/v1/graph/related/{doc_id}", tags=["Graph"])
async def graph_related(doc_id: str, depth: int = 2):
    """Tìm các văn bản liên quan đến doc_id qua Knowledge Graph."""
    if not retriever:
        raise HTTPException(status_code=503, detail="Retriever chưa sẵn sàng")

    try:
        related = retriever.graph_pipeline.query_related_docs(doc_id, depth=depth)
        return {"doc_id": doc_id, "related": related}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi: {str(e)}")


# ============================================================
# Run
# ============================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=(settings.APP_ENV == "development"),
    )
