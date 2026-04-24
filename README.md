# Legal Chatbot Assistant (Agentic GraphRAG)

Hệ thống hỏi đáp pháp luật Việt Nam dùng mô hình GraphRAG để giảm hallucination và bắt buộc trả lời dựa trên ngữ cảnh có nguồn.

## 1. System Architecture

- `frontend`: React + Vite UI, chạy qua Nginx khi containerized.
- `orchestrator`: FastAPI + LangGraph, xử lý truy hồi và tổng hợp câu trả lời.
- `db`: PostgreSQL schema cho hội thoại, retrieval logs, feedback.
- `databricks_platform`: ETL pipeline tạo chunk, embedding, sync sang Qdrant/Neo4j.
- Infra local: Docker Compose cho `postgres`, `redis`, `neo4j`, `qdrant`, `backend`, `frontend`.

### Query Flow

1. User gửi câu hỏi từ frontend.
2. Orchestrator truy hồi `Qdrant` (vector).
3. Orchestrator mở rộng ngữ cảnh từ `Neo4j` (graph).
4. Gemini tổng hợp câu trả lời có trích dẫn.
5. Kết quả lưu vào PostgreSQL, cache qua Redis.

## 2. Repository Layout

```text
.
|- frontend/
|- orchestrator/
|- databricks_platform/
|- db/
|- docker-compose.yml
|- README.md
```

## 3. Quickstart Local

### Prerequisites

- Docker + Docker Compose
- Python 3.11+ (nếu chạy backend ngoài Docker)
- Node.js 18+ (nếu chạy frontend ngoài Docker)

### Setup

```bash
cp orchestrator/.env.example orchestrator/.env
docker compose up -d
```

### Verify

```bash
curl http://localhost:8001/health
```

Frontend mặc định ở `http://localhost`.

## 4. Quickstart Cloud (Phase 1)

Mục tiêu cloud phase 1: deploy `orchestrator` lên Cloud Run.

### Build image

```bash
gcloud builds submit orchestrator \
  --config orchestrator/cloudbuild.yaml \
  --substitutions _AR_REPO=legal-chatbot,_IMAGE_NAME=orchestrator
```

### Deploy image

```bash
bash orchestrator/deploy-cloud-run.sh <project_id> <region> <service_name> <image_tag>
```

### Cloud dependencies (recommended)

- Qdrant Cloud
- Neo4j AuraDB
- Cloud SQL PostgreSQL
- Memorystore Redis
- Secret Manager cho toàn bộ credentials

## 5. Testing

Backend tests nằm trong `orchestrator/tests`.

```bash
pip install -r orchestrator/requirements-dev.txt
pytest orchestrator/tests
```

Scope hiện có:

- Unit tests: config + graph helper functions.
- Integration tests: API endpoints với mocked dependencies.

## 6. Service Docs

- `orchestrator/README.md`
- `frontend/README.md`
- `databricks_platform/README.md`
- `db/README.md`

## 7. Security Notes

- Không commit secrets (`.env`, API keys, passwords).
- Inject secrets qua Secret Manager trong môi trường cloud.
- Luôn trả về disclaimer pháp lý trong response.

## 8. Production Checklist

- [ ] Health check pass cho tất cả dependencies.
- [ ] Unit + integration tests pass.
- [ ] Cloud Run deployment verify với traffic thật.
- [ ] CORS và rate limiting được cấu hình đúng.
- [ ] Audit logs và alerting được bật.
- [ ] README/service docs khớp cấu hình thực tế.

## 9. Disclaimer

Nội dung chatbot chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.
