# Orchestrator Service (FastAPI + LangGraph)

`orchestrator` là backend chính của Legal Chatbot. Service này nhận câu hỏi, chạy GraphRAG (`Qdrant + Neo4j + Gemini`), lưu hội thoại vào PostgreSQL và cache kết quả bằng Redis.

## 1. Responsibilities

- Expose API cho chat, search, conversation và feedback.
- Orchestrate graph flow: `retrieve -> synthesize`.
- Persist chat messages, retrieval logs và feedback.
- Publish analytics events qua Redis stream.

## 2. API Endpoints

- `GET /health`
- `POST /api/v1/chat`
- `POST /api/v1/search`
- `GET /api/v1/graph/info`
- `POST /api/v1/conversations`
- `GET /api/v1/conversations`
- `GET /api/v1/conversations/{id}`
- `GET /api/v1/conversations/{id}/messages`
- `DELETE /api/v1/conversations/{id}`
- `POST /api/v1/messages/{id}/feedback`

## 3. Local Development

### Prerequisites

- Python 3.11+
- PostgreSQL, Redis, Neo4j, Qdrant (khuyên dùng `docker compose`)

### Setup

```bash
cp orchestrator/.env.example orchestrator/.env
pip install -r orchestrator/requirements.txt
```

### Run standalone

```bash
python -m orchestrator.app
```

Service default chạy tại `http://localhost:8001`.

### Run with Docker Compose (recommended)

Từ root project:

```bash
docker compose up -d postgres redis neo4j qdrant backend
```

Verify:

```bash
curl http://localhost:8001/health
```

## 4. Configuration

Biến môi trường mẫu xem ở:

- `orchestrator/.env.example` (local)
- `orchestrator/.env.cloudrun.example` (Cloud Run)

Secrets cần inject an toàn:

- `GEMINI_API_KEY`
- `QDRANT_API_KEY` (nếu dùng Qdrant Cloud)
- `NEO4J_PASSWORD`
- `POSTGRES_PASSWORD`

## 5. Testing

Test files nằm ở `orchestrator/tests`.

```bash
pip install -r orchestrator/requirements-dev.txt
pytest orchestrator/tests
```

Bao gồm:

- Unit tests cho config và helper functions của graph.
- API integration tests với mocked dependencies.

## 6. Cloud Run Deployment

### Build image with Cloud Build

```bash
gcloud builds submit orchestrator \
  --config orchestrator/cloudbuild.yaml \
  --substitutions _AR_REPO=legal-chatbot,_IMAGE_NAME=orchestrator
```

### Deploy service

```bash
bash orchestrator/deploy-cloud-run.sh <project_id> <region> <service_name> <image_tag>
```

Ví dụ:

```bash
bash orchestrator/deploy-cloud-run.sh my-project asia-southeast1 legal-orchestrator abc123
```

### Post-deploy runbook

1. `GET /health` phải trả về `status=ok`.
2. Gửi 1 request `POST /api/v1/chat` với câu hỏi pháp lý mẫu.
3. Kiểm tra log Cloud Run để đảm bảo không có lỗi kết nối `Postgres/Redis/Neo4j/Qdrant`.
4. Kiểm tra p95 latency trước khi mở traffic thật.
