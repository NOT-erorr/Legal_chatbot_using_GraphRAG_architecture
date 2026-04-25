# Legal Chatbot using GraphRAG Architecture

Hệ thống hỏi đáp pháp luật Việt Nam theo kiến trúc GraphRAG:

- Truy hồi ngữ nghĩa từ Qdrant (vector search)
- Mở rộng ngữ cảnh từ Neo4j (knowledge graph)
- Tổng hợp câu trả lời bằng Gemini
- Lưu hội thoại, feedback và retrieval logs trong PostgreSQL

README này tập trung vào setup step-by-step để chạy được dự án từ local.

## 1. Tổng quan kiến trúc

Các thành phần chính:

- `frontend`: React + Vite (UI)
- `orchestrator`: FastAPI + LangGraph (API và GraphRAG pipeline)
- `db`: schema PostgreSQL cho dữ liệu ứng dụng
- `databricks_platform`: pipeline ingest/chunk/embed/sync dữ liệu pháp lý
- `docker-compose.yml`: chạy full local stack

Luồng xử lý câu hỏi:

1. User gửi câu hỏi từ frontend.
2. Orchestrator embed query, tìm chunk liên quan trong Qdrant.
3. Orchestrator truy quan hệ liên quan trong Neo4j.
4. Gemini sinh câu trả lời có citations/disclaimer.
5. Kết quả được lưu PostgreSQL, cache Redis (nếu bật).

## 2. Yêu cầu trước khi cài

Tối thiểu cần:

- Docker Desktop + Docker Compose
- Git

Nếu muốn chạy service ngoài Docker:

- Python 3.11+
- Node.js 20+

## 3. Setup local step-by-step (khuyến nghị)

### Bước 1: Clone project

```bash
git clone <repo_url>
cd Legal_chatbot_using_GraphRAG_architecture
```

### Bước 2: Tạo file môi trường cho orchestrator

```bash
cp orchestrator/.env.example orchestrator/.env
```

Trên Windows PowerShell có thể dùng:

```powershell
Copy-Item orchestrator/.env.example orchestrator/.env
```

Sau đó mở `orchestrator/.env` và cập nhật ít nhất:

- `GEMINI_API_KEY`

Gợi ý:

- Nếu chạy full bằng Docker Compose local, giữ nguyên các host dạng `postgres`, `redis`, `qdrant`, `neo4j`.
- Nếu chạy orchestrator ngoài Docker, đổi host thành `localhost` tương ứng.

### Bước 3: Khởi động toàn bộ stack bằng Docker Compose

```bash
docker compose up -d --build
```

Các container sẽ chạy:

- `graphrag-postgres`
- `graphrag-redis`
- `graphrag-neo4j`
- `graphrag-qdrant`
- `graphrag-backend`
- `graphrag-frontend`

### Bước 4: Kiểm tra trạng thái container

```bash
docker compose ps
```

Nếu cần xem log backend:

```bash
docker compose logs -f backend
```

### Bước 5: Health check orchestrator

```bash
curl http://localhost:8001/health
```

Kỳ vọng nhận JSON có `status: ok`.

### Bước 6: Truy cập ứng dụng

- Frontend: http://localhost
- Backend OpenAPI: http://localhost:8001/docs
- Neo4j Browser: http://localhost:7474
- Qdrant API: http://localhost:6333

## 4. Lưu ý quan trọng về frontend API khi chạy local

Hiện tại file `frontend/src/services/api.js` đang hard-code endpoint Cloud Run:

`https://graphrag-orchestrator-376046964237.asia-southeast1.run.app`

Điều này có nghĩa:

- Dù bạn chạy local, frontend vẫn gọi backend cloud (không gọi `localhost:8001`).

Nếu bạn muốn test local end-to-end, sửa tạm `API_BASE` trong file trên thành:

- `http://localhost:8001` (gọi trực tiếp backend local), hoặc
- `/api` (nếu bạn cấu hình reverse proxy `/api` trong Nginx/dev server)

## 5. Chạy từng service thủ công (không full Docker)

### 5.1 Chạy infra trước (Postgres, Redis, Neo4j, Qdrant)

```bash
docker compose up -d postgres redis neo4j qdrant
```

### 5.2 Chạy orchestrator bằng Python

```bash
pip install -r orchestrator/requirements.txt
python -m orchestrator.app
```

Mặc định orchestrator chạy tại `http://localhost:8001`.

### 5.3 Chạy frontend bằng Vite

```bash
cd frontend
npm install
npm run dev
```

Frontend dev mặc định ở `http://localhost:5173`.

## 6. Chạy test

```bash
pip install -r orchestrator/requirements-dev.txt
pytest orchestrator/tests
```

Scope test hiện có:

- API endpoint cơ bản
- Config và graph helper

## 7. Dữ liệu và pipeline Databricks (tùy chọn)

Thư mục `databricks_platform` dùng để ingest và đồng bộ dữ liệu pháp lý vào Qdrant/Neo4j.

Lệnh thường dùng:

```bash
cd databricks_platform
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run vn_legal_lakehouse_etl -t dev
```

Chạy pipeline theo stage bằng Python:

```bash
python jobs/run_pipeline.py --stage bronze
python jobs/run_pipeline.py --stage silver
python jobs/run_pipeline.py --stage gold
python jobs/run_pipeline.py --stage all
```

Một số biến môi trường quan trọng cho gold stage:

- `GEMINI_API_KEY`
- `QDRANT_URL`, `QDRANT_API_KEY`, `QDRANT_COLLECTION`
- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`

## 8. Troubleshooting nhanh

1. Backend lên nhưng trả lỗi kết nối DB/vector/graph

- Kiểm tra `docker compose ps`
- Kiểm tra host trong `orchestrator/.env` có đúng môi trường chạy (Docker network hay localhost)

2. Frontend hiển thị nhưng không chat được local

- Kiểm tra `frontend/src/services/api.js` có còn hard-code API cloud không

3. Schema PostgreSQL chưa có bảng

- Schema auto-run khi volume Postgres khởi tạo lần đầu
- Nếu cần reset sạch local:

```bash
docker compose down -v
docker compose up -d --build
```

4. Thiếu dữ liệu retrieval

- Bạn cần nạp/sync dữ liệu vào Qdrant và Neo4j trước khi kỳ vọng câu trả lời có ngữ cảnh tốt

## 9. Tài liệu chi tiết từng module

- `orchestrator/README.md`
- `frontend/README.md`
- `databricks_platform/README.md`
- `db/README.md`

## 10. Security và lưu ý pháp lý

- Không commit secrets (`.env`, API key, password).
- Với cloud, dùng Secret Manager/secret scope thay vì plaintext.
- Nội dung chatbot chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.
