# Chatbot Luat phap dung GraphRAG

He thong hoi dap phap ly thong minh theo kien truc GraphRAG, ket hop tri thuc do thi va tim kiem ngu nghia de tang do tin cay, giam hallucination va bat buoc trich dan nguon.

## Muc luc
- [1. Muc tieu du an](#1-muc-tieu-du-an)
- [2. Pham vi va doi tuong su dung](#2-pham-vi-va-doi-tuong-su-dung)
- [3. Kien truc tong the](#3-kien-truc-tong-the)
- [4. Luong xu ly du lieu va truy van](#4-luong-xu-ly-du-lieu-va-truy-van)
- [5. Cong nghe su dung](#5-cong-nghe-su-dung)
- [6. Cau truc du an de xuat](#6-cau-truc-du-an-de-xuat)
- [7. Cai dat va khoi chay local](#7-cai-dat-va-khoi-chay-local)
- [8. Cau hinh moi truong](#8-cau-hinh-moi-truong)
- [9. API contract mau](#9-api-contract-mau)
- [10. Danh gia chat luong va kiem thu](#10-danh-gia-chat-luong-va-kiem-thu)
- [11. Bao mat va tuan thu](#11-bao-mat-va-tuan-thu)
- [12. Van hanh va giam sat](#12-van-hanh-va-giam-sat)
- [13. Lo trinh phat trien](#13-lo-trinh-phat-trien)
- [14. Tuyen bo mien tru trach nhiem](#14-tuyen-bo-mien-tru-trach-nhiem)

## 1. Muc tieu du an
Du an huong den viec xay dung tro ly tra cuu phap luat co the:
- Tra loi dua tren ngu canh tai lieu phap ly thuc te, khong "doan".
- Ket hop truy hoi vector va truy hoi graph de tao ngu canh day du hon.
- Giam toi da hallucination cua LLM.
- Cung cap trich dan ro rang (dieu luat, van ban, nguon goc).
- Dat yeu cau an toan thong tin va on dinh van hanh theo mo hinh production.

## 2. Pham vi va doi tuong su dung
### Pham vi
- Tra cuu thong tin luat, dieu luat, van ban huong dan, tien le.
- Ho tro hoi dap thong tin phap ly tham khao.
- Khong thay the y kien tu van cua luat su/co quan co tham quyen.

### Doi tuong su dung
- Ca nhan can tim hieu quy dinh phap luat.
- Luat su/chuyen vien phap che.
- Doanh nghiep can doi chieu quy dinh lien quan den hoat dong kinh doanh.

## 3. Kien truc tong the
He thong trien khai theo microservices:

- Frontend: ReactJS.
- Backend API: FastAPI.
- CSDL nghiep vu: Postgres.
- Cache/message bus: Redis.
- Graph database: Neo4j.
- Vector database: Qdrant hoac Faiss.
- LLM + Embedding: Gemini API (Google).

Mo hinh GraphRAG:
- Graph retrieval: tim quan he phap ly giua cac thuc the (van ban, dieu khoan, chu de, tien le).
- Vector retrieval: tim doan van ban lien quan theo nghia.
- Retriever fusion: hop nhat ket qua truy hoi de tao bo context co xep hang.
- Generation: Gemini sinh cau tra loi dua tren context da grounding.

## 4. Luong xu ly du lieu va truy van
### 4.1 Ingestion (ETL tri thuc phap ly)
1. Thu thap du lieu luat/van ban/tien le.
2. Lam sach, chuan hoa dinh dang, bo metadata.
3. Chia doan (chunking) theo cau truc van ban.
4. Tao embedding cho tung chunk bang Gemini.
5. Luu embedding vao VectorDB.
6. Trich xuat thuc the, quan he va luu vao Neo4j.

### 4.2 Query flow (Hoi dap)
1. Nhan cau hoi tu frontend.
2. Chuyen hoa truy van (query rewrite neu can).
3. Truy hoi vector top-k chunk lien quan.
4. Truy hoi graph top-k node/path lien quan.
5. Hop nhat va rerank context.
6. Gui context vao Gemini de sinh cau tra loi.
7. Tra ve cau tra loi kem danh sach trich dan.

## 5. Cong nghe su dung
- Frontend: ReactJS, Vite (de xuat).
- Backend: FastAPI, Pydantic, Uvicorn.
- Data/Storage: Postgres, Redis, Neo4j, Qdrant/Faiss.
- AI: Gemini API cho chat completion + embedding.
- Infrastructure: Docker Compose (local), CI/CD (de xuat).

## 6. Cau truc du an de xuat
Luu y: Cau truc duoi day la chuan muc tieu de trien khai day du.

```text
.
|- frontend/
|- backend/
|  |- app/
|  |  |- api/
|  |  |- services/
|  |  |- rag/
|  |  |- models/
|  |  |- core/
|  |- tests/
|- ingestion/
|  |- pipelines/
|  |- parsers/
|  |- chunkers/
|  |- embeddings/
|- infra/
|  |- docker/
|  |- monitoring/
|- docs/
|- README.md
```

## 7. Cai dat va khoi chay local
## 7.1 Yeu cau
- Docker + Docker Compose.
- Node.js 18+.
- Python 3.11+.
- Tai khoan Google AI co Gemini API key.

## 7.2 Chuan bi
1. Clone repository:

```bash
git clone https://github.com/NOT-erorr/Lawyer-chatbot-using-GraphRAG.git
cd Lawyer-chatbot-using-GraphRAG
```

2. Tao file moi truong:

```bash
cp .env.example .env
```

3. Dien day du bien trong `.env` (xem muc 8).

## 7.3 Khoi chay bang Docker Compose (de xuat)
```bash
docker compose up -d
```

## 7.4 Chay thu cong tung service (neu khong dung Docker)
Backend:
```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Frontend:
```bash
cd frontend
npm install
npm run dev
```

## 8. Cau hinh moi truong
Duoi day la danh sach bien moi truong toi thieu:

```env
# LLM / Embedding
GEMINI_API_KEY=
GEMINI_CHAT_MODEL=gemini-1.5-pro
GEMINI_EMBED_MODEL=text-embedding-004

# Backend
APP_ENV=development
APP_PORT=8000
JWT_SECRET=

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=law_chatbot
POSTGRES_USER=postgres
POSTGRES_PASSWORD=

# Redis
REDIS_URL=redis://localhost:6379/0

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=

# Vector DB (chon 1)
VECTOR_PROVIDER=qdrant
QDRANT_URL=http://localhost:6333
QDRANT_API_KEY=
FAISS_INDEX_PATH=./data/faiss.index

# Retrieval
RETRIEVAL_TOP_K_VECTOR=8
RETRIEVAL_TOP_K_GRAPH=6
RETRIEVAL_RERANK=true

# Security
TLS_ENABLED=true
PII_MASKING=true
AUDIT_LOG_ENABLED=true
```

## 9. API contract mau
### 9.1 Health check
Request:
```http
GET /health
```

Response:
```json
{
  "status": "ok",
  "services": {
    "postgres": "up",
    "redis": "up",
    "neo4j": "up",
    "vector_db": "up",
    "gemini": "up"
  }
}
```

### 9.2 Chat Q&A
Request:
```http
POST /api/v1/chat
Content-Type: application/json
```

```json
{
  "question": "Doanh nghiep co bat buoc dong BHXH cho lao dong thu viec khong?",
  "conversation_id": "abc-123",
  "user_context": {
    "jurisdiction": "VN",
    "language": "vi"
  }
}
```

Response:
```json
{
  "answer": "...noi dung tra loi...",
  "citations": [
    {
      "source_id": "vbpl-2020-xx",
      "title": "Ten van ban",
      "article": "Dieu 12",
      "url": "https://..."
    }
  ],
  "disclaimer": "Noi dung chi mang tinh tham khao, khong phai tu van phap ly rang buoc.",
  "trace": {
    "vector_hits": 8,
    "graph_hits": 6,
    "model": "gemini-1.5-pro"
  }
}
```

### 9.3 Ingestion trigger (noi bo)
Request:
```http
POST /api/v1/ingestion/run
```

```json
{
  "dataset": "legal_corpus_v1",
  "reindex": false
}
```

## 10. Danh gia chat luong va kiem thu
Can co bo benchmark rieng cho bai toan phap ly:
- Retrieval quality: Recall@k, MRR, NDCG.
- Answer quality: factual correctness, citation coverage.
- Safety/compliance: khong dua ket luan vuot qua pham vi, khong xuat PII.

Quy trinh test de xuat:
1. Unit test cho parser, chunker, retriever, citation mapper.
2. Integration test cho FastAPI + Postgres/Redis/Neo4j/VectorDB.
3. E2E test voi bo cau hoi phap ly chuan.
4. Regression test sau moi thay doi prompt/retrieval strategy.

## 11. Bao mat va tuan thu
Nguyen tac bat buoc:
- Khong dua loi khuyen phap ly rang buoc.
- Biet ro nguon va muc do tin cay cua tung trich dan.
- Co co che xoa/an danh du lieu nhay cam.

Kiem soat bao mat:
- TLS cho moi ket noi client-service va service-service.
- Ma hoa du lieu nhay cam khi luu tru.
- Phan quyen theo vai tro (RBAC), audit log day du.
- Rate limit + anti-abuse cho endpoint cong khai.

Tuan thu:
- GDPR (neu xu ly du lieu cong dan EU).
- HIPAA (neu lien quan du lieu y te theo khu vuc ap dung).
- Chinh sach luu tru, retention va xoa du lieu ro rang.

## 12. Van hanh va giam sat
Chi so can theo doi:
- p95/p99 latency cua API chat.
- Ty le loi backend va ty le timeout voi Gemini.
- Chat luong retrieval (truot nguong canh bao).
- Ty le cau tra loi co trich dan hop le.

Canh bao de xuat:
- Gemini API loi lien tuc tren nguong.
- Neo4j/Qdrant khong san sang.
- Ty le hallucination (danh gia hau kiem) tang bat thuong.

## 13. Lo trinh phat trien
1. Hoan thien data ingestion cho legal corpus tieng Viet.
2. Chuan hoa ontology/phap danh cho do thi tri thuc.
3. Toi uu retriever fusion va rerank.
4. Bat buoc citation-first generation trong prompt.
5. Bo sung dashboard giam sat chat luong RAG.
6. Thiet lap CI/CD va bo test hoi quy tu dong.

## 14. Tuyen bo mien tru trach nhiem
He thong nay cung cap thong tin phap ly tham khao va khong thay the y kien tu van phap ly chuyen nghiep. Nguoi dung can tham van luat su hoac co quan co tham quyen truoc khi dua ra quyet dinh phap ly quan trong.
