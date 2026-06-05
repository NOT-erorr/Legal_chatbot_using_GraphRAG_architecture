# Agentic GraphRAG System: Vietnamese Legal Assistant

A comprehensive, production-ready Agentic GraphRAG (Retrieval-Augmented Generation) system tailored for Vietnamese legal documents. This project fundamentally reduces LLM hallucination by forcing the underlying Large Language Model to synthesize answers solely from heavily verified semantic chunks and knowledge graph relationships.

---

## 1. System Architecture

The architecture is composed of four strictly separated layers, operating synchronously as a hybrid service:

```text
+-------------------+                               +-----------------------+
|  User / Client    |                               |  Databricks Platform  |
|  (React UI/Nginx) | <============================ |  (Medallion ETL)      |
+---------+---------+                               +-----------+-----------+
          |                                                     |
          | HTTP/REST                                           | Sync
          v                                                     v
+---------+---------+                               +-----------+-----------+
|    Orchestrator   |       +-----------------+     | Neo4j Aura (Graph)    |
| (FastAPI backend) | <===> | Google Gemini   |     | Qdrant Cloud (Vector) |
| LangGraph Router  |       | 1.5/2.5 / Embed |     | PostgreSQL (Memory)   |
+---------+---------+       +-----------------+     +-----------+-----------+
          |                                                     ^
          | Lucene / Cosine Similarity / SQL                    |
          +=====================================================+
```

### Components
1. **Frontend (`/frontend`)**: React + Vite SPA. Handles real-time chat sessions with streaming capabilities.
2. **Orchestrator (`/orchestrator`)**: The FastAPI backbone integrating LangGraph. Orchestrates the parallel execution of the Hybrid Retriever (combining Vector searches with Graph traversals) to build contextually dense prompts.
3. **Database & State (`/db`)**: Manages conversation continuity (PostgreSQL) and temporary graph states (Redis).
4. **ETL Pipeline (`/databricks_platform`)**: A robust Medallion data engineering pipeline cleaning raw laws to unified Vector-Graph chunks.

---

## 2. Deep Dive: Hybrid Graph Retrieval Strategy

The system utilizes a `HybridSearchResult` flow that maximizes accuracy:
1. **Semantic Vector Search:** Leverages `google-genai` SDK (`gemini-embedding-001` mapping to `text-embedding-004`) to encode user queries and retrieves top semantic matches from Qdrant.
2. **Keyword Full-Text Search:** Falls back to BM25/Lucene keyword filtering natively initialized inside Neo4j using the `legal_fulltext` index (Resolving Python parameter shadowing dynamically).
3. **Graph Expansion:** Uses identifiers retrieved in Phase 1 to rapidly expand node boundaries via Cypher, absorbing sibling regulations and linked legal precedents.
4. **Reciprocal Rank Fusion (RRF):** The 3 result sources are numerically ranked, merged, and squashed. The final top components form the AI's strictly bound memory set.

---

## 3. Quickstart & Deployment

### 3.1 Local Container Execution
*Prerequisites*: Docker, Docker Compose, Python 3.11+, Node 18+.

1. Populate your credentials:
   ```bash
   cp .env.production .env   # Then populate missing local keys
   ```
2. Spawn local dependencies:
   ```bash
   docker-compose up -d neo4j qdrant postgres redis
   ```
3. Test Backend Logic independently (without UI):
   ```bash
   python test_workflow.py
   ```

### 3.2 CI/CD Deployment via GitHub Actions (free-tier)

Push lên `main` sẽ tự động build + deploy qua GitHub Actions:

| Thành phần | Hạ tầng | Workflow | Chi phí |
| --- | --- | --- | --- |
| Orchestrator (FastAPI) | Google Cloud Run (`min-instances=0`, scale-to-zero) | [`.github/workflows/deploy-backend.yml`](.github/workflows/deploy-backend.yml) | Free-tier (cần billing account) |
| Frontend (React/Vite) | GitHub Pages | [`.github/workflows/deploy-frontend.yml`](.github/workflows/deploy-frontend.yml) | Miễn phí |
| Lint/test offline | GitHub Actions | [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Miễn phí |

**Managed state (free tier):** Qdrant Cloud, Neo4j Aura, PostgreSQL (Neon/Supabase — **bắt buộc** vì chat bị auth-gate). Redis là tùy chọn (cache + rate-limit).

**Cấu hình một lần** (GitHub → Settings → Secrets and variables → Actions):

- *Secrets*: `GCP_SA_KEY` (JSON service account có quyền Cloud Run Admin + Cloud Build + Service Account User), `GCP_PROJECT_ID`, `GEMINI_API_KEY`, `QDRANT_URL`, `QDRANT_API_KEY`, `NEO4J_URI`, `NEO4J_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `JWT_SECRET`, `GOOGLE_CLIENT_ID`.
- *Variables*: `BACKEND_URL` — điền URL Cloud Run sau lần deploy backend đầu tiên (frontend build dùng làm `VITE_API_URL`).

**Các bước:**
1. Bật Pages: Settings → Pages → Source = **GitHub Actions**.
2. Tạo secrets/variables ở trên.
3. Push `main` → backend deploy → copy URL vào variable `BACKEND_URL` → chạy lại workflow frontend (Actions → Run workflow).
4. Thêm origin Pages (`https://<user>.github.io`) vào **Authorized JavaScript origins** của Google OAuth Client.

> Bảo mật hơn (không dùng key dài hạn): thay `GCP_SA_KEY` bằng **Workload Identity Federation** với `google-github-actions/auth`.
> Muốn **không cần thẻ tín dụng**: thay Cloud Run bằng Hugging Face Docker Space (free) cho backend.

---

## 4. Known Issues & Developer Notes

- `qdrant-client` Discrepancy: Following version `1.10.x`, Qdrant removed the traditional `.search()` interface. The system uses`.query_points()` with a graceful `.search()` fallback to guarantee environment compatibility.
- **Neo4j Cypher Collision:** In `orchestrator/retrievers.py`, we purposely map the runtime argument as `$search_keyword` to successfully dodge Neo4j Driver's `query` positional parameter conflicts. 
- **Google GenAI API Maps:** Ensure the backend leverages exactly `gemini-embedding-001`. Using strings like `models/embedding-001` natively inside `google-genai` > v1 on endpoints like `v1beta` will yield immediate HTTP 404 blocks.

---
**Disclaimer**: Outputs provided by this Agentic GraphRAG system are primarily for analytical reference. They do not constitute official Legal Advice designated by legislative authorities.
