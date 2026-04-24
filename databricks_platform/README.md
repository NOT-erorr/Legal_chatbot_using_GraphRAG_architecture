# Databricks Platform (Ingestion + Lakehouse ETL)

`databricks_platform` xử lý pipeline dữ liệu pháp lý từ raw documents đến vector/graph stores để phục vụ `orchestrator`.

## 1. Pipeline Overview

1. Bronze (ingestion)
   - Source: HuggingFace dataset `th1nhng0/vietnamese-legal-documents`.
   - Output: `main.law_lakehouse.bronze_legal_documents`.
2. Silver (transform)
   - Normalize + hierarchical chunking: `chuong -> muc -> dieu -> khoan -> diem`.
   - Output: documents/chunks/relations tables.
3. Gold (embedding + sync)
   - Generate embeddings with Gemini.
   - Sync vectors to Qdrant và graph relations sang Neo4j.

## 2. Folder Structure

- `src/config.py`: đọc env config.
- `src/chunking.py`: chunking logic.
- `src/rate_limit.py`: RPM/TPM guard.
- `src/gemini_embedding.py`: embedding client và retry.
- `src/sinks.py`: connector Qdrant + Neo4j.
- `src/lakehouse_pipeline.py`: orchestration Bronze/Silver/Gold.
- `jobs/run_pipeline.py`: CLI entrypoint theo stage.
- `databricks.yml`: asset bundle workflow.
- `cloud_run_job/`: one-shot sync job.

## 3. Required Environment Variables

Set qua Databricks job env hoặc secret scope:

```env
DBX_CATALOG=main
DBX_SCHEMA=law_lakehouse
HF_DATASET_NAME=th1nhng0/vietnamese-legal-documents
HF_DATASET_CONFIG=content
HF_DATASET_SPLIT=data
MAX_SOURCE_RECORDS=0
SOURCE_BATCH_SIZE=500

CHUNK_TARGET_TOKENS=220
CHUNK_MAX_TOKENS=300
CHUNK_OVERLAP_TOKENS=30

GEMINI_API_KEY=<secret>
GEMINI_EMBEDDING_MODEL=gemini-embedding-001
GEMINI_RPM_LIMIT=3000
GEMINI_TPM_LIMIT=1000000

SYNC_TO_QDRANT=true
QDRANT_URL=https://<qdrant-host>
QDRANT_API_KEY=<secret>
QDRANT_COLLECTION=vn_legal_chunks

SYNC_TO_NEO4J=true
NEO4J_URI=neo4j+s://<neo4j-host>
NEO4J_USER=neo4j
NEO4J_PASSWORD=<secret>
NEO4J_DATABASE=neo4j
```

## 4. Run Commands

Từ thư mục `databricks_platform`:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run vn_legal_lakehouse_etl -t dev
```

Chạy từng stage:

```bash
python jobs/run_pipeline.py --stage bronze
python jobs/run_pipeline.py --stage silver
python jobs/run_pipeline.py --stage gold
python jobs/run_pipeline.py --stage all
```

## 5. Integration Contract with Orchestrator

- Qdrant collection mặc định: `vn_legal_chunks`.
- Neo4j graph model:
  - `(:Document)-[:HAS_CHUNK]->(:Chunk)`
  - `(:Document)-[:REFERS_TO]->(:Document)`
- Metadata fields trong payload cần giữ ổn định (`doc_id`, `doc_number`, `doc_title`, `chunk_text`, `article`, `clause`) để `orchestrator/retrievers.py` truy hồi đúng.

## 6. Troubleshooting

- Throttling từ Gemini: giảm `GEMINI_EMBED_BATCH_SIZE`, tăng backoff.
- Sync lỗi Qdrant/Neo4j: kiểm tra API key, TLS URI, network egress.
- Chunk quality kém: điều chỉnh `CHUNK_TARGET_TOKENS`, `CHUNK_OVERLAP_TOKENS`.

## 7. Production Hardening

- Dùng secret scope thay vì plaintext env.
- Thêm idempotent MERGE cho incremental updates.
- Theo dõi throughput/error bằng MLflow hoặc hệ quan sát tương đương.
