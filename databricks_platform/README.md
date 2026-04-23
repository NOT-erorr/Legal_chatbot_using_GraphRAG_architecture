# Databricks ETL + Lakehouse Platform for VN Legal Data

This module adds a production-oriented Lakehouse ETL platform on Databricks for Vietnamese legal documents.

## 1) Target architecture (microservice-style)

The platform is split into task-level services:

1. Ingestion Service (Bronze)
- Source: HuggingFace dataset `th1nhng0/vietnamese-legal-documents`
- Output table: `main.law_lakehouse.bronze_legal_documents`
- Persist raw payload + normalized base fields.

2. Transform Service (Silver)
- Tokenization + Rule-based Hierarchical Chunking.
- Structural chunk boundaries: `chuong -> muc -> dieu -> khoan -> diem`.
- Output tables:
  - `main.law_lakehouse.silver_legal_documents`
  - `main.law_lakehouse.silver_legal_chunks`
  - `main.law_lakehouse.silver_legal_relations`

3. Embedding Service (Gold)
- Model: `gemini-embedding-001`.
- Optimized with dual-limit throttle:
  - RPM guard
  - TPM guard
  - batch sizing by token budget
  - retry with exponential backoff for 429/quota pressure
- Output table: `main.law_lakehouse.gold_legal_chunk_embeddings`

4. Vector Sync Service
- Sink: Qdrant collection (upsert vectors + metadata payload)

5. Graph Sync Service
- Sink: Neo4j AuraDB
- Graph model: `(:Document)-[:HAS_CHUNK]->(:Chunk)` and `(:Document)-[:REFERS_TO]->(:Document)`

## 2) Folder map

- `src/config.py`: platform config from environment variables
- `src/chunking.py`: tokenizer + structural hierarchical chunking
- `src/rate_limit.py`: RPM/TPM limiter
- `src/gemini_embedding.py`: Gemini embedding client + retry/throttle
- `src/sinks.py`: Qdrant + Neo4j AuraDB connectors
- `src/lakehouse_pipeline.py`: Bronze/Silver/Gold orchestration
- `jobs/run_pipeline.py`: entrypoint by stage (`bronze|silver|gold|all`)
- `databricks.yml`: Databricks Asset Bundle workflow

## 3) Required environment variables

Set these as Databricks job environment variables or secret-backed values:

```env
DBX_CATALOG=main
DBX_SCHEMA=law_lakehouse

HF_DATASET_NAME=th1nhng0/vietnamese-legal-documents
HF_DATASET_SPLIT=train
MAX_SOURCE_RECORDS=0
SOURCE_BATCH_SIZE=500

CHUNK_TARGET_TOKENS=220
CHUNK_MAX_TOKENS=300
CHUNK_OVERLAP_TOKENS=30

GEMINI_API_KEY=<from secret scope>
GEMINI_EMBEDDING_MODEL=gemini-embedding-001
GEMINI_RPM_LIMIT=3000
GEMINI_TPM_LIMIT=1000000
RATE_LIMIT_HEADROOM=0.85
GEMINI_EMBED_BATCH_SIZE=32

SYNC_TO_QDRANT=true
QDRANT_URL=https://<qdrant-host>
QDRANT_API_KEY=<from secret scope>
QDRANT_COLLECTION=vn_legal_chunks
QDRANT_VECTOR_SIZE=3072
QDRANT_DISTANCE=Cosine

SYNC_TO_NEO4J=true
NEO4J_URI=neo4j+s://<aura-host>
NEO4J_USER=neo4j
NEO4J_PASSWORD=<from secret scope>
NEO4J_DATABASE=neo4j
```

## 4) Deploy and run in Databricks

From this folder:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run vn_legal_lakehouse_etl -t dev
```

## 5) Run one stage only

```bash
python jobs/run_pipeline.py --stage bronze
python jobs/run_pipeline.py --stage silver
python jobs/run_pipeline.py --stage gold
python jobs/run_pipeline.py --stage all
```

Optional controls:

```bash
python jobs/run_pipeline.py --stage bronze --max-source-records 1000
python jobs/run_pipeline.py --stage gold --disable-qdrant
python jobs/run_pipeline.py --stage gold --disable-neo4j
```

## 6) Notes for production hardening

1. For very large corpora, convert Silver chunking to distributed `mapInPandas` or Spark UDF partitions.
2. Add idempotent MERGE logic if you need incremental CDC updates instead of overwrite in Silver/Gold.
3. Put all secrets in Databricks Secret Scope and avoid plaintext env values.
4. Add MLflow tracking for embedding throughput, errors, and vector drift metrics.
