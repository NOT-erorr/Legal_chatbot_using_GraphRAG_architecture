# Cloud Run temporary direct sync job

This folder contains a one-shot Python job that pushes Vietnamese legal data directly to Qdrant and Neo4j AuraDB.

## What this job does

1. Reads source documents from one of two sources:
- HuggingFace dataset: th1nhng0/vietnamese-legal-documents
- Local JSON mounted into the container

2. Applies transform before indexing:
- Rule-based hierarchical chunking (chapter -> section -> article -> clause -> point)
- Token counting

3. Calls Gemini embedding model:
- Model: gemini-embedding-001
- Built-in RPM/TPM limiter and retry strategy

4. Pushes data to targets directly:
- Qdrant: vectors + payload metadata
- Neo4j AuraDB: Document, Chunk, REFERS_TO graph

## Required environment variables

- GEMINI_API_KEY
- QDRANT_URL
- QDRANT_API_KEY
- QDRANT_COLLECTION (default: vn_legal_chunks)
- NEO4J_URI
- NEO4J_USER
- NEO4J_PASSWORD

## Optional environment variables

- SOURCE_MODE=auto|hf|json (default: auto)
- SOURCE_JSON_PATH=/workspace/data.json
- HF_DATASET_NAME=th1nhng0/vietnamese-legal-documents
- HF_DATASET_SPLIT=train
- MAX_SOURCE_RECORDS=0
- CHUNK_TARGET_TOKENS=220
- CHUNK_MAX_TOKENS=300
- CHUNK_OVERLAP_TOKENS=30
- GEMINI_RPM_LIMIT=3000
- GEMINI_TPM_LIMIT=1000000
- RATE_LIMIT_HEADROOM=0.85
- GEMINI_EMBED_BATCH_SIZE=32
- EMBED_FLUSH_CHUNKS=200
- NEO4J_DOC_BATCH=200
- NEO4J_REL_BATCH=500

## Build image

From databricks_platform folder:

- gcloud builds submit --config cloud_run_job/cloudbuild.yaml .

## Create Cloud Run Job

- gcloud run jobs create vn-legal-direct-sync \
  --image gcr.io/PROJECT_ID/vn-legal-direct-sync:latest \
  --region REGION \
  --task-timeout 3600 \
  --max-retries 0 \
  --memory 2Gi \
  --cpu 1 \
  --set-env-vars SOURCE_MODE=hf,HF_DATASET_NAME=th1nhng0/vietnamese-legal-documents,HF_DATASET_SPLIT=train,MAX_SOURCE_RECORDS=200,GEMINI_RPM_LIMIT=3000,GEMINI_TPM_LIMIT=1000000,QDRANT_COLLECTION=vn_legal_chunks \
  --set-secrets GEMINI_API_KEY=GEMINI_API_KEY:latest,QDRANT_API_KEY=QDRANT_API_KEY:latest,NEO4J_PASSWORD=NEO4J_PASSWORD:latest \
  --set-env-vars QDRANT_URL=https://YOUR_QDRANT_HOST,NEO4J_URI=neo4j+s://YOUR_AURA_HOST,NEO4J_USER=neo4j

## Execute once (temporary run)

- gcloud run jobs execute vn-legal-direct-sync --region REGION --wait

## Update existing job image

- gcloud run jobs update vn-legal-direct-sync \
  --image gcr.io/PROJECT_ID/vn-legal-direct-sync:latest \
  --region REGION
