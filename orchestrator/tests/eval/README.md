# Đánh giá Orchestrator bằng RAGAS

Đo chất lượng pipeline GraphRAG (`orchestrator/graph.py`) trên bộ câu hỏi luật thuế VN
bằng [RAGAS](https://docs.ragas.io), dùng **Gemini** làm LLM judge.

## Cách chạy

```bash
# 1. Cài dependency runtime (qdrant-client, neo4j, google-genai, langgraph...)
#    + dev (ragas, langchain-google-genai). Eval chạy pipeline thật nên cần cả hai.
pip install -r orchestrator/requirements.txt
pip install -r orchestrator/requirements-dev.txt

# 2. Đảm bảo credentials có trong .env.production (root) hoặc orchestrator/.env:
#    GEMINI_API_KEY, QDRANT_URL, QDRANT_API_KEY,
#    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# 3. Chạy từ project root
python orchestrator/tests/eval/eval_ragas.py
```

> Nên dùng **venv riêng**: bộ dev pin langchain 0.3.x (ragas 0.2.x cần), tránh
> đụng các project khác. Free-tier Gemini có thể trả **429 (rate limit)** khi chạy
> 15 câu liên tục → một số điểm có thể thành `nan`; chạy lại hoặc giãn thời gian.

> ⚠️ Eval gọi **API thật** (Gemini) và cần **Qdrant Cloud + Neo4j Aura sống**.
> Mỗi lần chạy đầy đủ 15 câu sẽ tốn quota Gemini (pipeline + LLM judge).
> Để smoke test rẻ, tạm bớt số câu trong `eval_dataset.json`.

## Kết quả

- Bảng điểm in ra console (`RAGAS EVALUATION REPORT`).
- File chi tiết từng câu: `orchestrator/tests/eval/ragas_results.csv`.

## Metrics (reference-free — không cần đáp án chuẩn)

| Metric | Ý nghĩa |
| --- | --- |
| **Faithfulness** | Câu trả lời có bịa thông tin ngoài ngữ cảnh retrieved không? (chống hallucination) |
| **Answer Relevancy** | Câu trả lời có đúng trọng tâm câu hỏi không? (dùng embeddings) |
| **Context Precision** | Các chunk lấy về có thực sự liên quan không? |

## Nâng lên đánh giá có ground truth (5 metric)

Thêm field `ground_truth` (đáp án chuẩn) cho mỗi câu trong `eval_dataset.json`:

```json
{ "id": "q001", "question": "...", "category": "gtgt", "ground_truth": "Thuế suất 0%..." }
```

Khi **mọi** câu đều có `ground_truth`, script tự bật thêm:
- **Context Recall** — ngữ cảnh có bao phủ đủ thông tin trong đáp án chuẩn không?
- **Answer Correctness** — câu trả lời có khớp đáp án chuẩn không?
