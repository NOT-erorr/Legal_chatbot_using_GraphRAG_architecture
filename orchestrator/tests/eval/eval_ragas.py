"""RAGAS evaluation script cho Vietnamese Legal GraphRAG system.

Chạy:
    cd khoa_branch
    python orchestrator/tests/eval/eval_ragas.py

Metrics (reference-free — không cần ground truth):
    - Faithfulness          : answer có bịa thông tin ngoài context không?
    - AnswerRelevancy       : answer có trả lời đúng câu hỏi không?
    - ContextPrecision      : chunks retrieved có thực sự liên quan không?

Mở rộng lên Phase 2 (có ground truth):
    Thêm field "ground_truth" vào eval_dataset.json —
    script sẽ tự động thêm LLMContextRecall và AnswerCorrectness.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# Windows console mặc định cp1252 → in tiếng Việt sẽ UnicodeEncodeError.
# Ép stdout/stderr sang UTF-8 (no-op trên *nix hoặc khi đã là utf-8).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass

# --- resolve project root so imports work when run from any directory ---
_HERE = Path(__file__).resolve().parent          # .../orchestrator/tests/eval
_ORCHESTRATOR_ROOT = _HERE.parents[1]            # .../orchestrator
_PROJECT_ROOT = _HERE.parents[2]                 # project root (GraphRAG_Legal)
# graph.py/retrievers.py dùng import tuyệt đối `orchestrator.*` → project root phải
# nằm trên sys.path.
for _p in [str(_PROJECT_ROOT), str(_ORCHESTRATOR_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from dotenv import load_dotenv

# Ưu tiên .env.production ở project root; fallback orchestrator/.env.
for _env_file in (_PROJECT_ROOT / ".env.production", _ORCHESTRATOR_ROOT / ".env"):
    if _env_file.exists():
        load_dotenv(_env_file)
        break

# --- third-party imports (installed via requirements-dev.txt) ---
try:
    from ragas import EvaluationDataset, SingleTurnSample, evaluate
    from ragas.llms import LangchainLLMWrapper
    from ragas.metrics import (
        AnswerRelevancy,
        Faithfulness,
        LLMContextPrecisionWithoutReference,
    )
    from ragas.metrics import LLMContextRecall, AnswerCorrectness  # phase-2
    from ragas.embeddings import LangchainEmbeddingsWrapper
    from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
except ImportError as exc:
    print(
        f"[error] Missing dependency: {exc}\n"
        "Run: pip install -r orchestrator/requirements-dev.txt"
    )
    sys.exit(1)

# --- project imports ---
from orchestrator.config import OrchestratorConfig
from orchestrator.graph import build_graph, invoke_graph


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATASET_PATH = _HERE / "eval_dataset.json"
_OUTPUT_CSV = _HERE / "ragas_results.csv"


def _load_dataset(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _run_pipeline(compiled_graph, item: dict, top_k: int = 5) -> dict:
    """Call invoke_graph and return answer + chunks for one question."""
    return invoke_graph(
        compiled_graph=compiled_graph,
        question=item["question"],
        top_k_vector=top_k,
        top_k_graph=top_k,
        return_chunks=True,
    )


def _build_ragas_dataset(
    compiled_graph,
    questions: list[dict],
    top_k: int = 5,
) -> tuple[EvaluationDataset, list[dict]]:
    """Run pipeline for every question and wrap results in RAGAS format."""
    samples: list[SingleTurnSample] = []
    run_records: list[dict] = []

    print(f"\nChạy pipeline cho {len(questions)} câu hỏi...")
    for i, item in enumerate(questions, start=1):
        q_id = item.get("id", f"q{i:03d}")
        category = item.get("category", "")
        print(f"  [{i:02d}/{len(questions)}] {q_id} ({category}) ...", end=" ", flush=True)

        t0 = time.perf_counter()
        try:
            result = _run_pipeline(compiled_graph, item, top_k=top_k)
        except Exception as exc:
            print(f"LỖI: {exc}")
            continue

        elapsed = round((time.perf_counter() - t0) * 1000)
        chunks = result.get("vector_chunks", [])
        contexts = [
            c.chunk_text for c in chunks if hasattr(c, "chunk_text") and c.chunk_text
        ]
        answer = result.get("answer", "")

        print(f"OK ({elapsed}ms, {len(contexts)} chunks)")

        sample_kwargs: dict = dict(
            user_input=item["question"],
            response=answer,
            retrieved_contexts=contexts if contexts else ["(không tìm thấy ngữ cảnh)"],
        )
        if "ground_truth" in item and item["ground_truth"]:
            sample_kwargs["reference"] = item["ground_truth"]

        samples.append(SingleTurnSample(**sample_kwargs))
        run_records.append(
            {
                "id": q_id,
                "category": category,
                "question": item["question"],
                "answer": answer,
                "num_chunks": len(contexts),
                "pipeline_ms": elapsed,
                "ground_truth": item.get("ground_truth", ""),
            }
        )

    return EvaluationDataset(samples=samples), run_records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # 1. Load config & build graph
    print("Nạp cấu hình và khởi tạo pipeline (Qdrant + Neo4j)...")
    try:
        cfg = OrchestratorConfig.from_env()
        compiled_graph = build_graph(cfg)
        print("Pipeline sẵn sàng.")
    except Exception as exc:
        print(f"[error] Không thể khởi tạo pipeline: {exc}")
        sys.exit(1)

    # 2. Configure RAGAS LLM judge (Gemini)
    judge_model = cfg.gemini_chat_model
    llm = LangchainLLMWrapper(
        ChatGoogleGenerativeAI(
            model=judge_model,
            temperature=0,
            google_api_key=cfg.gemini_api_key,
        )
    )
    # AnswerRelevancy cần embeddings (cosine similarity giữa câu hỏi gốc và câu hỏi
    # sinh ngược từ answer). Không truyền → RAGAS mặc định gọi OpenAI → fail.
    # Dùng cùng embedding model với pipeline (gemini-embedding-001); langchain yêu
    # cầu tiền tố "models/".
    embed_model = cfg.gemini_embedding_model
    if not embed_model.startswith("models/"):
        embed_model = f"models/{embed_model}"
    embeddings = LangchainEmbeddingsWrapper(
        GoogleGenerativeAIEmbeddings(
            model=embed_model,
            google_api_key=cfg.gemini_api_key,
        )
    )

    # 3. Load dataset
    questions = _load_dataset(_DATASET_PATH)
    has_ground_truth = all("ground_truth" in q and q["ground_truth"] for q in questions)

    # 4. Define metrics
    base_metrics = [
        Faithfulness(llm=llm),
        AnswerRelevancy(llm=llm, embeddings=embeddings),
        LLMContextPrecisionWithoutReference(llm=llm),
    ]
    extended_metrics = [
        LLMContextRecall(llm=llm),
        AnswerCorrectness(llm=llm),
    ]
    metrics = base_metrics + (extended_metrics if has_ground_truth else [])

    print(f"\nMetrics: {[type(m).__name__ for m in metrics]}")
    if has_ground_truth:
        print("Ground truth detected — chạy full evaluation (5 metrics).")
    else:
        print("Không có ground truth — chạy reference-free evaluation (3 metrics).")

    # 5. Run pipeline + build RAGAS dataset
    ragas_dataset, run_records = _build_ragas_dataset(compiled_graph, questions)

    if not ragas_dataset.samples:
        print("[error] Không có sample nào thành công. Kiểm tra kết nối Qdrant/Neo4j.")
        sys.exit(1)

    # 6. Evaluate
    print(f"\nĐánh giá với RAGAS (LLM judge: {judge_model})...")
    try:
        results = evaluate(dataset=ragas_dataset, metrics=metrics, raise_exceptions=False)
    except Exception as exc:
        print(f"[error] RAGAS evaluate thất bại: {exc}")
        sys.exit(1)

    # 7. Console report
    scores: dict[str, float] = {}
    metric_names = {
        "faithfulness": "Faithfulness",
        "answer_relevancy": "Answer Relevancy",
        "llm_context_precision_without_reference": "Context Precision",
        "context_recall": "Context Recall",
        "answer_correctness": "Answer Correctness",
    }

    print("\n" + "=" * 52)
    print("   RAGAS EVALUATION REPORT")
    print("=" * 52)
    print(f"   Tổng câu hỏi  : {len(ragas_dataset.samples)}")
    print(f"   LLM Judge     : {judge_model}")
    print(f"   Dataset       : {_DATASET_PATH.name}")
    print("-" * 52)
    print(f"   {'Metric':<35} {'Score':>6}")
    print(f"   {'-'*35} {'-'*6}")

    results_dict = results._repr_dict if hasattr(results, "_repr_dict") else {}
    if not results_dict:
        # fallback: try to_pandas
        try:
            df = results.to_pandas()
            for col in df.columns:
                if col in metric_names:
                    results_dict[col] = float(df[col].mean())
        except Exception:
            pass

    for key, label in metric_names.items():
        if key in results_dict:
            score = float(results_dict[key])
            scores[key] = score
            print(f"   {label:<35} {score:>6.3f}")

    if scores:
        avg = sum(scores.values()) / len(scores)
        print(f"   {'-'*35} {'-'*6}")
        print(f"   {'Average':<35} {avg:>6.3f}")
    else:
        print("   [warn] Không đọc được điểm metric nào từ kết quả RAGAS.")
        print(f"   [debug] keys nhận được: {list(results_dict.keys())}")

    print("=" * 52)

    # 8. Save CSV
    try:
        df = results.to_pandas()
        # Prepend pipeline metadata columns
        import pandas as pd
        meta_df = pd.DataFrame(run_records)[
            ["id", "category", "question", "num_chunks", "pipeline_ms"]
        ]
        combined = pd.concat(
            [meta_df.reset_index(drop=True), df.reset_index(drop=True)], axis=1
        )
        combined.to_csv(_OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n   Results saved: {_OUTPUT_CSV}")
    except Exception as exc:
        print(f"\n   [warn] Không thể lưu CSV: {exc}")

    print("=" * 52 + "\n")


if __name__ == "__main__":
    main()
