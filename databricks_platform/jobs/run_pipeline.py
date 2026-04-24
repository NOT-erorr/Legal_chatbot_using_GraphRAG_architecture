from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _resolve_root_dir() -> Path:
    # Databricks serverless may execute this file via exec() without __file__.
    file_hint = globals().get("__file__") or globals().get("filename")
    if file_hint:
        script_path = Path(str(file_hint)).resolve()
        if script_path.parent.name == "jobs":
            return script_path.parent.parent
        return script_path.parent

    cwd = Path.cwd().resolve()
    search_roots = [cwd, *list(cwd.parents)[:4]]
    for candidate in search_roots:
        if (candidate / "jobs").exists() and (candidate / "src").exists():
            return candidate

    return cwd


ROOT_DIR = _resolve_root_dir()

# ── Manually load .env to avoid python-dotenv dependency issues ────────
_env_file = ROOT_DIR / ".env"
if _env_file.exists():
    try:
        with _env_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip("'").strip('"')
                    if key and not os.environ.get(key):
                        os.environ[key] = value
        print(f"[info] Successfully loaded local variables from {_env_file.name}")
    except Exception as e:
        print(f"[warn] Failed to parse .env file: {e}")
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import PlatformConfig
from lakehouse_pipeline import LakehousePipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Databricks Lakehouse ETL for Vietnamese legal corpus"
    )
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline stage to execute",
    )
    parser.add_argument(
        "--max-source-records",
        type=int,
        default=None,
        help="Optional limit for source records during bronze ingest",
    )
    parser.add_argument(
        "--disable-qdrant",
        action="store_true",
        help="Skip Qdrant sync in gold stage",
    )
    parser.add_argument(
        "--disable-neo4j",
        action="store_true",
        help="Skip Neo4j sync in gold stage",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.max_source_records is not None:
        os.environ["MAX_SOURCE_RECORDS"] = str(args.max_source_records)
    if args.disable_qdrant:
        os.environ["SYNC_TO_QDRANT"] = "false"
    if args.disable_neo4j:
        os.environ["SYNC_TO_NEO4J"] = "false"

    # Attempt to securely load Databricks secrets for the Gold stage
    if args.stage in ("gold", "all") and not os.environ.get("GEMINI_API_KEY"):
        try:
            from databricks.sdk.runtime import dbutils # type: ignore
            key = dbutils.secrets.get(scope="my_secrets", key="gemini_api_key")
            if key:
                os.environ["GEMINI_API_KEY"] = key
                print("[info] Loaded GEMINI_API_KEY from Databricks Secrets (my_secrets).")
        except Exception as e:
            print(f"[warn] Failed to load GEMINI_API_KEY from secrets: {e}")

    spark = SparkSession.builder.getOrCreate()
    config = PlatformConfig.from_env()
    pipeline = LakehousePipeline(spark=spark, config=config)

    if args.stage == "bronze":
        summary = pipeline.run_bronze_ingestion()
    elif args.stage == "silver":
        summary = pipeline.run_silver_transform()
    elif args.stage == "gold":
        summary = pipeline.run_gold_embeddings_and_sync()
    else:
        summary = pipeline.run_all()

    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
