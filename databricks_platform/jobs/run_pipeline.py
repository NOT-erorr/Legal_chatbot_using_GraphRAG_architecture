from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parents[1]
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
