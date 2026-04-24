from __future__ import annotations

import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence

try:
    from datasets import load_dataset  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional import guard
    load_dataset = None  # type: ignore[assignment]

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from chunking import RuleBasedHierarchicalChunker
from config import PlatformConfig
from gemini_embedding import GeminiEmbeddingClient
from html_to_text import html_to_text
from sinks import Neo4jAuraSink, QdrantSink


@dataclass(frozen=True)
class JobSettings:
    source_mode: str = "auto"  # auto | hf | json
    source_json_path: str = ""
    max_source_records: int = 0
    embed_flush_chunks: int = 200
    neo4j_doc_batch: int = 200
    neo4j_rel_batch: int = 500
    log_every_docs: int = 20

    @classmethod
    def from_env(cls) -> "JobSettings":
        return cls(
            source_mode=os.getenv("SOURCE_MODE", "auto").strip().lower() or "auto",
            source_json_path=os.getenv("SOURCE_JSON_PATH", "").strip(),
            max_source_records=_read_int("MAX_SOURCE_RECORDS", 0),
            embed_flush_chunks=max(1, _read_int("EMBED_FLUSH_CHUNKS", 200)),
            neo4j_doc_batch=max(1, _read_int("NEO4J_DOC_BATCH", 200)),
            neo4j_rel_batch=max(1, _read_int("NEO4J_REL_BATCH", 500)),
            log_every_docs=max(1, _read_int("LOG_EVERY_DOCS", 20)),
        )


def run() -> Dict[str, Any]:
    cfg = PlatformConfig.from_env()
    settings = JobSettings.from_env()

    if settings.max_source_records > 0:
        cfg = _replace_max_records(cfg, settings.max_source_records)

    if not cfg.sync_to_qdrant and not cfg.sync_to_neo4j:
        raise ValueError("Both sinks are disabled. Enable SYNC_TO_QDRANT and/or SYNC_TO_NEO4J.")

    cfg.validate_for_gold()

    chunker = RuleBasedHierarchicalChunker(
        target_tokens=cfg.chunk_target_tokens,
        max_tokens=cfg.chunk_max_tokens,
        overlap_tokens=cfg.chunk_overlap_tokens,
    )

    embed_client = GeminiEmbeddingClient(
        api_key=cfg.gemini_api_key,
        model_name=cfg.gemini_embedding_model,
        rpm_limit=cfg.gemini_rpm_limit,
        tpm_limit=cfg.gemini_tpm_limit,
        safety_factor=cfg.rate_limit_headroom,
        batch_size=cfg.gemini_embed_batch_size,
    )

    neo4j_sink: Optional[Neo4jAuraSink] = None
    if cfg.sync_to_neo4j:
        neo4j_sink = Neo4jAuraSink(
            uri=cfg.neo4j_uri,
            user=cfg.neo4j_user,
            password=cfg.neo4j_password,
            database=cfg.neo4j_database,
            batch_size=cfg.neo4j_batch_size,
        )
        neo4j_sink.ensure_constraints()

    qdrant_sink: Optional[QdrantSink] = None

    doc_buffer: List[Dict[str, Any]] = []
    relation_buffer: List[Dict[str, Any]] = []
    chunk_buffer: List[Dict[str, Any]] = []

    stats: Dict[str, int] = {
        "source_documents": 0,
        "processed_documents": 0,
        "skipped_documents": 0,
        "generated_chunks": 0,
        "embedded_chunks": 0,
        "qdrant_points": 0,
        "neo4j_documents": 0,
        "neo4j_chunks": 0,
        "neo4j_relations": 0,
    }

    try:
        for idx, source_doc in enumerate(iter_source_documents(cfg, settings)):
            stats["source_documents"] += 1

            prepared = prepare_document(source_doc, idx)
            if prepared is None:
                stats["skipped_documents"] += 1
                continue

            doc_row, raw_text, explicit_relations = prepared
            doc_buffer.append(doc_row)
            stats["processed_documents"] += 1

            chunks = chunker.chunk_document(
                doc_id=doc_row["doc_id"],
                doc_number=doc_row.get("doc_number") or "",
                doc_title=doc_row.get("title") or "",
                raw_text=raw_text,
                base_metadata={
                    "source_mode": settings.source_mode,
                    "dataset": cfg.hf_dataset_name,
                    "split": cfg.hf_split,
                },
            )

            mention_targets = set()
            for chunk in chunks:
                chunk_row = {
                    "chunk_uid": chunk.chunk_uid,
                    "doc_id": chunk.doc_id,
                    "doc_number": chunk.doc_number,
                    "doc_title": chunk.doc_title,
                    "chunk_order": int(chunk.chunk_order),
                    "chunk_text": chunk.chunk_text,
                    "token_count": int(chunk.token_count),
                    "chapter": chunk.chapter,
                    "section": chunk.section,
                    "article": chunk.article,
                    "clause": chunk.clause,
                    "point": chunk.point,
                    "metadata_json": _json_dumps(chunk.metadata),
                }
                chunk_buffer.append(chunk_row)
                stats["generated_chunks"] += 1
                mention_targets.update(chunk.references_to)

            relation_buffer.extend(
                build_relation_rows(
                    source_doc_id=doc_row["doc_id"],
                    source_doc_number=doc_row.get("doc_number") or "",
                    explicit_relations=explicit_relations,
                    mention_targets=mention_targets,
                )
            )

            if neo4j_sink is not None and len(doc_buffer) >= settings.neo4j_doc_batch:
                stats["neo4j_documents"] += neo4j_sink.upsert_documents(doc_buffer)
                doc_buffer.clear()

            if neo4j_sink is not None and len(relation_buffer) >= settings.neo4j_rel_batch:
                stats["neo4j_relations"] += neo4j_sink.upsert_relations(relation_buffer)
                relation_buffer.clear()

            if len(chunk_buffer) >= settings.embed_flush_chunks:
                qdrant_sink, flush_stats = flush_embedded_chunks(
                    chunk_buffer=chunk_buffer,
                    embed_client=embed_client,
                    qdrant_sink=qdrant_sink,
                    neo4j_sink=neo4j_sink,
                    cfg=cfg,
                )
                stats["embedded_chunks"] += flush_stats["embedded_chunks"]
                stats["qdrant_points"] += flush_stats["qdrant_points"]
                stats["neo4j_chunks"] += flush_stats["neo4j_chunks"]
                chunk_buffer.clear()

            if stats["processed_documents"] % settings.log_every_docs == 0:
                print(
                    "[progress] "
                    f"processed_docs={stats['processed_documents']} "
                    f"generated_chunks={stats['generated_chunks']} "
                    f"embedded_chunks={stats['embedded_chunks']}"
                )

        if neo4j_sink is not None and doc_buffer:
            stats["neo4j_documents"] += neo4j_sink.upsert_documents(doc_buffer)
            doc_buffer.clear()

        if neo4j_sink is not None and relation_buffer:
            stats["neo4j_relations"] += neo4j_sink.upsert_relations(relation_buffer)
            relation_buffer.clear()

        if chunk_buffer:
            qdrant_sink, flush_stats = flush_embedded_chunks(
                chunk_buffer=chunk_buffer,
                embed_client=embed_client,
                qdrant_sink=qdrant_sink,
                neo4j_sink=neo4j_sink,
                cfg=cfg,
            )
            stats["embedded_chunks"] += flush_stats["embedded_chunks"]
            stats["qdrant_points"] += flush_stats["qdrant_points"]
            stats["neo4j_chunks"] += flush_stats["neo4j_chunks"]
            chunk_buffer.clear()

        return stats

    finally:
        if neo4j_sink is not None:
            neo4j_sink.close()


def flush_embedded_chunks(
    chunk_buffer: Sequence[Dict[str, Any]],
    embed_client: GeminiEmbeddingClient,
    qdrant_sink: Optional[QdrantSink],
    neo4j_sink: Optional[Neo4jAuraSink],
    cfg: PlatformConfig,
) -> tuple[Optional[QdrantSink], Dict[str, int]]:
    texts = [str(row["chunk_text"]) for row in chunk_buffer]
    token_counts = [int(row.get("token_count") or 1) for row in chunk_buffer]
    vectors = embed_client.embed_texts(texts=texts, token_counts=token_counts)

    rows_with_embeddings: List[Dict[str, Any]] = []
    for row, vector in zip(chunk_buffer, vectors):
        out = dict(row)
        out["embedding"] = [float(value) for value in vector]
        rows_with_embeddings.append(out)

    stats = {
        "embedded_chunks": len(rows_with_embeddings),
        "qdrant_points": 0,
        "neo4j_chunks": 0,
    }

    if cfg.sync_to_qdrant:
        if qdrant_sink is None:
            vector_size = len(rows_with_embeddings[0]["embedding"])
            qdrant_sink = QdrantSink(
                url=cfg.qdrant_url,
                api_key=cfg.qdrant_api_key,
                collection_name=cfg.qdrant_collection,
                vector_size=vector_size,
                distance=cfg.qdrant_distance,
                batch_size=cfg.qdrant_batch_size,
            )
        stats["qdrant_points"] = qdrant_sink.upsert_embeddings(rows_with_embeddings)

    if neo4j_sink is not None:
        stats["neo4j_chunks"] = neo4j_sink.upsert_chunks(rows_with_embeddings)

    return qdrant_sink, stats


def iter_source_documents(cfg: PlatformConfig, settings: JobSettings) -> Iterator[Dict[str, Any]]:
    source_mode = settings.source_mode
    json_path = settings.source_json_path

    if source_mode in {"auto", "json"} and json_path:
        path = Path(json_path)
        if not path.exists() and source_mode == "json":
            raise FileNotFoundError(f"SOURCE_JSON_PATH not found: {path}")

        if path.exists():
            print(f"[source] reading JSON: {path}")
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)

            documents = payload.get("documents", []) if isinstance(payload, dict) else payload
            if not isinstance(documents, list):
                raise ValueError("JSON source must contain a list of documents")

            for idx, row in enumerate(documents):
                if settings.max_source_records > 0 and idx >= settings.max_source_records:
                    break
                if isinstance(row, dict):
                    yield row
            return

    if source_mode == "json":
        raise ValueError("SOURCE_MODE=json requires a valid SOURCE_JSON_PATH")

    if load_dataset is None:
        raise ImportError("datasets package is required for HuggingFace source mode")

    # ── Load auxiliary configs for joining ───────────────────────────────
    metadata_map, relations_map = _load_hf_auxiliary_configs(cfg)
    print(
        f"[source] loaded {len(metadata_map)} metadata rows, "
        f"{len(relations_map)} relation groups"
    )

    # ── Stream content config ────────────────────────────────────────────
    dataset_config = cfg.hf_dataset_config.strip()
    cache_dir = str(cfg.hf_cache_dir or "").strip()
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    load_kwargs: Dict[str, Any] = {
        "split": cfg.hf_split,
        "streaming": True,
    }
    if cache_dir:
        load_kwargs["cache_dir"] = cache_dir

    if dataset_config:
        print(
            "[source] reading HuggingFace: "
            f"{cfg.hf_dataset_name}/{dataset_config} ({cfg.hf_split})"
        )
        dataset = load_dataset(
            cfg.hf_dataset_name,
            dataset_config,
            **load_kwargs,
        )
    else:
        print(f"[source] reading HuggingFace: {cfg.hf_dataset_name} ({cfg.hf_split})")
        dataset = load_dataset(cfg.hf_dataset_name, **load_kwargs)

    for idx, row in enumerate(dataset):
        if settings.max_source_records > 0 and idx >= settings.max_source_records:
            break
        if isinstance(row, dict):
            yield _join_hf_row(row, metadata_map, relations_map)


def prepare_document(
    source_doc: Dict[str, Any],
    index: int,
) -> Optional[tuple[Dict[str, Any], str, List[Dict[str, Any]]]]:
    doc_number = str(_pick_first(source_doc, ["so_ky_hieu", "doc_number", "so_hieu"], "")).strip()
    title = str(_pick_first(source_doc, ["title", "ten_van_ban"], "")).strip()
    doc_type = str(_pick_first(source_doc, ["loai_van_ban", "doc_type"], "")).strip()
    issuing_body = str(_pick_first(source_doc, ["co_quan_ban_hanh", "issuing_body"], "")).strip()
    issued_date = str(_pick_first(source_doc, ["ngay_ban_hanh", "issued_date"], "")).strip()
    status = str(_pick_first(source_doc, ["tinh_trang_hieu_luc", "status"], "active")).strip()

    doc_id = str(_pick_first(source_doc, ["doc_id", "id"], "")).strip()
    if not doc_id:
        doc_id = make_doc_id(doc_number=doc_number, title=title, index=index)

    # ── Extract content text ────────────────────────────────────────────
    # Priority: content_text > content_html > raw chunks
    content_text = str(source_doc.get("content_text") or "").strip()

    if not content_text:
        raw_html = source_doc.get("content_html")
        if raw_html:
            content_text = html_to_text(str(raw_html))

    raw_chunks = source_doc.get("chunks")
    if not isinstance(raw_chunks, list):
        raw_chunks = []

    if not content_text:
        content_text = compose_document_text(raw_chunks)

    if not content_text:
        return None

    doc_row = {
        "doc_id": doc_id,
        "doc_number": doc_number,
        "title": title,
        "doc_type": doc_type,
        "issuing_body": issuing_body,
        "issued_date": issued_date,
        "status": status,
    }

    explicit_relations = source_doc.get("relations")
    if not isinstance(explicit_relations, list):
        explicit_relations = []

    return doc_row, content_text, explicit_relations


def compose_document_text(raw_chunks: Sequence[Dict[str, Any]]) -> str:
    if not raw_chunks:
        return ""

    lines: List[str] = []
    previous_chapter = ""
    previous_article = ""

    for chunk in raw_chunks:
        if not isinstance(chunk, dict):
            continue

        chapter = str(chunk.get("chapter") or "").strip()
        article = str(chunk.get("article") or "").strip()
        clause = str(chunk.get("clause") or "").strip()
        content = str(chunk.get("content") or "").strip()

        if chapter and chapter != previous_chapter:
            lines.append(chapter)
            previous_chapter = chapter

        if article and article != previous_article:
            lines.append(article)
            previous_article = article

        if clause:
            lines.append(clause)

        if content:
            lines.append(content)

    return "\n".join(lines).strip()


def build_relation_rows(
    source_doc_id: str,
    source_doc_number: str,
    explicit_relations: Sequence[Dict[str, Any]],
    mention_targets: Iterable[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for relation in explicit_relations:
        if not isinstance(relation, dict):
            continue

        target_number = _pick_first(
            relation,
            ["target_so_ky_hieu", "target_doc_number", "target_doc_id"],
            "",
        )
        relation_type = _pick_first(relation, ["type", "relation_type"], "REFERS_TO")
        if not target_number:
            continue

        rows.append(
            {
                "source_doc_id": source_doc_id,
                "source_doc_number": source_doc_number,
                "target_doc_number": str(target_number),
                "relation_type": str(relation_type),
                "relation_source": "explicit_relation",
            }
        )

    for target in mention_targets:
        rows.append(
            {
                "source_doc_id": source_doc_id,
                "source_doc_number": source_doc_number,
                "target_doc_number": str(target),
                "relation_type": "MENTIONS",
                "relation_source": "chunk_reference",
            }
        )

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (
            row["source_doc_id"],
            row["target_doc_number"],
            row["relation_type"],
            row["relation_source"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def make_doc_id(doc_number: str, title: str, index: int) -> str:
    seed = (doc_number or title or f"doc-{index}").strip()
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]
    normalized = "".join(char if char.isalnum() else "_" for char in seed)
    normalized = "_".join(part for part in normalized.split("_") if part)
    normalized = normalized[:40] if normalized else "document"
    return f"{normalized}_{digest}".lower()


def _load_hf_auxiliary_configs(
    cfg: PlatformConfig,
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Load ``metadata`` and ``relationships`` HF configs into memory dicts.

    Returns:
        metadata_map:  {str(id) -> {field: value, ...}}
        relations_map: {str(doc_id) -> [{other_doc_id, relationship}, ...]}
    """
    if load_dataset is None:
        print("[warn] datasets package not available; skipping auxiliary config loading")
        return {}, {}

    cache_dir = str(cfg.hf_cache_dir or "").strip()
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    base_kwargs: Dict[str, Any] = {"streaming": True}
    if cache_dir:
        base_kwargs["cache_dir"] = cache_dir

    metadata_map: Dict[str, Dict[str, Any]] = {}
    relations_map: Dict[str, List[Dict[str, Any]]] = {}

    # ── metadata ────────────────────────────────────────────────────────
    try:
        meta_ds = load_dataset(
            cfg.hf_dataset_name, "metadata", split="data", **base_kwargs
        )
        for row in meta_ds:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("id", "")).strip()
            if doc_id:
                metadata_map[doc_id] = row
    except Exception as exc:
        print(f"[warn] Failed to load metadata config: {exc}")

    # ── relationships ───────────────────────────────────────────────────
    try:
        rels_ds = load_dataset(
            cfg.hf_dataset_name, "relationships", split="data", **base_kwargs
        )
        for row in rels_ds:
            if not isinstance(row, dict):
                continue
            doc_id = str(row.get("doc_id", "")).strip()
            if doc_id:
                relations_map.setdefault(doc_id, []).append(row)
    except Exception as exc:
        print(f"[warn] Failed to load relationships config: {exc}")

    return metadata_map, relations_map


def _join_hf_row(
    content_row: Dict[str, Any],
    metadata_map: Dict[str, Dict[str, Any]],
    relations_map: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """Merge a ``content`` row with matching ``metadata`` and ``relationships``."""
    doc_id = str(content_row.get("id", "")).strip()
    joined: Dict[str, Any] = dict(content_row)

    # Merge metadata fields (title, so_ky_hieu, co_quan_ban_hanh, etc.)
    meta = metadata_map.get(doc_id)
    if meta:
        for key, value in meta.items():
            if key == "id":
                continue
            if key not in joined or joined[key] is None:
                joined[key] = value

    # Attach relationships as "relations" list
    rels = relations_map.get(doc_id, [])
    joined_relations: List[Dict[str, Any]] = []
    for rel in rels:
        target = str(rel.get("other_doc_id", "")).strip()
        rel_type = str(rel.get("relationship", "REFERS_TO")).strip()
        if target:
            target_meta = metadata_map.get(target)
            target_doc_number = (
                str(target_meta.get("so_ky_hieu", "")).strip()
                if target_meta
                else ""
            )
            joined_relations.append({
                "target_doc_id": target,
                "target_so_ky_hieu": target_doc_number,
                "type": rel_type,
            })
    if joined_relations:
        joined["relations"] = joined_relations

    return joined


def _replace_max_records(cfg: PlatformConfig, max_source_records: int) -> PlatformConfig:
    return PlatformConfig(
        catalog=cfg.catalog,
        schema=cfg.schema,
        bronze_documents_table=cfg.bronze_documents_table,
        silver_documents_table=cfg.silver_documents_table,
        silver_chunks_table=cfg.silver_chunks_table,
        silver_relations_table=cfg.silver_relations_table,
        gold_embeddings_table=cfg.gold_embeddings_table,
        hf_dataset_name=cfg.hf_dataset_name,
        hf_split=cfg.hf_split,
        source_batch_size=cfg.source_batch_size,
        max_source_records=max_source_records,
        chunk_target_tokens=cfg.chunk_target_tokens,
        chunk_max_tokens=cfg.chunk_max_tokens,
        chunk_overlap_tokens=cfg.chunk_overlap_tokens,
        gemini_api_key=cfg.gemini_api_key,
        gemini_embedding_model=cfg.gemini_embedding_model,
        gemini_embed_batch_size=cfg.gemini_embed_batch_size,
        gemini_rpm_limit=cfg.gemini_rpm_limit,
        gemini_tpm_limit=cfg.gemini_tpm_limit,
        rate_limit_headroom=cfg.rate_limit_headroom,
        sync_to_qdrant=cfg.sync_to_qdrant,
        qdrant_url=cfg.qdrant_url,
        qdrant_api_key=cfg.qdrant_api_key,
        qdrant_collection=cfg.qdrant_collection,
        qdrant_vector_size=cfg.qdrant_vector_size,
        qdrant_distance=cfg.qdrant_distance,
        qdrant_batch_size=cfg.qdrant_batch_size,
        sync_to_neo4j=cfg.sync_to_neo4j,
        neo4j_uri=cfg.neo4j_uri,
        neo4j_user=cfg.neo4j_user,
        neo4j_password=cfg.neo4j_password,
        neo4j_database=cfg.neo4j_database,
        neo4j_batch_size=cfg.neo4j_batch_size,
    )


def _pick_first(row: Dict[str, Any], keys: Sequence[str], default: Any = "") -> Any:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _read_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def main() -> None:
    summary = run()
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
