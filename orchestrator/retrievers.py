"""
orchestrator/retrievers.py — Optimized Hybrid Search (v2)

Thay đổi so với v1:
  ┌─────────────────────────────────────────────────────────────────┐
  │ 1. TRUE PARALLEL  — Qdrant + Neo4j chạy đồng thời (asyncio)    │
  │ 2. RRF FUSION     — Reciprocal Rank Fusion merge 3 sources      │
  │ 3. KEYWORD SEARCH — Neo4j fulltext index song song vector       │
  │ 4. TYPE FIX       — expand_from_doc_ids trả List[ChunkResult]   │
  │ 5. EMBED CACHE    — LRU cache tránh gọi Gemini API lặp lại      │
  │ 6. CONN POOL      — Neo4j driver singleton với connection pool   │
  └─────────────────────────────────────────────────────────────────┘

Flow:
  query ──┬──► embed_query() ──► QdrantRetriever.search()     ─┐
          ├──► Neo4jRetriever.keyword_search()                  ├──► rrf_fusion() ──► List[ChunkResult]
          └──► (doc_ids từ Qdrant) ──► Neo4jRetriever.expand() ─┘
               (chạy sau khi Qdrant xong, không block keyword search)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from orchestrator.config import OrchestratorConfig

logger = logging.getLogger(__name__)


# Data model

@dataclass
class ChunkResult:
    """Kết quả thống nhất từ mọi retriever source."""
    chunk_uid: str
    doc_id: str
    doc_number: str
    doc_title: str
    chunk_text: str
    chunk_order: int = 0
    chapter: Optional[str] = None
    section: Optional[str] = None
    article: Optional[str] = None
    clause: Optional[str] = None
    point: Optional[str] = None
    token_count: int = 0
    # Scores — mỗi source điền phần của mình
    vector_score: float = 0.0     # cosine similarity từ Qdrant (0–1)
    keyword_score: float = 0.0    # BM25/fulltext score từ Neo4j (0–1, normalized)
    graph_score: float = 0.0      # proximity score từ graph traversal (0–1)
    rrf_score: float = 0.0        # Reciprocal Rank Fusion — score tổng hợp cuối
    sources: List[str] = field(default_factory=list)  # ["qdrant", "neo4j_keyword", ...]


@dataclass
class HybridSearchResult:
    chunks: List[ChunkResult]
    vector_count: int
    keyword_count: int
    graph_count: int
    total_unique: int
    elapsed_ms: float
    alpha: float  # trọng số vector vs keyword đã dùng


# Embedding client — singleton + LRU cache

class EmbeddingClient:
    """
    Wrapper cho Gemini embedding API.
    - Singleton per api_key+model để tái dùng HTTP connection
    - LRU cache 512 entries để tránh re-embed cùng query
    """

    _instances: Dict[str, "EmbeddingClient"] = {}

    def __new__(cls, api_key: str, model: str) -> "EmbeddingClient":
        key = f"{api_key[:8]}:{model}"
        if key not in cls._instances:
            instance = super().__new__(cls)
            instance._initialized = False
            cls._instances[key] = instance
        return cls._instances[key]

    def __init__(self, api_key: str, model: str) -> None:
        if self._initialized:
            return
        self.api_key = api_key
        self.model = model
        self._client: Any = None
        self._use_new_sdk = False
        self._initialized = True

    def _ensure_client(self) -> None:
        if self._client is not None:
            return
        try:
            from google import genai
            self._client = genai.Client(api_key=self.api_key)
            self._use_new_sdk = True
            logger.info("[embed] using google-genai SDK (new)")
        except (ImportError, Exception):
            import google.generativeai as legacy
            legacy.configure(api_key=self.api_key)
            self._client = legacy
            self._use_new_sdk = False
            logger.info("[embed] using google-generativeai SDK (legacy)")

    def embed(self, text: str) -> List[float]:
        """Embed text — kết quả được cache theo nội dung."""
        return self._embed_cached(_normalize_query(text))

    @lru_cache(maxsize=512)
    def _embed_cached(self, text: str) -> List[float]:
        self._ensure_client()
        t0 = time.perf_counter()

        if self._use_new_sdk:
            resp = self._client.models.embed_content(
                model=self.model,
                contents=[text],
                config={"task_type": "RETRIEVAL_QUERY"},
            )
            embeddings = resp.embeddings
            if not embeddings:
                raise RuntimeError("Empty embedding response from Gemini")
            vec = embeddings[0]
            result = list(vec.values) if hasattr(vec, "values") else list(vec)
        else:
            resp = self._client.embed_content(
                model=self.model,
                content=text,
                task_type="retrieval_query",
            )
            if isinstance(resp, dict) and "embedding" in resp:
                result = list(resp["embedding"])
            else:
                raise RuntimeError(f"Unexpected embed response: {type(resp)}")

        elapsed = (time.perf_counter() - t0) * 1000
        logger.debug(f"[embed] {len(text)} chars → {len(result)}d vector in {elapsed:.1f}ms")
        return result


def _normalize_query(text: str) -> str:
    """Chuẩn hoá query để tăng cache hit rate."""
    return " ".join(text.lower().strip().split())


# Reciprocal Rank Fusion

def reciprocal_rank_fusion(
    result_lists: List[List[ChunkResult]],
    weights: Optional[List[float]] = None,
    k: int = 60,
) -> List[ChunkResult]:
    """
    Merge nhiều ranked lists thành một list duy nhất bằng RRF.

    Công thức: RRF(d) = Σ weight_i / (k + rank_i(d))

    Args:
        result_lists: Nhiều list ChunkResult, mỗi list đã được sort theo score desc
        weights: Trọng số cho từng list (mặc định equal weight)
        k: Hằng số RRF (paper gốc dùng 60, thực nghiệm 40–100)

    Returns:
        Merged list, sort theo rrf_score desc, deduplicated theo chunk_uid
    """
    if not result_lists:
        return []

    n = len(result_lists)
    weights = weights or [1.0 / n] * n
    assert len(weights) == n, "weights phải cùng độ dài với result_lists"

    # chunk_uid → merged ChunkResult
    merged: Dict[str, ChunkResult] = {}
    rrf_scores: Dict[str, float] = {}

    for list_idx, (result_list, weight) in enumerate(zip(result_lists, weights)):
        for rank, chunk in enumerate(result_list, start=1):
            uid = chunk.chunk_uid
            rrf_contribution = weight / (k + rank)

            if uid not in merged:
                merged[uid] = chunk
                rrf_scores[uid] = 0.0

            # Merge scores từ các sources
            if chunk.vector_score > 0:
                merged[uid].vector_score = max(merged[uid].vector_score, chunk.vector_score)
            if chunk.keyword_score > 0:
                merged[uid].keyword_score = max(merged[uid].keyword_score, chunk.keyword_score)
            if chunk.graph_score > 0:
                merged[uid].graph_score = max(merged[uid].graph_score, chunk.graph_score)

            # Merge sources list
            for src in chunk.sources:
                if src not in merged[uid].sources:
                    merged[uid].sources.append(src)

            rrf_scores[uid] += rrf_contribution

    # Gán rrf_score và sort
    for uid, score in rrf_scores.items():
        merged[uid].rrf_score = round(score, 6)

    return sorted(merged.values(), key=lambda c: c.rrf_score, reverse=True)


# Qdrant Retriever

class QdrantRetriever:
    """
    Semantic search trên Qdrant.
    Hỗ trợ cả sync search() và async search_async().
    """

    def __init__(self, cfg: OrchestratorConfig) -> None:
        self.cfg = cfg
        self._client: Any = None
        self._embed_client: Optional[EmbeddingClient] = None

    @property
    def client(self) -> Any:
        if self._client is None:
            from qdrant_client import QdrantClient
            self._client = QdrantClient(
                url=self.cfg.qdrant_url,
                api_key=self.cfg.qdrant_api_key or None,
                timeout=10,
                # Nếu dùng Qdrant Cloud (6333), không được gài prefer_grpc=True vì gRPC chạy cổng 6334.
                prefer_grpc=False,
            )
            logger.info(f"[qdrant] connected → {self.cfg.qdrant_url}")
        return self._client

    @property
    def embed(self) -> EmbeddingClient:
        if self._embed_client is None:
            self._embed_client = EmbeddingClient(
                api_key=self.cfg.gemini_api_key,
                model=self.cfg.gemini_embedding_model,
            )
        return self._embed_client

    def search(
        self,
        query: str,
        top_k: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[ChunkResult]:
        """
        Vector search trên Qdrant.

        Args:
            query: Câu hỏi/query của user
            top_k: Số kết quả
            filters: Dict payload filter, vd: {"legal_type": "Luật", "year": 2024}

        Returns:
            List[ChunkResult] sort theo vector_score desc
        """
        top_k = top_k or self.cfg.qdrant_top_k
        t0 = time.perf_counter()

        query_vector = self.embed.embed(query)

        # Build Qdrant filter nếu có
        qdrant_filter = _build_qdrant_filter(filters) if filters else None

        # qdrant-client >= 1.10 uses query_points instead of search
        try:
            resp = self.client.query_points(
                collection_name=self.cfg.qdrant_collection,
                query=query_vector,
                limit=top_k,
                score_threshold=self.cfg.qdrant_score_threshold,
                query_filter=qdrant_filter,
                with_payload=True,
            )
            results = resp.points
        except AttributeError:
            # Fallback for older versions if needed
            results = self.client.search(
                collection_name=self.cfg.qdrant_collection,
                query_vector=query_vector,
                limit=top_k,
                score_threshold=self.cfg.qdrant_score_threshold,
                query_filter=qdrant_filter,
                with_payload=True,
            )

        hits = [_qdrant_point_to_chunk(point) for point in results]
        elapsed = (time.perf_counter() - t0) * 1000
        logger.debug(f"[qdrant] {len(hits)} hits in {elapsed:.1f}ms (query={query[:50]!r})")
        return hits

    async def search_async(
        self,
        query: str,
        top_k: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[ChunkResult]:
        """Async wrapper — chạy sync search trong executor để không block event loop."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.search(query, top_k, filters)
        )


def _qdrant_point_to_chunk(point: Any) -> ChunkResult:
    payload = point.payload or {}
    return ChunkResult(
        chunk_uid=payload.get("chunk_uid", str(point.id)),
        doc_id=payload.get("doc_id", ""),
        doc_number=payload.get("doc_number", ""),
        doc_title=payload.get("doc_title", ""),
        chunk_text=payload.get("chunk_text", ""),
        chunk_order=payload.get("chunk_order", 0),
        chapter=payload.get("chapter"),
        section=payload.get("section"),
        article=payload.get("article"),
        clause=payload.get("clause"),
        point=payload.get("point"),
        token_count=payload.get("token_count", 0),
        vector_score=float(point.score),
        sources=["qdrant"],
    )


def _build_qdrant_filter(filters: Dict[str, Any]) -> Any:
    """Chuyển dict filter đơn giản sang Qdrant Filter object."""
    from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

    conditions = []
    for field_name, value in filters.items():
        if isinstance(value, dict):
            # Range filter: {"year": {"gte": 2020, "lte": 2024}}
            conditions.append(FieldCondition(
                key=field_name,
                range=Range(**value),
            ))
        else:
            conditions.append(FieldCondition(
                key=field_name,
                match=MatchValue(value=value),
            ))
    return Filter(must=conditions) if conditions else None


# Neo4j Retriever

class Neo4jRetriever:
    """
    Graph retrieval trên Neo4j Aura.

    Hai mode:
      1. keyword_search()        — fulltext search trực tiếp trên Neo4j index
      2. expand_from_doc_ids()   — graph traversal từ doc_ids (sau Qdrant)

    Connection pool: driver singleton, max 10 connections.
    """

    def __init__(self, cfg: OrchestratorConfig) -> None:
        self.cfg = cfg
        self._driver: Any = None

    @property
    def driver(self) -> Any:
        if self._driver is None:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(
                self.cfg.neo4j_uri,
                auth=(self.cfg.neo4j_user, self.cfg.neo4j_password),
                max_connection_pool_size=10,
                connection_timeout=10,
                max_transaction_retry_time=30,
            )
            logger.info(f"[neo4j] driver connected → {self.cfg.neo4j_uri}")
        return self._driver

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    # ── 1. Keyword / Fulltext Search ──────────────────────────────────────

    def keyword_search(
        self,
        query: str,
        top_k: Optional[int] = None,
        legal_type: Optional[str] = None,
    ) -> List[ChunkResult]:
        """
        Tìm kiếm bằng Neo4j fulltext index 'legal_fulltext'.

        Dùng BM25-style scoring của Lucene để tìm chunks chứa từ khoá.
        Bổ sung cho vector search — giỏi với tên luật, số điều cụ thể.

        Args:
            query: Query text (không cần embedding)
            top_k: Số kết quả
            legal_type: Optional filter theo loại văn bản

        Returns:
            List[ChunkResult] sort theo keyword_score desc
        """
        top_k = top_k or self.cfg.qdrant_top_k
        t0 = time.perf_counter()

        # Escape Lucene special chars
        safe_query = _escape_lucene(query)

        # Filter clause nếu có
        type_filter = (
            "AND d.doc_type = $legal_type" if legal_type else ""
        )

        cypher = f"""
            CALL db.index.fulltext.queryNodes(
                'legal_fulltext', $search_keyword
            ) YIELD node, score
            WHERE node:Chunk
            WITH node AS c, score
            MATCH (d:Document)-[:HAS_CHUNK]->(c)
            {type_filter}
            RETURN c.chunk_uid  AS chunk_uid,
                   c.doc_id     AS doc_id,
                   d.doc_number AS doc_number,
                   d.title      AS doc_title,
                   c.text       AS chunk_text,
                   c.chapter    AS chapter,
                   c.section    AS section,
                   c.article    AS article,
                   c.clause     AS clause,
                   c.point      AS point,
                   c.chunk_order AS chunk_order,
                   c.token_count AS token_count,
                   score
            ORDER BY score DESC
            LIMIT $top_k
        """

        params: Dict[str, Any] = {
            "search_keyword": safe_query,
            "top_k": top_k,
        }
        if legal_type:
            params["legal_type"] = legal_type

        with self.driver.session(database=self.cfg.neo4j_database) as session:
            records = list(session.run(cypher, **params))

        # Normalize keyword scores về [0, 1]
        raw_scores = [r["score"] for r in records]
        max_score = max(raw_scores) if raw_scores else 1.0

        hits = []
        for rec in records:
            r = dict(rec)
            normalized_score = float(r["score"]) / max_score if max_score > 0 else 0.0
            hits.append(ChunkResult(
                chunk_uid=r.get("chunk_uid") or _generate_uid(r),
                doc_id=r.get("doc_id", ""),
                doc_number=r.get("doc_number", ""),
                doc_title=r.get("doc_title", ""),
                chunk_text=r.get("chunk_text", ""),
                chunk_order=r.get("chunk_order") or 0,
                chapter=r.get("chapter"),
                section=r.get("section"),
                article=r.get("article"),
                clause=r.get("clause"),
                point=r.get("point"),
                token_count=r.get("token_count") or 0,
                keyword_score=normalized_score,
                sources=["neo4j_keyword"],
            ))

        elapsed = (time.perf_counter() - t0) * 1000
        logger.debug(f"[neo4j keyword] {len(hits)} hits in {elapsed:.1f}ms")
        return hits

    async def keyword_search_async(
        self,
        query: str,
        top_k: Optional[int] = None,
        legal_type: Optional[str] = None,
    ) -> List[ChunkResult]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.keyword_search(query, top_k, legal_type)
        )

    # ── 2. Graph Expansion từ doc_ids ─────────────────────────────────────

    def expand_from_doc_ids(
        self,
        doc_ids: List[str],
        depth: Optional[int] = None,
        max_related: Optional[int] = None,
    ) -> List[ChunkResult]:
        """
        Graph traversal từ danh sách doc_ids.

        Duyệt quan hệ pháp lý (AMENDS, SUPERSEDES, IMPLEMENTS, REFERENCES, ...)
        để tìm văn bản liên quan, sau đó lấy top chunks của chúng.

        Returns:
            List[ChunkResult] với graph_score = proximity score (1/hop_distance)
        """
        if not doc_ids:
            return []

        depth = depth or self.cfg.neo4j_max_depth
        max_related = max_related or self.cfg.neo4j_max_related
        t0 = time.perf_counter()

        # Lấy related docs qua graph traversal
        related_docs = self._traverse_related(doc_ids, depth, max_related)
        if not related_docs:
            logger.debug("[neo4j graph] no related docs found")
            return []

        # Lấy chunks của related docs, kèm graph_score
        chunks = self._get_chunks_with_graph_score(related_docs)

        elapsed = (time.perf_counter() - t0) * 1000
        logger.debug(
            f"[neo4j graph] {len(related_docs)} related docs → "
            f"{len(chunks)} chunks in {elapsed:.1f}ms"
        )
        return chunks

    async def expand_from_doc_ids_async(
        self,
        doc_ids: List[str],
        depth: Optional[int] = None,
        max_related: Optional[int] = None,
    ) -> List[ChunkResult]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.expand_from_doc_ids(doc_ids, depth, max_related)
        )

    def _traverse_related(
        self,
        doc_ids: List[str],
        depth: int,
        max_related: int,
    ) -> List[Dict[str, Any]]:
        """
        Traverse graph, trả về list (doc_id, hop_distance, rel_types).
        hop_distance dùng để tính graph_score = 1/hop.
        """
        cypher = f"""
            UNWIND $doc_ids AS seed_id
            MATCH path = (src:Document {{doc_id: seed_id}})
                         -[r*1..{depth}]-
                         (related:Document)
            WHERE related.doc_id <> seed_id
              AND NOT related.doc_id IN $doc_ids
            WITH DISTINCT related,
                 min(length(path)) AS hop_distance,
                 collect(DISTINCT [rel IN relationships(path) | type(rel)])[0] AS rel_types
            RETURN related.doc_id   AS doc_id,
                   related.doc_number AS doc_number,
                   related.title    AS title,
                   related.doc_type AS doc_type,
                   hop_distance,
                   rel_types
            ORDER BY hop_distance ASC
            LIMIT {max_related}
        """

        with self.driver.session(database=self.cfg.neo4j_database) as session:
            result = session.run(cypher, doc_ids=doc_ids)
            return [dict(r) for r in result]

    def _get_chunks_with_graph_score(
        self,
        related_docs: List[Dict[str, Any]],
        chunks_per_doc: int = 3,
    ) -> List[ChunkResult]:
        """
        Lấy top chunks cho mỗi related doc.
        graph_score = 1 / hop_distance (doc gần hơn = score cao hơn).
        """
        doc_id_to_hop: Dict[str, int] = {
            r["doc_id"]: r.get("hop_distance", 1)
            for r in related_docs
            if r.get("doc_id")
        }
        doc_ids = list(doc_id_to_hop.keys())
        if not doc_ids:
            return []

        cypher = """
            UNWIND $doc_ids AS did
            MATCH (d:Document {doc_id: did})-[:HAS_CHUNK]->(c:Chunk)
            WITH d, c
            ORDER BY c.chunk_order ASC
            WITH d, collect(c)[..$chunks_per_doc] AS top_chunks
            UNWIND top_chunks AS c
            RETURN c.chunk_uid   AS chunk_uid,
                   c.doc_id      AS doc_id,
                   d.doc_number  AS doc_number,
                   d.title       AS doc_title,
                   c.text        AS chunk_text,
                   c.chapter     AS chapter,
                   c.section     AS section,
                   c.article     AS article,
                   c.clause      AS clause,
                   c.point       AS point,
                   c.chunk_order AS chunk_order,
                   c.token_count AS token_count
        """

        with self.driver.session(database=self.cfg.neo4j_database) as session:
            result = session.run(
                cypher, doc_ids=doc_ids, chunks_per_doc=chunks_per_doc
            )
            rows = [dict(r) for r in result]

        chunks = []
        for r in rows:
            hop = doc_id_to_hop.get(r.get("doc_id", ""), 1)
            graph_score = round(1.0 / hop, 4)
            chunks.append(ChunkResult(
                chunk_uid=r.get("chunk_uid") or _generate_uid(r),
                doc_id=r.get("doc_id", ""),
                doc_number=r.get("doc_number", ""),
                doc_title=r.get("doc_title", ""),
                chunk_text=r.get("chunk_text", ""),
                chunk_order=r.get("chunk_order") or 0,
                chapter=r.get("chapter"),
                section=r.get("section"),
                article=r.get("article"),
                clause=r.get("clause"),
                point=r.get("point"),
                token_count=r.get("token_count") or 0,
                graph_score=graph_score,
                sources=["neo4j_graph"],
            ))
        return chunks


# HybridRetriever — orchestrates everything

class HybridRetriever:
    """
    True hybrid search: Vector + Keyword + Graph chạy song song,
    kết quả merge bằng Reciprocal Rank Fusion.

    Pipeline:
      ┌─────────────────────────────────────────────────────┐
      │  asyncio.gather():                                  │
      │    Task A: Qdrant vector search      (I/O bound)   │
      │    Task B: Neo4j keyword search      (I/O bound)   │
      │                                                     │
      │  Sau khi A xong:                                    │
      │    Task C: Neo4j graph expand        (I/O bound)   │
      │            (dùng doc_ids từ Task A)                 │
      └─────────────────────────────────────────────────────┘
      ↓
      RRF fusion(A, B, C) → top_k unified results

    Usage:
        retriever = HybridRetriever(cfg)
        result = retriever.search(
            query="quyền thuê đất khi hợp đồng hết hạn",
            top_k=10,
            alpha=0.7,           # 70% vector, 30% keyword trong RRF weight
            filters={"year": {"gte": 2020}},
        )
    """

    def __init__(self, cfg: OrchestratorConfig) -> None:
        self.cfg = cfg
        self.qdrant = QdrantRetriever(cfg)
        self.neo4j = Neo4jRetriever(cfg)

    def search(
        self,
        query: str,
        top_k: Optional[int] = None,
        alpha: float = 0.7,
        enable_graph: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        legal_type: Optional[str] = None,
    ) -> HybridSearchResult:
        """
        Hybrid search đồng bộ — wrapper cho async version.

        Args:
            query: Câu hỏi của user
            top_k: Số kết quả cuối cùng sau fusion
            alpha: Trọng số vector trong RRF (0=chỉ keyword, 1=chỉ vector)
                   Khuyến nghị: 0.6–0.8 cho pháp lý
            enable_graph: Có dùng graph expansion không
            filters: Payload filters cho Qdrant (year, legal_type, ...)
            legal_type: Filter loại văn bản cho Neo4j keyword search

        Returns:
            HybridSearchResult với chunks đã fused và metadata
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Đang trong async context (vd: FastAPI, LangGraph)
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    # Việc tạo coroutine self._search_async(...) phải nằm BÊN TRONG thread mới.
                    # Nếu tạo ở main thread rồi đẩy vào asyncio.run(coroutine) sẽ bị lỗi RuntimeError: Task attached to a different loop.
                    future = pool.submit(
                        lambda: asyncio.run(
                            self._search_async(query, top_k, alpha, enable_graph, filters, legal_type)
                        )
                    )
                    return future.result()
        except RuntimeError:
            pass

        return asyncio.run(
            self._search_async(query, top_k, alpha, enable_graph, filters, legal_type)
        )

    async def search_async(
        self,
        query: str,
        top_k: Optional[int] = None,
        alpha: float = 0.7,
        enable_graph: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        legal_type: Optional[str] = None,
    ) -> HybridSearchResult:
        return await self._search_async(
            query, top_k, alpha, enable_graph, filters, legal_type
        )

    async def _search_async(
        self,
        query: str,
        top_k: Optional[int],
        alpha: float,
        enable_graph: bool,
        filters: Optional[Dict[str, Any]],
        legal_type: Optional[str],
    ) -> HybridSearchResult:
        top_k = top_k or self.cfg.qdrant_top_k
        t0 = time.perf_counter()

        # ── Phase 1: Vector + Keyword song song ──────────────────────────
        vector_task = asyncio.create_task(
            self.qdrant.search_async(query, top_k=top_k * 2, filters=filters)
        )
        keyword_task = asyncio.create_task(
            self.neo4j.keyword_search_async(query, top_k=top_k * 2, legal_type=legal_type)
        )

        vector_results, keyword_results = await asyncio.gather(
            vector_task, keyword_task, return_exceptions=True
        )

        # Trả thẳng Exception ra thành 1 chunk giả để Debug hiển thị lên UI
        if isinstance(vector_results, Exception):
            logger.error(f"[hybrid] Qdrant search failed: {vector_results}")
            vector_results = [ChunkResult(chunk_uid="ERR_QDRANT", doc_id="", doc_number="QDRANT CRASH", doc_title=str(type(vector_results).__name__), chunk_text=f"Chi tiết lỗi Qdrant: {str(vector_results)}")]
        if isinstance(keyword_results, Exception):
            logger.error(f"[hybrid] Neo4j keyword search failed: {keyword_results}")
            keyword_results = [ChunkResult(chunk_uid="ERR_NEO4J", doc_id="", doc_number="NEO4J CRASH", doc_title=str(type(keyword_results).__name__), chunk_text=f"Chi tiết lỗi Neo4j Keyword: {str(keyword_results)}")]

        # ── Phase 2: Graph expansion (dùng doc_ids từ vector) ────────────
        graph_results: List[ChunkResult] = []
        if enable_graph and vector_results:
            doc_ids = list({c.doc_id for c in vector_results if c.doc_id})[:20]
            try:
                graph_results = await self.neo4j.expand_from_doc_ids_async(
                    doc_ids,
                    depth=self.cfg.neo4j_max_depth,
                    max_related=top_k * 2,
                )
            except Exception as e:
                logger.error(f"[hybrid] Neo4j graph expand failed: {e}")
                graph_results = [ChunkResult(chunk_uid="ERR_GRAPH", doc_id="", doc_number="GRAPH EXPAND CRASH", doc_title=str(type(e).__name__), chunk_text=f"Chi tiết lỗi Neo4j Graph Expand: {str(e)}")]

        # ── Phase 3: RRF Fusion ───────────────────────────────────────────
        # Trọng số: alpha cho vector, (1-alpha)/2 cho keyword, (1-alpha)/2 cho graph
        keyword_weight = (1.0 - alpha) * 0.6
        graph_weight = (1.0 - alpha) * 0.4

        result_lists = [vector_results, keyword_results]
        weights = [alpha, keyword_weight]

        if graph_results:
            result_lists.append(graph_results)
            weights.append(graph_weight)
            # Normalize weights
            total_w = sum(weights)
            weights = [w / total_w for w in weights]

        fused = reciprocal_rank_fusion(result_lists, weights=weights)
        top_chunks = fused[:top_k]

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            f"[hybrid] query={query[:60]!r} | "
            f"vector={len(vector_results)} keyword={len(keyword_results)} "
            f"graph={len(graph_results)} → fused={len(fused)} "
            f"top={top_k} | {elapsed:.1f}ms"
        )

        return HybridSearchResult(
            chunks=top_chunks,
            vector_count=len(vector_results),
            keyword_count=len(keyword_results),
            graph_count=len(graph_results),
            total_unique=len(fused),
            elapsed_ms=round(elapsed, 1),
            alpha=alpha,
        )

    def close(self) -> None:
        self.neo4j.close()


# Utilities

def _escape_lucene(query: str) -> str:
    """Escape Lucene special characters cho Neo4j fulltext query."""
    special = r'\+-&&||!(){}[]^"~*?:/'
    result = []
    for ch in query:
        if ch in special:
            result.append(f"\\{ch}")
        else:
            result.append(ch)
    return "".join(result)


def _generate_uid(row: Dict[str, Any]) -> str:
    """Tạo chunk_uid từ doc_id + chunk_order khi không có uid sẵn."""
    key = f"{row.get('doc_id', '')}_{row.get('chunk_order', 0)}"
    return hashlib.md5(key.encode()).hexdigest()[:16]