from __future__ import annotations

from orchestrator.graph import _build_citations, _build_context
from orchestrator.retrievers import ChunkResult


def _chunk(**kw) -> ChunkResult:
    """ChunkResult với các field bắt buộc mặc định, override qua kwargs."""
    base = dict(chunk_uid="c", doc_id="d", doc_number="", doc_title="", chunk_text="")
    base.update(kw)
    return ChunkResult(**base)


def test_build_context_includes_headers_and_text():
    chunks = [
        _chunk(
            chunk_uid="c1",
            doc_id="doc-1",
            doc_number="59/2020/QH14",
            doc_title="Luật Doanh nghiệp 2020",
            article="Điều 17",
            chunk_text="Quy định về quyền thành lập doanh nghiệp.",
            rrf_score=0.92,
        ),
        _chunk(
            chunk_uid="c2",
            doc_id="doc-2",
            doc_number="45/2019/QH14",
            doc_title="Bộ luật Lao động 2019",
            article="Điều 168",
            chunk_text="Quyền lợi bảo hiểm xã hội của người lao động.",
            rrf_score=0.81,
        ),
    ]

    context = _build_context(chunks)
    assert "KẾT QUẢ TÌM KIẾM" in context
    assert "59/2020/QH14" in context
    assert "Điều 168" in context
    assert "quyền thành lập doanh nghiệp" in context.lower()


def test_build_citations_deduplicates_by_doc_id():
    chunks = [
        _chunk(chunk_uid="c1", doc_id="doc-1", doc_number="A", doc_title="Law A", sources=["qdrant"]),
        _chunk(chunk_uid="c2", doc_id="doc-1", doc_number="A", doc_title="Law A", sources=["neo4j_keyword"]),
        _chunk(chunk_uid="c3", doc_id="doc-2", doc_number="B", doc_title="Law B", sources=["neo4j_keyword"]),
    ]

    citations = _build_citations(chunks)
    assert len(citations) == 2
    assert {c["doc_id"] for c in citations} == {"doc-1", "doc-2"}
