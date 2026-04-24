from __future__ import annotations

from orchestrator.graph import _build_citations, _build_context


def test_build_context_merges_vector_and_graph_sections():
    vector_chunks = [
        {
            "doc_id": "doc-1",
            "doc_number": "59/2020/QH14",
            "article": "Điều 17",
            "score": 0.92,
            "chunk_text": "Quy định về quyền thành lập doanh nghiệp.",
        }
    ]
    graph_chunks = [
        {
            "doc_id": "doc-2",
            "doc_title": "Bộ luật Lao động 2019",
            "article": "Điều 168",
            "chunk_text": "Quyền lợi bảo hiểm xã hội của người lao động.",
        }
    ]

    context = _build_context(vector_chunks, graph_chunks)
    assert "KẾT QUẢ TÌM KIẾM NGỮ NGHĨA" in context
    assert "VĂN BẢN LIÊN QUAN" in context
    assert "59/2020/QH14" in context
    assert "Điều 168" in context


def test_build_citations_deduplicates_by_doc_id():
    vector_chunks = [
        {"doc_id": "doc-1", "doc_number": "A", "doc_title": "Law A", "source": "vector"}
    ]
    graph_chunks = [
        {"doc_id": "doc-1", "doc_number": "A", "doc_title": "Law A", "source": "graph"},
        {"doc_id": "doc-2", "doc_number": "B", "doc_title": "Law B", "source": "graph"},
    ]

    citations = _build_citations(vector_chunks, graph_chunks)
    assert len(citations) == 2
    assert {c["doc_id"] for c in citations} == {"doc-1", "doc-2"}
