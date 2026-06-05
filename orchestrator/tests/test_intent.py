from __future__ import annotations

from orchestrator.intent import (
    Intent,
    Reference,
    classify_intent,
    _detect_intent,
    _extract,
    _parse_llm_json,
)


# ── Intent detection ─────────────────────────────────────────────────────────

def test_general_qa_when_no_keywords():
    res = classify_intent("Thuế thu nhập cá nhân được tính như thế nào?")
    assert res.intent == Intent.GENERAL_QA
    assert res.references == []


def test_explain_article_explicit():
    res = classify_intent("Giải thích chi tiết Điều 8 Luật Thuế thu nhập cá nhân")
    assert res.intent == Intent.EXPLAIN_ARTICLE
    assert len(res.references) == 1
    ref = res.references[0]
    assert ref.article == "8"
    assert ref.doc_name and "thuế thu nhập" in ref.doc_name.lower()


def test_explain_with_doc_number():
    res = classify_intent("Phân tích Điều 12 Khoản 2 của 45/2019/QH14")
    assert res.intent == Intent.EXPLAIN_ARTICLE
    ref = res.references[0]
    assert ref.doc_number == "45/2019/QH14"
    assert ref.article == "12"
    assert ref.clause == "2"


def test_summarize_doc():
    res = classify_intent("Tóm tắt Nghị định 65/2013/NĐ-CP")
    assert res.intent == Intent.SUMMARIZE_DOC
    assert res.references[0].doc_number == "65/2013/NĐ-CP"


def test_list_related():
    res = classify_intent("Văn bản nào sửa đổi Luật Thuế thu nhập cá nhân?")
    assert res.intent == Intent.LIST_RELATED
    assert res.references[0].has_doc()


def test_compare_two_docs():
    res = classify_intent(
        "So sánh Điều 8 Luật Thuế thu nhập cá nhân và Điều 9 Luật Thuế giá trị gia tăng"
    )
    assert res.intent == Intent.COMPARE
    assert len(res.references) == 2
    assert {r.article for r in res.references} == {"8", "9"}


def test_explicit_only_fallback_when_no_reference():
    # "điều này" không có số + không có LLM → hạ về general_qa (explicit-only v1).
    res = classify_intent("Giải thích chi tiết điều luật này")
    assert res.intent == Intent.GENERAL_QA


# ── Reference extraction ─────────────────────────────────────────────────────

def test_extract_components():
    docnums, docnames, articles, clauses, points = _extract(
        "Điểm a Khoản 3 Điều 7 Thông tư 111/2013/TT-BTC"
    )
    assert "111/2013/TT-BTC" in docnums
    assert articles == ["7"]
    assert clauses == ["3"]
    assert points == ["a"]


def test_docname_strips_trailing_filler():
    _, docnames, _, _, _ = _extract("Tóm tắt Luật Thuế thu nhập cá nhân này")
    assert any("thuế thu nhập cá nhân" in n.lower() for n in docnames)
    assert all(not n.lower().endswith("này") for n in docnames)


# ── LLM fallback parsing ─────────────────────────────────────────────────────

def test_parse_llm_json_strips_fence():
    raw = '```json\n{"intent": "explain_article", "references": [{"doc_name": "Luật X", "article": "5"}]}\n```'
    res = _parse_llm_json(raw)
    assert res is not None
    assert res.intent == Intent.EXPLAIN_ARTICLE
    assert res.references[0].article == "5"


def test_parse_llm_json_invalid_returns_none():
    assert _parse_llm_json("không phải json") is None
    assert _parse_llm_json('{"intent": "khong_hop_le"}') is None


def test_llm_fallback_invoked_when_heuristic_insufficient():
    # Câu có intent compare nhưng heuristic chỉ thấy 1 ref → gọi LLM.
    calls = []

    def fake_llm(prompt: str) -> str:
        calls.append(prompt)
        return (
            '{"intent": "compare", "references": ['
            '{"doc_name": "Luật A", "article": "1"},'
            '{"doc_name": "Luật B", "article": "2"}]}'
        )

    res = classify_intent("so sánh hai quy định về thuế", llm_caller=fake_llm)
    assert len(calls) == 1
    assert res.intent == Intent.COMPARE
    assert res.used_llm is True
    assert len(res.references) == 2
