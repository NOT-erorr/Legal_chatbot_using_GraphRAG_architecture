"""
orchestrator/intent.py — Phân loại ý định (intent) + trích xuất tham chiếu tường minh.

Hỗ trợ multi-intent cho trợ lý pháp luật:
    - GENERAL_QA       : hỏi đáp ngữ nghĩa thông thường (luồng hybrid cũ)
    - EXPLAIN_ARTICLE  : "giải thích chi tiết Điều 8 Luật Thuế TNCN"
    - SUMMARIZE_DOC    : "tóm tắt Nghị định 65/2013"
    - COMPARE          : "so sánh Điều 8 Luật A và Điều 9 Luật B"
    - LIST_RELATED     : "văn bản nào sửa đổi Luật Thuế TNCN"

Chiến lược classify HYBRID:
    1. Heuristic (regex) bắt nhanh intent + tham chiếu rõ ràng → 0ms.
    2. Nếu intent thuộc nhóm cấu trúc nhưng tham chiếu chưa đủ → fallback LLM
       (Gemini) để trích lại. Nếu vẫn không đủ → hạ về GENERAL_QA (explicit-only v1).

Module này KHÔNG gọi mạng trừ khi `llm_caller` được truyền vào → dễ unit-test.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class Intent:
    GENERAL_QA = "general_qa"
    EXPLAIN_ARTICLE = "explain_article"
    SUMMARIZE_DOC = "summarize_doc"
    COMPARE = "compare"
    LIST_RELATED = "list_related"

    ALL = {
        GENERAL_QA,
        EXPLAIN_ARTICLE,
        SUMMARIZE_DOC,
        COMPARE,
        LIST_RELATED,
    }
    STRUCTURED = {EXPLAIN_ARTICLE, SUMMARIZE_DOC, COMPARE, LIST_RELATED}


@dataclass
class Reference:
    """Một tham chiếu tới văn bản / điều / khoản / điểm."""
    doc_number: Optional[str] = None   # số hiệu, vd "45/2019/QH14"
    doc_name: Optional[str] = None     # tên, vd "Luật Thuế thu nhập cá nhân"
    article: Optional[str] = None      # số điều, vd "8"
    clause: Optional[str] = None       # số khoản, vd "2"
    point: Optional[str] = None        # điểm, vd "a"

    def has_doc(self) -> bool:
        return bool(self.doc_number or self.doc_name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "doc_number": self.doc_number,
            "doc_name": self.doc_name,
            "article": self.article,
            "clause": self.clause,
            "point": self.point,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Reference":
        return cls(
            doc_number=d.get("doc_number"),
            doc_name=d.get("doc_name"),
            article=d.get("article"),
            clause=d.get("clause"),
            point=d.get("point"),
        )


@dataclass
class ClassifyResult:
    intent: str
    references: List[Reference] = field(default_factory=list)
    used_llm: bool = False


# ── Regex patterns ───────────────────────────────────────────────────────────

_RE_ARTICLE = re.compile(r"điều\s+(\d+)", re.IGNORECASE)
_RE_CLAUSE = re.compile(r"khoản\s+(\d+)", re.IGNORECASE)
_RE_POINT = re.compile(r"điểm\s+([a-zđ])\b", re.IGNORECASE)
# Số hiệu văn bản: 45/2019/QH14, 65/2013/NĐ-CP, 111/2013/TT-BTC ...
_RE_DOCNUM = re.compile(r"\d+\s*/\s*\d{2,4}\s*/\s*[0-9a-zđ\-]+", re.IGNORECASE)
# Tên văn bản bắt đầu bằng loại văn bản
_RE_DOCNAME = re.compile(
    r"(?:bộ luật|luật|nghị định|thông tư|nghị quyết|quyết định|pháp lệnh|hiến pháp)"
    r"\s+[^,.;?\n]+",
    re.IGNORECASE,
)

_KW_EXPLAIN = [
    "giải thích", "giải nghĩa", "phân tích", "làm rõ", "nói rõ",
    "diễn giải", "giải đáp chi tiết",
]
_KW_SUMMARIZE = [
    "tóm tắt", "tóm lược", "tổng quan", "khái quát", "tóm gọn", "nội dung chính",
]
_KW_COMPARE = [
    "so sánh", "đối chiếu", "phân biệt", "khác nhau", "khác biệt",
]
_KW_RELATED = [
    "liên quan", "sửa đổi", "thay thế", "bổ sung", "hướng dẫn", "căn cứ",
    "hết hiệu lực", "văn bản nào", "dẫn chiếu",
]

# Từ thừa hay đứng cuối tên văn bản, cần cắt bỏ.
_TRAILING_FILLERS = [
    "hiện hành", "này", "đó", "trên", "là gì", "ra sao", "như thế nào",
    "quy định gì", "không", "ạ", "nhé",
]


def _detect_intent(q: str) -> str:
    ql = q.lower()
    if any(k in ql for k in _KW_COMPARE):
        return Intent.COMPARE
    if any(k in ql for k in _KW_RELATED):
        return Intent.LIST_RELATED
    if any(k in ql for k in _KW_SUMMARIZE):
        return Intent.SUMMARIZE_DOC
    if any(k in ql for k in _KW_EXPLAIN):
        return Intent.EXPLAIN_ARTICLE
    # "chi tiết" / "đọc" + có Điều → coi là explain
    if ("chi tiết" in ql or "đọc" in ql) and _RE_ARTICLE.search(ql):
        return Intent.EXPLAIN_ARTICLE
    return Intent.GENERAL_QA


def _clean_docname(raw: str) -> Optional[str]:
    s = raw.strip()
    changed = True
    while changed:
        changed = False
        low = s.lower()
        for filler in _TRAILING_FILLERS:
            if low.endswith(filler):
                s = s[: len(s) - len(filler)].strip(" ,")
                changed = True
                break
    s = s.strip(" ,")
    # Chỉ còn mỗi loại văn bản (vd "Luật") → không đủ cụ thể.
    head = s.lower().split()
    if len(head) <= 1:
        return None
    return s


def _extract(q: str):
    docnums = [re.sub(r"\s+", "", m.group(0)) for m in _RE_DOCNUM.finditer(q)]
    docnames_raw = [m.group(0) for m in _RE_DOCNAME.finditer(q)]
    docnames = [n for n in (_clean_docname(x) for x in docnames_raw) if n]
    articles = [m.group(1) for m in _RE_ARTICLE.finditer(q)]
    clauses = [m.group(1) for m in _RE_CLAUSE.finditer(q)]
    points = [m.group(1).lower() for m in _RE_POINT.finditer(q)]
    return docnums, docnames, articles, clauses, points


def _mk_ref(kind: str, val: str, article=None, clause=None, point=None) -> Reference:
    if kind == "num":
        return Reference(doc_number=val, article=article, clause=clause, point=point)
    return Reference(doc_name=val, article=article, clause=clause, point=point)


def _build_references(
    intent: str, docnums, docnames, articles, clauses, points
) -> List[Reference]:
    doc_tokens = [("num", n) for n in docnums] + [("name", nm) for nm in docnames]
    refs: List[Reference] = []

    if intent == Intent.COMPARE:
        if len(doc_tokens) >= 2:
            for i, (kind, val) in enumerate(doc_tokens[:2]):
                art = articles[i] if i < len(articles) else None
                refs.append(_mk_ref(kind, val, art))
        elif len(doc_tokens) == 1 and len(articles) >= 2:
            kind, val = doc_tokens[0]
            for art in articles[:2]:
                refs.append(_mk_ref(kind, val, art))
        elif doc_tokens:
            kind, val = doc_tokens[0]
            refs.append(_mk_ref(kind, val, articles[0] if articles else None))
        return refs

    if doc_tokens:
        kind, val = doc_tokens[0]
        refs.append(_mk_ref(
            kind, val,
            article=articles[0] if articles else None,
            clause=clauses[0] if clauses else None,
            point=points[0] if points else None,
        ))
    return refs


def _refs_sufficient(intent: str, refs: List[Reference]) -> bool:
    usable = [r for r in refs if r.has_doc()]
    if intent == Intent.COMPARE:
        return len(usable) >= 2
    if intent in Intent.STRUCTURED:
        return len(usable) >= 1
    return True


# ── LLM fallback ─────────────────────────────────────────────────────────────

_LLM_PROMPT = """Bạn là bộ phân loại câu hỏi pháp luật Việt Nam. Hãy phân loại câu hỏi \
và trích xuất tham chiếu văn bản. CHỈ trả về một JSON hợp lệ (không markdown, không giải thích).

Định dạng:
{{"intent": "<intent>", "references": [{{"doc_number": null, "doc_name": null, "article": null, "clause": null, "point": null}}]}}

intent ∈ ["general_qa", "explain_article", "summarize_doc", "compare", "list_related"]:
- explain_article: yêu cầu giải thích/phân tích chi tiết một điều/khoản cụ thể.
- summarize_doc: yêu cầu tóm tắt một văn bản.
- compare: so sánh hai điều/văn bản (references phải có >= 2 phần tử).
- list_related: hỏi các văn bản liên quan / sửa đổi / thay thế / hướng dẫn.
- general_qa: mọi trường hợp còn lại.

Quy ước:
- doc_name: tên văn bản, vd "Luật Thuế thu nhập cá nhân".
- doc_number: số hiệu, vd "45/2019/QH14" (để null nếu không có).
- article/clause/point: SỐ ở dạng chuỗi (vd "8"), null nếu không có.

Câu hỏi: {q}
JSON:"""


def _parse_llm_json(raw: str) -> Optional[ClassifyResult]:
    if not raw:
        return None
    text = raw.strip()
    # Bỏ code fence nếu có
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    # Lấy đoạn { ... } đầu tiên
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        data = json.loads(text[start : end + 1])
    except Exception:
        return None

    intent = str(data.get("intent", "")).strip().lower()
    if intent not in Intent.ALL:
        return None

    refs: List[Reference] = []
    for item in data.get("references") or []:
        if isinstance(item, dict):
            refs.append(Reference.from_dict({
                k: (str(v) if v is not None else None)
                for k, v in item.items()
            }))
    return ClassifyResult(intent=intent, references=refs)


def _llm_classify(
    question: str, llm_caller: Callable[[str], str]
) -> Optional[ClassifyResult]:
    try:
        raw = llm_caller(_LLM_PROMPT.format(q=question))
    except Exception:
        return None
    return _parse_llm_json(raw)


# ── Public API ───────────────────────────────────────────────────────────────

def classify_intent(
    question: str,
    llm_caller: Optional[Callable[[str], str]] = None,
) -> ClassifyResult:
    """Phân loại ý định + trích xuất tham chiếu (hybrid heuristic → LLM fallback)."""
    intent = _detect_intent(question)

    if intent == Intent.GENERAL_QA:
        return ClassifyResult(Intent.GENERAL_QA, [])

    docnums, docnames, articles, clauses, points = _extract(question)
    refs = _build_references(intent, docnums, docnames, articles, clauses, points)

    if _refs_sufficient(intent, refs):
        return ClassifyResult(intent, refs)

    # Heuristic chưa đủ tham chiếu → thử LLM (nếu có).
    if llm_caller is not None:
        llm_res = _llm_classify(question, llm_caller)
        if llm_res and llm_res.intent in Intent.ALL and _refs_sufficient(
            llm_res.intent, llm_res.references
        ):
            llm_res.used_llm = True
            return llm_res

    # explicit-only v1: không resolve được → hạ về Q&A thường.
    return ClassifyResult(Intent.GENERAL_QA, [])
