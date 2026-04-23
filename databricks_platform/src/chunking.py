from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

TOKEN_PATTERN = re.compile(r"\w+|[^\w\s]", re.UNICODE)
DOC_NUMBER_PATTERN = re.compile(r"\b\d{1,4}/\d{4}/[A-Z0-9\-]+\b")

CHAPTER_RE = re.compile(r"^chuong\s+([ivxlcdm]+|\d+)\b")
SECTION_RE = re.compile(r"^muc\s+([ivxlcdm]+|\d+)\b")
ARTICLE_RE = re.compile(r"^dieu\s+(\d+[a-z]?)\b")
CLAUSE_RE = re.compile(r"^(?:khoan\s+)?(\d+)[\.)]\s+")
POINT_RE = re.compile(r"^([a-z])\)\s+")


@dataclass
class ChunkRecord:
    chunk_uid: str
    doc_id: str
    doc_number: str
    doc_title: str
    chunk_order: int
    chunk_text: str
    token_count: int
    tokens: List[str]
    chapter: Optional[str]
    section: Optional[str]
    article: Optional[str]
    clause: Optional[str]
    point: Optional[str]
    references_to: List[str]
    metadata: Dict[str, Any]


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(text: str) -> str:
    if not text:
        return ""
    lines = [line.strip() for line in text.replace("\r", "\n").split("\n")]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def tokenize_vi(text: str) -> List[str]:
    if not text:
        return []
    return TOKEN_PATTERN.findall(text)


def extract_doc_references(text: str) -> List[str]:
    if not text:
        return []
    refs = set(match.group(0) for match in DOC_NUMBER_PATTERN.finditer(text.upper()))
    return sorted(refs)


class RuleBasedHierarchicalChunker:
    """
    Structural chunker for Vietnamese legal documents.

    Splits by hierarchy boundaries:
      chapter -> section -> article -> clause -> point
    and enforces token targets for each chunk.
    """

    def __init__(
        self,
        target_tokens: int = 220,
        max_tokens: int = 300,
        overlap_tokens: int = 30,
    ) -> None:
        if target_tokens <= 0 or max_tokens <= 0:
            raise ValueError("target_tokens and max_tokens must be > 0")
        if target_tokens > max_tokens:
            raise ValueError("target_tokens cannot be larger than max_tokens")

        self.target_tokens = target_tokens
        self.max_tokens = max_tokens
        self.overlap_tokens = max(0, overlap_tokens)

    def chunk_document(
        self,
        doc_id: str,
        doc_number: str,
        doc_title: str,
        raw_text: str,
        base_metadata: Optional[Dict[str, Any]] = None,
    ) -> List[ChunkRecord]:
        text = normalize_text(raw_text)
        if not text:
            return []

        metadata = dict(base_metadata or {})
        lines = text.split("\n")

        hierarchy: Dict[str, Optional[str]] = {
            "chapter": None,
            "section": None,
            "article": None,
            "clause": None,
            "point": None,
        }

        chunks: List[ChunkRecord] = []
        buffer_lines: List[str] = []
        chunk_order = 0

        def flush_buffer() -> None:
            nonlocal chunk_order
            if not buffer_lines:
                return

            chunk_text = "\n".join(buffer_lines).strip()
            if not chunk_text:
                buffer_lines.clear()
                return

            chunk_order += 1
            tokens = tokenize_vi(chunk_text)
            chunk_uid = f"{_safe_id(doc_id)}__{chunk_order:05d}"

            merged_metadata = {
                **metadata,
                "hierarchy_path": {
                    "chapter": hierarchy["chapter"],
                    "section": hierarchy["section"],
                    "article": hierarchy["article"],
                    "clause": hierarchy["clause"],
                    "point": hierarchy["point"],
                },
            }

            chunks.append(
                ChunkRecord(
                    chunk_uid=chunk_uid,
                    doc_id=doc_id,
                    doc_number=doc_number,
                    doc_title=doc_title,
                    chunk_order=chunk_order,
                    chunk_text=chunk_text,
                    token_count=len(tokens),
                    tokens=tokens,
                    chapter=hierarchy["chapter"],
                    section=hierarchy["section"],
                    article=hierarchy["article"],
                    clause=hierarchy["clause"],
                    point=hierarchy["point"],
                    references_to=extract_doc_references(chunk_text),
                    metadata=merged_metadata,
                )
            )
            buffer_lines.clear()

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            boundary_level, boundary_value = self._parse_boundary(stripped)
            if boundary_level in {"chapter", "section", "article", "clause", "point"}:
                if boundary_level in {"chapter", "section", "article"}:
                    flush_buffer()
                elif boundary_level in {"clause", "point"} and buffer_lines:
                    flush_buffer()

                self._update_hierarchy(hierarchy, boundary_level, boundary_value)

            candidate_lines = buffer_lines + [stripped]
            candidate_tokens = len(tokenize_vi("\n".join(candidate_lines)))
            if candidate_tokens > self.max_tokens and buffer_lines:
                flush_buffer()

            buffer_lines.append(stripped)

            current_token_count = len(tokenize_vi("\n".join(buffer_lines)))
            if current_token_count >= self.target_tokens and stripped.endswith((".", ";", ":")):
                flush_buffer()

        flush_buffer()

        if self.overlap_tokens > 0 and len(chunks) > 1:
            chunks = self._apply_overlap(chunks)

        return chunks

    def _parse_boundary(self, line: str) -> tuple[Optional[str], Optional[str]]:
        normalized = strip_accents(line).lower()

        chapter_match = CHAPTER_RE.match(normalized)
        if chapter_match:
            return "chapter", chapter_match.group(0)

        section_match = SECTION_RE.match(normalized)
        if section_match:
            return "section", section_match.group(0)

        article_match = ARTICLE_RE.match(normalized)
        if article_match:
            return "article", article_match.group(0)

        clause_match = CLAUSE_RE.match(normalized)
        if clause_match:
            return "clause", clause_match.group(0)

        point_match = POINT_RE.match(normalized)
        if point_match:
            return "point", point_match.group(0)

        return None, None

    @staticmethod
    def _update_hierarchy(
        hierarchy: Dict[str, Optional[str]],
        level: str,
        value: Optional[str],
    ) -> None:
        if level == "chapter":
            hierarchy["chapter"] = value
            hierarchy["section"] = None
            hierarchy["article"] = None
            hierarchy["clause"] = None
            hierarchy["point"] = None
            return

        if level == "section":
            hierarchy["section"] = value
            hierarchy["article"] = None
            hierarchy["clause"] = None
            hierarchy["point"] = None
            return

        if level == "article":
            hierarchy["article"] = value
            hierarchy["clause"] = None
            hierarchy["point"] = None
            return

        if level == "clause":
            hierarchy["clause"] = value
            hierarchy["point"] = None
            return

        if level == "point":
            hierarchy["point"] = value

    def _apply_overlap(self, chunks: List[ChunkRecord]) -> List[ChunkRecord]:
        if self.overlap_tokens <= 0:
            return chunks

        merged: List[ChunkRecord] = [chunks[0]]
        for idx in range(1, len(chunks)):
            prev = merged[-1]
            current = chunks[idx]

            overlap_tokens = prev.tokens[-self.overlap_tokens :] if prev.tokens else []
            if overlap_tokens:
                overlap_prefix = " ".join(overlap_tokens)
                combined_text = f"{overlap_prefix}\n{current.chunk_text}"
                combined_tokens = tokenize_vi(combined_text)

                current = ChunkRecord(
                    chunk_uid=current.chunk_uid,
                    doc_id=current.doc_id,
                    doc_number=current.doc_number,
                    doc_title=current.doc_title,
                    chunk_order=current.chunk_order,
                    chunk_text=combined_text,
                    token_count=len(combined_tokens),
                    tokens=combined_tokens,
                    chapter=current.chapter,
                    section=current.section,
                    article=current.article,
                    clause=current.clause,
                    point=current.point,
                    references_to=current.references_to,
                    metadata=current.metadata,
                )

            merged.append(current)

        return merged


def _safe_id(text: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", text.strip())
    safe = re.sub(r"_+", "_", safe)
    return safe.strip("_") or "doc"
