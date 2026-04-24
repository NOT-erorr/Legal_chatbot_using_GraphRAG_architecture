"""Convert raw ``content_html`` from the HuggingFace dataset into clean,
structured plain text that the :class:`RuleBasedHierarchicalChunker` can parse.

The module uses only the Python standard library (``html.parser``).
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import List, Optional


# Tags whose content should be completely ignored.
_SKIP_TAGS = frozenset({
    "script", "style", "noscript", "iframe", "svg", "math",
    "head", "meta", "link",
})

# Block-level tags that should produce a line break *before* their content.
_BLOCK_TAGS = frozenset({
    "p", "div", "br", "hr", "li", "ol", "ul",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "table", "tr", "td", "th", "thead", "tbody", "tfoot",
    "blockquote", "pre", "section", "article", "header", "footer",
    "dt", "dd", "dl", "figcaption", "figure", "main", "nav", "aside",
})

# Collapse runs of whitespace (but not newlines which we use as structure).
_MULTI_SPACE = re.compile(r"[^\S\n]+")
# Collapse more than 2 consecutive blank lines.
_MULTI_NEWLINE = re.compile(r"\n{3,}")


class _LegalHTMLParser(HTMLParser):
    """Streaming HTML → text converter that preserves block structure."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: List[str] = []
        self._skip_depth: int = 0

    # --- parser callbacks ---------------------------------------------------

    def handle_starttag(self, tag: str, attrs: list) -> None:
        tag_lower = tag.lower()
        if tag_lower in _SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth > 0:
            return
        if tag_lower in _BLOCK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower in _SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth > 0:
            return
        if tag_lower in _BLOCK_TAGS:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        self._parts.append(data)

    # --- public API ----------------------------------------------------------

    def get_text(self) -> str:
        raw = "".join(self._parts)
        # Collapse inline whitespace (spaces, tabs) but keep newlines.
        raw = _MULTI_SPACE.sub(" ", raw)
        # Normalise line breaks.
        lines = [line.strip() for line in raw.split("\n")]
        text = "\n".join(lines)
        # Remove excessive blank lines.
        text = _MULTI_NEWLINE.sub("\n\n", text)
        return text.strip()


def html_to_text(html: Optional[str]) -> str:
    """Convert a raw HTML string into clean plain text.

    Returns an empty string for ``None`` or empty input.
    """
    if not html or not html.strip():
        return ""

    parser = _LegalHTMLParser()
    try:
        parser.feed(html)
    except Exception:
        # If the parser chokes on malformed HTML, fall back to aggressive
        # regex-based tag stripping.
        return _fallback_strip(html)

    text = parser.get_text()
    return text if text else _fallback_strip(html)


def _fallback_strip(html: str) -> str:
    """Last-resort tag stripper for badly broken HTML."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = _MULTI_SPACE.sub(" ", text)
    lines = [line.strip() for line in text.split("\n")]
    lines = [line for line in lines if line]
    return "\n".join(lines).strip()
