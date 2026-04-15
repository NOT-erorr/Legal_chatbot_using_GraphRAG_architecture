"""
chunker.py — Module chia nhỏ (chunking) văn bản luật theo cấu trúc pháp lý.
Hỗ trợ chunking theo Chương/Điều/Khoản/Mục.
"""

import re
from typing import List, Dict, Any


class LegalChunker:
    """
    Chia văn bản luật thành các chunks theo cấu trúc pháp lý.
    Mỗi chunk tương ứng với một Điều hoặc Khoản trong văn bản.
    """

    # Regex patterns cho cấu trúc văn bản luật Việt Nam
    CHAPTER_PATTERN = re.compile(
        r"(Chương\s+[IVXLCDM]+|CHƯƠNG\s+[IVXLCDM]+|Chương\s+\d+)",
        re.IGNORECASE
    )
    ARTICLE_PATTERN = re.compile(
        r"(Điều\s+\d+[a-z]?\.?)",
        re.IGNORECASE
    )
    CLAUSE_PATTERN = re.compile(
        r"(\d+\.\s)",
    )

    def __init__(self, doc_id: str, max_chunk_size: int = 1000):
        """
        Args:
            doc_id: Định danh văn bản gốc
            max_chunk_size: Kích thước tối đa của mỗi chunk (ký tự)
        """
        self.doc_id = doc_id
        self.max_chunk_size = max_chunk_size

    def chunk_document(self, text: str) -> List[Dict[str, Any]]:
        """
        Chia văn bản thành các chunks dựa trên cấu trúc Chương/Điều.

        Args:
            text: Nội dung toàn văn văn bản luật

        Returns:
            List các dict: {chunk_id, content, chapter, article, clause}
        """
        chunks = []
        current_chapter = None
        current_article = None
        current_content = []
        chunk_counter = 0

        lines = text.split("\n")

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            # Phát hiện Chương mới
            chapter_match = self.CHAPTER_PATTERN.match(stripped)
            if chapter_match:
                # Lưu chunk hiện tại trước khi chuyển chương
                if current_content:
                    chunk_counter += 1
                    chunks.append(self._create_chunk(
                        chunk_counter, current_chapter, current_article,
                        "\n".join(current_content)
                    ))
                    current_content = []
                current_chapter = chapter_match.group(1).strip()
                current_article = None
                continue

            # Phát hiện Điều mới
            article_match = self.ARTICLE_PATTERN.match(stripped)
            if article_match:
                # Lưu chunk hiện tại trước khi chuyển điều
                if current_content:
                    chunk_counter += 1
                    chunks.append(self._create_chunk(
                        chunk_counter, current_chapter, current_article,
                        "\n".join(current_content)
                    ))
                    current_content = []
                current_article = article_match.group(1).strip().rstrip(".")
                current_content.append(stripped)
                continue

            # Thêm nội dung vào chunk hiện tại
            current_content.append(stripped)

            # Nếu chunk quá lớn, tách ra
            if len("\n".join(current_content)) > self.max_chunk_size:
                chunk_counter += 1
                chunks.append(self._create_chunk(
                    chunk_counter, current_chapter, current_article,
                    "\n".join(current_content)
                ))
                current_content = []

        # Lưu chunk cuối cùng
        if current_content:
            chunk_counter += 1
            chunks.append(self._create_chunk(
                chunk_counter, current_chapter, current_article,
                "\n".join(current_content)
            ))

        return chunks

    def _create_chunk(
        self,
        counter: int,
        chapter: str,
        article: str,
        content: str,
    ) -> Dict[str, Any]:
        """Tạo một chunk dict."""
        # Tạo chunk_id dạng: DOC-ID_C1_D5
        parts = [self.doc_id]
        if chapter:
            # Trích số chương
            ch_num = re.search(r"(\d+|[IVXLCDM]+)", chapter)
            if ch_num:
                parts.append(f"C{ch_num.group(1)}")
        if article:
            # Trích số điều
            art_num = re.search(r"(\d+[a-z]?)", article)
            if art_num:
                parts.append(f"D{art_num.group(1)}")
        parts.append(str(counter))

        chunk_id = "_".join(parts)

        return {
            "chunk_id": chunk_id,
            "content": content.strip(),
            "chapter": chapter,
            "article": article,
            "clause": None,
            "section": None,
        }
