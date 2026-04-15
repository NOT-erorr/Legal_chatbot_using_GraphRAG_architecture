"""
vector_pipeline.py — Reader 1: PostgreSQL → FAISS
Module đọc dữ liệu từ PostgreSQL, tạo embedding bằng sentence-transformers,
và index vào FAISS cho tìm kiếm ngữ nghĩa (semantic search).
"""

import os
import json
import numpy as np
import faiss
from pathlib import Path
from typing import List, Dict, Any, Tuple
from sentence_transformers import SentenceTransformer

import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings
from ingestion.document_loader import DocumentLoader


class VectorPipeline:
    """
    Reader 1: Đọc chunks từ PostgreSQL → Tạo embeddings → Index vào FAISS.
    
    Workflow:
        1. Kết nối PostgreSQL, lấy toàn bộ legal_chunks
        2. Encode nội dung bằng paraphrase-multilingual-MiniLM-L12-v2
        3. Normalize vectors (L2) cho cosine similarity
        4. Build FAISS IndexFlatIP
        5. Lưu mapping {faiss_index → chunk_id} để truy xuất ngược
    """

    def __init__(self):
        print(f"📦 Loading embedding model: {settings.EMBEDDING_MODEL}...")
        self.model = SentenceTransformer(settings.EMBEDDING_MODEL)
        self.dimension = settings.EMBEDDING_DIMENSION
        self.index = None
        self.chunk_id_map: List[str] = []  # index position → chunk_id
        self.doc_loader = DocumentLoader()

    def build_index(self) -> int:
        """
        Đọc toàn bộ chunks từ PostgreSQL → Tạo FAISS index.

        Returns:
            Số lượng vectors đã index
        """
        print("🔄 Đang đọc chunks từ PostgreSQL...")
        chunks = self.doc_loader.get_all_chunks()

        if not chunks:
            print("⚠️  Không tìm thấy chunks trong database!")
            return 0

        print(f"📝 Tìm thấy {len(chunks)} chunks. Đang tạo embeddings...")

        # Lấy nội dung text để encode
        texts = [chunk["content"] for chunk in chunks]
        self.chunk_id_map = [chunk["chunk_id"] for chunk in chunks]

        # Tạo embeddings
        embeddings = self.model.encode(
            texts,
            show_progress_bar=True,
            convert_to_numpy=True,
            batch_size=32,
        )
        embeddings = embeddings.astype("float32")

        # Normalize cho cosine similarity
        faiss.normalize_L2(embeddings)

        # Tạo FAISS index (Inner Product = Cosine Similarity cho normalized vectors)
        self.index = faiss.IndexFlatIP(self.dimension)
        self.index.add(embeddings)

        print(f"✅ FAISS index đã tạo: {self.index.ntotal} vectors, {self.dimension} dimensions")
        return self.index.ntotal

    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Tìm kiếm ngữ nghĩa: Embed query → Search FAISS → Trả về chunk_ids.

        Args:
            query: Câu hỏi/truy vấn từ người dùng
            top_k: Số lượng kết quả trả về

        Returns:
            List[Dict] với keys: chunk_id, score, rank
        """
        if self.index is None or self.index.ntotal == 0:
            print("⚠️  FAISS index chưa được build hoặc trống!")
            return []

        # Encode query
        query_embedding = self.model.encode(
            [query], convert_to_numpy=True
        ).astype("float32")
        faiss.normalize_L2(query_embedding)

        # Search
        scores, indices = self.index.search(query_embedding, min(top_k, self.index.ntotal))

        results = []
        for rank, (score, idx) in enumerate(zip(scores[0], indices[0])):
            if idx == -1:  # FAISS returns -1 for invalid results
                continue
            results.append({
                "chunk_id": self.chunk_id_map[idx],
                "score": float(score),
                "rank": rank + 1,
            })

        return results

    def save_index(self, path: str = None):
        """
        Lưu FAISS index và chunk_id mapping ra disk.

        Args:
            path: Thư mục lưu (mặc định: settings.FAISS_INDEX_PATH)
        """
        if self.index is None:
            print("⚠️  Chưa có index để lưu!")
            return

        save_dir = Path(path or settings.FAISS_INDEX_PATH)
        save_dir.mkdir(parents=True, exist_ok=True)

        # Lưu FAISS index
        index_path = save_dir / "faiss.index"
        faiss.write_index(self.index, str(index_path))

        # Lưu chunk_id mapping
        mapping_path = save_dir / "chunk_id_map.json"
        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(self.chunk_id_map, f, ensure_ascii=False, indent=2)

        print(f"💾 Index đã lưu tại: {save_dir}")
        print(f"   - faiss.index: {index_path.stat().st_size / 1024:.1f} KB")
        print(f"   - chunk_id_map.json: {len(self.chunk_id_map)} entries")

    def load_index(self, path: str = None):
        """
        Load FAISS index và chunk_id mapping từ disk.

        Args:
            path: Thư mục chứa index (mặc định: settings.FAISS_INDEX_PATH)
        """
        load_dir = Path(path or settings.FAISS_INDEX_PATH)

        index_path = load_dir / "faiss.index"
        mapping_path = load_dir / "chunk_id_map.json"

        if not index_path.exists() or not mapping_path.exists():
            raise FileNotFoundError(
                f"Không tìm thấy index tại {load_dir}. "
                f"Hãy chạy build_index() trước."
            )

        # Load FAISS index
        self.index = faiss.read_index(str(index_path))

        # Load chunk_id mapping
        with open(mapping_path, "r", encoding="utf-8") as f:
            self.chunk_id_map = json.load(f)

        print(f"📂 Loaded FAISS index: {self.index.ntotal} vectors")
        print(f"   Chunk mapping: {len(self.chunk_id_map)} entries")

    def close(self):
        """Đóng kết nối database."""
        self.doc_loader.close()
