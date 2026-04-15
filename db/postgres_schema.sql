-- ============================================================
-- GraphRAG Vietnamese Law — PostgreSQL Schema
-- Central Storage (Source of Truth)
-- ============================================================

-- Bảng lưu metadata văn bản luật gốc
CREATE TABLE IF NOT EXISTS legal_documents (
    id              SERIAL PRIMARY KEY,
    doc_id          VARCHAR(50) UNIQUE NOT NULL,      -- VD: "LUAT-2019-45"
    doc_type        VARCHAR(100),                     -- Luật, Nghị định, Thông tư, Quyết định...
    doc_number      VARCHAR(100),                     -- Số hiệu: 45/2019/QH14
    title           TEXT NOT NULL,                    -- Tên đầy đủ văn bản
    issuing_body    VARCHAR(200),                     -- Cơ quan ban hành
    issued_date     DATE,                             -- Ngày ban hành
    effective_date  DATE,                             -- Ngày có hiệu lực
    status          VARCHAR(50) DEFAULT 'active',     -- active / replaced / expired
    references_to   TEXT[] DEFAULT '{}',              -- Danh sách doc_id mà văn bản này dẫn chiếu tới
    replaced_by     VARCHAR(50),                      -- doc_id của văn bản thay thế (nếu có)
    guided_by       TEXT[] DEFAULT '{}',              -- doc_id của các văn bản hướng dẫn
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Bảng lưu nội dung đã chunking
CREATE TABLE IF NOT EXISTS legal_chunks (
    id              SERIAL PRIMARY KEY,
    doc_id          VARCHAR(50) NOT NULL REFERENCES legal_documents(doc_id) ON DELETE CASCADE,
    chunk_id        VARCHAR(100) UNIQUE NOT NULL,     -- VD: "LUAT-2019-45_C2_D5_K1"
    content         TEXT NOT NULL,                    -- Nội dung text của chunk
    chapter         VARCHAR(100),                     -- Chương (nếu có)
    article         VARCHAR(100),                     -- Điều
    clause          VARCHAR(100),                     -- Khoản
    section         VARCHAR(100),                     -- Mục
    metadata        JSONB DEFAULT '{}',               -- Metadata bổ sung linh hoạt
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Indexes cho truy vấn nhanh
CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON legal_chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunks_chunk_id ON legal_chunks(chunk_id);
CREATE INDEX IF NOT EXISTS idx_docs_doc_id ON legal_documents(doc_id);
CREATE INDEX IF NOT EXISTS idx_docs_status ON legal_documents(status);
CREATE INDEX IF NOT EXISTS idx_chunks_metadata ON legal_chunks USING GIN (metadata);
