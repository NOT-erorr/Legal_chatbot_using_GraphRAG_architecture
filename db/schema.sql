-- ============================================================================
-- GraphRAG Vietnamese Law  —  PostgreSQL Schema (schema.sql)
-- 3 bảng: DOCUMENTS ← DOCUMENT_CONTENT (1-1) ← DOCUMENT_CHUNKS (1-N)
-- ============================================================================

-- ============================================================================
-- 1. DOCUMENTS  —  Metadata Văn bản Quy phạm Pháp luật
-- ============================================================================
CREATE TABLE IF NOT EXISTS documents (
    id                  SERIAL PRIMARY KEY,
    title               TEXT          NOT NULL,          -- Tên đầy đủ văn bản
    so_ky_hieu          VARCHAR(100)  UNIQUE NOT NULL,   -- VD: 45/2019/QH14
    ngay_ban_hanh       DATE,                            -- Ngày ban hành
    loai_van_ban        VARCHAR(100),                    -- Luật, Nghị định, Thông tư…
    nganh               VARCHAR(150),                    -- Ngành: Lao động, Tài chính…
    linh_vuc            VARCHAR(150),                    -- Lĩnh vực: BHXH, Doanh nghiệp…
    co_quan_ban_hanh    VARCHAR(200),                    -- Quốc hội, Chính phủ…
    tinh_trang_hieu_luc VARCHAR(50) DEFAULT 'Còn hiệu lực',
    created_at          TIMESTAMP   DEFAULT NOW()
);

-- ============================================================================
-- 2. DOCUMENT_CONTENT  —  Nội dung đầy đủ (1-1 với DOCUMENTS)
-- ============================================================================
CREATE TABLE IF NOT EXISTS document_content (
    id              INTEGER PRIMARY KEY
                        REFERENCES documents(id) ON DELETE CASCADE,
    content_html    TEXT,                               -- Nội dung HTML gốc
    content_text    TEXT          NOT NULL,              -- Plaintext (dùng cho chunking)
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ============================================================================
-- 3. DOCUMENT_CHUNKS  —  Các đoạn văn bản đã cắt nhỏ (RecursiveCharacterTextSplitter)
-- ============================================================================
CREATE TABLE IF NOT EXISTS document_chunks (
    chunk_id        VARCHAR(120) PRIMARY KEY,           -- VD: "45_2019_QH14__chunk_003"
    doc_id          INTEGER      NOT NULL
                        REFERENCES documents(id) ON DELETE CASCADE,
    chunk_index     INTEGER      NOT NULL,              -- Thứ tự chunk trong văn bản
    content         TEXT         NOT NULL,               -- Nội dung text chunk
    chapter         VARCHAR(100),                        -- Chương (nếu trích xuất được)
    article         VARCHAR(100),                        -- Điều
    clause          VARCHAR(100),                        -- Khoản
    metadata        JSONB        DEFAULT '{}',           -- Metadata bổ sung
    created_at      TIMESTAMP    DEFAULT NOW()
);

-- ============================================================================
-- Indexes
-- ============================================================================
CREATE INDEX IF NOT EXISTS idx_documents_so_ky_hieu       ON documents(so_ky_hieu);
CREATE INDEX IF NOT EXISTS idx_documents_loai_van_ban     ON documents(loai_van_ban);
CREATE INDEX IF NOT EXISTS idx_documents_co_quan          ON documents(co_quan_ban_hanh);
CREATE INDEX IF NOT EXISTS idx_documents_hieu_luc         ON documents(tinh_trang_hieu_luc);
CREATE INDEX IF NOT EXISTS idx_chunks_doc_id              ON document_chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunks_chunk_id            ON document_chunks(chunk_id);
CREATE INDEX IF NOT EXISTS idx_chunks_metadata            ON document_chunks USING GIN (metadata);

-- ============================================================================
-- Bảng phụ: NEO4J_DOCUMENT & NEO4J_RELATION  (Audit / Mirror cho Graph Pipeline)
-- Dùng để theo dõi dữ liệu đã đồng bộ sang Neo4j
-- ============================================================================
CREATE TABLE IF NOT EXISTS neo4j_document (
    doc_id          INTEGER PRIMARY KEY
                        REFERENCES documents(id) ON DELETE CASCADE,
    neo4j_node_id   BIGINT,                             -- Element ID trong Neo4j
    synced_at       TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS neo4j_relation (
    id              SERIAL PRIMARY KEY,
    source_doc_id   INTEGER      NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    target_doc_id   INTEGER      NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    relation_type   VARCHAR(30)  NOT NULL               -- AMENDS, REFERENCES, REPEALS, GUIDES
                        CHECK (relation_type IN ('AMENDS','REFERENCES','REPEALS','GUIDES')),
    description     TEXT,                                -- Mô tả quan hệ (tuỳ chọn)
    synced_at       TIMESTAMP    DEFAULT NOW(),
    UNIQUE (source_doc_id, target_doc_id, relation_type)
);

CREATE INDEX IF NOT EXISTS idx_neo4j_rel_source ON neo4j_relation(source_doc_id);
CREATE INDEX IF NOT EXISTS idx_neo4j_rel_target ON neo4j_relation(target_doc_id);
CREATE INDEX IF NOT EXISTS idx_neo4j_rel_type   ON neo4j_relation(relation_type);
