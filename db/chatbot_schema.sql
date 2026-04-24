
-- PostgreSQL Schema — Legal Chatbot Assistant v2.0
-- Database: legal_chatbot


CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- full-text search

-- ── ENUM types    
CREATE TYPE user_role      AS ENUM ('admin', 'user', 'guest');
CREATE TYPE conv_status    AS ENUM ('active', 'archived', 'deleted');
CREATE TYPE message_role   AS ENUM ('user', 'assistant', 'system');
CREATE TYPE retrieval_type AS ENUM ('vector', 'graph', 'hybrid', 'keyword');
CREATE TYPE feedback_type  AS ENUM ('thumbs_up', 'thumbs_down', 'flag', 'correction');
CREATE TYPE pipeline_status AS ENUM ('running', 'completed', 'failed', 'skipped');


-- 1. users

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    full_name       VARCHAR(255),
    hashed_password VARCHAR(255) NOT NULL,
    role            user_role    NOT NULL DEFAULT 'user',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email    ON users (email);
CREATE INDEX idx_users_role     ON users (role);
CREATE INDEX idx_users_active   ON users (is_active) WHERE is_active = TRUE;


-- 2. conversations — phiên hội thoại

CREATE TABLE conversations (
    id         UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID         NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title      VARCHAR(500),                      -- auto-generated từ turn đầu
    status     conv_status  NOT NULL DEFAULT 'active',
    metadata   JSONB        NOT NULL DEFAULT '{}', -- {legal_domain, tags, ...}
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_conv_user_id   ON conversations (user_id);
CREATE INDEX idx_conv_status    ON conversations (status);
CREATE INDEX idx_conv_user_time ON conversations (user_id, created_at DESC);
CREATE INDEX idx_conv_metadata  ON conversations USING GIN (metadata);


-- 3. messages — lịch sử trò chuyện (bảng chính)

CREATE TABLE messages (
    id                  UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    conversation_id     UUID         NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role                message_role NOT NULL,                    -- 'user' | 'assistant'
    content             TEXT         NOT NULL,                    -- nội dung tin nhắn
    citations           JSONB        NOT NULL DEFAULT '[]',       -- [{doc_id, chunk_id, article, url}]
    sources             JSONB        NOT NULL DEFAULT '[]',       -- [{title, doc_number, url, relevance}]
    confidence_score    FLOAT,                                    -- 0.0–1.0 từ LangGraph validate node
    -- LLM usage tracking
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    latency_ms          INTEGER,                                  -- thời gian phản hồi
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    -- Không có updated_at: messages là immutable sau khi tạo
);

CREATE INDEX idx_msg_conversation  ON messages (conversation_id, created_at ASC);
CREATE INDEX idx_msg_role          ON messages (role);
CREATE INDEX idx_msg_citations     ON messages USING GIN (citations);
CREATE INDEX idx_msg_sources       ON messages USING GIN (sources);
-- Full-text search trên nội dung tin nhắn
CREATE INDEX idx_msg_content_fts   ON messages USING GIN (to_tsvector('simple', content));

COMMENT ON TABLE  messages                   IS 'Lịch sử toàn bộ tin nhắn trong mỗi phiên hội thoại';
COMMENT ON COLUMN messages.citations         IS 'Danh sách văn bản pháp lý được trích dẫn trong câu trả lời';
COMMENT ON COLUMN messages.confidence_score  IS 'Độ tin cậy từ LangGraph validate node (0–1)';


-- 4. retrieval_logs — log tra cứu vector/graph

CREATE TABLE retrieval_logs (
    id             UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_id     UUID          NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    retrieval_type retrieval_type NOT NULL,
    query_used     TEXT          NOT NULL,   -- embedding query hoặc Cypher
    results_meta   JSONB         NOT NULL DEFAULT '[]', -- [{chunk_id, score, doc_id}]
    result_count   INTEGER       NOT NULL DEFAULT 0,
    latency_ms     INTEGER,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_retr_message    ON retrieval_logs (message_id);
CREATE INDEX idx_retr_type       ON retrieval_logs (retrieval_type);
CREATE INDEX idx_retr_created    ON retrieval_logs (created_at DESC);


-- 5. feedback — đánh giá câu trả lời

CREATE TABLE feedback (
    id            UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_id    UUID          NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    user_id       UUID          NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    rating        SMALLINT      CHECK (rating BETWEEN 1 AND 5),
    comment       TEXT,
    feedback_type feedback_type NOT NULL DEFAULT 'thumbs_up',
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (message_id, user_id)  -- mỗi user chỉ feedback 1 lần / message
);

CREATE INDEX idx_fb_message ON feedback (message_id);
CREATE INDEX idx_fb_user    ON feedback (user_id);
CREATE INDEX idx_fb_type    ON feedback (feedback_type);


-- 6. pipeline_runs — trạng thái ETL Databricks

CREATE TABLE pipeline_runs (
    id               UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_version VARCHAR(20)     NOT NULL DEFAULT 'v2.0',
    status           pipeline_status NOT NULL DEFAULT 'running',
    docs_processed   INTEGER         NOT NULL DEFAULT 0,
    chunks_created   INTEGER         NOT NULL DEFAULT 0,
    vectors_upserted INTEGER         NOT NULL DEFAULT 0,
    graph_nodes      INTEGER         NOT NULL DEFAULT 0,
    error_details    JSONB,          -- NULL nếu thành công
    started_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX idx_pipeline_status  ON pipeline_runs (status);
CREATE INDEX idx_pipeline_started ON pipeline_runs (started_at DESC);


-- Triggers: tự động cập nhật updated_at

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_conv_updated_at
    BEFORE UPDATE ON conversations
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- Views hữu ích


-- Thống kê hội thoại theo user
CREATE VIEW vw_user_conversation_stats AS
SELECT
    u.id           AS user_id,
    u.email,
    COUNT(DISTINCT c.id)                        AS total_conversations,
    COUNT(m.id) FILTER (WHERE m.role = 'user')  AS total_questions,
    AVG(m.confidence_score)                     AS avg_confidence,
    AVG(m.latency_ms)                           AS avg_latency_ms,
    MAX(m.created_at)                           AS last_activity
FROM users u
LEFT JOIN conversations c ON c.user_id = u.id AND c.status = 'active'
LEFT JOIN messages m      ON m.conversation_id = c.id AND m.role = 'assistant'
GROUP BY u.id, u.email;

-- Lấy full conversation history (dùng cho LangGraph memory)
CREATE VIEW vw_conversation_history AS
SELECT
    m.id,
    m.conversation_id,
    m.role,
    m.content,
    m.citations,
    m.confidence_score,
    m.created_at,
    c.user_id,
    c.metadata AS conv_metadata
FROM messages m
JOIN conversations c ON c.id = m.conversation_id
ORDER BY m.conversation_id, m.created_at ASC;
