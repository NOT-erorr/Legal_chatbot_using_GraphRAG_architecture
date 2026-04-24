"""
orchestrator/persistence.py — PostgreSQL persistence layer.

Quản lý conversations, messages, retrieval_logs, feedback
theo schema ``db/chatbot_schema.sql``.

Sử dụng ``psycopg`` (async-ready) với connection pool.
"""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg2
    import psycopg2.extras

    # Register UUID adapter for psycopg2
    psycopg2.extras.register_uuid()
    _PG_AVAILABLE = True
except ImportError:
    _PG_AVAILABLE = False

from orchestrator.config import OrchestratorConfig


class ChatPersistence:
    """PostgreSQL CRUD cho chatbot schema (users, conversations, messages, …)."""

    def __init__(self, cfg: OrchestratorConfig) -> None:
        if not _PG_AVAILABLE:
            raise ImportError(
                "psycopg2 is required for persistence. "
                "pip install psycopg2-binary"
            )
        self.cfg = cfg
        self._conn = None

    # ── Connection ──────────────────────────────────────────────────────

    @property
    def conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.cfg.pg_host,
                port=self.cfg.pg_port,
                dbname=self.cfg.pg_database,
                user=self.cfg.pg_user,
                password=self.cfg.pg_password,
            )
            self._conn.autocommit = False
        return self._conn

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    @contextmanager
    def _cursor(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()

    # Users

    def get_or_create_guest_user(self) -> str:
        """Get or create a default guest user. Returns user_id as string."""
        guest_email = "guest@legal-chatbot.local"
        with self._cursor() as cur:
            cur.execute("SELECT id FROM users WHERE email = %s", (guest_email,))
            row = cur.fetchone()
            if row:
                return str(row["id"])

            user_id = uuid.uuid4()
            cur.execute(
                """
                INSERT INTO users (id, email, full_name, hashed_password, role)
                VALUES (%s, %s, %s, %s, 'guest')
                ON CONFLICT (email) DO NOTHING
                RETURNING id
                """,
                (user_id, guest_email, "Guest User", "no-password"),
            )
            result = cur.fetchone()
            return str(result["id"] if result else user_id)

    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT id, email, full_name, role, is_active, created_at FROM users WHERE id = %s",
                (user_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    # Conversations

    def create_conversation(
        self,
        user_id: str,
        title: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Create a new conversation. Returns conversation_id."""
        conv_id = uuid.uuid4()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO conversations (id, user_id, title, metadata)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (conv_id, user_id, title, json.dumps(metadata or {})),
            )
            result = cur.fetchone()
            return str(result["id"])

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM conversations WHERE id = %s AND status = 'active'",
                (conversation_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def list_conversations(
        self, user_id: str, limit: int = 20, offset: int = 0
    ) -> List[Dict[str, Any]]:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.title, c.status, c.metadata, c.created_at, c.updated_at,
                       COUNT(m.id) AS message_count,
                       MAX(m.created_at) AS last_message_at
                FROM conversations c
                LEFT JOIN messages m ON m.conversation_id = c.id
                WHERE c.user_id = %s AND c.status = 'active'
                GROUP BY c.id
                ORDER BY c.updated_at DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            return [dict(row) for row in cur.fetchall()]

    def update_conversation_title(self, conversation_id: str, title: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE conversations SET title = %s WHERE id = %s",
                (title, conversation_id),
            )

    def archive_conversation(self, conversation_id: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE conversations SET status = 'archived' WHERE id = %s",
                (conversation_id,),
            )

    # Messages

    def save_user_message(
        self, conversation_id: str, content: str
    ) -> str:
        """Save a user message. Returns message_id."""
        msg_id = uuid.uuid4()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages (id, conversation_id, role, content)
                VALUES (%s, %s, 'user', %s)
                RETURNING id
                """,
                (msg_id, conversation_id, content),
            )
            result = cur.fetchone()
            return str(result["id"])

    def save_assistant_message(
        self,
        conversation_id: str,
        content: str,
        citations: Optional[List[Dict]] = None,
        sources: Optional[List[Dict]] = None,
        confidence_score: Optional[float] = None,
        input_tokens: Optional[int] = None,
        output_tokens: Optional[int] = None,
        latency_ms: Optional[int] = None,
    ) -> str:
        """Save an assistant message with retrieval metadata. Returns message_id."""
        msg_id = uuid.uuid4()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages
                    (id, conversation_id, role, content, citations, sources,
                     confidence_score, input_tokens, output_tokens, latency_ms)
                VALUES (%s, %s, 'assistant', %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    msg_id,
                    conversation_id,
                    content,
                    json.dumps(citations or []),
                    json.dumps(sources or []),
                    confidence_score,
                    input_tokens,
                    output_tokens,
                    latency_ms,
                ),
            )
            result = cur.fetchone()
            return str(result["id"])

    def get_conversation_history(
        self, conversation_id: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get messages for a conversation (chronological order)."""
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT id, role, content, citations, sources,
                       confidence_score, latency_ms, created_at
                FROM messages
                WHERE conversation_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (conversation_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]

    # Retrieval Logs

    def log_retrieval(
        self,
        message_id: str,
        retrieval_type: str,  # 'vector', 'graph', 'hybrid'
        query_used: str,
        results_meta: List[Dict],
        latency_ms: Optional[int] = None,
    ) -> str:
        """Log a retrieval operation. Returns log_id."""
        log_id = uuid.uuid4()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO retrieval_logs
                    (id, message_id, retrieval_type, query_used,
                     results_meta, result_count, latency_ms)
                VALUES (%s, %s, %s::retrieval_type, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    log_id,
                    message_id,
                    retrieval_type,
                    query_used,
                    json.dumps(results_meta),
                    len(results_meta),
                    latency_ms,
                ),
            )
            result = cur.fetchone()
            return str(result["id"])

    # Feedback

    def save_feedback(
        self,
        message_id: str,
        user_id: str,
        feedback_type: str = "thumbs_up",
        rating: Optional[int] = None,
        comment: Optional[str] = None,
    ) -> str:
        """Save user feedback on a message. Returns feedback_id."""
        fb_id = uuid.uuid4()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO feedback (id, message_id, user_id, feedback_type, rating, comment)
                VALUES (%s, %s, %s, %s::feedback_type, %s, %s)
                ON CONFLICT (message_id, user_id)
                DO UPDATE SET feedback_type = EXCLUDED.feedback_type,
                              rating = EXCLUDED.rating,
                              comment = EXCLUDED.comment,
                              created_at = NOW()
                RETURNING id
                """,
                (fb_id, message_id, user_id, feedback_type, rating, comment),
            )
            result = cur.fetchone()
            return str(result["id"])

    # Pipeline Runs

    def log_pipeline_run(
        self,
        status: str = "running",
        docs_processed: int = 0,
        chunks_created: int = 0,
        vectors_upserted: int = 0,
        graph_nodes: int = 0,
        error_details: Optional[Dict] = None,
    ) -> str:
        """Log a pipeline run. Returns run_id."""
        run_id = uuid.uuid4()
        completed_at = (
            datetime.now(timezone.utc)
            if status in ("completed", "failed")
            else None
        )
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (id, status, docs_processed, chunks_created,
                     vectors_upserted, graph_nodes, error_details, completed_at)
                VALUES (%s, %s::pipeline_status, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    run_id,
                    status,
                    docs_processed,
                    chunks_created,
                    vectors_upserted,
                    graph_nodes,
                    json.dumps(error_details) if error_details else None,
                    completed_at,
                ),
            )
            result = cur.fetchone()
            return str(result["id"])

    # Health Check

    def health_check(self) -> Dict[str, Any]:
        """Check PostgreSQL connectivity and basic stats."""
        try:
            with self._cursor() as cur:
                cur.execute("SELECT 1")
                cur.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM users) AS users,
                        (SELECT count(*) FROM conversations WHERE status = 'active') AS conversations,
                        (SELECT count(*) FROM messages) AS messages
                    """
                )
                row = cur.fetchone()
                return {"status": "up", "stats": dict(row) if row else {}}
        except Exception as exc:
            return {"status": "down", "error": str(exc)}
