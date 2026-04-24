"""
orchestrator/message_queue.py — Redis-based message queue.

Sử dụng Redis Streams để:
  1. Publish các chat tasks (câu hỏi) vào queue
  2. Consume async (workers xử lý offline: analytics, re-ranking, etc.)
  3. Cache recent responses để tránh duplicate queries
  4. Pub/Sub cho real-time events (typing indicator, status updates)

Redis Streams được chọn thay vì List-based queue vì:
  - Consumer groups cho horizontal scaling
  - Message acknowledgment (không mất message)
  - Message history (replay)
"""

from __future__ import annotations

import json
import hashlib
import time
from typing import Any, Dict, List, Optional

try:
    import redis

    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

from orchestrator.config import OrchestratorConfig


# Stream names

STREAM_CHAT_REQUESTS = "stream:chat_requests"
STREAM_CHAT_RESULTS = "stream:chat_results"
STREAM_ANALYTICS = "stream:analytics"

CHANNEL_EVENTS = "channel:orchestrator_events"

CACHE_PREFIX = "cache:chat:"
CACHE_TTL_SECONDS = 300  # 5 phút


class RedisMessageQueue:
    """Message queue + cache + pub/sub qua Redis."""

    def __init__(self, cfg: OrchestratorConfig) -> None:
        if not _REDIS_AVAILABLE:
            raise ImportError(
                "redis package is required. pip install redis"
            )
        self.cfg = cfg
        self._client: Optional[redis.Redis] = None

    # ── Connection ──────────────────────────────────────────────────────

    @property
    def client(self) -> "redis.Redis":
        if self._client is None:
            self._client = redis.Redis.from_url(
                self.cfg.redis_url,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # ═════════════════════════════════════════════════════════════════════
    # 1. Response Cache — tránh duplicate queries
    # ═════════════════════════════════════════════════════════════════════

    @staticmethod
    def _cache_key(question: str) -> str:
        """Tạo cache key từ câu hỏi (normalized + hashed)."""
        normalized = question.strip().lower()
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
        return f"{CACHE_PREFIX}{digest}"

    def get_cached_response(self, question: str) -> Optional[Dict[str, Any]]:
        """Kiểm tra cache cho câu hỏi. Returns None nếu cache miss."""
        try:
            key = self._cache_key(question)
            raw = self.client.get(key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return None

    def cache_response(
        self, question: str, response: Dict[str, Any], ttl: int = CACHE_TTL_SECONDS
    ) -> None:
        """Cache response cho một câu hỏi."""
        try:
            key = self._cache_key(question)
            payload = json.dumps(response, ensure_ascii=False, default=str)
            self.client.setex(key, ttl, payload)
        except Exception:
            pass  # Cache failure should not break the flow

    def invalidate_cache(self, question: str) -> None:
        """Xóa cache cho một câu hỏi cụ thể."""
        try:
            key = self._cache_key(question)
            self.client.delete(key)
        except Exception:
            pass

    # ═════════════════════════════════════════════════════════════════════
    # 2. Chat Request Stream — async task queue
    # ═════════════════════════════════════════════════════════════════════

    def publish_chat_request(
        self,
        conversation_id: str,
        user_message_id: str,
        question: str,
        top_k_vector: int = 5,
        top_k_graph: int = 5,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Publish một chat request vào Redis Stream.

        Returns:
            message_id trong Redis Stream
        """
        entry = {
            "conversation_id": conversation_id,
            "user_message_id": user_message_id,
            "question": question,
            "top_k_vector": str(top_k_vector),
            "top_k_graph": str(top_k_graph),
            "metadata": json.dumps(metadata or {}),
            "timestamp": str(time.time()),
        }
        msg_id = self.client.xadd(STREAM_CHAT_REQUESTS, entry)
        return msg_id

    def publish_chat_result(
        self,
        conversation_id: str,
        assistant_message_id: str,
        answer: str,
        latency_ms: float,
        metadata: Optional[Dict] = None,
    ) -> str:
        """Publish kết quả chat vào result stream."""
        entry = {
            "conversation_id": conversation_id,
            "assistant_message_id": assistant_message_id,
            "answer": answer[:500],  # Truncate cho stream
            "latency_ms": str(latency_ms),
            "metadata": json.dumps(metadata or {}),
            "timestamp": str(time.time()),
        }
        msg_id = self.client.xadd(STREAM_CHAT_RESULTS, entry)
        return msg_id

    def consume_chat_requests(
        self,
        consumer_group: str = "orchestrator_workers",
        consumer_name: str = "worker_1",
        count: int = 1,
        block_ms: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Consume chat requests từ stream (consumer group).

        Tạo consumer group nếu chưa tồn tại.
        """
        # Ensure consumer group exists
        try:
            self.client.xgroup_create(
                STREAM_CHAT_REQUESTS, consumer_group, id="0", mkstream=True
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        messages = self.client.xreadgroup(
            groupname=consumer_group,
            consumername=consumer_name,
            streams={STREAM_CHAT_REQUESTS: ">"},
            count=count,
            block=block_ms,
        )

        results = []
        if messages:
            for stream_name, entries in messages:
                for msg_id, fields in entries:
                    results.append({"stream_id": msg_id, **fields})
        return results

    def ack_chat_request(
        self,
        stream_id: str,
        consumer_group: str = "orchestrator_workers",
    ) -> None:
        """Acknowledge đã xử lý xong một chat request."""
        self.client.xack(STREAM_CHAT_REQUESTS, consumer_group, stream_id)

    # ═════════════════════════════════════════════════════════════════════
    # 3. Analytics Stream — log cho phân tích offline
    # ═════════════════════════════════════════════════════════════════════

    def publish_analytics_event(
        self,
        event_type: str,
        data: Dict[str, Any],
    ) -> str:
        """Publish analytics event (retrieval stats, feedback, etc.)."""
        entry = {
            "event_type": event_type,
            "data": json.dumps(data, ensure_ascii=False, default=str),
            "timestamp": str(time.time()),
        }
        msg_id = self.client.xadd(
            STREAM_ANALYTICS, entry, maxlen=10000  # Keep last 10K events
        )
        return msg_id

    # ═════════════════════════════════════════════════════════════════════
    # 4. Pub/Sub — real-time events
    # ═════════════════════════════════════════════════════════════════════

    def publish_event(self, event: Dict[str, Any]) -> int:
        """Publish a real-time event (typing, status update) via Pub/Sub."""
        payload = json.dumps(event, ensure_ascii=False, default=str)
        return self.client.publish(CHANNEL_EVENTS, payload)

    def subscribe_events(self):
        """Subscribe to real-time events. Returns a PubSub object."""
        pubsub = self.client.pubsub()
        pubsub.subscribe(CHANNEL_EVENTS)
        return pubsub

    # ═════════════════════════════════════════════════════════════════════
    # 5. Rate Limiting (per-user)
    # ═════════════════════════════════════════════════════════════════════

    def check_rate_limit(
        self,
        user_id: str,
        max_requests: int = 30,
        window_seconds: int = 60,
    ) -> bool:
        """Check nếu user còn trong rate limit. Returns True nếu allowed."""
        key = f"ratelimit:{user_id}"
        try:
            pipe = self.client.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds)
            result = pipe.execute()
            current_count = result[0]
            return current_count <= max_requests
        except Exception:
            return True  # Fail-open: cho phép nếu Redis lỗi

    # ═════════════════════════════════════════════════════════════════════
    # Health Check
    # ═════════════════════════════════════════════════════════════════════

    def health_check(self) -> Dict[str, Any]:
        """Check Redis connectivity."""
        try:
            self.client.ping()
            info = self.client.info("memory")
            return {
                "status": "up",
                "used_memory_human": info.get("used_memory_human", "N/A"),
            }
        except Exception as exc:
            return {"status": "down", "error": str(exc)}
