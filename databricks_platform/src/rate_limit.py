from __future__ import annotations

import threading
import time
from typing import Iterable, List


class DualTokenBucketRateLimiter:
    """A dual-bucket limiter for requests-per-minute and tokens-per-minute."""

    def __init__(
        self,
        rpm_limit: int,
        tpm_limit: int,
        safety_factor: float = 0.85,
    ) -> None:
        if rpm_limit <= 0 or tpm_limit <= 0:
            raise ValueError("rpm_limit and tpm_limit must be > 0")

        if not 0 < safety_factor <= 1:
            raise ValueError("safety_factor must be in (0, 1]")

        self.rpm_capacity = max(1.0, float(rpm_limit) * safety_factor)
        self.tpm_capacity = max(1.0, float(tpm_limit) * safety_factor)

        self._available_requests = self.rpm_capacity
        self._available_tokens = self.tpm_capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_refill)
        self._last_refill = now

        requests_per_second = self.rpm_capacity / 60.0
        tokens_per_second = self.tpm_capacity / 60.0

        self._available_requests = min(
            self.rpm_capacity,
            self._available_requests + elapsed * requests_per_second,
        )
        self._available_tokens = min(
            self.tpm_capacity,
            self._available_tokens + elapsed * tokens_per_second,
        )

    def acquire(self, token_cost: int) -> None:
        if token_cost <= 0:
            token_cost = 1

        while True:
            with self._lock:
                self._refill()

                enough_requests = self._available_requests >= 1.0
                enough_tokens = self._available_tokens >= float(token_cost)
                if enough_requests and enough_tokens:
                    self._available_requests -= 1.0
                    self._available_tokens -= float(token_cost)
                    return

                missing_requests = max(0.0, 1.0 - self._available_requests)
                missing_tokens = max(0.0, float(token_cost) - self._available_tokens)

                request_wait = (
                    missing_requests / (self.rpm_capacity / 60.0)
                    if missing_requests > 0
                    else 0.0
                )
                token_wait = (
                    missing_tokens / (self.tpm_capacity / 60.0)
                    if missing_tokens > 0
                    else 0.0
                )
                sleep_seconds = max(request_wait, token_wait, 0.02)

            # Sleep outside lock to let concurrent workers refill/acquire.
            time.sleep(min(sleep_seconds, 2.0))


def estimate_text_tokens(text: str) -> int:
    """Cheap token estimate for rate limiting; no tokenizer dependency required."""
    if not text:
        return 1

    # For Vietnamese legal text, chars/4 is conservative enough for quota control.
    return max(1, int(len(text) / 4))


def estimate_batch_tokens(texts: Iterable[str]) -> int:
    return sum(estimate_text_tokens(text) for text in texts)


def chunk_list(items: List[str], batch_size: int) -> List[List[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
