from __future__ import annotations

import random
import time
from typing import Any, Iterable, List, Optional, Sequence

from rate_limit import DualTokenBucketRateLimiter, estimate_text_tokens


class GeminiEmbeddingClient:
    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-embedding-001",
        rpm_limit: int = 3000,
        tpm_limit: int = 1_000_000,
        safety_factor: float = 0.85,
        batch_size: int = 32,
        max_batch_tokens: int = 12_000,
        max_attempts: int = 6,
    ) -> None:
        if not api_key:
            raise ValueError("Gemini API key cannot be empty")

        self.model_name = model_name
        self.batch_size = max(1, batch_size)
        self.max_batch_tokens = max(1, max_batch_tokens)
        self.max_attempts = max(1, max_attempts)

        self._limiter = DualTokenBucketRateLimiter(
            rpm_limit=rpm_limit,
            tpm_limit=tpm_limit,
            safety_factor=safety_factor,
        )

        self._provider = ""
        self._client: Any = None

        try:
            from google import genai  # type: ignore

            self._client = genai.Client(api_key=api_key)
            self._provider = "google_genai"
        except Exception:
            try:
                import google.generativeai as legacy_genai  # type: ignore

                legacy_genai.configure(api_key=api_key)
                self._client = legacy_genai
                self._provider = "legacy_generativeai"
            except Exception as exc:
                raise ImportError(
                    "Install `google-genai` or `google-generativeai` to use Gemini embeddings"
                ) from exc

    def embed_texts(
        self,
        texts: Sequence[str],
        token_counts: Optional[Sequence[int]] = None,
    ) -> List[List[float]]:
        if not texts:
            return []

        if token_counts is None:
            token_counts = [estimate_text_tokens(text) for text in texts]

        if len(token_counts) != len(texts):
            raise ValueError("token_counts length must match texts length")

        output: List[List[float]] = []
        for batch_texts, batch_token_cost in self._build_batches(texts, token_counts):
            self._limiter.acquire(batch_token_cost)
            batch_vectors = self._embed_batch_with_retry(batch_texts)
            if len(batch_vectors) != len(batch_texts):
                raise RuntimeError(
                    "Gemini embedding response size mismatch: "
                    f"expected {len(batch_texts)}, got {len(batch_vectors)}"
                )
            output.extend(batch_vectors)

        return output

    def _build_batches(
        self,
        texts: Sequence[str],
        token_counts: Sequence[int],
    ) -> Iterable[tuple[List[str], int]]:
        batch_texts: List[str] = []
        batch_token_cost = 0

        for text, token_cost in zip(texts, token_counts):
            token_cost = max(1, int(token_cost))

            would_exceed_size = len(batch_texts) >= self.batch_size
            would_exceed_tokens = (batch_token_cost + token_cost) > self.max_batch_tokens

            if batch_texts and (would_exceed_size or would_exceed_tokens):
                yield batch_texts, batch_token_cost
                batch_texts = []
                batch_token_cost = 0

            batch_texts.append(text)
            batch_token_cost += token_cost

        if batch_texts:
            yield batch_texts, batch_token_cost

    def _embed_batch_with_retry(self, batch_texts: List[str]) -> List[List[float]]:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return self._embed_batch(batch_texts)
            except Exception as exc:
                message = str(exc).lower()
                retriable = any(
                    token in message
                    for token in [
                        "429",
                        "quota",
                        "rate",
                        "unavailable",
                        "temporar",
                        "timeout",
                        "deadline",
                    ]
                )
                if not retriable or attempt >= self.max_attempts:
                    raise

                backoff_seconds = min(30.0, (2 ** (attempt - 1)) + random.random())
                time.sleep(backoff_seconds)

        raise RuntimeError("Unexpected retry exit in _embed_batch_with_retry")

    def _embed_batch(self, batch_texts: List[str]) -> List[List[float]]:
        if self._provider == "google_genai":
            response = self._client.models.embed_content(
                model=self.model_name,
                contents=batch_texts,
                config={"task_type": "RETRIEVAL_DOCUMENT"},
            )
            return self._extract_embeddings(response)

        if self._provider == "legacy_generativeai":
            payload = batch_texts if len(batch_texts) > 1 else batch_texts[0]
            response = self._client.embed_content(
                model=self.model_name,
                content=payload,
                task_type="retrieval_document",
            )
            vectors = self._extract_embeddings(response)
            if len(batch_texts) == 1 and len(vectors) == 1:
                return vectors
            return vectors

        raise RuntimeError("Gemini provider was not initialized")

    def _extract_embeddings(self, response: object) -> List[List[float]]:
        # New google-genai objects typically expose response.embeddings.
        if hasattr(response, "embeddings"):
            return self._extract_from_iterable(getattr(response, "embeddings"))

        # Dict fallback (legacy libs / explicit to_dict).
        if isinstance(response, dict):
            if "embeddings" in response:
                return self._extract_from_iterable(response["embeddings"])
            if "embedding" in response:
                single = self._extract_vector(response["embedding"])
                return [single]

        # Some SDK versions return list directly.
        if isinstance(response, list):
            return self._extract_from_iterable(response)

        # Dataclass-like objects may provide to_dict.
        to_dict = getattr(response, "to_dict", None)
        if callable(to_dict):
            obj = to_dict()
            if isinstance(obj, dict):
                if "embeddings" in obj:
                    return self._extract_from_iterable(obj["embeddings"])
                if "embedding" in obj:
                    single = self._extract_vector(obj["embedding"])
                    return [single]

        raise RuntimeError(f"Unsupported Gemini embedding response type: {type(response)}")

    def _extract_from_iterable(self, items: object) -> List[List[float]]:
        if not isinstance(items, list):
            items = list(items)  # type: ignore[arg-type]

        vectors: List[List[float]] = []
        for item in items:
            vectors.append(self._extract_vector(item))
        return vectors

    def _extract_vector(self, item: object) -> List[float]:
        if isinstance(item, list):
            return [float(value) for value in item]

        if isinstance(item, dict):
            if "values" in item:
                return [float(value) for value in item["values"]]
            if "embedding" in item:
                return [float(value) for value in item["embedding"]]

        if hasattr(item, "values"):
            values = getattr(item, "values")
            return [float(value) for value in values]

        raise RuntimeError(f"Unsupported embedding vector item type: {type(item)}")
