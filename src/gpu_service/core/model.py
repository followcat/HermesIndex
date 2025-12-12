import hashlib
from typing import List

import numpy as np

from gpu_service.settings import settings


class EmbeddingModel:
    def __init__(self, model_name: str, device: str) -> None:
        self.model = None
        self.dim = 384
        self.max_length = settings.max_length
        self.model_name = model_name
        self.device = device
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore

            self.model = SentenceTransformer(model_name, device=device)
            self.dim = self.model.get_sentence_embedding_dimension()
        except Exception:
            self.model = None

    def encode(self, texts: List[str]) -> np.ndarray:
        truncated = [t[: self.max_length] for t in texts]
        if self.model:
            return np.asarray(
                self.model.encode(
                    truncated,
                    batch_size=settings.batch_size,
                    show_progress_bar=False,
                    normalize_embeddings=True,
                ),
                dtype=np.float32,
            )
        return np.stack([self._hash_embed(t) for t in truncated])

    def _hash_embed(self, text: str) -> np.ndarray:
        h = hashlib.sha1(text.encode("utf-8")).digest()
        seed = int.from_bytes(h[:8], "big") ^ settings.seed
        rng = np.random.default_rng(seed)
        v = rng.normal(0, 1, self.dim).astype(np.float32)
        norm = np.linalg.norm(v)
        return v if norm == 0 else v / norm


class NSFWClassifier:
    def __init__(self, keywords: List[str]) -> None:
        self.keywords = [k.strip().lower() for k in keywords if k.strip()]

    def classify(self, texts: List[str]) -> List[float]:
        return [float(self._score_text(t)) for t in texts]

    def _score_text(self, text: str) -> float:
        lower = text.lower()
        if not lower:
            return 0.05
        hits = sum(1 for kw in self.keywords if kw and kw in lower)
        base = min(0.15 * hits, 0.9)
        entropy = float(len(set(lower.split())) / max(len(lower.split()), 1))
        return min(base + max(0.05, 0.6 * (1 - entropy)), 1.0)
