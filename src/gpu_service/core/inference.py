from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import numpy as np

from gpu_service.core.model import EmbeddingModel, NSFWClassifier
from gpu_service.settings import settings


class InferenceService:
    def __init__(self) -> None:
        devices = settings.gpu_devices or ([settings.device] if settings.device else [])
        self.embedders = [EmbeddingModel(settings.model_name, d) for d in devices]
        if not self.embedders:
            self.embedders = [EmbeddingModel(settings.model_name, "cpu")]
        self.classifier = NSFWClassifier(settings.nsfw_keywords)

    @property
    def dim(self) -> int:
        return self.embedders[0].dim

    def embed(self, texts: List[str]) -> np.ndarray:
        if len(self.embedders) == 1:
            return self.embedders[0].encode(texts)
        return self._encode_multi(texts)

    def classify(self, texts: List[str]) -> List[float]:
        return self.classifier.classify(texts)

    def infer(self, texts: List[str]) -> Tuple[np.ndarray, List[float]]:
        embeddings = self.embed(texts)
        scores = self.classify(texts)
        return embeddings, scores

    def _encode_multi(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, self.dim), dtype=np.float32)
        buckets = [[] for _ in self.embedders]
        for idx, text in enumerate(texts):
            buckets[idx % len(self.embedders)].append((idx, text))
        with ThreadPoolExecutor(max_workers=len(self.embedders)) as executor:
            futures = []
            for embedder, bucket in zip(self.embedders, buckets):
                if not bucket:
                    continue
                idxs, chunk = zip(*bucket)
                futures.append((idxs, executor.submit(embedder.encode, list(chunk))))
        output = np.empty((len(texts), self.dim), dtype=np.float32)
        for idxs, future in futures:
            chunk = future.result()
            for offset, idx in enumerate(idxs):
                output[idx] = chunk[offset]
        return output
