from typing import List, Tuple

import numpy as np

from gpu_service.core.model import EmbeddingModel, NSFWClassifier
from gpu_service.settings import settings


class InferenceService:
    def __init__(self) -> None:
        self.embedder = EmbeddingModel(settings.model_name, settings.device)
        self.classifier = NSFWClassifier(settings.nsfw_keywords)

    @property
    def dim(self) -> int:
        return self.embedder.dim

    def embed(self, texts: List[str]) -> np.ndarray:
        return self.embedder.encode(texts)

    def classify(self, texts: List[str]) -> List[float]:
        return self.classifier.classify(texts)

    def infer(self, texts: List[str]) -> Tuple[np.ndarray, List[float]]:
        embeddings = self.embed(texts)
        scores = self.classify(texts)
        return embeddings, scores
