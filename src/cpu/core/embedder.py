from typing import List

import numpy as np

from gpu_service.core.model import EmbeddingModel  # reuse lightweight fallback


class LocalEmbedder:
    def __init__(self, model_name: str, device: str = "cpu") -> None:
        self.model = EmbeddingModel(model_name, device)

    def embed(self, texts: List[str]) -> np.ndarray:
        return self.model.encode(texts)

    @property
    def dim(self) -> int:
        return self.model.dim
