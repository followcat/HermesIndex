from typing import List, Tuple

import httpx
import numpy as np


class GPUClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def infer(self, texts: List[str]) -> Tuple[np.ndarray, List[float]]:
        payload = {"texts": texts}
        url = f"{self.base_url}/infer"
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        embeddings = np.asarray(data["embeddings"], dtype="float32")
        scores = [float(x) for x in data.get("nsfw_scores", [])]
        return embeddings, scores

    def embed(self, texts: List[str]) -> np.ndarray:
        payload = {"texts": texts}
        url = f"{self.base_url}/embed"
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        return np.asarray(data["embeddings"], dtype="float32")
