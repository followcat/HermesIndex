from typing import List, Tuple

import httpx
import numpy as np


class GPUClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _post(self, path: str, payload: dict) -> dict:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        last_status: int | None = None
        for attempt in range(3):
            try:
                with httpx.Client(timeout=self.timeout, trust_env=False) as client:
                    resp = client.post(url, json=payload)
                if resp.status_code in {502, 503, 504}:
                    last_status = int(resp.status_code)
                    last_exc = RuntimeError(f"HTTP {resp.status_code} from GPU service")
                    import time

                    time.sleep(0.3 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                last_exc = exc
                import time

                time.sleep(0.3 * (attempt + 1))
        suffix = f" status={last_status}" if last_status is not None else ""
        raise RuntimeError(f"GPU service request failed: POST {url}{suffix} error={last_exc}") from last_exc

    def infer(self, texts: List[str]) -> Tuple[np.ndarray, List[float]]:
        data = self._post("/infer", {"texts": texts})
        embeddings = np.asarray(data["embeddings"], dtype="float32")
        scores = [float(x) for x in data.get("nsfw_scores", [])]
        return embeddings, scores

    def embed(self, texts: List[str]) -> np.ndarray:
        data = self._post("/embed", {"texts": texts})
        return np.asarray(data["embeddings"], dtype="float32")
