import json
import os
import threading
from typing import Any, Dict, List

import hnswlib
import numpy as np


class HNSWVectorStore:
    def __init__(
        self,
        path: str,
        dim: int,
        max_elements: int = 1_500_000,
        metric: str = "cosine",
        ef_construction: int = 200,
        m: int = 16,
        ef_search: int = 64,
    ) -> None:
        self.path = path
        self.index_path = os.path.join(path, "index.bin")
        self.meta_path = os.path.join(path, "meta.json")
        self.dim = dim
        self.metric = metric
        self.max_elements = max_elements
        self.ef_construction = ef_construction
        self.m = m
        self.ef_search = ef_search
        self.index = None
        self.meta: Dict[int, Dict[str, Any]] = {}
        self.key_index: Dict[tuple[str, str], int] = {}
        self.next_label = 0
        self.lock = threading.Lock()
        self._load_or_init()

    def _load_or_init(self) -> None:
        os.makedirs(self.path, exist_ok=True)
        self.index = hnswlib.Index(space=self.metric, dim=self.dim)
        if os.path.exists(self.index_path):
            self.index.load_index(self.index_path, max_elements=self.max_elements)
        else:
            self.index.init_index(
                max_elements=self.max_elements,
                allow_replace_deleted=True,
                ef_construction=self.ef_construction,
                M=self.m,
            )
        self.index.set_ef(self.ef_search)
        self._load_meta()

    def _load_meta(self) -> None:
        if not os.path.exists(self.meta_path):
            return
        with open(self.meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.next_label = int(data.get("next_label", 0))
        for item in data.get("items", []):
            label = int(item["label"])
            meta = {k: v for k, v in item.items() if k != "label"}
            self.meta[label] = meta
            key = (meta["source"], str(meta["pg_id"]))
            self.key_index[key] = label
        if self.meta:
            self.next_label = max(self.meta.keys()) + 1

    def _persist(self) -> None:
        items = []
        for label, meta in self.meta.items():
            record = {"label": int(label)}
            record.update(meta)
            items.append(record)
        with open(self.meta_path, "w", encoding="utf-8") as f:
            json.dump({"next_label": self.next_label, "items": items}, f)
        self.index.save_index(self.index_path)

    def add(self, embeddings: np.ndarray, metas: List[Dict[str, Any]]) -> List[int]:
        labels: List[int] = []
        with self.lock:
            for emb, meta in zip(embeddings, metas):
                key = (meta["source"], str(meta["pg_id"]))
                label = self.key_index.get(key)
                if label is None:
                    label = self.next_label
                    self.next_label += 1
                else:
                    try:
                        self.index.mark_deleted(label)
                    except Exception:
                        pass
                labels.append(label)
                meta["pg_id"] = str(meta["pg_id"])
                self.meta[label] = meta
                self.key_index[key] = label
            self.index.add_items(embeddings, labels)
            self._persist()
        return labels

    def query(self, embedding: np.ndarray, topk: int = 20) -> List[Dict[str, Any]]:
        if not self.meta:
            return []
        k = min(topk, len(self.meta))
        labels, distances = self.index.knn_query(embedding, k=k)
        results: List[Dict[str, Any]] = []
        for label, distance in zip(labels[0], distances[0]):
            meta = self.meta.get(int(label))
            if not meta:
                continue
            score = float(1 - distance) if self.metric == "cosine" else float(-distance)
            result = {"score": score}
            result.update(meta)
            results.append(result)
        return results

    def size(self) -> int:
        return len(self.meta)
