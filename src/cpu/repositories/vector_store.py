import json
import os
import threading
import uuid
from typing import Any, Dict, List

import hnswlib
import numpy as np


class BaseVectorStore:
    dim: int

    def add(self, embeddings: np.ndarray, metas: List[Dict[str, Any]]) -> List[Any]:
        raise NotImplementedError

    def query(
        self,
        embedding: np.ndarray,
        topk: int = 20,
        metadata_filter: Dict[str, Any] | None = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def size(self) -> int:
        raise NotImplementedError


class HNSWVectorStore(BaseVectorStore):
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

    def query(
        self,
        embedding: np.ndarray,
        topk: int = 20,
        metadata_filter: Dict[str, Any] | None = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        if not self.meta:
            return []
        k = min(topk + max(offset, 0), len(self.meta))
        labels, distances = self.index.knn_query(embedding, k=k)
        results: List[Dict[str, Any]] = []
        sliced = list(zip(labels[0], distances[0]))[offset : offset + topk]
        for label, distance in sliced:
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


class QdrantVectorStore(BaseVectorStore):
    def __init__(
        self,
        url: str,
        collection: str,
        dim: int,
        metric: str = "cosine",
        api_key: str | None = None,
    ) -> None:
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http.models import Distance, VectorParams
        except Exception as exc:  # pragma: no cover - optional dep
            raise ImportError("qdrant-client is required for Qdrant vector store") from exc
        self.dim = dim
        self.collection = collection
        self.metric = metric
        self.url = url
        distance = Distance.COSINE if metric == "cosine" else Distance.DOT
        self.client = QdrantClient(url=url, api_key=api_key, timeout=60)
        if collection not in [c.name for c in self.client.get_collections().collections]:
            self.client.recreate_collection(
                collection_name=collection,
                vectors_config=VectorParams(size=dim, distance=distance),
            )

    def add(self, embeddings: np.ndarray, metas: List[Dict[str, Any]]) -> List[str]:
        from qdrant_client.http.models import PointStruct

        points: List[PointStruct] = []
        for idx, (emb, meta) in enumerate(zip(embeddings, metas)):
            raw_id = f"{meta['source']}:{meta['pg_id']}"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))
            points.append(PointStruct(id=point_id, vector=emb.tolist(), payload=meta))
        self.client.upsert(collection_name=self.collection, points=points, wait=True)
        return [str(p.id) for p in points]

    def query(
        self,
        embedding: np.ndarray,
        topk: int = 20,
        metadata_filter: Dict[str, Any] | None = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query_filter = None
        if metadata_filter and (metadata_filter.get("has_tmdb") or metadata_filter.get("genres")):
            from qdrant_client.http.models import (
                Filter,
                FieldCondition,
                MatchAny,
                MatchValue,
            )

            must_conditions = []
            if metadata_filter.get("has_tmdb"):
                must_conditions.append(FieldCondition(key="has_tmdb", match=MatchValue(value=True)))
            genres = metadata_filter.get("genres") or []
            if genres:
                must_conditions.append(
                    FieldCondition(key="genre_tags", match=MatchAny(any=genres))
                )
            file_type = metadata_filter.get("file_type")
            if file_type:
                must_conditions.append(
                    FieldCondition(key="file_type", match=MatchValue(value=file_type))
                )
            audio_langs = metadata_filter.get("audio_langs") or []
            if audio_langs:
                must_conditions.append(
                    FieldCondition(key="audio_langs", match=MatchAny(any=audio_langs))
                )
            subtitle_langs = metadata_filter.get("subtitle_langs") or []
            if subtitle_langs:
                must_conditions.append(
                    FieldCondition(key="subtitle_langs", match=MatchAny(any=subtitle_langs))
                )
            if must_conditions:
                query_filter = Filter(must=must_conditions)
        query_vector = embedding[0].tolist()
        if hasattr(self.client, "search"):
            hits = self.client.search(
                collection_name=self.collection,
                query_vector=query_vector,
                limit=topk,
                with_payload=True,
                query_filter=query_filter,
                offset=offset,
            )
        elif hasattr(self.client, "search_points"):
            hits = self.client.search_points(
                collection_name=self.collection,
                query_vector=query_vector,
                limit=topk,
                with_payload=True,
                query_filter=query_filter,
                offset=offset,
            )
        else:
            import httpx

            base_url = self.url.rstrip("/")
            filter_payload = None
            if metadata_filter and (metadata_filter.get("has_tmdb") or metadata_filter.get("genres")):
                must_conditions = []
                if metadata_filter.get("has_tmdb"):
                    must_conditions.append({"key": "has_tmdb", "match": {"value": True}})
                genres = metadata_filter.get("genres") or []
                if genres:
                    must_conditions.append(
                        {"key": "genre_tags", "match": {"any": genres}}
                    )
                file_type = metadata_filter.get("file_type")
                if file_type:
                    must_conditions.append(
                        {"key": "file_type", "match": {"value": file_type}}
                    )
                audio_langs = metadata_filter.get("audio_langs") or []
                if audio_langs:
                    must_conditions.append(
                        {"key": "audio_langs", "match": {"any": audio_langs}}
                    )
                subtitle_langs = metadata_filter.get("subtitle_langs") or []
                if subtitle_langs:
                    must_conditions.append(
                        {"key": "subtitle_langs", "match": {"any": subtitle_langs}}
                    )
                if must_conditions:
                    filter_payload = {"must": must_conditions}
            resp = httpx.post(
                f"{base_url}/collections/{self.collection}/points/search",
                json={
                    "vector": query_vector,
                    "limit": topk,
                    "with_payload": True,
                    "filter": filter_payload,
                    "offset": offset,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("result", [])
        results: List[Dict[str, Any]] = []
        for hit in hits:
            if hasattr(hit, "payload"):
                payload = hit.payload or {}
                payload["score"] = float(hit.score)
            else:
                payload = hit.get("payload") or {}
                payload["score"] = float(hit.get("score", 0.0))
            results.append(payload)
        return results

    def size(self) -> int:
        info = self.client.get_collection(collection_name=self.collection)
        return int(info.points_count or 0)


class MilvusVectorStore(BaseVectorStore):
    def __init__(
        self,
        uri: str,
        collection: str,
        dim: int,
        metric: str = "cosine",
    ) -> None:
        try:
            from pymilvus import (
                Collection,
                CollectionSchema,
                DataType,
                FieldSchema,
                connections,
                utility,
            )
        except Exception as exc:  # pragma: no cover - optional dep
            raise ImportError("pymilvus is required for Milvus vector store") from exc

        connections.connect(alias="default", uri=uri)
        self.collection_name = collection
        self.metric = metric
        self.dim = dim
        self._schema = (
            FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=128),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="payload", dtype=DataType.JSON),
        )
        if not utility.has_collection(self.collection_name):
            schema = CollectionSchema(fields=list(self._schema), description="HermesIndex vectors")
            self.collection = Collection(name=self.collection_name, schema=schema)
            self.collection.create_index(
                field_name="vector",
                index_params={"index_type": "HNSW", "metric_type": "IP" if metric == "dot" else "COSINE", "params": {}},
            )
        else:
            self.collection = Collection(self.collection_name)

    def add(self, embeddings: np.ndarray, metas: List[Dict[str, Any]]) -> List[str]:
        ids = [f"{m['source']}:{m['pg_id']}" for m in metas]
        payloads = metas
        self.collection.insert([ids, embeddings.tolist(), payloads])
        self.collection.flush()
        return ids

    def query(
        self,
        embedding: np.ndarray,
        topk: int = 20,
        metadata_filter: Dict[str, Any] | None = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        expr = ""
        search_params = {"metric_type": "IP" if self.metric == "dot" else "COSINE"}
        res = self.collection.search(
            data=embedding.tolist(),
            anns_field="vector",
            param=search_params,
            limit=topk + max(offset, 0),
            expr=expr,
            output_fields=["payload"],
        )
        results: List[Dict[str, Any]] = []
        for hit in res[0][offset : offset + topk]:
            payload = dict(hit.entity.get("payload") or {})
            payload["score"] = float(hit.score)
            results.append(payload)
        return results

    def size(self) -> int:
        return int(self.collection.num_entities)


def create_vector_store(cfg: Dict[str, Any]) -> BaseVectorStore:
    store_type = cfg.get("type", "hnsw").lower()
    if store_type == "hnsw":
        return HNSWVectorStore(
            path=cfg.get("path", "./data/index"),
            dim=int(cfg.get("dim", 768)),
            max_elements=int(cfg.get("max_elements", 1_500_000)),
            metric=cfg.get("metric", "cosine"),
            ef_construction=int(cfg.get("ef_construction", 200)),
            m=int(cfg.get("M", 16)),
            ef_search=int(cfg.get("ef_search", 64)),
        )
    if store_type == "qdrant":
        return QdrantVectorStore(
            url=cfg["url"],
            collection=cfg.get("collection", "hermes_vectors"),
            dim=int(cfg.get("dim", 768)),
            metric=cfg.get("metric", "cosine"),
            api_key=cfg.get("api_key"),
        )
    if store_type == "milvus":
        return MilvusVectorStore(
            uri=cfg["uri"],
            collection=cfg.get("collection", "hermes_vectors"),
            dim=int(cfg.get("dim", 768)),
            metric=cfg.get("metric", "cosine"),
        )
    raise ValueError(f"Unsupported vector_store.type={store_type}")
