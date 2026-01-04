import json
import os
import threading
import time
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
        size_min = metadata_filter.get("size_min") if metadata_filter else None
        overfetch = 200 if size_min else 0
        k = min(topk + max(offset, 0) + overfetch, len(self.meta))
        labels, distances = self.index.knn_query(embedding, k=k)
        candidates = list(zip(labels[0], distances[0]))
        results: List[Dict[str, Any]] = []
        for label, distance in candidates:
            meta = self.meta.get(int(label))
            if not meta:
                continue
            if size_min is not None:
                raw_size = meta.get("size")
                try:
                    size_val = float(raw_size)
                except (TypeError, ValueError):
                    size_val = None
                if size_val is None or size_val < float(size_min):
                    continue
            score = float(1 - distance) if self.metric == "cosine" else float(-distance)
            result = {"score": score}
            result.update(meta)
            results.append(result)
        return results[offset : offset + topk]

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
            from qdrant_client.http.exceptions import UnexpectedResponse
        except Exception as exc:  # pragma: no cover - optional dep
            raise ImportError("qdrant-client is required for Qdrant vector store") from exc
        self.dim = dim
        self.collection = collection
        self.metric = metric
        self.url = url
        distance = Distance.COSINE if metric == "cosine" else Distance.DOT
        # qdrant-client defaults to probing server version via GET / (can be noisy and may fail transiently),
        # so disable compatibility checks and rely on request retries/fallback instead.
        self.client = QdrantClient(url=url, api_key=api_key, timeout=60, check_compatibility=False)
        self._api_key = api_key
        self._http_timeout = 30
        self._distance_name = "Cosine" if metric == "cosine" else "Dot"
        vectors_config = VectorParams(size=dim, distance=distance)
        init_error: Exception | None = None
        for _ in range(3):
            try:
                self.client.get_collection(collection_name=collection)
                init_error = None
                break
            except UnexpectedResponse as exc:
                if getattr(exc, "status_code", None) == 404:
                    try:
                        self.client.recreate_collection(
                            collection_name=collection,
                            vectors_config=vectors_config,
                        )
                        init_error = None
                        break
                    except Exception as inner:
                        init_error = inner
                        time.sleep(0.5)
                        continue
                init_error = exc
                time.sleep(0.5)
            except Exception as exc:  # transient (e.g. 502) or gateway issues
                init_error = exc
                time.sleep(0.5)
        if init_error is not None:
            # Fall back to raw HTTP mode (still supports query/add), so sync_runner won't crash.
            self.client = None
            self._ensure_collection_http()

    def _http_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self._api_key:
            headers["api-key"] = str(self._api_key)
        return headers

    def _http_request(self, method: str, path: str, json_body: Dict[str, Any] | None = None) -> Dict[str, Any]:
        import httpx

        base_url = self.url.rstrip("/")
        url = f"{base_url}{path}"
        last_exc: Exception | None = None
        last_status: int | None = None
        for attempt in range(3):
            try:
                resp = httpx.request(
                    method,
                    url,
                    json=json_body,
                    headers=self._http_headers(),
                    timeout=self._http_timeout,
                    trust_env=False,
                )
                if resp.status_code in {502, 503, 504}:
                    last_status = int(resp.status_code)
                    last_exc = RuntimeError(f"HTTP {resp.status_code} from Qdrant")
                    time.sleep(0.3 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return resp.json() if resp.content else {}
            except Exception as exc:
                last_exc = exc
                time.sleep(0.3 * (attempt + 1))
        suffix = f" status={last_status}" if last_status is not None else ""
        raise RuntimeError(
            f"Qdrant HTTP request failed: {method} {path}{suffix} error={last_exc}"
        ) from last_exc

    def _ensure_collection_http(self) -> None:
        try:
            info = self._http_request("GET", f"/collections/{self.collection}")
            if info.get("result") and info.get("status") == "ok":
                return
        except Exception:
            # Qdrant may be temporarily unavailable (e.g. 502); defer collection ensure to later retries.
            return
        payload = {"vectors": {"size": int(self.dim), "distance": self._distance_name}}
        try:
            self._http_request("PUT", f"/collections/{self.collection}", json_body=payload)
        except Exception:
            return

    def add(self, embeddings: np.ndarray, metas: List[Dict[str, Any]]) -> List[str]:
        points: List[Dict[str, Any]] = []
        point_ids: List[str] = []
        for emb, meta in zip(embeddings, metas):
            raw_id = f"{meta['source']}:{meta['pg_id']}"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))
            point_ids.append(point_id)
            points.append({"id": point_id, "vector": emb.tolist(), "payload": meta})
        if self.client is not None:
            try:
                from qdrant_client.http.models import PointStruct

                structs = [PointStruct(id=p["id"], vector=p["vector"], payload=p["payload"]) for p in points]
                self.client.upsert(collection_name=self.collection, points=structs, wait=True)
                return point_ids
            except Exception:
                # Qdrant may be temporarily unavailable (e.g. 502); fall back to raw HTTP retries.
                self.client = None
        self._ensure_collection_http()
        self._http_request("PUT", f"/collections/{self.collection}/points?wait=true", json_body={"points": points})
        return point_ids

    def query(
        self,
        embedding: np.ndarray,
        topk: int = 20,
        metadata_filter: Dict[str, Any] | None = None,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        query_filter = None
        if metadata_filter and (
            metadata_filter.get("has_tmdb")
            or metadata_filter.get("genres")
            or metadata_filter.get("file_type")
            or metadata_filter.get("audio_langs")
            or metadata_filter.get("subtitle_langs")
            or metadata_filter.get("size_min") is not None
        ):
            from qdrant_client.http.models import (
                Filter,
                FieldCondition,
                MatchAny,
                MatchValue,
                Range,
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
            size_min = metadata_filter.get("size_min")
            if size_min is not None:
                must_conditions.append(
                    FieldCondition(key="size", range=Range(gte=float(size_min)))
                )
            if must_conditions:
                query_filter = Filter(must=must_conditions)
        query_vector = embedding[0].tolist()
        hits = None
        if self.client is not None and hasattr(self.client, "search"):
            try:
                hits = self.client.search(
                    collection_name=self.collection,
                    query_vector=query_vector,
                    limit=topk,
                    with_payload=True,
                    query_filter=query_filter,
                    offset=offset,
                )
            except Exception:
                self.client = None
                hits = None
        elif self.client is not None and hasattr(self.client, "search_points"):
            try:
                hits = self.client.search_points(
                    collection_name=self.collection,
                    query_vector=query_vector,
                    limit=topk,
                    with_payload=True,
                    query_filter=query_filter,
                    offset=offset,
                )
            except Exception:
                self.client = None
                hits = None
        if hits is None:
            self._ensure_collection_http()
            filter_payload = None
            if metadata_filter and (
                metadata_filter.get("has_tmdb")
                or metadata_filter.get("genres")
                or metadata_filter.get("file_type")
                or metadata_filter.get("audio_langs")
                or metadata_filter.get("subtitle_langs")
                or metadata_filter.get("size_min") is not None
            ):
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
                size_min = metadata_filter.get("size_min")
                if size_min is not None:
                    must_conditions.append(
                        {"key": "size", "range": {"gte": float(size_min)}}
                    )
                if must_conditions:
                    filter_payload = {"must": must_conditions}
            data = self._http_request(
                "POST",
                f"/collections/{self.collection}/points/search",
                json_body={
                    "vector": query_vector,
                    "limit": topk,
                    "with_payload": True,
                    "filter": filter_payload,
                    "offset": offset,
                },
            )
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
        if self.client is not None:
            try:
                info = self.client.get_collection(collection_name=self.collection)
                return int(info.points_count or 0)
            except Exception:
                self.client = None
        self._ensure_collection_http()
        data = self._http_request("GET", f"/collections/{self.collection}")
        result = data.get("result") or {}
        return int((result.get("points_count") or 0))


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
