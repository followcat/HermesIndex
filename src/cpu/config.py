from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class Config:
    gpu_endpoint: str
    embedding_model_version: str
    nsfw_threshold: float
    postgres: Dict[str, Any]
    vector_store: Dict[str, Any]
    sync: Dict[str, Any]
    sources: List[Dict[str, Any]]
    local_embedder: Dict[str, Any] = field(default_factory=dict)
    celery: Dict[str, Any] = field(default_factory=dict)
    bitmagnet: Dict[str, Any] = field(default_factory=dict)
    tmdb: Dict[str, Any] = field(default_factory=dict)
    tpdb: Dict[str, Any] = field(default_factory=dict)
    auth: Dict[str, Any] = field(default_factory=dict)


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return Config(
        gpu_endpoint=raw.get("gpu_endpoint", "http://localhost:8001"),
        embedding_model_version=raw.get("embedding_model_version", "bge-m3"),
        nsfw_threshold=float(raw.get("nsfw_threshold", 0.7)),
        postgres=raw.get("postgres", {}),
        vector_store=raw.get("vector_store", {}),
        sync=raw.get("sync", {}),
        sources=raw.get("sources", []),
        local_embedder=raw.get("local_embedder", {}),
        celery=raw.get("celery", {}),
        bitmagnet=raw.get("bitmagnet", {}),
        tmdb=raw.get("tmdb", {}),
        tpdb=raw.get("tpdb", {}),
        auth=raw.get("auth", {}),
    )


def source_batch_size(source: Dict[str, Any], default_sync: Dict[str, Any]) -> int:
    return int(source.get("sync", {}).get("batch_size") or default_sync.get("batch_size") or 128)


def source_concurrency(source: Dict[str, Any], default_sync: Dict[str, Any]) -> int:
    return int(source.get("sync", {}).get("concurrency") or default_sync.get("concurrency") or 1)
