import os
from typing import Any, Dict, List

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from cpu.config import load_config
from cpu.core.embedder import LocalEmbedder
from cpu.clients.gpu_client import GPUClient
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import HNSWVectorStore

CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/example.yaml")

cfg = load_config(CONFIG_PATH)
pg_client = PGClient(cfg.postgres["dsn"])
vector_store = HNSWVectorStore(
    path=cfg.vector_store.get("path", "./data/index"),
    dim=int(cfg.vector_store.get("dim", 768)),
    max_elements=int(cfg.vector_store.get("max_elements", 1_500_000)),
    metric=cfg.vector_store.get("metric", "cosine"),
    ef_construction=int(cfg.vector_store.get("ef_construction", 200)),
    m=int(cfg.vector_store.get("M", 16)),
    ef_search=int(cfg.vector_store.get("ef_search", 64)),
)
gpu_client = GPUClient(cfg.gpu_endpoint) if cfg.gpu_endpoint else None
local_embedder = None
if cfg.local_embedder.get("enabled"):
    local_embedder = LocalEmbedder(cfg.local_embedder.get("model_name", "BAAI/bge-small-zh-v1.5"))

source_map: Dict[str, Dict[str, Any]] = {s["name"]: s for s in cfg.sources}

app = FastAPI(title="HermesIndex Search API")


class SearchResult(BaseModel):
    score: float
    source: str
    pg_id: str
    title: str
    nsfw: bool
    nsfw_score: float
    metadata: Dict[str, Any] = {}


def embed_query(text: str) -> np.ndarray:
    if not text:
        raise HTTPException(status_code=400, detail="Empty query")
    if local_embedder:
        try:
            return local_embedder.embed([text])[0]
        except Exception:
            pass
    if gpu_client:
        try:
            return gpu_client.embed([text])[0]
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"GPU embed failed: {exc}") from exc
    raise HTTPException(status_code=500, detail="No embedding backend available")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "vector_index_size": vector_store.size(),
        "embedding_model_version": cfg.embedding_model_version,
    }


@app.get("/search")
def search(
    q: str = Query(..., description="query text"),
    topk: int = Query(20, ge=1, le=100),
    exclude_nsfw: bool = Query(True),
) -> Dict[str, Any]:
    query_vec = embed_query(q)
    results = vector_store.query(np.asarray([query_vec], dtype="float32"), topk=topk)
    filtered = []
    for r in results:
        if exclude_nsfw and r.get("nsfw"):
            continue
        filtered.append(r)
    ids_by_source: Dict[str, List[str]] = {}
    for r in filtered:
        ids_by_source.setdefault(r["source"], []).append(str(r["pg_id"]))
    enriched: List[SearchResult] = []
    for source_name, ids in ids_by_source.items():
        source_cfg = source_map.get(source_name)
        if not source_cfg:
            continue
        rows = pg_client.fetch_by_ids(source_cfg, ids)
        for r in filtered:
            if r["source"] != source_name:
                continue
            pg_row = rows.get(str(r["pg_id"]), {})
            title = pg_row.get(source_cfg["pg"]["text_field"], "")
            meta = {k: pg_row.get(k) for k in pg_row if k not in (source_cfg["pg"]["id_field"], source_cfg["pg"]["text_field"])}
            enriched.append(
                SearchResult(
                    score=float(r["score"]),
                    source=source_name,
                    pg_id=str(r["pg_id"]),
                    title=title,
                    nsfw=bool(r.get("nsfw", False)),
                    nsfw_score=float(r.get("nsfw_score", 0.0)),
                    metadata=meta,
                )
            )
    enriched.sort(key=lambda x: x.score, reverse=True)
    return {"count": len(enriched), "results": [e.model_dump() for e in enriched]}
