import argparse
import logging
from typing import Any, Dict, List

import numpy as np

from cpu.clients.gpu_client import GPUClient
from cpu.config import load_config, source_batch_size
from cpu.core.utils import text_hash
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import BaseVectorStore, create_vector_store
from cpu.services.tmdb_enrich import ensure_tmdb_enrichment

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def sync_source(
    source: Dict[str, Any],
    vector_store: BaseVectorStore,
    pg_client: PGClient,
    gpu_client: GPUClient,
    embedding_version: str,
    nsfw_threshold: float,
    batch_size: int,
    tmdb_cfg: Dict[str, Any],
    tmdb_schema: str,
) -> None:
    logger.info("Sync start for source=%s", source["name"])
    while True:
        rows = pg_client.fetch_pending(source, batch_size=batch_size)
        if not rows:
            logger.info("No pending rows for source=%s", source["name"])
            break
        if source.get("pg", {}).get("tmdb_enrich"):
            tmdb_refs = [
                (r.get("type"), r.get("tmdb_id")) for r in rows if r.get("tmdb_id") and r.get("type")
            ]
            if tmdb_refs and tmdb_cfg.get("enabled") and tmdb_cfg.get("auto_enrich"):
                with pg_client.connect() as conn:
                    ensure_tmdb_enrichment(conn, tmdb_schema, tmdb_refs, tmdb_cfg)
                ids = [str(r["pg_id"]) for r in rows]
                refreshed = pg_client.fetch_by_ids(source, ids)
                updated_rows: List[Dict[str, Any]] = []
                updated_at_field = source.get("pg", {}).get("updated_at_field")
                extra_fields = source.get("pg", {}).get("extra_fields", [])
                for pg_id in ids:
                    pg_row = refreshed.get(pg_id)
                    if not pg_row:
                        continue
                    text = pg_row.get(source["pg"]["text_field"], "")
                    new_row = {
                        "pg_id": pg_id,
                        "text": text,
                        "text_hash": text_hash(text),
                    }
                    if updated_at_field and updated_at_field in pg_row:
                        new_row["updated_at"] = pg_row.get(updated_at_field)
                    for field in extra_fields:
                        if field in pg_row:
                            new_row[field] = pg_row.get(field)
                    updated_rows.append(new_row)
                rows = updated_rows
        texts = [r["text"] for r in rows]
        try:
            embeddings, scores = gpu_client.infer(texts)
        except Exception as exc:
            logger.exception("GPU inference failed: %s", exc)
            for r in rows:
                pg_client.mark_failure(source["name"], str(r["pg_id"]), str(exc))
            break
        if embeddings.shape[1] != vector_store.dim:
            raise ValueError(
                f"Embedding dim mismatch: got {embeddings.shape[1]}, expected {vector_store.dim}"
            )
        metas: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []
        for row, score in zip(rows, scores):
            nsfw_flag = score >= nsfw_threshold if source.get("tagging", {}).get("nsfw", True) else False
            metas.append(
                {
                    "source": source["name"],
                    "pg_id": str(row["pg_id"]),
                    "nsfw": nsfw_flag,
                    "nsfw_score": float(score),
                    "text_hash": row.get("text_hash") or text_hash(row["text"]),
                    "embedding_version": embedding_version,
                }
            )
            updates.append(
                {
                    "pg_id": str(row["pg_id"]),
                    "text_hash": row.get("text_hash") or text_hash(row["text"]),
                    "embedding_version": embedding_version,
                    "nsfw_score": float(score),
                }
            )
        labels = vector_store.add(np.asarray(embeddings, dtype="float32"), metas)
        for meta, label, update in zip(metas, labels, updates):
            update["vector_id"] = label
        pg_client.upsert_sync_state(source["name"], updates)
        logger.info(
            "Synced batch size=%d for source=%s, total_index_size=%d",
            len(rows),
            source["name"],
            vector_store.size(),
        )


def run_sync(config_path: str, target_source: str | None = None) -> None:
    cfg = load_config(config_path)
    pg_client = PGClient(cfg.postgres["dsn"])
    pg_client.ensure_tables()
    vector_store = create_vector_store(cfg.vector_store)
    gpu_client = GPUClient(cfg.gpu_endpoint)
    tmdb_schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    sources = [s for s in cfg.sources if (not target_source or s["name"] == target_source)]
    if not sources:
        logger.warning("No sources matched for sync (target=%s)", target_source)
        return
    for source in sources:
        batch_size = source_batch_size(source, cfg.sync)
        sync_source(
            source,
            vector_store,
            pg_client,
            gpu_client,
            cfg.embedding_model_version,
            cfg.nsfw_threshold,
            batch_size,
            cfg.tmdb,
            tmdb_schema,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="HermesIndex sync runner")
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--source", help="only sync specified source name")
    args = parser.parse_args()

    run_sync(args.config, args.source)


if __name__ == "__main__":
    main()
