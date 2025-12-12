import argparse
import logging
from typing import Any, Dict, List

import numpy as np

from cpu.clients.gpu_client import GPUClient
from cpu.config import load_config, source_batch_size
from cpu.core.utils import text_hash
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import create_vector_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def sync_source(
    source: Dict[str, Any],
    vector_store,
    pg_client: PGClient,
    gpu_client: GPUClient,
    embedding_version: str,
    nsfw_threshold: float,
    batch_size: int,
) -> None:
    logger.info("Sync start for source=%s", source["name"])
    while True:
        rows = pg_client.fetch_pending(source, batch_size=batch_size)
        if not rows:
            logger.info("No pending rows for source=%s", source["name"])
            break
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


def main() -> None:
    parser = argparse.ArgumentParser(description="HermesIndex sync runner")
    parser.add_argument("--config", default="configs/example.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    pg_client = PGClient(cfg.postgres["dsn"])
    pg_client.ensure_tables()
    vector_store = create_vector_store(cfg.vector_store)
    gpu_client = GPUClient(cfg.gpu_endpoint)
    for source in cfg.sources:
        batch_size = source_batch_size(source, cfg.sync)
        sync_source(
            source,
            vector_store,
            pg_client,
            gpu_client,
            cfg.embedding_model_version,
            cfg.nsfw_threshold,
            batch_size,
        )


if __name__ == "__main__":
    main()
