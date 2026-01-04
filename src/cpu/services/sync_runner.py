import argparse
import logging
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Dict, List

import numpy as np
from psycopg import sql

from cpu.clients.gpu_client import GPUClient
from cpu.config import load_config, source_batch_size, source_concurrency
from cpu.core.utils import normalize_title_text, text_hash
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import BaseVectorStore, create_vector_store
from cpu.services.tmdb_enrich import ensure_tmdb_enrichment
from cpu.services.tpdb_enrich import ensure_tpdb_enrichment

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _parse_genre_tags(raw: Any) -> List[str]:
    if not raw:
        return []
    text = str(raw)
    parts = [p.strip() for p in text.replace("，", ",").split(",") if p.strip()]
    return parts


def _extract_extension(text: str) -> str:
    if not text:
        return ""
    if "." not in text:
        return ""
    ext = text.rsplit(".", 1)[-1]
    return ext.lower().strip()


def _detect_file_type(extension: str) -> str:
    if not extension:
        return "other"
    video = {"mp4", "mkv", "avi", "mov", "wmv", "flv", "ts", "m2ts", "webm"}
    audio = {"mp3", "flac", "aac", "m4a", "ogg", "wav"}
    image = {"jpg", "jpeg", "png", "gif", "webp", "bmp"}
    subtitle = {"srt", "ass", "ssa", "vtt", "sub"}
    archive = {"zip", "rar", "7z", "tar", "gz"}
    ext = extension.lower()
    if ext in video:
        return "video"
    if ext in audio:
        return "audio"
    if ext in image:
        return "image"
    if ext in subtitle:
        return "subtitle"
    if ext in archive:
        return "archive"
    return "other"


def _detect_languages(text: str) -> tuple[List[str], List[str]]:
    if not text:
        return [], []
    lower = text.lower()
    audio_langs: List[str] = []
    subtitle_langs: List[str] = []

    def add_lang(target: List[str], code: str) -> None:
        if code not in target:
            target.append(code)

    lang_map = {
        "zh": ["中文", "国语", "简体", "繁体", "chinese", "chs", "cht", "chi", "mandarin"],
        "en": ["英文", "英语", "english", "eng"],
        "jp": ["日语", "日文", "japanese", "jpn"],
        "kr": ["韩语", "韩文", "korean", "kor"],
        "fr": ["法语", "french", "fre"],
        "de": ["德语", "german", "ger"],
        "es": ["西语", "西班牙", "spanish", "spa"],
        "ru": ["俄语", "russian", "rus"],
    }
    subtitle_keys = ["字幕", "中字", "双语", "sub", "subs", "subtitle"]
    is_subtitle = any(k in lower for k in subtitle_keys)
    for code, keys in lang_map.items():
        if any(k.lower() in lower for k in keys):
            if is_subtitle:
                add_lang(subtitle_langs, code)
            else:
                add_lang(audio_langs, code)
                add_lang(subtitle_langs, code)
    return audio_langs, subtitle_langs


def sync_source(
    source: Dict[str, Any],
    vector_store: BaseVectorStore,
    pg_client: PGClient,
    gpu_client: GPUClient,
    embedding_version: str,
    nsfw_threshold: float,
    batch_size: int,
    concurrency: int,
    tmdb_cfg: Dict[str, Any],
    tmdb_schema: str,
    tpdb_cfg: Dict[str, Any],
    tpdb_schema: str,
) -> None:
    logger.info("Sync start for source=%s", source["name"])
    total_rows = 0
    total_batches = 0
    total_time = 0.0
    concurrency = max(int(concurrency or 1), 1)
    in_flight: set[str] = set()

    def process_batch(rows: List[Dict[str, Any]]) -> Dict[str, float]:
        batch_start = time.perf_counter()
        if source.get("pg", {}).get("tmdb_enrich"):
            tmdb_refs = [
                (r.get("type"), r.get("tmdb_id")) for r in rows if r.get("tmdb_id") and r.get("type")
            ]
            if tmdb_refs and tmdb_cfg.get("enabled") and tmdb_cfg.get("auto_enrich"):
                logger.info(
                    "Auto-enrich TMDB refs=%d for source=%s",
                    len(tmdb_refs),
                    source["name"],
                )
                enrich_start = time.perf_counter()
                with pg_client.connect() as conn:
                    ensure_tmdb_enrichment(conn, tmdb_schema, tmdb_refs, tmdb_cfg)
                enrich_cost = time.perf_counter() - enrich_start
                ids = [str(r["pg_id"]) for r in rows]
                refresh_start = time.perf_counter()
                extra_fields = list(source.get("pg", {}).get("extra_fields", []))
                if "genre" not in extra_fields:
                    extra_fields.append("genre")
                if "keywords" not in extra_fields:
                    extra_fields.append("keywords")
                source_cfg = {
                    **source,
                    "pg": {
                        **source.get("pg", {}),
                        "extra_fields": extra_fields,
                    },
                }
                refreshed = pg_client.fetch_by_ids(source_cfg, ids)
                refresh_cost = time.perf_counter() - refresh_start
                updated_rows: List[Dict[str, Any]] = []
                updated_at_field = source.get("pg", {}).get("updated_at_field")
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
                logger.info(
                    "Refreshed rows after TMDB enrich=%d for source=%s enrich_cost=%.3fs refresh_cost=%.3fs",
                    len(rows),
                    source["name"],
                    enrich_cost,
                    refresh_cost,
                )
        if source.get("pg", {}).get("tpdb_enrich"):
            tpdb_refs = []
            tpdb_pg_cfg = source.get("pg", {})
            default_content_type = tpdb_pg_cfg.get("tpdb_content_type") or source.get("name")
            default_content_source = tpdb_pg_cfg.get("tpdb_content_source") or source.get("name")
            tpdb_content_id_by_pg_id: Dict[str, str] = {}
            for r in rows:
                content_type = r.get("type") or default_content_type
                content_source = r.get("source") or default_content_source
                content_id = r.get("id") or r.get("pg_id")
                if content_type and content_source and content_id:
                    pg_id = str(r.get("pg_id"))
                    tpdb_content_id_by_pg_id[pg_id] = str(content_id)
                    tpdb_refs.append(
                        {
                            "content_type": content_type,
                            "content_source": content_source,
                            "content_id": str(content_id),
                            "title": r.get("title"),
                            "original_title": r.get("original_title"),
                            "text": r.get("text"),
                            "release_year": r.get("release_year"),
                            "site": r.get("site"),
                            "tpdb_type": tpdb_pg_cfg.get("tpdb_type"),
                        }
                    )
            if tpdb_refs and tpdb_cfg.get("enabled") and tpdb_cfg.get("auto_enrich"):
                logger.info(
                    "Auto-enrich TPDB refs=%d for source=%s",
                    len(tpdb_refs),
                    source["name"],
                )
                enrich_start = time.perf_counter()
                with pg_client.connect() as conn:
                    ensure_tpdb_enrichment(conn, tpdb_schema, tpdb_refs, tpdb_cfg)
                enrich_cost = time.perf_counter() - enrich_start
                tpdb_fields = [
                    "tpdb_id",
                    "tpdb_title",
                    "tpdb_original_title",
                    "tpdb_aka",
                    "tpdb_actors",
                    "tpdb_tags",
                    "tpdb_studio",
                    "tpdb_series",
                    "tpdb_site",
                    "tpdb_release_date",
                    "tpdb_plot",
                    "tpdb_poster_url",
                ]

                base_extra_fields = list(source.get("pg", {}).get("extra_fields", []))
                supports_tpdb_fields = any(field in base_extra_fields for field in tpdb_fields)

                ids = [str(r["pg_id"]) for r in rows]
                refreshed = None
                refresh_cost = 0.0
                if supports_tpdb_fields:
                    refresh_start = time.perf_counter()
                    extra_fields = list(base_extra_fields)
                    for field in tpdb_fields:
                        if field not in extra_fields:
                            extra_fields.append(field)
                    source_cfg = {
                        **source,
                        "pg": {
                            **source.get("pg", {}),
                            "extra_fields": extra_fields,
                        },
                    }
                    refreshed = pg_client.fetch_by_ids(source_cfg, ids)
                    refresh_cost = time.perf_counter() - refresh_start

                tpdb_rows_by_key: Dict[str, Dict[str, Any]] = {}
                try:
                    groups: Dict[tuple[str, str], List[str]] = {}
                    for ref in tpdb_refs:
                        key = (str(ref["content_type"]), str(ref["content_source"]))
                        groups.setdefault(key, []).append(str(ref["content_id"]))
                    with pg_client.connect() as conn:
                        with conn.cursor() as cur:
                            for (content_type, content_source), content_ids in groups.items():
                                cur.execute(
                                    sql.SQL(
                                        """
                                        SELECT content_id::text AS content_id,
                                               tpdb_id,
                                               title AS tpdb_title,
                                               original_title AS tpdb_original_title,
                                               aka AS tpdb_aka,
                                               actors AS tpdb_actors,
                                               tags AS tpdb_tags,
                                               studio AS tpdb_studio,
                                               series AS tpdb_series,
                                               site AS tpdb_site,
                                               release_date AS tpdb_release_date,
                                               plot AS tpdb_plot,
                                               poster_url AS tpdb_poster_url,
                                               status AS tpdb_status,
                                               updated_at AS tpdb_updated_at
                                        FROM {schema}.tpdb_enrichment
                                        WHERE content_type = %s
                                          AND content_source = %s
                                          AND content_id = ANY(%s)
                                        """
                                    ).format(schema=sql.Identifier(tpdb_schema)),
                                    (content_type, content_source, content_ids),
                                )
                                for row in cur.fetchall():
                                    tpdb_rows_by_key[str(row["content_id"])] = dict(row)
                except Exception as exc:
                    logger.warning("Failed to load TPDB fields for source=%s error=%s", source["name"], exc)

                updated_rows: List[Dict[str, Any]] = []
                updated_at_field = source.get("pg", {}).get("updated_at_field")
                for r in rows:
                    pg_id = str(r["pg_id"])
                    pg_row = refreshed.get(pg_id) if refreshed is not None else r
                    if not pg_row:
                        continue
                    text = pg_row.get(source["pg"]["text_field"], "") or pg_row.get("text", "")
                    new_row = {
                        "pg_id": pg_id,
                        "text": text,
                        "text_hash": text_hash(text),
                    }
                    if updated_at_field and updated_at_field in pg_row:
                        new_row["updated_at"] = pg_row.get(updated_at_field)
                    for field in (base_extra_fields if not supports_tpdb_fields else source_cfg["pg"]["extra_fields"]):
                        if field in pg_row:
                            new_row[field] = pg_row.get(field)
                    tpdb_key = tpdb_content_id_by_pg_id.get(pg_id) or pg_id
                    tpdb_row = tpdb_rows_by_key.get(tpdb_key)
                    if tpdb_row:
                        for field in tpdb_fields:
                            if field in tpdb_row:
                                new_row[field] = tpdb_row.get(field)
                    updated_rows.append(new_row)
                rows = updated_rows
                logger.info(
                    "Refreshed rows after TPDB enrich=%d for source=%s enrich_cost=%.3fs refresh_cost=%.3fs",
                    len(rows),
                    source["name"],
                    enrich_cost,
                    refresh_cost,
                )
        texts = []
        for r in rows:
            original = str(r.get("text", ""))
            cleaned = normalize_title_text(original)
            texts.append(cleaned if cleaned else original)
        logger.info("Embedding batch size=%d for source=%s", len(texts), source["name"])
        try:
            infer_start = time.perf_counter()
            embeddings, scores = gpu_client.infer(texts)
            infer_cost = time.perf_counter() - infer_start
        except Exception as exc:
            logger.exception("GPU inference failed: %s", exc)
            for r in rows:
                pg_client.mark_failure(source["name"], str(r["pg_id"]), str(exc))
            raise
        if embeddings.shape[1] != vector_store.dim:
            raise ValueError(
                f"Embedding dim mismatch: got {embeddings.shape[1]}, expected {vector_store.dim}"
            )
        metas: List[Dict[str, Any]] = []
        updates: List[Dict[str, Any]] = []
        for row, score in zip(rows, scores):
            nsfw_flag = score >= nsfw_threshold if source.get("tagging", {}).get("nsfw", True) else False
            tmdb_id = row.get("tmdb_id")
            has_tmdb = bool(tmdb_id)
            tpdb_id = row.get("tpdb_id")
            has_tpdb = bool(tpdb_id)
            genre_tags = _parse_genre_tags(row.get("genre"))
            extension = row.get("extension") or _extract_extension(str(row.get("text", "")))
            file_type = _detect_file_type(str(extension))
            audio_langs, subtitle_langs = _detect_languages(str(row.get("text", "")))
            size_field = (source.get("pg") or {}).get("size_field", "size")
            size_value = None
            if size_field:
                raw_size = row.get(size_field)
                try:
                    size_num = float(raw_size)
                except (TypeError, ValueError):
                    size_num = None
                if size_num is not None and size_num > 0:
                    size_value = size_num
            metas.append(
                {
                    "source": source["name"],
                    "pg_id": str(row["pg_id"]),
                    "nsfw": nsfw_flag,
                    "nsfw_score": float(score),
                    "text_hash": row.get("text_hash") or text_hash(row["text"]),
                    "embedding_version": embedding_version,
                    "has_tmdb": has_tmdb,
                    "tmdb_id": str(tmdb_id) if tmdb_id is not None else None,
                    "has_tpdb": has_tpdb,
                    "tpdb_id": str(tpdb_id) if tpdb_id is not None else None,
                    "genre_tags": genre_tags,
                    "file_type": file_type,
                    "audio_langs": audio_langs,
                    "subtitle_langs": subtitle_langs,
                    "size": size_value,
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
        add_start = time.perf_counter()
        try:
            labels = vector_store.add(np.asarray(embeddings, dtype="float32"), metas)
        except Exception as exc:
            logger.warning(
                "Vector store add failed; will retry later source=%s error=%s",
                source["name"],
                exc,
            )
            return {"rows": 0.0, "batch_cost": time.perf_counter() - batch_start}
        add_cost = time.perf_counter() - add_start
        for meta, label, update in zip(metas, labels, updates):
            update["vector_id"] = label
        upsert_start = time.perf_counter()
        try:
            pg_client.upsert_sync_state(source["name"], updates)
        except Exception as exc:
            logger.warning(
                "Sync state upsert failed; will retry later source=%s error=%s",
                source["name"],
                exc,
            )
            return {"rows": 0.0, "batch_cost": time.perf_counter() - batch_start}
        upsert_cost = time.perf_counter() - upsert_start
        try:
            index_size = vector_store.size()
        except Exception:
            index_size = -1
        logger.info(
            "Synced batch size=%d for source=%s, total_index_size=%d infer_cost=%.3fs add_cost=%.3fs upsert_cost=%.3fs",
            len(rows),
            source["name"],
            index_size,
            infer_cost,
            add_cost,
            upsert_cost,
        )
        batch_cost = time.perf_counter() - batch_start
        throughput = len(rows) / batch_cost if batch_cost > 0 else 0.0
        logger.info(
            "Batch done size=%d source=%s total_cost=%.3fs throughput=%.2f rows/s",
            len(rows),
            source["name"],
            batch_cost,
            throughput,
        )
        return {"rows": float(len(rows)), "batch_cost": batch_cost}

    def drain_completed(block: bool) -> None:
        nonlocal total_rows, total_batches, total_time
        if not futures:
            return
        done = None
        if block:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
        else:
            done = [f for f in futures if f.done()]
        for future in done:
            ids = futures.pop(future, [])
            for pg_id in ids:
                in_flight.discard(pg_id)
            try:
                result = future.result()
            except Exception as exc:
                logger.warning("Batch failed source=%s error=%s", source["name"], exc)
                continue
            total_rows += int(result["rows"])
            total_batches += 1
            total_time += float(result["batch_cost"])

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures: Dict[Any, List[str]] = {}
        while True:
            drain_completed(block=False)
            if len(futures) >= concurrency:
                drain_completed(block=True)
                continue
            fetch_start = time.perf_counter()
            rows = pg_client.fetch_pending(source, batch_size=batch_size)
            fetch_cost = time.perf_counter() - fetch_start
            if not rows:
                drain_completed(block=True)
                if not futures:
                    logger.info("No pending rows for source=%s", source["name"])
                    break
                continue
            filtered = [r for r in rows if str(r["pg_id"]) not in in_flight]
            if not filtered:
                logger.info(
                    "Fetched pending rows=%d for source=%s cost=%.3fs skipped_inflight=%d",
                    len(rows),
                    source["name"],
                    fetch_cost,
                    len(rows),
                )
                if futures:
                    drain_completed(block=True)
                else:
                    time.sleep(0.2)
                continue
            logger.info(
                "Fetched pending rows=%d for source=%s cost=%.3fs",
                len(filtered),
                source["name"],
                fetch_cost,
            )
            batch_ids = [str(r["pg_id"]) for r in filtered]
            for pg_id in batch_ids:
                in_flight.add(pg_id)
            future = executor.submit(process_batch, filtered)
            futures[future] = batch_ids
            drain_completed(block=False)
    if total_batches:
        avg_throughput = total_rows / total_time if total_time > 0 else 0.0
        logger.info(
            "Sync summary source=%s batches=%d total_rows=%d total_time=%.3fs avg_throughput=%.2f rows/s",
            source["name"],
            total_batches,
            total_rows,
        total_time,
        avg_throughput,
    )


def run_sync(config_path: str, target_source: str | None = None) -> None:
    cfg = load_config(config_path)
    pg_client = PGClient(cfg.postgres["dsn"])
    pg_client.ensure_tables()
    vector_store = create_vector_store(cfg.vector_store)
    gpu_client = GPUClient(cfg.gpu_endpoint)
    tmdb_schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    tpdb_schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    sources = [s for s in cfg.sources if (not target_source or s["name"] == target_source)]
    if not sources:
        logger.warning("No sources matched for sync (target=%s)", target_source)
        return
    for source in sources:
        batch_size = source_batch_size(source, cfg.sync)
        concurrency = source_concurrency(source, cfg.sync)
        sync_source(
            source,
            vector_store,
            pg_client,
            gpu_client,
            cfg.embedding_model_version,
            cfg.nsfw_threshold,
            batch_size,
            concurrency,
            cfg.tmdb,
            tmdb_schema,
            cfg.tpdb,
            tpdb_schema,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="HermesIndex sync runner")
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--source", help="only sync specified source name")
    args = parser.parse_args()

    run_sync(args.config, args.source)


if __name__ == "__main__":
    main()
