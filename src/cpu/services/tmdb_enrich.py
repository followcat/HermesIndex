import argparse
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple

import httpx
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from cpu.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.themoviedb.org/3"
DEFAULT_LANGUAGE = "zh-CN"

TMDB_TYPES = {
    "movie": "movie",
    "tv_show": "tv",
    "tv": "tv",
}


def load_tmdb_key(cfg: Dict[str, Any]) -> str:
    direct_key = cfg.get("api_key")
    if direct_key:
        return str(direct_key)
    env_name = cfg.get("api_key_env", "TMDB_API_KEY")
    key = os.getenv(env_name)
    if not key:
        raise ValueError(f"Missing TMDB API key in env: {env_name}")
    return key


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def fetch_tmdb_refs(
    conn: psycopg.Connection, schema: str, limit: int, force: bool
) -> List[Tuple[str, str]]:
    if force:
        query = sql.SQL(
            """
            SELECT c.type AS content_type, c.id AS tmdb_id
            FROM public.content c
            WHERE c.source = 'tmdb'
            ORDER BY c.id
            LIMIT %s
            """
        )
    else:
        query = sql.SQL(
            """
            SELECT c.type AS content_type, c.id AS tmdb_id
            FROM public.content c
            LEFT JOIN {schema}.tmdb_enrichment te
                ON te.content_type = c.type AND te.tmdb_id = c.id
            WHERE c.source = 'tmdb'
              AND te.tmdb_id IS NULL
            ORDER BY c.id
            LIMIT %s
            """
        ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        return [(row["content_type"], row["tmdb_id"]) for row in cur.fetchall()]


def normalize_tmdb_payload(payload: Dict[str, Any], limits: Dict[str, int]) -> Dict[str, Any]:
    genres = ", ".join([g.get("name", "") for g in payload.get("genres", []) if g.get("name")])
    keywords_obj = payload.get("keywords") or {}
    keywords_list = keywords_obj.get("keywords") or keywords_obj.get("results") or []
    keywords = ", ".join([k.get("name", "") for k in keywords_list if k.get("name")])

    credits = payload.get("credits") or {}
    cast = credits.get("cast", [])
    crew = credits.get("crew", [])
    actors = ", ".join(
        [c.get("name", "") for c in cast[: limits.get("actors", 10)] if c.get("name")]
    )
    directors_list = [
        c.get("name", "")
        for c in crew
        if c.get("job") == "Director" and c.get("name")
    ]
    directors = ", ".join(directors_list[: limits.get("directors", 5)])

    alt_titles = payload.get("alternative_titles") or {}
    alt_list = alt_titles.get("titles") or alt_titles.get("results") or []
    aka = ", ".join(
        [t.get("title", "") for t in alt_list[: limits.get("aka", 10)] if t.get("title")]
    )

    return {
        "aka": aka,
        "keywords": keywords,
        "actors": actors,
        "directors": directors,
        "plot": payload.get("overview") or "",
        "genre": genres,
    }


def upsert_tmdb(
    conn: psycopg.Connection,
    schema: str,
    content_type: str,
    tmdb_id: str,
    values: Dict[str, Any],
    raw: Dict[str, Any],
) -> None:
    statement = sql.SQL(
        """
        INSERT INTO {schema}.tmdb_enrichment
            (content_type, tmdb_id, aka, keywords, actors, directors, plot, genre, raw, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
        ON CONFLICT (content_type, tmdb_id) DO UPDATE
        SET aka = EXCLUDED.aka,
            keywords = EXCLUDED.keywords,
            actors = EXCLUDED.actors,
            directors = EXCLUDED.directors,
            plot = EXCLUDED.plot,
            genre = EXCLUDED.genre,
            raw = EXCLUDED.raw,
            updated_at = now()
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(
            statement,
            (
                content_type,
                tmdb_id,
                values.get("aka"),
                values.get("keywords"),
                values.get("actors"),
                values.get("directors"),
                values.get("plot"),
                values.get("genre"),
                json.dumps(raw),
            ),
        )


def fetch_tmdb_payload(
    client: httpx.Client,
    base_url: str,
    api_key: str,
    content_type: str,
    tmdb_id: str,
    language: str,
) -> Dict[str, Any]:
    tmdb_type = TMDB_TYPES.get(content_type)
    if not tmdb_type:
        raise ValueError(f"Unsupported TMDB type: {content_type}")
    url = f"{base_url}/{tmdb_type}/{tmdb_id}"
    params = {
        "api_key": api_key,
        "language": language,
        "append_to_response": "credits,keywords,alternative_titles",
    }
    resp = client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def filter_missing_tmdb_refs(
    conn: psycopg.Connection, schema: str, refs: Iterable[Tuple[str, str]]
) -> List[Tuple[str, str]]:
    unique: Set[Tuple[str, str]] = {(t, i) for t, i in refs if t and i}
    if not unique:
        return []
    content_types = list({t for t, _ in unique})
    tmdb_ids = list({i for _, i in unique})
    query = sql.SQL(
        """
        SELECT content_type, tmdb_id
        FROM {schema}.tmdb_enrichment
        WHERE content_type = ANY(%s) AND tmdb_id = ANY(%s)
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(query, (content_types, tmdb_ids))
        existing = {(row["content_type"], row["tmdb_id"]) for row in cur.fetchall()}
    return [ref for ref in unique if ref not in existing]


def ensure_tmdb_enrichment(
    conn: psycopg.Connection,
    schema: str,
    tmdb_refs: Iterable[Tuple[str, str]],
    tmdb_cfg: Dict[str, Any],
) -> None:
    if not tmdb_cfg.get("enabled") or not tmdb_cfg.get("auto_enrich"):
        return
    missing = filter_missing_tmdb_refs(conn, schema, tmdb_refs)
    if not missing:
        return
    max_per_batch = int(tmdb_cfg.get("max_per_batch", 50))
    if max_per_batch > 0:
        missing = missing[:max_per_batch]
    api_key = load_tmdb_key(tmdb_cfg)
    base_url = tmdb_cfg.get("base_url", DEFAULT_BASE_URL)
    language = tmdb_cfg.get("language", DEFAULT_LANGUAGE)
    limits = tmdb_cfg.get("limits", {"actors": 10, "directors": 5, "aka": 10})
    sleep_seconds = float(tmdb_cfg.get("sleep_seconds", 1.0))
    timeout = float(tmdb_cfg.get("timeout_seconds", 10))

    with httpx.Client(timeout=timeout) as client:
        for content_type, tmdb_id in missing:
            try:
                payload = fetch_tmdb_payload(client, base_url, api_key, content_type, tmdb_id, language)
                values = normalize_tmdb_payload(payload, limits)
                upsert_tmdb(conn, schema, content_type, tmdb_id, values, payload)
                logger.info("Auto-enriched tmdb %s:%s", content_type, tmdb_id)
            except Exception as exc:
                logger.warning("Auto-enrich failed tmdb %s:%s error=%s", content_type, tmdb_id, exc)
            time.sleep(sleep_seconds)


def run_enrich(config_path: str, limit: int, force: bool, loop: bool) -> None:
    cfg = load_config(config_path)
    tmdb_cfg = cfg.tmdb or {}
    if not tmdb_cfg.get("enabled"):
        logger.info("tmdb enrichment disabled in config")
        return
    api_key = load_tmdb_key(tmdb_cfg)
    base_url = tmdb_cfg.get("base_url", DEFAULT_BASE_URL)
    language = tmdb_cfg.get("language", DEFAULT_LANGUAGE)
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    dsn = cfg.postgres.get("dsn")
    if not dsn:
        raise ValueError("postgres.dsn missing in config")

    limits = tmdb_cfg.get("limits", {"actors": 10, "directors": 5, "aka": 10})
    sleep_seconds = float(tmdb_cfg.get("sleep_seconds", 1.0))
    timeout = float(tmdb_cfg.get("timeout_seconds", 10))

    with connect(dsn) as conn:
        with httpx.Client(timeout=timeout) as client:
            while True:
                refs = fetch_tmdb_refs(conn, schema, limit=limit, force=force)
                if not refs:
                    logger.info("No tmdb ids to enrich")
                    return
                logger.info("Enriching tmdb refs: %d", len(refs))
                for content_type, tmdb_id in refs:
                    try:
                        payload = fetch_tmdb_payload(client, base_url, api_key, content_type, tmdb_id, language)
                        values = normalize_tmdb_payload(payload, limits)
                        upsert_tmdb(conn, schema, content_type, tmdb_id, values, payload)
                        logger.info("Enriched tmdb %s:%s", content_type, tmdb_id)
                    except Exception as exc:
                        logger.warning("Failed tmdb %s:%s error=%s", content_type, tmdb_id, exc)
                    time.sleep(sleep_seconds)
                if not loop:
                    return


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich content with TMDB API")
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--force", action="store_true", help="re-fetch even if cached")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="keep running until no missing TMDB ids",
    )
    args = parser.parse_args()
    run_enrich(args.config, args.limit, args.force, args.loop)


if __name__ == "__main__":
    main()
