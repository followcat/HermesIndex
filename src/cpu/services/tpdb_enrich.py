import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Tuple

import httpx
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from cpu.config import load_config
from cpu.services.bitmagnet_setup import ensure_tpdb_table
from cpu.core.utils import normalize_title_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://theporndb.net/graphql?type=JAV"


def load_tpdb_token(cfg: Dict[str, Any]) -> str:
    direct = cfg.get("api_token")
    if direct:
        return str(direct)
    env_name = cfg.get("api_token_env", "TPDB_API_TOKEN")
    token = os.getenv(env_name)
    if not token:
        raise ValueError(f"Missing TPDB API token in env: {env_name}")
    return token


def connect(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)


def _normalize_text(value: str) -> str:
    cleaned = normalize_title_text(value)
    return cleaned if cleaned else value


def _extract_code(text: str) -> str | None:
    if not text:
        return None
    import re

    match = re.search(r"\b([A-Z]{2,6})[-_ ]?(\d{2,5})\b", text, re.IGNORECASE)
    if not match:
        return None
    return f"{match.group(1).upper()}-{match.group(2)}"


def _extract_names(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        names: List[str] = []
        for item in value:
            if isinstance(item, str):
                if item:
                    names.append(item)
            elif isinstance(item, dict):
                if item.get("performer") and isinstance(item.get("performer"), dict):
                    performer = item.get("performer") or {}
                    name = performer.get("name") or performer.get("title")
                else:
                    name = item.get("name") or item.get("title") or item.get("label")
                if name:
                    names.append(str(name))
        return ", ".join([n for n in names if n])
    if isinstance(value, dict):
        if value.get("performer") and isinstance(value.get("performer"), dict):
            performer = value.get("performer") or {}
            name = performer.get("name") or performer.get("title")
        else:
            name = value.get("name") or value.get("title") or value.get("label")
        return str(name) if name else ""
    return ""


def _extract_image_url(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("url", "path", "src"):
            if value.get(key):
                return str(value[key])
        return None
    if isinstance(value, list):
        for item in value:
            url = _extract_image_url(item)
            if url:
                return url
    return None


def _extract_items(payload: Dict[str, Any], result_path: str | None) -> List[Dict[str, Any]]:
    data: Any = payload
    if result_path:
        for part in result_path.split("."):
            if not part:
                continue
            if isinstance(data, dict):
                data = data.get(part)
            else:
                data = None
                break
    if data is None:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("items", "results", "scenes", "movies", "javs"):
            items = data.get(key)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return [data]
    return []


def _pick_best_item(items: List[Dict[str, Any]], code: str | None, title: str | None) -> Tuple[Dict[str, Any], str, float]:
    if not items:
        raise ValueError("items must not be empty")
    if code:
        for item in items:
            item_code = item.get("code") or _extract_code(str(item.get("title") or ""))
            if item_code and item_code.upper() == code.upper():
                return item, "code", 1.0
    if title:
        norm_title = _normalize_text(title).lower()
        for item in items:
            item_title = item.get("title") or item.get("name")
            if not item_title:
                continue
            norm_item = _normalize_text(str(item_title)).lower()
            if norm_item == norm_title:
                return item, "title_exact", 0.9
        for item in items:
            item_title = item.get("title") or item.get("name")
            if not item_title:
                continue
            norm_item = _normalize_text(str(item_title)).lower()
            if norm_title in norm_item or norm_item in norm_title:
                return item, "title_partial", 0.7
    return items[0], "fallback", 0.5


def normalize_tpdb_item(item: Dict[str, Any]) -> Dict[str, Any]:
    title = item.get("title") or item.get("name") or ""
    original_title = item.get("original_title") or item.get("originalTitle") or ""
    aka = item.get("aka") or item.get("alternateTitles") or ""
    actors = _extract_names(item.get("performers") or item.get("actors"))
    tags = _extract_names(item.get("tags"))
    studio = _extract_names(item.get("studio"))
    series = _extract_names(item.get("series"))
    urls = item.get("urls")
    url_sites: List[str] = []
    if isinstance(urls, list):
        for url_item in urls:
            if isinstance(url_item, dict):
                site_obj = url_item.get("site")
                if isinstance(site_obj, dict):
                    site_name = site_obj.get("name")
                    if site_name:
                        url_sites.append(str(site_name))
                elif isinstance(site_obj, str):
                    url_sites.append(site_obj)
    site = _extract_names(item.get("site") or url_sites)
    release_date = (
        item.get("release_date")
        or item.get("releaseDate")
        or item.get("date")
        or item.get("production_date")
        or ""
    )
    plot = (
        item.get("description")
        or item.get("overview")
        or item.get("plot")
        or item.get("details")
        or ""
    )
    poster_url = _extract_image_url(item.get("image") or item.get("images") or item.get("poster"))
    if not aka and item.get("code"):
        aka = item.get("code")
    return {
        "tpdb_id": item.get("id") or item.get("uuid"),
        "external_type": item.get("type") or item.get("__typename"),
        "title": title,
        "original_title": original_title,
        "aka": aka if isinstance(aka, str) else _extract_names(aka),
        "actors": actors,
        "tags": tags,
        "studio": studio,
        "series": series,
        "site": site,
        "release_date": str(release_date) if release_date is not None else "",
        "plot": plot,
        "poster_url": poster_url,
    }


def upsert_tpdb(
    conn: psycopg.Connection,
    schema: str,
    content_type: str,
    content_source: str,
    content_id: str,
    values: Dict[str, Any],
    raw: Dict[str, Any] | None,
    match_method: str,
    match_score: float | None,
    status: str,
    error_message: str | None = None,
) -> None:
    statement = sql.SQL(
        """
        INSERT INTO {schema}.tpdb_enrichment
            (content_type, content_source, content_id, tpdb_id, external_type, title, original_title, aka,
             actors, tags, studio, series, site, release_date, plot, poster_url, match_method, match_score,
             status, error_message, raw, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
        ON CONFLICT (content_type, content_source, content_id) DO UPDATE
        SET tpdb_id = EXCLUDED.tpdb_id,
            external_type = EXCLUDED.external_type,
            title = EXCLUDED.title,
            original_title = EXCLUDED.original_title,
            aka = EXCLUDED.aka,
            actors = EXCLUDED.actors,
            tags = EXCLUDED.tags,
            studio = EXCLUDED.studio,
            series = EXCLUDED.series,
            site = EXCLUDED.site,
            release_date = EXCLUDED.release_date,
            plot = EXCLUDED.plot,
            poster_url = EXCLUDED.poster_url,
            match_method = EXCLUDED.match_method,
            match_score = EXCLUDED.match_score,
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            raw = EXCLUDED.raw,
            updated_at = now()
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(
            statement,
            (
                content_type,
                content_source,
                content_id,
                values.get("tpdb_id"),
                values.get("external_type"),
                values.get("title"),
                values.get("original_title"),
                values.get("aka"),
                values.get("actors"),
                values.get("tags"),
                values.get("studio"),
                values.get("series"),
                values.get("site"),
                values.get("release_date"),
                values.get("plot"),
                values.get("poster_url"),
                match_method,
                match_score,
                status,
                error_message,
                json.dumps(raw or {}),
            ),
        )


def fetch_tpdb_payload(
    client: httpx.Client,
    endpoint: str,
    token: str,
    query: str,
    variables: Dict[str, Any],
    auth_header: str,
    auth_prefix: str | None,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if auth_prefix:
        headers[auth_header] = f"{auth_prefix} {token}"
    else:
        headers[auth_header] = token
    resp = client.post(endpoint, json={"query": query, "variables": variables}, headers=headers)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise RuntimeError(f"TPDB GraphQL errors: {payload['errors']}")
    return payload


def _filter_missing_tpdb_refs(
    conn: psycopg.Connection,
    schema: str,
    refs: List[Dict[str, Any]],
    ttl_hours: float,
    not_found_ttl_hours: float,
) -> List[Dict[str, Any]]:
    keys = list(
        {
            (r["content_type"], r["content_source"], r["content_id"])
            for r in refs
            if r.get("content_type") and r.get("content_source") and r.get("content_id")
        }
    )
    if not keys:
        return []
    placeholders = sql.SQL(", ").join(sql.SQL("(%s, %s, %s)") for _ in keys)
    query = sql.SQL(
        """
        SELECT content_type, content_source, content_id, status, updated_at
        FROM {schema}.tpdb_enrichment
        WHERE (content_type, content_source, content_id) IN ({placeholders})
        """
    ).format(schema=sql.Identifier(schema), placeholders=placeholders)
    params: List[Any] = []
    for key in keys:
        params.extend(key)
    existing: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(query, params)
        for row in cur.fetchall():
            existing[(row["content_type"], row["content_source"], row["content_id"])] = row
    now = datetime.now(timezone.utc)
    fresh_refs: List[Dict[str, Any]] = []
    for ref in refs:
        key = (ref["content_type"], ref["content_source"], ref["content_id"])
        row = existing.get(key)
        if not row:
            fresh_refs.append(ref)
            continue
        updated_at = row.get("updated_at")
        if updated_at and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        age_hours = (
            (now - updated_at).total_seconds() / 3600 if isinstance(updated_at, datetime) else None
        )
        ttl = not_found_ttl_hours if row.get("status") == "not_found" else ttl_hours
        if ttl <= 0:
            fresh_refs.append(ref)
            continue
        if age_hours is None or age_hours >= ttl:
            fresh_refs.append(ref)
    return fresh_refs


def _build_variables(ref: Dict[str, Any], search_limit: int) -> Dict[str, Any]:
    title = ref.get("title") or ref.get("original_title") or ref.get("text") or ""
    cleaned = _normalize_text(str(title)) if title else ""
    code = _extract_code(str(title))
    release_date = ref.get("release_date") or ref.get("release_year")
    site = ref.get("site") or ref.get("provider")
    term = code or cleaned or str(title)
    return {
        "term": term,
        "limit": int(search_limit) if search_limit else None,
        "title": cleaned,
        "raw_title": str(title) if title else "",
        "code": code,
        "site": site,
        "date": str(release_date) if release_date is not None else "",
    }


def ensure_tpdb_enrichment(
    conn: psycopg.Connection,
    schema: str,
    refs: Iterable[Dict[str, Any]],
    tpdb_cfg: Dict[str, Any],
) -> None:
    if not tpdb_cfg.get("enabled") or not tpdb_cfg.get("auto_enrich"):
        return
    token = load_tpdb_token(tpdb_cfg)
    timeout = float(tpdb_cfg.get("timeout_seconds", 15))
    sleep_seconds = float(tpdb_cfg.get("sleep_seconds", 1.0))
    max_per_batch = int(tpdb_cfg.get("max_per_batch", 50))
    ttl_hours = float(tpdb_cfg.get("cache_ttl_hours", 168))
    not_found_ttl_hours = float(tpdb_cfg.get("not_found_ttl_hours", 720))
    search_limit = int(tpdb_cfg.get("search_limit", 10))
    require_code = bool(tpdb_cfg.get("require_code", False))
    auth_header = tpdb_cfg.get("auth_header", "ApiKey")
    auth_prefix = tpdb_cfg.get("auth_prefix")
    endpoints = tpdb_cfg.get("endpoints", {})
    queries = tpdb_cfg.get("queries", {})
    result_paths = tpdb_cfg.get("result_paths", {})
    default_endpoint = tpdb_cfg.get("endpoint", DEFAULT_ENDPOINT)
    default_query = tpdb_cfg.get("query")
    default_result_path = tpdb_cfg.get("result_path")
    default_type = tpdb_cfg.get("default_type") or ("jav" if isinstance(queries, dict) and "jav" in queries else "")

    prepared: List[Dict[str, Any]] = []
    for ref in refs:
        if not ref.get("content_type") or not ref.get("content_source") or not ref.get("content_id"):
            continue
        prepared.append(ref)
    if not prepared:
        return

    candidates = _filter_missing_tpdb_refs(conn, schema, prepared, ttl_hours, not_found_ttl_hours)
    if not candidates:
        return
    batch = candidates[:max_per_batch]
    with httpx.Client(timeout=timeout) as client:
        for ref in batch:
            tpdb_type = str(ref.get("tpdb_type") or default_type or "").lower()
            query = (queries or {}).get(tpdb_type) or default_query
            if not query:
                logger.warning(
                    "tpdb.query is required for TPDB enrich (type=%s, default_type=%s)",
                    tpdb_type or "default",
                    default_type or "none",
                )
                return
            endpoint = (endpoints or {}).get(tpdb_type) or default_endpoint
            result_path = (result_paths or {}).get(tpdb_type) or default_result_path
            variables = _build_variables(ref, search_limit)
            if require_code and not variables.get("code"):
                continue
            title = variables.get("raw_title") or variables.get("title")
            try:
                payload = fetch_tpdb_payload(
                    client,
                    endpoint,
                    token,
                    query,
                    variables,
                    auth_header,
                    auth_prefix,
                )
                items = _extract_items(payload, result_path)
                if not items:
                    upsert_tpdb(
                        conn,
                        schema,
                        ref["content_type"],
                        ref["content_source"],
                        ref["content_id"],
                        {},
                        payload,
                        "not_found",
                        None,
                        "not_found",
                    )
                    logger.info("TPDB not found for content=%s:%s:%s", ref["content_type"], ref["content_source"], ref["content_id"])
                else:
                    item, method, score = _pick_best_item(items, variables.get("code"), title)
                    normalized = normalize_tpdb_item(item)
                    upsert_tpdb(
                        conn,
                        schema,
                        ref["content_type"],
                        ref["content_source"],
                        ref["content_id"],
                        normalized,
                        item,
                        method,
                        score,
                        "success",
                    )
                    logger.info(
                        "TPDB enriched content=%s:%s:%s method=%s",
                        ref["content_type"],
                        ref["content_source"],
                        ref["content_id"],
                        method,
                    )
            except Exception as exc:
                upsert_tpdb(
                    conn,
                    schema,
                    ref["content_type"],
                    ref["content_source"],
                    ref["content_id"],
                    {},
                    None,
                    "error",
                    None,
                    "error",
                    str(exc),
                )
                logger.warning(
                    "TPDB enrich failed content=%s:%s:%s error=%s",
                    ref["content_type"],
                    ref["content_source"],
                    ref["content_id"],
                    exc,
                )
            time.sleep(sleep_seconds)


def fetch_tpdb_refs(
    conn: psycopg.Connection, schema: str, limit: int, force: bool
) -> List[Dict[str, Any]]:
    if force:
        query = sql.SQL(
            """
            SELECT c.type AS content_type,
                   c.source AS content_source,
                   c.id AS content_id,
                   c.title,
                   c.original_title,
                   c.release_year
            FROM public.content c
            ORDER BY c.id
            LIMIT %s
            """
        )
    else:
        query = sql.SQL(
            """
            SELECT c.type AS content_type,
                   c.source AS content_source,
                   c.id AS content_id,
                   c.title,
                   c.original_title,
                   c.release_year
            FROM public.content c
            LEFT JOIN {schema}.tpdb_enrichment te
                ON te.content_type = c.type
                AND te.content_source = c.source
                AND te.content_id = c.id
            WHERE te.content_id IS NULL
            ORDER BY c.id
            LIMIT %s
            """
        ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()
        return [
            {
                "content_type": row["content_type"],
                "content_source": row["content_source"],
                "content_id": row["content_id"],
                "title": row.get("title"),
                "original_title": row.get("original_title"),
                "release_year": row.get("release_year"),
            }
            for row in rows
        ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich content with TPDB GraphQL")
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--loop_sleep_seconds", type=float, default=10.0)
    args = parser.parse_args()

    cfg = load_config(args.config)
    tpdb_cfg = cfg.tpdb or {}
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    if not tpdb_cfg.get("enabled"):
        logger.info("tpdb enrichment disabled in config")
        return
    while True:
        with connect(cfg.postgres["dsn"]) as conn:
            ensure_tpdb_table(conn, schema)
            refs = fetch_tpdb_refs(conn, schema, limit=args.limit, force=args.force)
            if not refs:
                logger.info("No TPDB refs to enrich")
                if args.loop:
                    time.sleep(args.loop_sleep_seconds)
                    continue
                return
            ensure_tpdb_enrichment(conn, schema, refs, tpdb_cfg)
        if not args.loop:
            return


if __name__ == "__main__":
    main()
