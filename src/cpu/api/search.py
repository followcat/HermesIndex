import os
from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from cpu.config import load_config
from cpu.core.embedder import LocalEmbedder
from cpu.core.utils import normalize_title_text
from cpu.clients.gpu_client import GPUClient
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import create_vector_store
from cpu.services import tmdb_enrich

CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/example.yaml")

cfg = load_config(CONFIG_PATH)
pg_client = PGClient(cfg.postgres["dsn"])
vector_store = create_vector_store(cfg.vector_store)
gpu_client = GPUClient(cfg.gpu_endpoint) if cfg.gpu_endpoint else None
local_embedder = None
if cfg.local_embedder.get("enabled"):
    local_embedder = LocalEmbedder(cfg.local_embedder.get("model_name", "BAAI/bge-m3"))

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


class TorrentFile(BaseModel):
    index: int
    path: str
    extension: str | None = None
    size: int | None = None
    updated_at: datetime | None = None


class LatestTmdbItem(BaseModel):
    content_uid: str
    tmdb_id: str
    title: str
    original_title: str | None = None
    release_year: int | None = None
    updated_at: datetime | None = None
    type: str | None = None
    genre: str | None = None
    keywords: str | None = None


class TmdbDetail(BaseModel):
    content_type: str
    tmdb_id: str
    aka: str | None = None
    keywords: str | None = None
    actors: str | None = None
    directors: str | None = None
    plot: str | None = None
    genre: str | None = None
    poster_url: str | None = None
    backdrop_url: str | None = None
    updated_at: datetime | None = None


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


def extract_genre_filters(query: str) -> List[str]:
    mapping = {
        "惊悚": ["惊悚", "Thriller"],
        "恐怖": ["恐怖", "Horror"],
        "悬疑": ["悬疑", "Mystery"],
        "动作": ["动作", "Action"],
        "科幻": ["科幻", "Science Fiction"],
        "犯罪": ["犯罪", "Crime"],
        "爱情": ["爱情", "Romance"],
        "喜剧": ["喜剧", "Comedy"],
        "剧情": ["剧情", "Drama"],
        "冒险": ["冒险", "Adventure"],
        "动画": ["动画", "Animation"],
        "奇幻": ["奇幻", "Fantasy"],
        "战争": ["战争", "War"],
        "纪录": ["纪录", "Documentary"],
        "家庭": ["家庭", "Family"],
        "音乐": ["音乐", "Music"],
        "历史": ["历史", "History"],
        "西部": ["西部", "Western"],
    }
    hits: List[str] = []
    for key, values in mapping.items():
        if key in query:
            hits.extend(values)
    seen = set()
    uniq: List[str] = []
    for item in hits:
        if item in seen:
            continue
        seen.add(item)
        uniq.append(item)
    return uniq


def extract_query_filters(query: str) -> tuple[str, Dict[str, Any]]:
    raw = query
    filters: Dict[str, Any] = {}
    lower = raw.lower()

    file_type_map = {
        "视频": "video",
        "影片": "video",
        "电影": "video",
        "音频": "audio",
        "音乐": "audio",
        "字幕": "subtitle",
        "图片": "image",
        "图片类": "image",
        "压缩": "archive",
    }
    for key, value in file_type_map.items():
        if key in raw:
            filters["file_type"] = value
            lower = lower.replace(key.lower(), "")
            raw = raw.replace(key, "")
            break

    audio_langs, subtitle_langs = _detect_query_languages(raw)
    if audio_langs:
        filters["audio_langs"] = audio_langs
    if subtitle_langs:
        filters["subtitle_langs"] = subtitle_langs

    genres = extract_genre_filters(raw)
    if genres:
        filters["genres"] = genres

    cleaned = raw.strip()
    return cleaned if cleaned else query, filters


def expand_query(query: str, extra_terms: Dict[str, int] | None = None) -> str:
    if not query:
        return query
    expansions = {
        "电影": ["影片", "movie", "film"],
        "影片": ["电影", "movie", "film"],
        "惊悚": ["thriller", "紧张"],
        "恐怖": ["horror", "恐怖片"],
        "悬疑": ["mystery", "疑案"],
        "爱情": ["romance"],
        "喜剧": ["comedy"],
        "科幻": ["sci-fi", "science fiction"],
        "动作": ["action"],
        "战争": ["war"],
        "动画": ["animation", "cartoon"],
        "纪录": ["documentary", "doc"],
        "犯罪": ["crime"],
        "奇幻": ["fantasy"],
        "冒险": ["adventure"],
        "剧情": ["drama"],
        "家庭": ["family"],
        "音乐": ["music"],
        "传记": ["biography", "biopic"],
        "历史": ["history"],
        "西部": ["western"],
        "体育": ["sport", "sports"],
        "真人秀": ["reality"],
        "综艺": ["variety"],
        "剧集": ["series", "tv", "show"],
        "电视剧": ["tv", "series", "drama"],
    }
    tokens = [query]
    for key, extra in expansions.items():
        if key in query:
            tokens.extend(extra)
    if extra_terms:
        for term, weight in extra_terms.items():
            count = max(1, min(int(weight), 3))
            for _ in range(count):
                tokens.append(term)
    seen = set()
    deduped = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return " ".join(deduped)


def _detect_query_languages(text: str) -> tuple[List[str], List[str]]:
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
    tmdb_only: bool = Query(False, description="only return items with tmdb_id"),
    page_size: int = Query(20, ge=1, le=100),
    cursor: int = Query(0, ge=0, description="cursor offset for pagination"),
) -> Dict[str, Any]:
    cleaned_query, filters = extract_query_filters(q)
    tmdb_extra = {}
    tmdb_cfg = cfg.tmdb or {}
    if tmdb_cfg.get("query_expand", True) and cleaned_query:
        tmdb_limit = int(tmdb_cfg.get("query_expand_limit", 20))
        schema = (cfg.bitmagnet or {}).get("schema", "hermes")
        tmdb_extra = pg_client.search_tmdb_expansions(schema, cleaned_query, limit=tmdb_limit)
    expanded_query = expand_query(cleaned_query, tmdb_extra)
    normalized_query = normalize_title_text(expanded_query)
    final_query = normalized_query or expanded_query
    if "bge" in str(cfg.embedding_model_version).lower():
        final_query = f"为这个句子生成用于检索的向量: {final_query}"
    query_vec = embed_query(final_query)
    genre_filters = filters.get("genres", [])
    fetch_k = min(100, max(topk, page_size))
    metadata_filter = None
    if tmdb_only or genre_filters or filters.get("file_type") or filters.get("audio_langs") or filters.get("subtitle_langs"):
        metadata_filter = {
            "has_tmdb": tmdb_only,
            "genres": genre_filters,
            "file_type": filters.get("file_type"),
            "audio_langs": filters.get("audio_langs"),
            "subtitle_langs": filters.get("subtitle_langs"),
        }
    results = vector_store.query(
        np.asarray([query_vec], dtype="float32"),
        topk=fetch_k,
        metadata_filter=metadata_filter,
        offset=cursor,
    )
    raw_count = len(results)
    filtered = []
    for r in results:
        if exclude_nsfw and r.get("nsfw"):
            continue
        filtered.append(r)
    next_cursor = cursor + raw_count if raw_count == fetch_k else None
    ids_by_source: Dict[str, List[str]] = {}
    for r in filtered:
        source = r.get("source")
        pg_id = r.get("pg_id")
        if not source or pg_id is None:
            continue
        ids_by_source.setdefault(source, []).append(str(pg_id))
    enriched: List[SearchResult] = []
    for source_name, ids in ids_by_source.items():
        source_cfg = source_map.get(source_name)
        if not source_cfg:
            continue
        pg_cfg = source_cfg.get("pg", {})
        tmdb_field = pg_cfg.get("tmdb_only_field", "tmdb_id")
        fields = set([pg_cfg.get("id_field"), pg_cfg.get("text_field")] + pg_cfg.get("extra_fields", []))
        rows_source_cfg = source_cfg
        if tmdb_only and tmdb_field in fields:
            rows_source_cfg = {
                **source_cfg,
                "pg": {
                    **pg_cfg,
                    "where": f"{tmdb_field} IS NOT NULL",
                },
            }
        rows = pg_client.fetch_by_ids(rows_source_cfg, ids)
        if pg_cfg.get("keyword_search") and cleaned_query:
            if not tmdb_only or tmdb_field in fields:
                keyword_hits = pg_client.search_by_keyword(rows_source_cfg, cleaned_query, limit=page_size * 3)
            else:
                keyword_hits = []
            for hit in keyword_hits:
                rows.setdefault(
                    str(hit["pg_id"]),
                    {source_cfg["pg"]["text_field"]: hit.get("title", "")},
                )
        for r in filtered:
            if r["source"] != source_name:
                continue
            pg_row = rows.get(str(r["pg_id"]), {})
            if not pg_row:
                continue
            title = pg_row.get(source_cfg["pg"]["text_field"], "")
            meta = {}
            for k, v in pg_row.items():
                if k in (source_cfg["pg"]["id_field"], source_cfg["pg"]["text_field"]):
                    continue
                meta[k] = _sanitize_value(v)
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
    return {
        "count": len(enriched),
        "next_cursor": next_cursor,
        "page_size": page_size,
        "results": [e.model_dump() for e in enriched],
    }


@app.get("/torrent_files")
def torrent_files(info_hash: str = Query(..., description="info_hash in \\x... text form")) -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    rows = pg_client.fetch_torrent_files(schema, info_hash)
    return {"count": len(rows), "files": [TorrentFile(**r).model_dump() for r in rows]}


@app.get("/tmdb_latest")
def tmdb_latest(limit: int = Query(50, ge=1, le=100)) -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    rows = pg_client.fetch_latest_tmdb(schema, limit=limit)
    return {"count": len(rows), "results": [LatestTmdbItem(**r).model_dump() for r in rows]}


@app.get("/tmdb_detail")
def tmdb_detail(
    tmdb_id: str = Query(...),
    content_type: str = Query("movie"),
) -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    row = pg_client.fetch_tmdb_detail(schema, content_type, tmdb_id)
    if not row:
        tmdb_cfg = cfg.tmdb or {}
        if tmdb_cfg.get("enabled"):
            try:
                api_key = tmdb_enrich.load_tmdb_key(tmdb_cfg)
                base_url = tmdb_cfg.get("base_url", tmdb_enrich.DEFAULT_BASE_URL)
                language = tmdb_cfg.get("language", tmdb_enrich.DEFAULT_LANGUAGE)
                limits = tmdb_cfg.get("limits", {"actors": 10, "directors": 5, "aka": 10})
                timeout = float(tmdb_cfg.get("timeout_seconds", 10))
                with httpx.Client(timeout=timeout) as client:
                    payload = tmdb_enrich.fetch_tmdb_payload(
                        client,
                        base_url,
                        api_key,
                        content_type,
                        tmdb_id,
                        language,
                    )
                    values = tmdb_enrich.normalize_tmdb_payload(payload, limits)
                with tmdb_enrich.connect(cfg.postgres["dsn"]) as conn:
                    tmdb_enrich.upsert_tmdb(conn, schema, content_type, tmdb_id, values, payload)
                row = pg_client.fetch_tmdb_detail(schema, content_type, tmdb_id)
            except Exception:
                row = None
    if not row:
        return {"detail": None}
    raw = row.get("raw") or {}
    base = "https://image.tmdb.org/t/p/w500"
    poster_path = raw.get("poster_path")
    backdrop_path = raw.get("backdrop_path")
    row["poster_url"] = f"{base}{poster_path}" if poster_path else None
    row["backdrop_url"] = f"{base}{backdrop_path}" if backdrop_path else None
    row.pop("raw", None)
    return {"detail": TmdbDetail(**row).model_dump()}


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        return f"\\x{data.hex()}"
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _sanitize_value(v) for k, v in value.items()}
    return value
