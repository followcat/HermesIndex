import os
import re
import logging
import threading
import time
from datetime import datetime
from ipaddress import ip_address
from typing import Any, Dict, List

import numpy as np
import httpx
from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request
from pydantic import BaseModel

from cpu.config import load_config
from cpu.clients.bitmagnet_graphql import BitmagnetGraphQLClient
from cpu.core.embedder import LocalEmbedder
from cpu.core.utils import normalize_title_text
from cpu.clients.gpu_client import GPUClient
from cpu.repositories.pg import PGClient
from cpu.repositories.vector_store import create_vector_store
from cpu.services import tmdb_enrich
from cpu.services.auth_store import AuthStore

CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/example.yaml")

logger = logging.getLogger(__name__)

cfg = load_config(CONFIG_PATH)
pg_client = PGClient(cfg.postgres["dsn"])
vector_store = create_vector_store(cfg.vector_store)
gpu_client = GPUClient(cfg.gpu_endpoint) if cfg.gpu_endpoint else None
local_embedder = None
if cfg.local_embedder.get("enabled"):
    local_embedder = LocalEmbedder(cfg.local_embedder.get("model_name", "BAAI/bge-m3"))

bitmagnet_graphql_client = None
bitmagnet_cfg = cfg.bitmagnet or {}
bitmagnet_graphql_endpoint = bitmagnet_cfg.get("graphql_endpoint") or bitmagnet_cfg.get("graphql_url")
if not bitmagnet_graphql_endpoint:
    host = bitmagnet_cfg.get("host")
    if host:
        scheme = str(bitmagnet_cfg.get("graphql_scheme") or "http")
        port = int(bitmagnet_cfg.get("graphql_port") or 3333)
        bitmagnet_graphql_endpoint = f"{scheme}://{host}:{port}/graphql"
if bitmagnet_graphql_endpoint:
    bitmagnet_graphql_client = BitmagnetGraphQLClient(
        str(bitmagnet_graphql_endpoint),
        timeout=float(bitmagnet_cfg.get("graphql_timeout_seconds", 15)),
    )

source_map: Dict[str, Dict[str, Any]] = {s["name"]: s for s in cfg.sources}

app = FastAPI(title="HermesIndex Search API")
auth_cfg = cfg.auth or {}
auth_enabled = bool(auth_cfg.get("enabled", False))
auth_store = None
if auth_enabled:
    admin_user = auth_cfg.get("admin_user", "")
    admin_password = auth_cfg.get("admin_password", "")
    if not admin_user or not admin_password:
        raise ValueError("auth.enabled=true requires auth.admin_user and auth.admin_password")
    user_store_path = auth_cfg.get("user_store_path", "data/users.json")
    token_store_path = auth_cfg.get("token_store_path")
    token_ttl = int(auth_cfg.get("token_ttl_seconds", 86400))
    auth_store = AuthStore(
        user_store_path,
        admin_user,
        admin_password,
        token_ttl=token_ttl,
        token_store_path=token_store_path,
    )


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
    imdb_id: str | None = None
    aka: str | None = None
    keywords: str | None = None
    actors: str | None = None
    directors: str | None = None
    plot: str | None = None
    genre: str | None = None
    imdb_rating: float | None = None
    douban_rating: float | None = None
    poster_url: str | None = None
    backdrop_url: str | None = None
    updated_at: datetime | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    username: str
    role: str


class UserSummary(BaseModel):
    username: str
    role: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str = "user"


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


def _search_result_key(item: SearchResult) -> str:
    meta = item.metadata or {}
    title = normalize_title_text(item.title or "") or normalize_title_text(str(meta.get("title") or ""))
    if title:
        return f"title:{title}"
    return f"id:{item.source}:{item.pg_id}"


def _dedupe_search_results(items: List[SearchResult]) -> List[SearchResult]:
    seen: set[str] = set()
    output: List[SearchResult] = []
    for item in items:
        key = _search_result_key(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _dedupe_vector_hits(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    output: List[Dict[str, Any]] = []
    for item in items:
        text_hash = str(item.get("text_hash") or "").strip()
        key = text_hash or f"{item.get('source')}:{item.get('pg_id')}"
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


class SyncStatusSource(BaseModel):
    name: str
    table: str
    total_rows: int
    synced_rows: int
    max_updated_at: datetime | None = None
    last_sync_updated_at: datetime | None = None
    max_synced_updated_at: datetime | None = None
    errors: int


class SyncStatusResponse(BaseModel):
    tmdb_content_total: int
    tmdb_content_latest: datetime | None = None
    tmdb_enrichment_total: int
    tmdb_enrichment_latest: datetime | None = None
    tmdb_enrichment_missing: int
    sources: List[SyncStatusSource]


sync_status_lock = threading.Lock()
sync_status_cache: Dict[str, Any] | None = None
sync_status_updated_at: float | None = None


def require_user(authorization: str | None = Header(default=None)) -> Dict[str, Any] | None:
    if not auth_enabled:
        return None
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    token = parts[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid token")
    assert auth_store is not None
    user = auth_store.verify_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return user


def require_admin(user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any] | None:
    if not auth_enabled:
        return None
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin required")
    return user


def _is_loopback(value: str | None) -> bool:
    if not value:
        return False
    try:
        return ip_address(value).is_loopback
    except ValueError:
        return False


def require_local(request: Request) -> None:
    client_host = getattr(request.client, "host", None) if request.client else None
    if not _is_loopback(client_host):
        raise HTTPException(status_code=403, detail="Local access only")
    forwarded = request.headers.get("x-forwarded-for") or ""
    real_ip = request.headers.get("x-real-ip") or ""
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first and not _is_loopback(first):
            raise HTTPException(status_code=403, detail="Local access only")
    if real_ip and not _is_loopback(real_ip.strip()):
        raise HTTPException(status_code=403, detail="Local access only")


def _sanitize_config(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            key = str(k).lower()
            if any(s in key for s in ("password", "token", "api_key", "apikey", "dsn", "secret")):
                continue
            out[k] = _sanitize_config(v)
        return out
    if isinstance(obj, list):
        return [_sanitize_config(v) for v in obj]
    return obj


@app.get("/debug/config")
def debug_config(request: Request) -> Dict[str, Any]:
    require_local(request)
    vector_cfg = _sanitize_config(cfg.vector_store or {})
    sources = [s.get("name") for s in (cfg.sources or []) if s.get("name")]
    try:
        vs_size = vector_store.size()
    except Exception as exc:
        vs_size = f"error: {exc}"
    return {
        "config_path": CONFIG_PATH,
        "vector_store": vector_cfg,
        "vector_store_size": vs_size,
        "sources": sources,
    }


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
        "视频文件": "video",
        "音频文件": "audio",
        "字幕文件": "subtitle",
        "图片文件": "image",
        "图片类文件": "image",
        "压缩包": "archive",
        "压缩文件": "archive",
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


def _merge_where(clauses: List[str]) -> str:
    cleaned = [c.strip() for c in clauses if c and str(c).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return " AND ".join([f"({c})" for c in cleaned])


def _safe_identifier(value: str) -> str:
    return value if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or "") else ""


def _meta_size(meta: Dict[str, Any]) -> float | None:
    if not meta:
        return None
    for key in ("size", "total_size", "torrent_size", "content_size", "files_size", "file_size", "length"):
        raw = meta.get(key)
        try:
            num = float(raw)
        except (TypeError, ValueError):
            continue
        if num > 0:
            return num
    return None


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


@app.post("/auth/login")
def login(req: LoginRequest) -> Dict[str, Any]:
    if not auth_enabled:
        raise HTTPException(status_code=400, detail="Auth disabled")
    assert auth_store is not None
    user = auth_store.login(req.username, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = auth_store.issue_token(user["username"], user["role"])
    return LoginResponse(token=token, username=user["username"], role=user["role"]).model_dump()


@app.get("/auth/me")
def me(user: Dict[str, Any] = Depends(require_user)) -> Dict[str, Any]:
    if not auth_enabled:
        return {"username": "anonymous", "role": "guest"}
    return {"username": user["username"], "role": user["role"]}


@app.get("/auth/users")
def list_users(_: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    assert auth_store is not None
    users = auth_store.list_users()
    return {"users": [UserSummary(**u).model_dump() for u in users]}


@app.post("/auth/users")
def create_user(req: CreateUserRequest, _: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    assert auth_store is not None
    auth_store.add_user(req.username, req.password, role=req.role)
    return {"status": "ok"}


@app.delete("/auth/users/{username}")
def delete_user(username: str, _: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    assert auth_store is not None
    if username == auth_store.admin_user:
        raise HTTPException(status_code=400, detail="Cannot delete admin user")
    auth_store.delete_user(username)
    return {"status": "ok"}


@app.post("/auth/password")
def change_password(
    req: ChangePasswordRequest,
    user: Dict[str, Any] = Depends(require_user),
) -> Dict[str, Any]:
    if not auth_enabled:
        raise HTTPException(status_code=400, detail="Auth disabled")
    assert auth_store is not None
    if user["username"] == auth_store.admin_user:
        raise HTTPException(status_code=400, detail="Admin password is managed in config")
    try:
        auth_store.update_password(user["username"], req.old_password, req.new_password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "ok"}


@app.get("/search")
def search(
    q: str = Query(..., description="query text"),
    topk: int = Query(20, ge=1, le=100),
    exclude_nsfw: bool = Query(True),
    tmdb_only: bool = Query(False, description="only return items with tmdb_id"),
    size_min_gb: float | None = Query(None, ge=0, description="min size in GB"),
    size_sort: str | None = Query(None, description="size sort: asc/desc"),
    page_size: int = Query(20, ge=1, le=100),
    cursor: int = Query(0, ge=0, description="cursor offset for pagination"),
    _: Dict[str, Any] | None = Depends(require_user),
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
    search_cfg = getattr(cfg, "search", {}) if hasattr(cfg, "search") else {}
    query_prefix = None
    if isinstance(search_cfg, dict):
        query_prefix = search_cfg.get("query_prefix")
    if query_prefix:
        final_query = f"{query_prefix}{final_query}"
    query_vec = embed_query(final_query)
    genre_filters = filters.get("genres", [])
    fetch_k = min(100, max(topk, page_size))
    size_sort_norm = (size_sort or "").strip().lower()
    size_min_bytes = None
    if size_min_gb is not None:
        size_min_bytes = int(max(size_min_gb, 0) * (1024**3))
    metadata_filter = None
    if (
        tmdb_only
        or genre_filters
        or filters.get("file_type")
        or filters.get("audio_langs")
        or filters.get("subtitle_langs")
        or size_min_bytes
    ):
        metadata_filter = {
            "has_tmdb": tmdb_only,
            "genres": genre_filters,
            "file_type": filters.get("file_type"),
            "audio_langs": filters.get("audio_langs"),
            "subtitle_langs": filters.get("subtitle_langs"),
            "size_min": size_min_bytes,
        }
    results = vector_store.query(
        np.asarray([query_vec], dtype="float32"),
        topk=fetch_k,
        metadata_filter=metadata_filter,
        offset=cursor,
    )
    raw_count = len(results)
    filtered = _dedupe_vector_hits(results)
    filtered = [r for r in filtered if not (exclude_nsfw and r.get("nsfw"))]
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
        where_clauses: List[str] = []
        base_where = pg_cfg.get("where")
        if base_where:
            where_clauses.append(str(base_where))
        if tmdb_only and tmdb_field in fields:
            where_clauses.append(f"{tmdb_field} IS NOT NULL")
        size_field = _safe_identifier(pg_cfg.get("size_field", "size"))
        if size_min_bytes and size_field and size_field in fields:
            where_clauses.append(f"t.{size_field} >= {size_min_bytes}")
        merged_where = _merge_where(where_clauses)
        rows_source_cfg = {
            **source_cfg,
            "pg": {
                **pg_cfg,
                "where": merged_where or pg_cfg.get("where"),
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
    deduped = _dedupe_search_results(enriched)
    if size_sort_norm in {"asc", "desc"}:
        reverse = size_sort_norm == "desc"

        def sort_key(item: SearchResult) -> tuple[int, float, float]:
            size_val = _meta_size(item.metadata)
            missing = 1 if size_val is None else 0
            if size_val is None:
                size_val = 0.0
            size_key = -size_val if reverse else size_val
            return (missing, size_key, -item.score)

        deduped.sort(key=sort_key)
    return {
        "count": len(deduped),
        "next_cursor": next_cursor,
        "page_size": page_size,
        "results": [e.model_dump() for e in deduped],
    }


def _keyword_hit_score(query: str, title: str) -> float:
    q = (query or "").strip().lower()
    t = (title or "").strip().lower()
    if not q or not t:
        return 0.0
    if q == t:
        return 1.0
    pos = t.find(q)
    if pos < 0:
        return 0.1
    return float(max(0.2, 0.9 / (1 + pos)))


def _normalize_info_hash(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    if text.startswith("\\x") and len(text) == 42:
        return text.lower()
    candidate = text.lower()
    if len(candidate) == 40 and re.fullmatch(r"[0-9a-f]{40}", candidate):
        return "\\x" + candidate
    return text


def _node_has_tmdb(node: Dict[str, Any]) -> bool:
    content = node.get("content")
    if not isinstance(content, dict):
        return False
    attrs = content.get("attributes") or []
    if not isinstance(attrs, list):
        return False
    for item in attrs:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").lower()
        val = str(item.get("value") or "").strip()
        if not val:
            continue
        if "tmdb" in key:
            return True
        if key == "id" and re.fullmatch(r"tmdb:[a-z_]+:\\d+", val.lower()):
            return True
    return False


@app.get("/search_keyword")
def search_keyword(
    q: str = Query(..., description="keyword query text"),
    topk: int = Query(20, ge=1, le=100),
    exclude_nsfw: bool = Query(True),
    tmdb_only: bool = Query(False, description="only return items with tmdb_id"),
    size_min_gb: float | None = Query(None, ge=0, description="min size in GB"),
    size_sort: str | None = Query(None, description="size sort: asc/desc"),
    page_size: int = Query(20, ge=1, le=100),
    cursor: int = Query(0, ge=0, description="cursor offset for pagination"),
    sources: str | None = Query(None, description="comma-separated source names; default uses sources with pg.keyword_search=true"),
    _: Dict[str, Any] | None = Depends(require_user),
) -> Dict[str, Any]:
    cleaned_query, _ = extract_query_filters(q)
    if not cleaned_query:
        raise HTTPException(status_code=400, detail="Empty query")
    size_sort_norm = (size_sort or "").strip().lower()
    size_min_bytes = None
    if size_min_gb is not None:
        size_min_bytes = int(max(size_min_gb, 0) * (1024**3))

    keyword_backend = str((cfg.search or {}).get("keyword_backend") or "auto").strip().lower()
    if keyword_backend != "pg" and bitmagnet_graphql_client is not None:
        per_source_limit = min(2000, max(cursor + page_size * 3, topk * 3))
        gql_limit_cap = int((cfg.bitmagnet or {}).get("graphql_search_limit_cap") or 200)
        gql_limit = min(per_source_limit, max(1, gql_limit_cap))
        try:
            payload = bitmagnet_graphql_client.search_torrents(cleaned_query, limit=gql_limit)
            nodes = bitmagnet_graphql_client.extract_torrent_nodes(payload)
        except Exception as exc:
            logger.warning(
                "Bitmagnet GraphQL keyword search failed endpoint=%s error=%s",
                getattr(bitmagnet_graphql_client, "endpoint", ""),
                exc,
            )
            raise HTTPException(status_code=502, detail=f"Bitmagnet GraphQL search failed: {exc}") from exc

        candidates: List[SearchResult] = []
        ids: List[str] = []
        for node in nodes:
            info_hash = _normalize_info_hash(str(node.get("infoHash") or ""))
            if info_hash:
                ids.append(info_hash)
        sync_scores = pg_client.fetch_sync_scores("bitmagnet_torrents", ids)
        for node in nodes:
            info_hash = _normalize_info_hash(str(node.get("infoHash") or ""))
            title = str(node.get("name") or "")
            if not info_hash or not title:
                continue
            if size_min_bytes is not None:
                try:
                    if float(node.get("size") or 0) < float(size_min_bytes):
                        continue
                except (TypeError, ValueError):
                    pass
            if tmdb_only and not _node_has_tmdb(node):
                continue
            nsfw_score = float((sync_scores.get(info_hash) or {}).get("nsfw_score") or 0.0)
            nsfw_flag = nsfw_score >= float(cfg.nsfw_threshold)
            if exclude_nsfw and nsfw_flag:
                continue
            meta = {
                "size": node.get("size"),
                "files_count": node.get("filesCount"),
                "seeders": node.get("seeders"),
                "leechers": node.get("leechers"),
                "published_at": node.get("publishedAt"),
                "content": node.get("content"),
            }
            candidates.append(
                SearchResult(
                    score=_keyword_hit_score(cleaned_query, title),
                    source="bitmagnet_torrents",
                    pg_id=info_hash,
                    title=title,
                    nsfw=bool(nsfw_flag),
                    nsfw_score=float(nsfw_score),
                    metadata=_sanitize_value(meta) or {},
                )
            )
        candidates.sort(key=lambda x: x.score, reverse=True)
        deduped = _dedupe_search_results(candidates)
        if size_sort_norm in {"asc", "desc"}:
            reverse = size_sort_norm == "desc"

            def sort_key(item: SearchResult) -> tuple[int, float, float]:
                size_val = _meta_size(item.metadata)
                missing = 1 if size_val is None else 0
                if size_val is None:
                    size_val = 0.0
                size_key = -size_val if reverse else size_val
                return (missing, size_key, -item.score)

            deduped.sort(key=sort_key)
        sliced = deduped[cursor : cursor + page_size]
        next_cursor = cursor + page_size if len(deduped) > cursor + page_size else None
        return {
            "count": len(sliced),
            "next_cursor": next_cursor,
            "page_size": page_size,
            "results": [e.model_dump() for e in sliced],
        }

    if sources:
        selected_sources = [s.strip() for s in sources.split(",") if s.strip()]
    else:
        selected_sources = [s["name"] for s in (cfg.sources or []) if (s.get("pg") or {}).get("keyword_search")]
        if not selected_sources:
            selected_sources = [s["name"] for s in (cfg.sources or []) if s.get("pg")]

    per_source_limit = min(2000, max(cursor + page_size * 3, topk * 3))
    candidates: List[SearchResult] = []
    for source_name in selected_sources:
        source_cfg = source_map.get(source_name)
        if not source_cfg:
            continue
        pg_cfg = source_cfg.get("pg", {})
        tmdb_field = pg_cfg.get("tmdb_only_field", "tmdb_id")
        fields = set([pg_cfg.get("id_field"), pg_cfg.get("text_field")] + pg_cfg.get("extra_fields", []))
        if tmdb_only and tmdb_field not in fields:
            continue
        where_clauses: List[str] = []
        base_where = pg_cfg.get("where")
        if base_where:
            where_clauses.append(str(base_where))
        if tmdb_only and tmdb_field in fields:
            where_clauses.append(f"{tmdb_field} IS NOT NULL")
        size_field = _safe_identifier(pg_cfg.get("size_field", "size"))
        if size_min_bytes and size_field and size_field in fields:
            where_clauses.append(f"t.{size_field} >= {size_min_bytes}")
        merged_where = _merge_where(where_clauses)
        rows_source_cfg = {
            **source_cfg,
            "pg": {
                **pg_cfg,
                "where": merged_where or pg_cfg.get("where"),
            },
        }
        keyword_hits = pg_client.search_by_keyword(rows_source_cfg, cleaned_query, limit=per_source_limit)
        if not keyword_hits:
            continue
        ids = [str(r["pg_id"]) for r in keyword_hits if r.get("pg_id")]
        rows = pg_client.fetch_by_ids(rows_source_cfg, ids)
        sync_scores = pg_client.fetch_sync_scores(source_name, ids)
        for hit in keyword_hits:
            pg_id = str(hit.get("pg_id") or "")
            if not pg_id:
                continue
            row = rows.get(pg_id) or {}
            title = row.get(pg_cfg.get("text_field") or "", "") or hit.get("title") or ""
            nsfw_score = float((sync_scores.get(pg_id) or {}).get("nsfw_score") or 0.0)
            nsfw_flag = (
                nsfw_score >= float(cfg.nsfw_threshold)
                if source_cfg.get("tagging", {}).get("nsfw", True)
                else False
            )
            if exclude_nsfw and nsfw_flag:
                continue
            meta = {}
            for k, v in row.items():
                if k in (pg_cfg.get("id_field"), pg_cfg.get("text_field")):
                    continue
                meta[k] = _sanitize_value(v)
            candidates.append(
                SearchResult(
                    score=_keyword_hit_score(cleaned_query, str(title)),
                    source=source_name,
                    pg_id=pg_id,
                    title=str(title),
                    nsfw=bool(nsfw_flag),
                    nsfw_score=float(nsfw_score),
                    metadata=meta,
                )
            )
    candidates.sort(key=lambda x: x.score, reverse=True)
    deduped = _dedupe_search_results(candidates)
    if size_sort_norm in {"asc", "desc"}:
        reverse = size_sort_norm == "desc"

        def sort_key(item: SearchResult) -> tuple[int, float, float]:
            size_val = _meta_size(item.metadata)
            missing = 1 if size_val is None else 0
            if size_val is None:
                size_val = 0.0
            size_key = -size_val if reverse else size_val
            return (missing, size_key, -item.score)

        deduped.sort(key=sort_key)
    sliced = deduped[cursor : cursor + page_size]
    next_cursor = cursor + page_size if len(deduped) > cursor + page_size else None
    return {
        "count": len(sliced),
        "next_cursor": next_cursor,
        "page_size": page_size,
        "results": [e.model_dump() for e in sliced],
    }


@app.get("/torrent_files")
def torrent_files(
    info_hash: str = Query(..., description="info_hash in \\x... text form"),
    _: Dict[str, Any] | None = Depends(require_user),
) -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    rows = pg_client.fetch_torrent_files(schema, info_hash)
    return {"count": len(rows), "files": [TorrentFile(**r).model_dump() for r in rows]}


@app.get("/tmdb_latest")
def tmdb_latest(limit: int = Query(50, ge=1, le=100), _: Dict[str, Any] | None = Depends(require_user)) -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    rows = pg_client.fetch_latest_tmdb(schema, limit=limit)
    return {"count": len(rows), "results": [LatestTmdbItem(**r).model_dump() for r in rows]}


@app.get("/tmdb_detail")
def tmdb_detail(
    tmdb_id: str = Query(...),
    content_type: str = Query("movie"),
    _: Dict[str, Any] | None = Depends(require_user),
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
                    imdb_cfg = tmdb_cfg.get("imdb") or {}
                    douban_cfg = tmdb_cfg.get("douban") or {}
                    imdb_id = values.get("imdb_id")
                    values["imdb_rating"] = tmdb_enrich.fetch_imdb_rating(client, imdb_cfg, imdb_id)
                    values["douban_rating"] = tmdb_enrich.fetch_douban_rating(client, douban_cfg, imdb_id)
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


@app.get("/sync_status")
def sync_status(_: Dict[str, Any] | None = Depends(require_user)) -> Dict[str, Any]:
    with sync_status_lock:
        if sync_status_cache is None:
            return {"status": "pending"}
        return {
            **sync_status_cache,
            "updated_at": sync_status_updated_at,
        }


def _compute_sync_status() -> Dict[str, Any]:
    schema = (cfg.bitmagnet or {}).get("schema", "hermes")
    sources = cfg.sources or []
    with pg_client.connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS total FROM public.content WHERE source = 'tmdb'")
        tmdb_content_total = int(cur.fetchone()["total"])
        cur.execute("SELECT max(updated_at) AS latest FROM public.content WHERE source = 'tmdb'")
        tmdb_content_latest = cur.fetchone()["latest"]

        cur.execute("SELECT count(*) AS total FROM {schema}.tmdb_enrichment".format(schema=schema))
        tmdb_enrichment_total = int(cur.fetchone()["total"])
        cur.execute("SELECT max(updated_at) AS latest FROM {schema}.tmdb_enrichment".format(schema=schema))
        tmdb_enrichment_latest = cur.fetchone()["latest"]
        cur.execute(
            """
            SELECT count(*) AS total
            FROM {schema}.tmdb_enrichment
            WHERE (aka IS NULL OR aka = '')
              AND (keywords IS NULL OR keywords = '')
            """.format(schema=schema)
        )
        tmdb_enrichment_missing = int(cur.fetchone()["total"])

        source_rows: List[SyncStatusSource] = []
        for source in sources:
            name = source.get("name", "")
            pg_cfg = source.get("pg", {})
            table = pg_cfg.get("table")
            id_field = pg_cfg.get("id_field")
            updated_at_field = pg_cfg.get("updated_at_field")
            if not table or not id_field:
                continue
            cur.execute(f"SELECT count(*) AS total FROM {table}")
            total = int(cur.fetchone()["total"])
            cur.execute(
                f"SELECT count(*) AS total FROM {schema}.sync_state WHERE source = %s",
                (name,),
            )
            synced = int(cur.fetchone()["total"])
            max_src = None
            max_sync = None
            max_synced_src = None
            if updated_at_field:
                cur.execute(f"SELECT max({updated_at_field}) AS latest FROM {table}")
                max_src = cur.fetchone()["latest"]
                cur.execute(
                    f"SELECT max(updated_at) AS latest FROM {schema}.sync_state WHERE source = %s",
                    (name,),
                )
                max_sync = cur.fetchone()["latest"]
                cur.execute(
                    f"""
                    SELECT max(t.{updated_at_field}) AS latest
                    FROM {table} t
                    JOIN {schema}.sync_state s
                      ON s.source = %s AND s.pg_id = t.{id_field}::text
                    """,
                    (name,),
                )
                max_synced_src = cur.fetchone()["latest"]
            cur.execute(
                f"SELECT count(*) AS total FROM {schema}.sync_state WHERE source = %s AND last_error IS NOT NULL",
                (name,),
            )
            errors = int(cur.fetchone()["total"])
            source_rows.append(
                SyncStatusSource(
                    name=name,
                    table=table,
                    total_rows=total,
                    synced_rows=synced,
                    max_updated_at=max_src,
                    last_sync_updated_at=max_sync,
                    max_synced_updated_at=max_synced_src,
                    errors=errors,
                )
            )
    response = SyncStatusResponse(
        tmdb_content_total=tmdb_content_total,
        tmdb_content_latest=tmdb_content_latest,
        tmdb_enrichment_total=tmdb_enrichment_total,
        tmdb_enrichment_latest=tmdb_enrichment_latest,
        tmdb_enrichment_missing=tmdb_enrichment_missing,
        sources=source_rows,
    )
    return response.model_dump()


def _sync_status_worker(interval_seconds: int = 60) -> None:
    global sync_status_cache, sync_status_updated_at
    while True:
        try:
            data = _compute_sync_status()
            with sync_status_lock:
                sync_status_cache = data
                sync_status_updated_at = time.time()
        except Exception:
            pass
        time.sleep(interval_seconds)


@app.on_event("startup")
def _start_sync_status_worker() -> None:
    thread = threading.Thread(target=_sync_status_worker, name="sync-status-worker", daemon=True)
    thread.start()


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        return f"\\x{data.hex()}"
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _sanitize_value(v) for k, v in value.items()}
    return value
