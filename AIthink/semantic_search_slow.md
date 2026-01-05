# Semantic search still slow — investigation

## Symptom
Semantic search (`/search`) still feels slow even with `lite=true`.

## What we added
Backend supports `debug=true` and now reports a timing breakdown in `_debug.timing_ms`:
- `tmdb_expand`: time spent querying Postgres for TMDB aka/keywords expansion
- `embed`: time spent calling the embed backend
- `qdrant`: time spent querying vector store
- `total`: end-to-end time inside `/search`

Per-source Postgres lookup time is already available in `_debug.pg_sources[*].pg_fetch_ms`.

## How to reproduce
Run:
```bash
curl -sG 'http://127.0.0.1:8000/search' \
  --data-urlencode 'q=jojo奇妙冒险' \
  -d topk=50 -d page_size=50 -d cursor=0 -d lite=true -d debug=true \
  | jq '._debug'
```

## How to interpret
1) If `embed` is large: bottleneck is the embedding service (GPU/CPU model). Check GPU server latency, batch size, model.
2) If `qdrant` is large: bottleneck is Qdrant (collection size, filters, HNSW params, IO). Check Qdrant logs + CPU/IO.
3) If `tmdb_expand` is large: Postgres TMDB enrichment table scan/slow index; consider disabling query_expand or adding indexes.
4) If `_debug.pg_sources[].pg_fetch_ms` is large: Postgres fetch_by_ids or joins/filters; keep `lite=true`, ensure id type matches index.

## Recommended Postgres indexes (TMDB expand)
If `tmdb_expand` dominates, `tmdb_enrichment` is likely doing a sequential scan with `ILIKE '%query%'`.

For the default schema `hermes`, you can add trigram indexes (requires `pg_trgm`):
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_aka_trgm ON hermes.tmdb_enrichment USING gin (aka gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tmdb_enrichment_keywords_trgm ON hermes.tmdb_enrichment USING gin (keywords gin_trgm_ops);
```

Or disable expansion in config for pure performance:
```yaml
tmdb:
  query_expand: false
```

## Next actions
- Capture 3 samples with `debug=true` and compare timings.
- If `embed` dominates, consider caching embeddings for identical queries on the API layer.
- If `qdrant` dominates, consider lowering `fetch_k`, or tune Qdrant HNSW / payload index.
