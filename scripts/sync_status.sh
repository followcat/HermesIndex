#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

python3 - <<'PY' "$CONFIG_PATH"
import sys

try:
    import yaml
    import psycopg
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Missing dependency: {exc}")

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f) or {}

postgres = cfg.get("postgres", {})
bitmagnet = cfg.get("bitmagnet", {})

schema = bitmagnet.get("schema", "hermes")
dsn = postgres.get("dsn")
if not dsn:
    raise SystemExit("postgres.dsn missing in config")

sources = cfg.get("sources", [])
if not sources:
    print("No sources configured")
    raise SystemExit(0)

with psycopg.connect(dsn, autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s)",
            (f"{schema}.tmdb_enrichment",),
        )
        tmdb_table = cur.fetchone()[0]
        if tmdb_table:
            cur.execute(f"SELECT count(*) FROM {schema}.tmdb_enrichment")
            tmdb_total = cur.fetchone()[0]
            cur.execute(f"SELECT max(updated_at) FROM {schema}.tmdb_enrichment")
            tmdb_max_updated = cur.fetchone()[0]
            cur.execute(
                f"""
                SELECT count(*)
                FROM {schema}.tmdb_enrichment
                WHERE (aka IS NULL OR aka = '')
                  AND (keywords IS NULL OR keywords = '')
                """
            )
            tmdb_missing = cur.fetchone()[0]
            print("TMDB enrichment:")
            print(f"  Table: {schema}.tmdb_enrichment")
            print(f"  Total rows: {tmdb_total}")
            print(f"  Max updated_at: {tmdb_max_updated}")
            print(f"  Missing aka/keywords: {tmdb_missing}")
        else:
            print("TMDB enrichment: not found")
        for source in sources:
            name = source.get("name")
            pg_cfg = source.get("pg", {})
            table = pg_cfg.get("table")
            id_field = pg_cfg.get("id_field")
            updated_at_field = pg_cfg.get("updated_at_field")
            print(f"Source: {name}")
            if not table or not id_field:
                print("  Missing table/id_field")
                continue
            cur.execute(
                f"SELECT count(*) FROM {table}",
            )
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM {schema}.sync_state WHERE source = %s",
                (name,),
            )
            synced = cur.fetchone()[0]
            print(f"  Table: {table}")
            print(f"  Total rows: {total}")
            print(f"  Synced rows: {synced}")
            if updated_at_field:
                cur.execute(
                    f"SELECT max({updated_at_field}) FROM {table}",
                )
                max_src = cur.fetchone()[0]
                cur.execute(
                    f"SELECT max(updated_at) FROM {schema}.sync_state WHERE source = %s",
                    (name,),
                )
                max_sync = cur.fetchone()[0]
                cur.execute(
                    f"""
                    SELECT max(t.{updated_at_field})
                    FROM {table} t
                    JOIN {schema}.sync_state s
                      ON s.source = %s AND s.pg_id = t.{id_field}::text
                    """,
                    (name,),
                )
                max_synced_src = cur.fetchone()[0]
                print(f"  Max {updated_at_field}: {max_src}")
                print(f"  Last sync updated_at: {max_sync}")
                print(f"  Max synced {updated_at_field}: {max_synced_src}")
            cur.execute(
                f"SELECT count(*) FROM {schema}.sync_state WHERE source = %s AND last_error IS NOT NULL",
                (name,),
            )
            err = cur.fetchone()[0]
            print(f"  Errors: {err}")
PY
