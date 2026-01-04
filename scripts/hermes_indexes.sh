#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
MODE=${1:-list}
shift || true

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

python3 - <<'PY' "$CONFIG_PATH" "$MODE" "$@"
import argparse
import shlex
import sys

try:
    import yaml
    import psycopg
    from psycopg import sql
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Missing dependency: {exc}")


def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def list_indexes(conn: psycopg.Connection, schema: str) -> list[dict]:
    q = sql.SQL(
        """
        SELECT
            ns.nspname AS schema,
            tbl.relname AS table_name,
            idx.relname AS index_name,
            pg_relation_size(idx.oid) AS bytes,
            COALESCE(st.idx_scan, 0) AS idx_scan,
            COALESCE(st.idx_tup_read, 0) AS idx_tup_read,
            COALESCE(st.idx_tup_fetch, 0) AS idx_tup_fetch,
            ix.indisprimary AS is_primary,
            ix.indisunique AS is_unique
        FROM pg_class idx
        JOIN pg_index ix ON ix.indexrelid = idx.oid
        JOIN pg_class tbl ON tbl.oid = ix.indrelid
        JOIN pg_namespace ns ON ns.oid = idx.relnamespace
        LEFT JOIN pg_stat_user_indexes st ON st.indexrelid = idx.oid
        WHERE ns.nspname = %s
        ORDER BY bytes DESC, index_name ASC
        """
    )
    with conn.cursor() as cur:
        cur.execute(q, (schema,))
        rows = cur.fetchall()
    out = []
    for r in rows:
        out.append(dict(r))
    return out


def pretty_bytes(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return str(n)
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(n)
    for u in units:
        if val < 1024 or u == units[-1]:
            return f"{val:.1f}{u}" if u != "B" else f"{int(val)}B"
        val /= 1024.0
    return f"{val:.1f}TB"


def normalize_names(raw: str) -> list[str]:
    parts = []
    for token in (raw or "").split(","):
        t = token.strip()
        if t:
            parts.append(t)
    return parts


def drop_indexes(conn: psycopg.Connection, schema: str, names: list[str], concurrently: bool) -> None:
    if not names:
        return
    for name in names:
        ident = sql.Identifier(schema, name)
        stmt = (
            sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(ident)
            if concurrently
            else sql.SQL("DROP INDEX IF EXISTS {}").format(ident)
        )
        with conn.cursor() as cur:
            cur.execute(stmt)


parser = argparse.ArgumentParser(description="List/suggest/drop Hermes schema indexes.")
parser.add_argument("config_path")
parser.add_argument("mode", choices=["list", "suggest", "drop"])
parser.add_argument("--schema", help="override schema (default from config.bitmagnet.schema)")
parser.add_argument("--min-bytes", type=int, default=50 * 1024 * 1024, help="suggest: minimum index size in bytes (default 50MB)")
parser.add_argument("--names", help="drop: comma-separated index names (schema-local)")
parser.add_argument("--suggested", action="store_true", help="drop: drop suggested indexes (idx_scan=0, non-unique, non-primary)")
parser.add_argument("--concurrently", action="store_true", help="drop: use DROP INDEX CONCURRENTLY")
parser.add_argument("--yes", action="store_true", help="drop: do not prompt")

args = parser.parse_args()

cfg = load_cfg(args.config_path)
dsn = (cfg.get("postgres") or {}).get("dsn")
if not dsn:
    raise SystemExit("postgres.dsn missing in config")
schema = args.schema or (cfg.get("bitmagnet") or {}).get("schema") or "hermes"

with psycopg.connect(dsn, autocommit=True, row_factory=psycopg.rows.dict_row) as conn:
    indexes = list_indexes(conn, schema)

    if args.mode in ("list", "suggest"):
        header = f"Indexes in schema={schema} total={len(indexes)}"
        print(header)
        if not indexes:
            raise SystemExit(0)
        for row in indexes:
            flags = []
            if row.get("is_primary"):
                flags.append("PRIMARY")
            if row.get("is_unique") and not row.get("is_primary"):
                flags.append("UNIQUE")
            flag_text = ",".join(flags) if flags else "-"
            print(
                f"- {row['index_name']} table={row['table_name']} size={pretty_bytes(row['bytes'])} "
                f"idx_scan={row['idx_scan']} {flag_text}"
            )
        if args.mode == "list":
            raise SystemExit(0)

        suggested = [
            r
            for r in indexes
            if not r.get("is_primary")
            and not r.get("is_unique")
            and int(r.get("idx_scan") or 0) == 0
            and int(r.get("bytes") or 0) >= int(args.min_bytes)
        ]
        print("")
        print(f"Suggested drop candidates (idx_scan=0, non-unique, >= {pretty_bytes(args.min_bytes)}): {len(suggested)}")
        for row in suggested:
            print(f"- {row['index_name']} size={pretty_bytes(row['bytes'])} table={row['table_name']}")
        raise SystemExit(0)

    # drop
    names = normalize_names(args.names or "")
    if args.suggested:
        names = [
            r["index_name"]
            for r in indexes
            if not r.get("is_primary")
            and not r.get("is_unique")
            and int(r.get("idx_scan") or 0) == 0
        ]
    names = sorted(set(names))
    if not names:
        raise SystemExit("No indexes selected to drop. Use --names or --suggested.")

    print(f"Config: {args.config_path}")
    print(f"Schema: {schema}")
    print(f"Drop indexes ({len(names)}): {', '.join(names)}")
    if not args.yes:
        confirm = input("Type 'DROP' to continue: ").strip()
        if confirm != "DROP":
            raise SystemExit("Aborted.")

    drop_indexes(conn, schema, names, concurrently=bool(args.concurrently))
    print("Done.")
PY

