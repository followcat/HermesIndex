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
    from psycopg import sql
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
        cur.execute("SELECT count(*) FROM public.content WHERE source = 'tmdb'")
        tmdb_content_total = cur.fetchone()[0]
        cur.execute("SELECT max(updated_at) FROM public.content WHERE source = 'tmdb'")
        tmdb_content_latest = cur.fetchone()[0]
        print("TMDB content:")
        print(f"  public.content (source=tmdb): {tmdb_content_total}")
        print(f"  public.content latest updated_at: {tmdb_content_latest}")
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
        cur.execute(
            "SELECT to_regclass(%s)",
            (f"{schema}.tpdb_enrichment",),
        )
        tpdb_table = cur.fetchone()[0]
        if tpdb_table:
            cur.execute(f"SELECT count(*) FROM {schema}.tpdb_enrichment")
            tpdb_total = cur.fetchone()[0]
            cur.execute(f"SELECT max(updated_at) FROM {schema}.tpdb_enrichment")
            tpdb_max_updated = cur.fetchone()[0]
            cur.execute(
                f"""
                SELECT status, count(*)
                FROM {schema}.tpdb_enrichment
                GROUP BY status
                """
            )
            status_rows = cur.fetchall()
            status_map = {row[0]: row[1] for row in status_rows}
            cur.execute(
                f"""
                SELECT content_type, content_source, content_id, tpdb_id, title, status, updated_at
                FROM {schema}.tpdb_enrichment
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                """
            )
            tpdb_latest = cur.fetchone()
            print("TPDB enrichment:")
            print(f"  Table: {schema}.tpdb_enrichment")
            print(f"  Total rows: {tpdb_total}")
            print(f"  Max updated_at: {tpdb_max_updated}")
            print(f"  Status counts: {status_map}")
            print(f"  Latest row: {tpdb_latest}")
        else:
            print("TPDB enrichment: not found")
        for source in sources:
            name = source.get("name")
            pg_cfg = source.get("pg", {})
            table = pg_cfg.get("table")
            id_field = pg_cfg.get("id_field")
            text_field = pg_cfg.get("text_field")
            updated_at_field = pg_cfg.get("updated_at_field")
            print(f"Source: {name}")
            if not table or not id_field:
                print("  Missing table/id_field")
                continue
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.SQL(table)))
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
                    sql.SQL("SELECT max({}) FROM {}").format(
                        sql.Identifier(updated_at_field),
                        sql.SQL(table),
                    )
                )
                max_src = cur.fetchone()[0]
                cur.execute(
                    f"SELECT max(updated_at) FROM {schema}.sync_state WHERE source = %s",
                    (name,),
                )
                max_sync = cur.fetchone()[0]
                cur.execute(
                    sql.SQL(
                        """
                        SELECT max(t.{updated_at})
                        FROM {table} t
                        JOIN {schema}.sync_state s
                          ON s.source = %s AND s.pg_id = t.{id_field}::text
                        """
                    ).format(
                        updated_at=sql.Identifier(updated_at_field),
                        table=sql.SQL(table),
                        schema=sql.Identifier(schema),
                        id_field=sql.Identifier(id_field),
                    ),
                    (name,),
                )
                max_synced_src = cur.fetchone()[0]
                print(f"  Max {updated_at_field}: {max_src}")
                print(f"  Last sync updated_at: {max_sync}")
                print(f"  Max synced {updated_at_field}: {max_synced_src}")
            cur.execute(
                f"SELECT max(updated_at) FROM {schema}.sync_state WHERE source = %s",
                (name,),
            )
            latest_sync = cur.fetchone()[0]
            latest_item = None
            if latest_sync:
                select_fields = [sql.SQL("t.{id_field}::text").format(id_field=sql.Identifier(id_field))]
                if text_field:
                    select_fields.append(sql.SQL("t.{text_field}").format(text_field=sql.Identifier(text_field)))
                cur.execute(
                    sql.SQL(
                        """
                        SELECT {fields}
                        FROM {table} t
                        JOIN {schema}.sync_state s
                          ON s.source = %s AND s.pg_id = t.{id_field}::text
                        ORDER BY s.updated_at DESC NULLS LAST
                        LIMIT 1
                        """
                    ).format(
                        fields=sql.SQL(", ").join(select_fields),
                        table=sql.SQL(table),
                        schema=sql.Identifier(schema),
                        id_field=sql.Identifier(id_field),
                    ),
                    (name,),
                )
                latest_item = cur.fetchone()
            print(f"  Latest sync updated_at: {latest_sync}")
            print(f"  Latest synced item: {latest_item}")
            if pg_cfg.get("tpdb_enrich"):
                tpdb_content_type = pg_cfg.get("tpdb_content_type") or name
                tpdb_content_source = pg_cfg.get("tpdb_content_source") or name
                cur.execute(
                    f"""
                    SELECT count(*)
                    FROM {schema}.tpdb_enrichment
                    WHERE content_type = %s AND content_source = %s
                    """,
                    (tpdb_content_type, tpdb_content_source),
                )
                tpdb_count = cur.fetchone()[0]
                cur.execute(
                    f"""
                    SELECT max(updated_at)
                    FROM {schema}.tpdb_enrichment
                    WHERE content_type = %s AND content_source = %s
                    """,
                    (tpdb_content_type, tpdb_content_source),
                )
                tpdb_latest_at = cur.fetchone()[0]
                cur.execute(
                    f"""
                    SELECT content_id, title, status, updated_at
                    FROM {schema}.tpdb_enrichment
                    WHERE content_type = %s AND content_source = %s
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (tpdb_content_type, tpdb_content_source),
                )
                tpdb_latest_item = cur.fetchone()
                print(f"  TPDB rows: {tpdb_count}")
                print(f"  TPDB latest updated_at: {tpdb_latest_at}")
                print(f"  TPDB latest item: {tpdb_latest_item}")
            cur.execute(
                f"SELECT count(*) FROM {schema}.sync_state WHERE source = %s AND last_error IS NOT NULL",
                (name,),
            )
            err = cur.fetchone()[0]
            print(f"  Errors: {err}")
PY
