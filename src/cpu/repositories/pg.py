import re
from typing import Any, Dict, Iterable, List, Sequence

import psycopg
import logging
from psycopg import sql
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

SYNC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sync_state (
    source TEXT NOT NULL,
    pg_id TEXT NOT NULL,
    text_hash TEXT,
    embedding_version TEXT,
    vector_id TEXT,
    nsfw_score REAL,
    updated_at TIMESTAMPTZ DEFAULT now(),
    last_error TEXT,
    PRIMARY KEY (source, pg_id)
);
CREATE INDEX IF NOT EXISTS idx_sync_state_updated_at ON sync_state (updated_at);
"""


class PGClient:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row, autocommit=True)

    def ensure_tables(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(SYNC_TABLE_SQL)

    def fetch_pending(self, source: Dict[str, Any], batch_size: int) -> List[Dict[str, Any]]:
        pg_cfg = source["pg"]
        table = pg_cfg["table"]
        id_field = pg_cfg["id_field"]
        text_field = pg_cfg["text_field"]
        updated_at_field = pg_cfg.get("updated_at_field")
        extra_fields = pg_cfg.get("extra_fields", [])

        columns = [
            f"t.{id_field}::text AS pg_id",
            f"t.{text_field} AS text",
            f"md5(t.{text_field}) AS text_hash",
        ]
        if updated_at_field:
            columns.append(f"t.{updated_at_field} AS updated_at")
        for field in extra_fields:
            columns.append(f"t.{field} AS {field}")

        if updated_at_field:
            conditions = [
                "s.pg_id IS NULL",
                f"t.{updated_at_field} > COALESCE(s.updated_at, to_timestamp(0))",
            ]
        else:
            conditions = ["s.pg_id IS NULL", f"s.text_hash IS DISTINCT FROM md5(t.{text_field})"]
        where = " OR ".join(f"({c})" for c in conditions)
        order_field = updated_at_field or id_field

        query = f"""
        SELECT {", ".join(columns)}
        FROM {table} t
        LEFT JOIN sync_state s ON s.source = %s AND s.pg_id = t.{id_field}::text
        WHERE {where}
        ORDER BY {order_field} NULLS LAST
        LIMIT %s
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(query, (source["name"], batch_size))
            return cur.fetchall()

    def upsert_sync_state(
        self,
        source: str,
        rows: Sequence[Dict[str, Any]],
    ) -> None:
        if not rows:
            return
        records = [
            (
                source,
                r["pg_id"],
                r.get("text_hash"),
                r.get("embedding_version"),
                str(r.get("vector_id")),
                float(r.get("nsfw_score", 0.0)),
            )
            for r in rows
        ]
        sql = """
        INSERT INTO sync_state (source, pg_id, text_hash, embedding_version, vector_id, nsfw_score, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (source, pg_id) DO UPDATE
        SET text_hash = EXCLUDED.text_hash,
            embedding_version = EXCLUDED.embedding_version,
            vector_id = EXCLUDED.vector_id,
            nsfw_score = EXCLUDED.nsfw_score,
            updated_at = now(),
            last_error = NULL
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.executemany(sql, records)

    def mark_failure(self, source: str, pg_id: str, error: str) -> None:
        sql = """
        INSERT INTO sync_state (source, pg_id, last_error, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (source, pg_id) DO UPDATE
        SET last_error = EXCLUDED.last_error, updated_at = now()
        """
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (source, pg_id, error[:512]))

    def fetch_by_ids(
        self,
        source: Dict[str, Any],
        ids: Iterable[str],
    ) -> Dict[str, Dict[str, Any]]:
        pg_cfg = source["pg"]
        table = pg_cfg["table"]
        id_field = pg_cfg["id_field"]
        text_field = pg_cfg["text_field"]
        if not ids:
            return {}
        fields = [id_field, text_field]
        for f in pg_cfg.get("extra_fields", []):
            fields.append(f)
        joins = pg_cfg.get("joins", [])
        id_list = list(ids)
        placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in id_list)
        select_cols = []
        group_by_cols = []
        for f in fields:
            if f == id_field:
                select_cols.append(
                    sql.SQL("t.{col}::text AS {alias}").format(
                        col=sql.Identifier(f),
                        alias=sql.Identifier(f),
                    )
                )
                group_by_cols.append(
                    sql.SQL("t.{col}::text").format(col=sql.Identifier(f))
                )
            else:
                select_cols.append(sql.SQL("t.{}").format(sql.Identifier(f)))
                group_by_cols.append(sql.SQL("t.{}").format(sql.Identifier(f)))
        join_clauses: List[sql.Composable] = []
        has_agg = False

        for idx, join_cfg in enumerate(joins):
            if not isinstance(join_cfg, dict):
                logger.warning("Skip invalid join config for source=%s index=%d", source.get("name"), idx)
                continue
            join_table = join_cfg.get("table")
            join_on = join_cfg.get("on")
            if not join_table or not join_on:
                logger.warning(
                    "Skip join without table/on for source=%s index=%d",
                    source.get("name"),
                    idx,
                )
                continue
            join_alias = join_cfg.get("alias") or f"j{idx}"
            join_type = str(join_cfg.get("type", "left")).lower()
            if join_type not in {"left", "inner"}:
                raise ValueError(f"Unsupported join type: {join_type}")
            join_fields = join_cfg.get("fields", [])
            join_clause = sql.SQL("{} JOIN {} AS {} ON {}").format(
                sql.SQL(join_type.upper()),
                self._table_identifier(join_table),
                sql.Identifier(join_alias),
                sql.SQL(join_on),
            )
            join_clauses.append(join_clause)
            for field_cfg in join_fields:
                column = field_cfg["column"]
                alias_name = field_cfg.get("alias") or column
                agg = field_cfg.get("agg")
                distinct = bool(field_cfg.get("distinct"))
                if agg:
                    agg_name = str(agg).lower()
                    if agg_name not in {"array_agg", "json_agg", "jsonb_agg"}:
                        raise ValueError(f"Unsupported aggregate: {agg}")
                    select_cols.append(
                        sql.SQL("{agg}({distinct}{col}) AS {alias}").format(
                            agg=sql.SQL(agg_name),
                            distinct=sql.SQL("DISTINCT ") if distinct else sql.SQL(""),
                            col=sql.Identifier(join_alias, column),
                            alias=sql.Identifier(alias_name),
                        )
                    )
                    has_agg = True
                else:
                    select_cols.append(
                        sql.SQL("{}.{} AS {}").format(
                            sql.Identifier(join_alias),
                            sql.Identifier(column),
                            sql.Identifier(alias_name),
                        )
                    )
                    group_by_cols.append(
                        sql.SQL("{}.{}").format(
                            sql.Identifier(join_alias),
                            sql.Identifier(column),
                        )
                    )

        where_extra = pg_cfg.get("where")
        base_sql = (
            "SELECT {selects} FROM {table} AS t {joins} "
            "WHERE t.{id_field}::text IN ({placeholders})"
        )
        if where_extra:
            base_sql += " AND ({where})"
        query = sql.SQL(base_sql).format(
            selects=sql.SQL(", ").join(select_cols),
            table=self._table_identifier(table),
            joins=sql.SQL(" ").join(join_clauses) if join_clauses else sql.SQL(""),
            id_field=sql.Identifier(id_field),
            placeholders=placeholders,
            where=sql.SQL(str(where_extra)) if where_extra else sql.SQL("TRUE"),
        )
        if has_agg:
            query = sql.SQL("{base} GROUP BY {group_by}").format(
                base=query,
                group_by=sql.SQL(", ").join(group_by_cols),
            )
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(query, id_list)
            rows = cur.fetchall()
            result = {}
            for row in rows:
                key = str(row[id_field])
                result[key] = {k: row[k] for k in row}
            return result

    def search_by_keyword(
        self,
        source: Dict[str, Any],
        query: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        pg_cfg = source["pg"]
        table = pg_cfg["table"]
        id_field = pg_cfg["id_field"]
        text_field = pg_cfg["text_field"]
        fields = pg_cfg.get("keyword_fields") or [text_field]
        where_extra = pg_cfg.get("where")
        if not query:
            return []
        clauses = " OR ".join([f"{f} ILIKE %s" for f in fields])
        where_sql = f"({clauses})"
        if where_extra:
            where_sql = f"{where_sql} AND ({where_extra})"
        sql_text = f"""
        SELECT DISTINCT ON ({text_field}) {id_field}::text AS pg_id, {text_field} AS title
        FROM {table}
        WHERE {where_sql}
        ORDER BY {text_field}, {id_field}
        LIMIT %s
        """
        params = [f"%{query}%"] * len(fields) + [limit]
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, params)
            return cur.fetchall()

    def fetch_torrent_files(self, schema: str, info_hash_text: str, limit: int = 2000) -> List[Dict[str, Any]]:
        sql_text = sql.SQL(
            """
            SELECT index, path, extension, size, updated_at
            FROM {schema}.torrent_files_view
            WHERE info_hash::text = %s
            ORDER BY index
            LIMIT %s
            """
        ).format(schema=sql.Identifier(schema))
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, (info_hash_text, limit))
            return cur.fetchall()

    def search_tmdb_expansions(
        self,
        schema: str,
        query: str,
        limit: int = 20,
    ) -> Dict[str, int]:
        if not query:
            return {}
        sql_text = sql.SQL(
            """
            SELECT aka, keywords
            FROM {schema}.tmdb_enrichment
            WHERE aka ILIKE %s OR keywords ILIKE %s
            LIMIT %s
            """
        ).format(schema=sql.Identifier(schema))
        pattern = f"%{query}%"
        tokens: Dict[str, int] = {}
        splitter = re.compile(r"[，,|/·\\s]+")
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, (pattern, pattern, limit))
            for row in cur.fetchall():
                aka = row.get("aka") or ""
                keywords = row.get("keywords") or ""
                for item in splitter.split(str(aka)):
                    token = item.strip()
                    if token:
                        tokens[token] = max(tokens.get(token, 0), 2)
                for item in splitter.split(str(keywords)):
                    token = item.strip()
                    if token:
                        tokens[token] = max(tokens.get(token, 0), 1)
        return tokens

    def fetch_latest_tmdb(self, schema: str, limit: int = 50) -> List[Dict[str, Any]]:
        sql_text = sql.SQL(
            """
            SELECT
                (c.type || ':' || c.source || ':' || c.id) AS content_uid,
                c.id AS tmdb_id,
                c.title,
                c.original_title,
                c.release_year,
                c.updated_at,
                c.type,
                te.genre,
                te.keywords
            FROM public.content c
            JOIN {schema}.tmdb_enrichment te
                ON te.content_type = c.type
                AND te.tmdb_id = c.id
            WHERE c.source = 'tmdb'
            ORDER BY c.updated_at DESC NULLS LAST
            LIMIT %s
            """
        ).format(schema=sql.Identifier(schema))
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, (limit,))
            return cur.fetchall()

    def fetch_tmdb_detail(
        self,
        schema: str,
        content_type: str,
        tmdb_id: str,
    ) -> Dict[str, Any] | None:
        sql_text = sql.SQL(
            """
            SELECT content_type,
                   tmdb_id,
                   aka,
                   keywords,
                   actors,
                   directors,
                   plot,
                   genre,
                   raw,
                   updated_at
            FROM {schema}.tmdb_enrichment
            WHERE content_type = %s AND tmdb_id = %s
            """
        ).format(schema=sql.Identifier(schema))
        fallback_sql = sql.SQL(
            """
            SELECT content_type,
                   tmdb_id,
                   aka,
                   keywords,
                   actors,
                   directors,
                   plot,
                   genre,
                   raw,
                   updated_at
            FROM {schema}.tmdb_enrichment
            WHERE tmdb_id = %s
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """
        ).format(schema=sql.Identifier(schema))
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql_text, (content_type, tmdb_id))
            row = cur.fetchone()
            if not row:
                cur.execute(fallback_sql, (tmdb_id,))
                row = cur.fetchone()
            return row

    @staticmethod
    def _table_identifier(table: str) -> sql.Identifier:
        if "." in table:
            schema, name = table.rsplit(".", 1)
            return sql.Identifier(schema, name)
        return sql.Identifier(table)
