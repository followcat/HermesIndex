from typing import Any, Dict, Iterable, List, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

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
        select_cols = [sql.SQL("t.{}").format(sql.Identifier(f)) for f in fields]
        group_by_cols = [sql.SQL("t.{}").format(sql.Identifier(f)) for f in fields]
        join_clauses: List[sql.Composable] = []
        has_agg = False

        for idx, join_cfg in enumerate(joins):
            join_table = join_cfg["table"]
            join_alias = join_cfg.get("alias") or f"j{idx}"
            join_type = str(join_cfg.get("type", "left")).lower()
            if join_type not in {"left", "inner"}:
                raise ValueError(f"Unsupported join type: {join_type}")
            join_fields = join_cfg.get("fields", [])
            join_clause = sql.SQL("{} JOIN {} AS {} ON {}").format(
                sql.SQL(join_type.upper()),
                self._table_identifier(join_table),
                sql.Identifier(join_alias),
                sql.SQL(join_cfg["on"]),
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

        query = sql.SQL("SELECT {selects} FROM {table} AS t {joins} WHERE t.{id_field} IN ({placeholders})").format(
            selects=sql.SQL(", ").join(select_cols),
            table=self._table_identifier(table),
            joins=sql.SQL(" ").join(join_clauses) if join_clauses else sql.SQL(""),
            id_field=sql.Identifier(id_field),
            placeholders=placeholders,
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

    @staticmethod
    def _table_identifier(table: str) -> sql.Identifier:
        if "." in table:
            schema, name = table.rsplit(".", 1)
            return sql.Identifier(schema, name)
        return sql.Identifier(table)
