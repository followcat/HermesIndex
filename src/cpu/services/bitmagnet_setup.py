import argparse
import logging
from typing import Any, Dict

import psycopg
from psycopg import sql

from cpu.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_dsn(cfg: Dict[str, Any]) -> str:
    dsn = cfg.get("dsn")
    if dsn:
        return dsn
    host = cfg.get("host")
    port = cfg.get("port", 5432)
    database = cfg.get("database") or cfg.get("db")
    user = cfg.get("user")
    password = cfg.get("password")
    if not all([host, database, user, password]):
        raise ValueError("bitmagnet config missing dsn or host/database/user/password")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def ensure_schema(conn: psycopg.Connection, schema: str, create_schema: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        exists = cur.fetchone()
        if exists:
            logger.info("Schema exists: %s", schema)
            return
        if not create_schema:
            raise ValueError(f"Schema {schema} does not exist and create_schema=false")
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema)))


def create_torrent_files_view(conn: psycopg.Connection, schema: str) -> None:
    view_sql = sql.SQL(
        """
        CREATE OR REPLACE VIEW {schema}.torrent_files_view AS
        SELECT
            (encode(info_hash, 'hex') || ':' || index::text) AS file_id,
            info_hash,
            index,
            path,
            extension,
            size,
            created_at,
            updated_at
        FROM public.torrent_files
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(view_sql)


def create_content_view(conn: psycopg.Connection, schema: str) -> None:
    view_sql = sql.SQL(
        """
        CREATE OR REPLACE VIEW {schema}.content_view AS
        SELECT
            (c.type || ':' || c.source || ':' || c.id) AS content_uid,
            c.type,
            c.source,
            c.id,
            c.title,
            c.original_title,
            c.overview,
            c.adult,
            c.release_year,
            c.updated_at,
            CASE WHEN c.source = 'tmdb' THEN c.id ELSE NULL END AS tmdb_id,
            te.genre AS genre,
            te.keywords AS keywords,
            trim(both ' ' from concat_ws(' ',
                c.title,
                c.original_title,
                c.overview,
                c.release_year::text,
                string_agg(DISTINCT cc.name, ' ') FILTER (WHERE cc.name IS NOT NULL),
                CASE WHEN c.source = 'tmdb' THEN c.id ELSE NULL END,
                te.aka,
                te.keywords,
                te.actors,
                te.directors,
                te.plot,
                te.genre
            )) AS search_text,
            te.aka AS aka,
            te.actors AS actors,
            te.directors AS directors,
            te.plot AS plot,
            te.raw->>'poster_path' AS poster_path,
            te.raw->>'backdrop_path' AS backdrop_path
        FROM public.content c
        LEFT JOIN public.content_collections_content ccc
            ON ccc.content_type = c.type
            AND ccc.content_source = c.source
            AND ccc.content_id = c.id
        LEFT JOIN public.content_collections cc
            ON cc.type = ccc.content_collection_type
            AND cc.source = ccc.content_collection_source
            AND cc.id = ccc.content_collection_id
        LEFT JOIN {schema}.tmdb_enrichment te
            ON te.content_type = c.type
            AND te.tmdb_id = c.id
            AND c.source = 'tmdb'
        GROUP BY
            c.type,
            c.source,
            c.id,
            c.title,
            c.original_title,
            c.overview,
            c.adult,
            c.release_year,
            c.updated_at,
            te.genre,
            te.keywords,
            te.aka,
            te.actors,
            te.directors,
            te.plot,
            te.raw->>'poster_path',
            te.raw->>'backdrop_path'
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(view_sql)


def ensure_tmdb_table(conn: psycopg.Connection, schema: str) -> None:
    table_sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {schema}.tmdb_enrichment (
            content_type TEXT NOT NULL,
            tmdb_id TEXT NOT NULL,
            aka TEXT,
            keywords TEXT,
            actors TEXT,
            directors TEXT,
            plot TEXT,
            genre TEXT,
            raw JSONB,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (content_type, tmdb_id)
        );
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(table_sql)


def setup_bitmagnet(config_path: str) -> None:
    cfg = load_config(config_path)
    bm_cfg = cfg.bitmagnet or {}
    if not bm_cfg.get("enabled"):
        logger.info("bitmagnet plugin disabled in config")
        return
    schema = bm_cfg.get("schema", "hermes")
    create_schema = bool(bm_cfg.get("create_schema", True))
    dsn = build_dsn(bm_cfg)
    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_schema(conn, schema, create_schema)
        ensure_tmdb_table(conn, schema)
        create_torrent_files_view(conn, schema)
        create_content_view(conn, schema)
    logger.info("bitmagnet views created in schema=%s", schema)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bitmagnet plugin setup")
    parser.add_argument("--config", default="configs/example.yaml")
    args = parser.parse_args()
    setup_bitmagnet(args.config)


if __name__ == "__main__":
    main()
