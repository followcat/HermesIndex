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


def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
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
            (type || ':' || source || ':' || id) AS content_uid,
            type,
            source,
            id,
            title,
            original_title,
            overview,
            adult,
            release_year,
            updated_at
        FROM public.content
        """
    ).format(schema=sql.Identifier(schema))
    with conn.cursor() as cur:
        cur.execute(view_sql)


def setup_bitmagnet(config_path: str) -> None:
    cfg = load_config(config_path)
    bm_cfg = cfg.bitmagnet or {}
    if not bm_cfg.get("enabled"):
        logger.info("bitmagnet plugin disabled in config")
        return
    schema = bm_cfg.get("schema", "hermes")
    dsn = build_dsn(bm_cfg)
    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_schema(conn, schema)
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
