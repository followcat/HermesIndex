"""Celery app for scheduling incremental syncs.

Usage:
  CONFIG_PATH=configs/example.yaml celery -A cpu.services.celery_app worker --loglevel=INFO
  CONFIG_PATH=configs/example.yaml celery -A cpu.services.celery_app beat --loglevel=INFO

Notes:
  - Default beat schedule uses config.celery.schedule_seconds; adjust per source if needed.
  - For file-based HNSW index, prefer single worker/concurrency=1 to avoid concurrent writes.
"""
import os
from datetime import timedelta

from celery import Celery

from cpu.config import load_config
from cpu.services.sync_runner import run_sync

CONFIG_PATH = os.getenv("CONFIG_PATH", "configs/example.yaml")
cfg = load_config(CONFIG_PATH)

celery_app = Celery(
    "hermes_sync",
    broker=cfg.celery.get("broker_url", "redis://localhost:6379/0"),
    backend=cfg.celery.get("backend_url", cfg.celery.get("broker_url", "redis://localhost:6379/0")),
)


@celery_app.task
def sync_all_sources() -> str:
    run_sync(CONFIG_PATH)
    return "ok"


@celery_app.task
def sync_source(source_name: str) -> str:
    run_sync(CONFIG_PATH, source_name)
    return f"ok:{source_name}"


schedule_seconds = int(cfg.celery.get("schedule_seconds", 0) or 0)
if schedule_seconds > 0:
    celery_app.conf.beat_schedule = {
        "hermes-sync-all": {
            "task": "cpu.services.celery_app.sync_all_sources",
            "schedule": timedelta(seconds=schedule_seconds),
        }
    }
