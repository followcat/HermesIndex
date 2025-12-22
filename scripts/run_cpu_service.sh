#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8000}

export CONFIG_PATH

PYTHONPATH=src uvicorn cpu.api.search:app --host "$HOST" --port "$PORT"
