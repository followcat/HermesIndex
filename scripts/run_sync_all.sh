#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
TMDB_LIMIT=${TMDB_LIMIT:-500}
SYNC_TARGET=${SYNC_TARGET:-}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

echo "Config: $CONFIG_PATH"
echo "TMDB enrich: loop mode, limit=$TMDB_LIMIT"
echo "Sync target: ${SYNC_TARGET:-all}"

cleanup() {
  if [[ -n "${TMDB_PID:-}" ]] && kill -0 "$TMDB_PID" 2>/dev/null; then
    kill "$TMDB_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

PYTHONPATH=src python -m cpu.services.tmdb_enrich --config "$CONFIG_PATH" --limit "$TMDB_LIMIT" --loop &
TMDB_PID=$!

if [[ -n "$SYNC_TARGET" ]]; then
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH" --source "$SYNC_TARGET"
else
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH"
fi
