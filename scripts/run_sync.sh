#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
SOURCE=${SOURCE:-}
TMDB_ENRICH=${TMDB_ENRICH:-true}
TMDB_LIMIT=${TMDB_LIMIT:-500}

if [[ -n "$SOURCE" ]]; then
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH" --source "$SOURCE"
else
  if [[ "$TMDB_ENRICH" == "true" ]]; then
    PYTHONPATH=src python -m cpu.services.tmdb_enrich --config "$CONFIG_PATH" --limit "$TMDB_LIMIT" --loop
  fi
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH"
fi
