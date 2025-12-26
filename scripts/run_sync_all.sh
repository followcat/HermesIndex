#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
TMDB_LIMIT=${TMDB_LIMIT:-500}
SYNC_TARGET=${SYNC_TARGET:-}
SYNC_SOURCES=${SYNC_SOURCES:-bitmagnet_torrents,bitmagnet_torrent_files,bitmagnet_content}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

echo "Config: $CONFIG_PATH"
echo "TMDB enrich: loop mode, limit=$TMDB_LIMIT"
echo "Sync target: ${SYNC_TARGET:-all}"
echo "Sync sources: ${SYNC_SOURCES}"

get_dsn() {
  local path="$1"
  python - "$path" <<'PY'
import sys

def find_postgres_dsn(path):
    in_postgres = False
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped.startswith("#") or not stripped:
                continue
            if not line.startswith(" ") and stripped.startswith("postgres:"):
                in_postgres = True
                continue
            if in_postgres:
                if not line.startswith(" "):
                    break
                if stripped.startswith("dsn:"):
                    value = stripped.split(":", 1)[1].strip().strip("\"")
                    return value
    return ""

print(find_postgres_dsn(sys.argv[1]) or "")
PY
}

ensure_bitmagnet_views() {
  if ! command -v psql >/dev/null 2>&1; then
    echo "psql not found; skip auto view check" >&2
    return
  fi
  local dsn
  dsn=$(get_dsn "$CONFIG_PATH")
  if [[ -z "$dsn" ]]; then
    echo "postgres.dsn missing in config; skip auto view check" >&2
    return
  fi
  local files_view
  local content_view
  files_view=$(psql "$dsn" -tAc "SELECT to_regclass('hermes.torrent_files_view')")
  content_view=$(psql "$dsn" -tAc "SELECT to_regclass('hermes.content_view')")
  if [[ -z "$files_view" || -z "$content_view" ]]; then
    echo "Bitmagnet views missing; running setup"
    PYTHONPATH=src python -m cpu.services.bitmagnet_setup --config "$CONFIG_PATH"
  fi
}
ensure_bitmagnet_views

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
  IFS=',' read -r -a source_list <<< "$SYNC_SOURCES"
  for source in "${source_list[@]}"; do
    source=$(echo "$source" | xargs)
    if [[ -z "$source" ]]; then
      continue
    fi
    PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH" --source "$source" &
  done
  wait
fi
