#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

read_config() {
  python3 - <<'PY' "$CONFIG_PATH"
import shlex
import sys

try:
    import yaml
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"pyyaml required to parse config: {exc}")

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f) or {}

postgres = cfg.get("postgres", {})
bitmagnet = cfg.get("bitmagnet", {})
vector = cfg.get("vector_store", {})

entries = {
    "HERMES_DSN": postgres.get("dsn", ""),
    "HERMES_SCHEMA": bitmagnet.get("schema", "hermes"),
    "VECTOR_TYPE": vector.get("type", "hnsw"),
    "VECTOR_PATH": vector.get("path", ""),
    "VECTOR_URL": vector.get("url", ""),
    "VECTOR_COLLECTION": vector.get("collection", ""),
}
for key, value in entries.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
}

eval "$(read_config)"

if [[ -z "$HERMES_DSN" ]]; then
  echo "postgres.dsn missing in config: $CONFIG_PATH" >&2
  exit 1
fi

VECTOR_TYPE_LOWER=$(echo "$VECTOR_TYPE" | tr '[:upper:]' '[:lower:]')

cleanup_note=""
if [[ "$VECTOR_TYPE_LOWER" == "hnsw" ]]; then
  cleanup_note="HNSW path: $VECTOR_PATH"
elif [[ "$VECTOR_TYPE_LOWER" == "qdrant" ]]; then
  cleanup_note="Qdrant: $VECTOR_URL collection=$VECTOR_COLLECTION"
else
  cleanup_note="Vector store: $VECTOR_TYPE (manual cleanup may be needed)"
fi

echo "WARNING: This will DROP schema '$HERMES_SCHEMA' and delete vector data."
echo "Config: $CONFIG_PATH"
echo "Database: $HERMES_DSN"
echo "$cleanup_note"
read -r -p "Type 'DROP' to continue: " first
if [[ "$first" != "DROP" ]]; then
  echo "Aborted."
  exit 1
fi
read -r -p "Type the schema name '$HERMES_SCHEMA' to confirm: " second
if [[ "$second" != "$HERMES_SCHEMA" ]]; then
  echo "Aborted."
  exit 1
fi

psql "$HERMES_DSN" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS \"$HERMES_SCHEMA\" CASCADE;"
psql "$HERMES_DSN" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS \"$HERMES_SCHEMA\";"

if [[ "$VECTOR_TYPE_LOWER" == "hnsw" ]]; then
  if [[ -n "$VECTOR_PATH" ]]; then
    rm -rf "$VECTOR_PATH"
    echo "Deleted HNSW vector data at $VECTOR_PATH"
  fi
elif [[ "$VECTOR_TYPE_LOWER" == "qdrant" ]]; then
  if [[ -n "$VECTOR_URL" && -n "$VECTOR_COLLECTION" ]]; then
    base_url=${VECTOR_URL%/}
    curl -sS -X DELETE "$base_url/collections/$VECTOR_COLLECTION" >/dev/null
    echo "Deleted Qdrant collection $VECTOR_COLLECTION"
  fi
else
  echo "Vector store cleanup skipped for type '$VECTOR_TYPE'"
fi

echo "Hermes schema '$HERMES_SCHEMA' reset complete."
