#!/usr/bin/env bash
set -euo pipefail

DSN=${HERMES_DSN:-}
SCHEMA=${HERMES_SCHEMA:-hermes}

if [[ -z "$DSN" ]]; then
  echo "HERMES_DSN is required (e.g. postgresql://user:pass@host:5432/db)" >&2
  exit 1
fi

echo "WARNING: This will DROP schema '$SCHEMA' and all data inside it."
echo "Database: $DSN"
read -r -p "Type 'DROP' to continue: " first
if [[ "$first" != "DROP" ]]; then
  echo "Aborted."
  exit 1
fi
read -r -p "Type the schema name '$SCHEMA' to confirm: " second
if [[ "$second" != "$SCHEMA" ]]; then
  echo "Aborted."
  exit 1
fi

psql "$DSN" -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS \"$SCHEMA\" CASCADE;"
psql "$DSN" -v ON_ERROR_STOP=1 -c "CREATE SCHEMA IF NOT EXISTS \"$SCHEMA\";"

echo "Schema '$SCHEMA' dropped and recreated."
