#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config not found: $CONFIG_PATH" >&2
  exit 1
fi

escape_regex() {
  printf '%s' "$1" | sed -e 's/[.[\*^$(){}+?|/]/\\&/g'
}

find_pids() {
  local module=$1
  local cfg_regex
  cfg_regex=$(escape_regex "$CONFIG_PATH")
  pgrep -f "python.*-m[[:space:]]+$module.*--config[[:space:]]+$cfg_regex" || true
}

pids=()
while read -r pid; do
  [[ -n "$pid" ]] && pids+=("$pid")
done < <(find_pids "cpu.services.tmdb_enrich")

while read -r pid; do
  [[ -n "$pid" ]] && pids+=("$pid")
done < <(find_pids "cpu.services.sync_runner")

if [[ ${#pids[@]} -eq 0 ]]; then
  echo "No matching sync processes found for config: $CONFIG_PATH"
  exit 0
fi

echo "Stopping processes: ${pids[*]}"
for pid in "${pids[@]}"; do
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
done

for _ in {1..10}; do
  still_running=()
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      still_running+=("$pid")
    fi
  done
  if [[ ${#still_running[@]} -eq 0 ]]; then
    echo "All sync processes stopped."
    exit 0
  fi
  sleep 1
done

echo "Force killing remaining processes: ${still_running[*]}"
for pid in "${still_running[@]}"; do
  kill -9 "$pid" 2>/dev/null || true
done
