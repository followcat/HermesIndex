#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
PID_DIR=${PID_DIR:-data/pids}

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

pidfile_path() {
  python - "$CONFIG_PATH" <<'PY'
import hashlib
import sys

path = sys.argv[1].encode("utf-8")
print(f"data/pids/run_sync_all.{hashlib.sha1(path).hexdigest()}.pid")
PY
}

stop_pid() {
  local pid=$1
  if [[ -z "$pid" ]]; then
    return
  fi
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
}

pids=()
pid_file=$(pidfile_path)
if [[ -f "$pid_file" ]]; then
  # shellcheck disable=SC1090
  source "$pid_file" || true
  stop_pid "${RUN_SYNC_ALL_PID:-}"
  stop_pid "${TMDB_PID:-}"
  stop_pid "${TPDB_PID:-}"
fi
while read -r pid; do
  [[ -n "$pid" ]] && pids+=("$pid")
done < <(find_pids "cpu.services.tmdb_enrich")

while read -r pid; do
  [[ -n "$pid" ]] && pids+=("$pid")
done < <(find_pids "cpu.services.tpdb_enrich")

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
