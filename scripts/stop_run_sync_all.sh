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
  python - "$PID_DIR" "$CONFIG_PATH" <<'PY'
import hashlib
import os
import sys

pid_dir = sys.argv[1]
cfg_path = os.path.abspath(sys.argv[2])
key = cfg_path.encode("utf-8")
name = f"run_sync_all.{hashlib.sha1(key).hexdigest()}.pid"
print(os.path.join(pid_dir, name))
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

dedupe_add_pid() {
  local pid=$1
  [[ -z "$pid" ]] && return
  [[ "$pid" =~ ^[0-9]+$ ]] || return
  if [[ -z "${SEEN_PIDS[$pid]:-}" ]]; then
    SEEN_PIDS[$pid]=1
    pids+=("$pid")
  fi
}

declare -A SEEN_PIDS=()
pids=()
pid_file=$(pidfile_path)
stopped_any=0
if [[ -f "$pid_file" ]]; then
  echo "Using PID file: $pid_file"
  # shellcheck disable=SC1090
  source "$pid_file" || true
  if [[ -n "${RUN_SYNC_ALL_PID:-}" ]]; then
    stop_pid "${RUN_SYNC_ALL_PID:-}"
    stopped_any=1
  fi
  if [[ -n "${TMDB_PID:-}" ]]; then
    stop_pid "${TMDB_PID:-}"
    stopped_any=1
  fi
  if [[ -n "${TPDB_PID:-}" ]]; then
    stop_pid "${TPDB_PID:-}"
    stopped_any=1
  fi
  if [[ -n "${SYNC_PIDS:-}" ]]; then
    for pid in ${SYNC_PIDS}; do
      stop_pid "$pid"
      stopped_any=1
    done
  fi
  rm -f "$pid_file" 2>/dev/null || true
fi
while read -r pid; do
  dedupe_add_pid "$pid"
done < <(find_pids "cpu.services.tmdb_enrich")

while read -r pid; do
  dedupe_add_pid "$pid"
done < <(find_pids "cpu.services.tpdb_enrich")

while read -r pid; do
  dedupe_add_pid "$pid"
done < <(find_pids "cpu.services.sync_runner")

if [[ ${#pids[@]} -eq 0 ]]; then
  if [[ $stopped_any -ne 0 ]]; then
    echo "Stopped run_sync_all via PID file."
    exit 0
  else
    echo "No matching sync processes found for config: $CONFIG_PATH"
    exit 0
  fi
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
