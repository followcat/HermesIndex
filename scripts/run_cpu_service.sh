#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8000}

export CONFIG_PATH

append_no_proxy() {
  local host=$1
  local current=${NO_PROXY:-${no_proxy:-}}
  if [[ -z "$current" ]]; then
    export NO_PROXY="$host"
    export no_proxy="$host"
    return
  fi
  if [[ ",$current," != *",$host,"* ]]; then
    export NO_PROXY="${current},${host}"
    export no_proxy="${NO_PROXY}"
  fi
}
append_no_proxy "127.0.0.1"
append_no_proxy "localhost"
append_no_proxy "::1"

PYTHONPATH=src uvicorn cpu.api.search:app --host "$HOST" --port "$PORT"
