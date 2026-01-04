#!/usr/bin/env bash
set -euo pipefail

MODEL_NAME=${MODEL_NAME:-BAAI/bge-m3}
MAX_TOKEN_LENGTH=${MAX_TOKEN_LENGTH:-256}
BATCH_SIZE=${BATCH_SIZE:-16}
DEVICE=${DEVICE:-cuda:0}
HOST=${HOST:-0.0.0.0}
PORT=${PORT:-8001}

export MODEL_NAME MAX_TOKEN_LENGTH BATCH_SIZE DEVICE

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

PYTHONPATH=src uvicorn gpu_service.main:app --host "$HOST" --port "$PORT"
