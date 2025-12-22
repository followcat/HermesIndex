#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH=${CONFIG_PATH:-configs/bitmagnet.yaml}
SOURCE=${SOURCE:-}

if [[ -n "$SOURCE" ]]; then
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH" --source "$SOURCE"
else
  PYTHONPATH=src python -m cpu.services.sync_runner --config "$CONFIG_PATH"
fi
