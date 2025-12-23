#!/usr/bin/env bash
set -euo pipefail

command=${1:-}
shift || true

case "$command" in
  gpu-multi)
    scripts/run_gpu_multi.sh "$@"
    ;;
  gpu-single)
    scripts/run_gpu_single.sh "$@"
    ;;
  cpu)
    scripts/run_cpu_service.sh "$@"
    ;;
  sync)
    scripts/run_sync.sh "$@"
    ;;
  sync-all)
    scripts/run_sync_all.sh "$@"
    ;;
  purge)
    scripts/purge_hermes_data.sh "$@"
    ;;
  *)
    echo "Usage: scripts/entry.sh {gpu-multi|gpu-single|cpu|sync|sync-all|purge}" >&2
    exit 1
    ;;
esac
