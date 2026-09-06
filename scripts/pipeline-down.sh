#!/usr/bin/env bash
# Stop the pipeline (both stack variants share the same compose project).
# Pass -v to also wipe the volumes: scripts/pipeline-down.sh -v
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo '== Stopping pipeline =='
docker compose -f development/docker-compose.yml down "$@"
echo 'Stopped. Start it again with: make up (or make up-dev)'
