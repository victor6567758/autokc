#!/usr/bin/env bash
# Print the exposed service URLs - parsed live from the docker-compose
# file(s) by utils/compose_urls.py, so the list stays in sync with the
# compose files instead of a hardcoded table (plain URLs - Ctrl+Click in
# GNOME Terminal).
#
# Usage:
#   print-urls.sh                                      # full stack (default)
#   print-urls.sh development/docker-compose.dev.yml   # dev stack (no zv-monitor)
#   print-urls.sh a.yml b.yml                          # several files, later win
#   print-urls.sh --json ...                           # machine-readable output
# With no args, $COMPOSE_FILE (docker's colon-separated form) is honoured
# when set.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ "$#" -gt 0 ]; then
  exec python3 "$ROOT/utils/compose_urls.py" "$@"
elif [ -n "${COMPOSE_FILE:-}" ]; then
  files=()
  IFS=: read -ra _cf <<< "$COMPOSE_FILE"
  for f in "${_cf[@]}"; do
    if [ -n "$f" ]; then files+=("$f"); fi
  done
  exec python3 "$ROOT/utils/compose_urls.py" "${files[@]}"
else
  exec python3 "$ROOT/utils/compose_urls.py" "$ROOT/development/docker-compose.yml"
fi

