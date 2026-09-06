#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UTILS_DIR="$(cd "${SCRIPT_DIR}/../utils" && pwd)"

echo $UTILS_DIR
for tool in python3; do
  command -v "${tool}" > /dev/null || { echo "ERROR: '${tool}' is required" >&2; exit 1; }
done

exec python3 "${UTILS_DIR}/metrics-diff.py" "$@"
