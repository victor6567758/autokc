#!/usr/bin/env bash
# zv-debezium branch comparison: what the feature branch carries relative to
# the release branch (commits, files, changed lines).
#
#   scripts/zv-debezium-track.sh [FEATURE=<branch>] [RELEASE=<branch>] [MAX_DIFF_LINES=<n>]
#
# Defaults: FEATURE = current HEAD of zv-debezium, RELEASE = origin/3.7.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${ZV_DBZ_DIR:-$ROOT/zv-debezium}"
feature="${FEATURE:-}"
release="${RELEASE:-origin/3.7}"

gc() { git -C "$DIR" "$@"; }

gc rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || { echo "ERROR: not a git repo: $DIR" >&2; exit 1; }

if [ -z "$feature" ]; then
  feature=$(gc symbolic-ref --quiet --short HEAD 2>/dev/null) \
    || { echo 'ERROR: zv-debezium is on a detached HEAD - pass FEATURE=<branch>' >&2; exit 1; }
fi
gc rev-parse --verify --quiet "$feature^{commit}" >/dev/null \
  || { echo "ERROR: feature branch not found: $feature" >&2; exit 1; }
gc rev-parse --verify --quiet "$release^{commit}" >/dev/null \
  || { echo "ERROR: release branch not found: $release (try: git -C zv-debezium fetch --prune origin)" >&2; exit 1; }

echo "== zv-debezium: $feature vs $release =="
echo "  $feature: $(gc log -1 --oneline "$feature")"
echo "  $release: $(gc log -1 --oneline "$release")"
echo "  merge base: $(gc rev-parse --short "$(gc merge-base "$feature" "$release")")"
read -r behind ahead < <(gc rev-list --left-right --count "$release...$feature")
echo "  => $feature is $ahead ahead / $behind behind $release"
echo
echo "-- only in $feature ($ahead) --"
gc log --oneline --no-decorate "$release..$feature"
echo
echo "-- only in $release ($behind) --"
gc log --oneline --no-decorate "$feature..$release"
echo
echo "-- files changed on $feature since merge base --"
gc diff --stat "$release...$feature" | tail -n 20
echo
echo '-- changed lines: file / line / change --'
gc diff -U0 --no-color "$release...$feature" | awk -v max="${MAX_DIFF_LINES:-300}" '
  /^diff --git/ { file=$4; sub(/^b\//, "", file); printf "\n%s\n", file; next }
  /^@@ /        { for (i=1; i<=NF; i++) {
                     if ($i ~ /^-[0-9]/) { split(substr($i,2), a, ","); o=a[1] }
                     if ($i ~ /^\+[0-9]/) { split(substr($i,2), b, ","); n=b[1] } }
                   next }
  /^--- / || /^\+\+\+ / { next }
  total >= max { total++; skip=1; next }
  /^-/ { total++; printf "  %6d - %s\n", o, substr($0,2); o++; next }
  /^\+/ { total++; printf "  %6d + %s\n", n, substr($0,2); n++; next }
  END { if (skip) printf "  ... capped at %d changed lines (set MAX_DIFF_LINES=N for more)\n", max }
'
echo "  full patch: git -C zv-debezium diff $release...$feature"
