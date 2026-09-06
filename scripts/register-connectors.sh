#!/usr/bin/env bash
# (Re-)register every connector config (*.json) from development/kafka-connect/
# via the Kafka Connect REST API and wait until connector + tasks are RUNNING.
#   scripts/register-connectors.sh [CONNECT_URL=http://localhost:8083]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
KCCFG="${KCCFG:-development/kafka-connect}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

command -v jq >/dev/null || { echo "ERROR: 'jq' is required" >&2; exit 1; }
shopt -s nullglob
configs=("$KCCFG"/*.json)
if [ ${#configs[@]} -eq 0 ]; then
  echo "ERROR: no connector configs (*.json) found in $KCCFG" >&2
  exit 1
fi
for cfg in "${configs[@]}"; do
  jq -e 'type == "object"' "$cfg" >/dev/null \
    || { echo "ERROR: $cfg is not a JSON object" >&2; exit 1; }
done

echo "Waiting for the Kafka Connect REST API at $CONNECT_URL ..."
for _ in $(seq 1 120); do
  curl -sf "$CONNECT_URL/" >/dev/null && break
  sleep 2
done
curl -sf "$CONNECT_URL/" >/dev/null \
  || { echo "Kafka Connect is not reachable at $CONNECT_URL" >&2; exit 1; }

names=()
for cfg in "${configs[@]}"; do
  name=$(basename "$cfg" .json)
  names+=("$name")
  echo "Registering/updating connector '$name' from $cfg ..."
  ok=0
  for attempt in 1 2 3; do
    if curl -sf -X PUT -H 'Content-Type: application/json' -d @"$cfg" \
        "$CONNECT_URL/connectors/$name/config" >/dev/null; then
      ok=1
      break
    fi
    echo "  attempt $attempt failed (worker may be rebalancing), retrying in 5s ..." >&2
    sleep 5
  done
  [ "$ok" -eq 1 ] || { echo "ERROR: could not register connector '$name'" >&2; exit 1; }
done

echo 'Waiting for connectors to reach RUNNING ...'
for _ in $(seq 1 90); do
  states=''
  running=1
  for name in "${names[@]}"; do
    state=$(curl -sf "$CONNECT_URL/connectors/$name/status" 2>/dev/null \
      | jq -r '[(.connector.state)] + [(.tasks // [])[] | .state] | unique | join(",")' \
      || echo MISSING)
    states+="$name=$state "
    [ "$state" = RUNNING ] || running=0
  done
  if [ "$running" -eq 1 ]; then
    echo "All connectors are RUNNING: ${names[*]}"
    exit 0
  fi
  sleep 2
done
echo "ERROR: connectors did not reach RUNNING within ~3 minutes ($states)." >&2
echo "Inspect: curl -s $CONNECT_URL/connectors?expand=status | jq ." >&2
exit 1
