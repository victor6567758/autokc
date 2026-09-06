#!/usr/bin/env bash
# Start the FULL pipeline in docker: build the connector plugin distributions
# if missing, build the images, start the stack (docker-compose.yml, incl. the
# zv-monitor container), register the connectors and print the service URLs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose -f development/docker-compose.yml"

for tool in docker jq; do
  command -v "$tool" >/dev/null || { echo "ERROR: '$tool' is required" >&2; exit 1; }
done

missing=0
for module in zv-debezium-connector-postgres zv-debezium-connector-jdbc; do
  ls "${module}/target/${module}-[0-9]*.tar.gz" >/dev/null 2>&1 || missing=1
done
if [ "$missing" -ne 0 ]; then
  echo '== Connector plugin distributions missing - building everything =='
  mvn -q clean install -Passembly
fi

echo '== Building images (1/3: connector plugin packagers) =='
$COMPOSE build zv-debezium-connector-postgres zv-debezium-connector-jdbc
echo '== Building images (2/3: kafka broker, kafka-connect) =='
$COMPOSE build kafka kafka-connect
echo '== Building images (3/3: zv-monitor) =='
$COMPOSE build zv-monitor
echo '== Starting stack =='
$COMPOSE up -d
echo '== Registering connectors =='
if ! "$SCRIPT_DIR/register-connectors.sh"; then
  echo >&2
  echo 'Connectors did not come up. Inspect:' >&2
  echo "  $COMPOSE logs kafka-connect" >&2
  echo '  curl -s localhost:8083/connectors?expand=status | jq .' >&2
fi
"$SCRIPT_DIR/print-urls.sh"
