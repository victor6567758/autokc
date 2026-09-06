#!/usr/bin/env bash
# Start the DEV pipeline: build the connector plugin distributions if missing,
# build the images, start the dev stack (docker-compose.dev.yml - no
# zv-monitor container), register the connectors and print the service URLs
# plus how to run zv-monitor locally.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose -f development/docker-compose.dev.yml"

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

echo '== Building images (1/2: connector plugin packagers) =='
$COMPOSE build zv-debezium-connector-postgres zv-debezium-connector-jdbc
echo '== Building images (2/2: kafka broker, kafka-connect) =='
$COMPOSE build kafka kafka-connect
# no zv-monitor image build here - in this stack it runs on your host
echo '== Starting dev stack (no zv-monitor container) =='
$COMPOSE up -d
echo '== Registering connectors =='
if ! "$SCRIPT_DIR/register-connectors.sh"; then
  echo >&2
  echo 'Connectors did not come up. Inspect:' >&2
  echo "  $COMPOSE logs kafka-connect" >&2
  echo '  curl -s localhost:8083/connectors?expand=status | jq .' >&2
fi
echo
echo '== Run zv-monitor locally (from the repo root) =='
echo '  CONNECT_REST_URL=http://localhost:8083 \'
echo '  CONNECTOR_NAMES=inventory-source,customers-sink \'
echo '  java -javaagent:lib/jmx_prometheus_javaagent-0.20.0.jar=5558:development/monitoring/jmx-exporter/zv-monitor-jmx.yml \'
echo '       -jar zv-monitor/target/zv-monitor.jar'
echo
echo '(jar version matches version.jmx-prometheus-javaagent in the parent pom;'
echo ' build it with: mvn -q clean package -pl zv-monitor -am)'
echo
echo 'Metrics:  http://localhost:5558/metrics'
echo 'Grafana:  http://localhost:3000 - picks the local instance up via'
echo '          Prometheus (host.docker.internal:5558) within one scrape.'
"$SCRIPT_DIR/print-urls.sh"
