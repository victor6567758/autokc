#!/usr/bin/env bash
# Print the exposed service URLs (plain URLs - Ctrl+Click in GNOME Terminal).
set -euo pipefail

printf '\n== Services (Ctrl+Click the URLs in GNOME Terminal) ==\n\n'
printf '  %-18s %-30s %s\n' 'Kafka UI'        'http://localhost:8080'         'no login, cluster "zv-monitor"'
printf '  %-18s %-30s %s\n' 'pgweb (Postgres)' 'http://localhost:8081'         'pick the sourcedb/sinkdb bookmark'
printf '  %-18s %-30s %s\n' 'Grafana'          'http://localhost:3000'         'admin/admin (anonymous viewer ok)'
printf '  %-18s %-30s %s\n' 'Prometheus'       'http://localhost:9090'         'no login, targets at /targets'
printf '  %-18s %-30s %s\n' 'Kafka Connect'    'http://localhost:8083'         'REST /connectors, metrics localhost:5557'
printf '  %-18s %-30s %s\n' 'zv-monitor'       'http://localhost:5558/metrics' 'container (make up) / local run (make up-dev)'
printf '  %-18s %-30s %s\n' 'Kafka broker'     'localhost:9092'                'metrics localhost:5559/metrics'
printf '  %-18s %-30s %s\n' 'PostgreSQL src'   'localhost:5432'                'sourcedb, postgres/postgres'
printf '  %-18s %-30s %s\n' 'PostgreSQL sink'  'localhost:5433'                'sinkdb, postgres/postgres'
printf '\n'
