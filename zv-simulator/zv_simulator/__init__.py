"""zv-simulator: deterministic fault injection for the zv-monitor pipeline.

Every scenario in zv_simulator.scenarios targets a failure class that
zv-monitor already knows how to detect (see zv-monitor/analysis/
loki-log-patterns.yaml and prometheus-metrics.yaml in the autokc repo).
The point isn't just to break things - it's to close the loop: inject a
known fault, then poll Loki/Prometheus/zv-monitor's own metrics to prove
detection actually fires, and how fast.
"""

import os

# All defaults assume you're running zv-simulator on the host, against the
# autokc `development/docker-compose.yml` stack brought up with `make up`
# (or `make up-dev`). Override via env vars for other topologies.

COMPOSE_PROJECT = os.environ.get("ZV_SIM_COMPOSE_PROJECT", "development")

CONNECT_REST_URL = os.environ.get("ZV_SIM_CONNECT_URL", "http://localhost:8083")
PROMETHEUS_URL = os.environ.get("ZV_SIM_PROM_URL", "http://localhost:9090")
LOKI_URL = os.environ.get("ZV_SIM_LOKI_URL", "http://localhost:3100")
TOXIPROXY_URL = os.environ.get("ZV_SIM_TOXIPROXY_URL", "http://localhost:8474")

PG_SOURCE = dict(
    host=os.environ.get("ZV_SIM_PG_SOURCE_HOST", "localhost"),
    port=int(os.environ.get("ZV_SIM_PG_SOURCE_PORT", "5432")),
    dbname="sourcedb",
    user="postgres",
    password="postgres",
)
PG_SINK = dict(
    host=os.environ.get("ZV_SIM_PG_SINK_HOST", "localhost"),
    port=int(os.environ.get("ZV_SIM_PG_SINK_PORT", "5433")),
    dbname="sinkdb",
    user="postgres",
    password="postgres",
)

# Container names as they appear via `docker ps` for the compose project
# (compose default naming: <project>-<service>-<n>; docker_ctl resolves by
# the com.docker.compose.service label so exact suffixing doesn't matter).
SERVICE_KAFKA = "kafka"
SERVICE_KAFKA_CONNECT = "kafka-connect"
SERVICE_PG_SOURCE = "postgres-source"
SERVICE_PG_SINK = "postgres-sink"

SOURCE_CONNECTOR = os.environ.get("ZV_SIM_SOURCE_CONNECTOR", "inventory-source")
SINK_CONNECTOR = os.environ.get("ZV_SIM_SINK_CONNECTOR", "customers-sink")
REPLICATION_SLOT = os.environ.get("ZV_SIM_SLOT_NAME", "dbz_customers_slot")
PUBLICATION_NAME = os.environ.get("ZV_SIM_PUBLICATION", "dbz_publication")
