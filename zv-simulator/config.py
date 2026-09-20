"""Shared config for zv-simulator: deterministic fault injection for the
zv-monitor pipeline.

Every scenario in scenarios.py targets a failure class that zv-monitor
already knows how to detect (see zv-monitor/analysis/loki-log-patterns.yaml
and prometheus-metrics.yaml in the autokc repo). The point isn't just to
break things - it's to close the loop: inject a known fault, then poll
Loki/Prometheus/zv-monitor's own metrics to prove detection actually
fires, and how fast.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Optional overrides live in the autokc repo root .env (template:
# .env.example, same directory). The file is loaded by path relative to this
# module - not the CWD - so the CLI behaves the same regardless of where it's
# invoked from. Precedence:
#   exported env var  >  repo-root .env  >  the defaults below
# (load_dotenv never overwrites variables that are already set in the shell.)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# All defaults assume you're running zv-simulator on the host, against the
# autokc `development/docker-compose.yml` stack brought up with `make up`
# (or `make up-dev`). Override via the repo-root .env or env vars for other
# topologies.

COMPOSE_PROJECT = os.environ.get("ZV_SIM_COMPOSE_PROJECT", "development")

CONNECT_REST_URL = os.environ.get("ZV_SIM_CONNECT_URL", "http://localhost:8083")
PROMETHEUS_URL = os.environ.get("ZV_SIM_PROM_URL", "http://localhost:9090")
LOKI_URL = os.environ.get("ZV_SIM_LOKI_URL", "http://localhost:3100")
TOXIPROXY_URL = os.environ.get("ZV_SIM_TOXIPROXY_URL", "http://localhost:8474")

# Toxiproxy fixture (network-cut-source / network-latency-source): the
# hostname/port the source connector is repointed to during the fault
# window. The simulator sidecar attaches to the stack network with exactly
# this alias, and the optional zv-simulator/docker-compose.override.yml
# service is named so compose DNS resolves the same way.
TOXIPROXY_HOST = os.environ.get("ZV_SIM_TOXIPROXY_HOST", "toxiproxy")
TOXIPROXY_LISTEN_PORT = os.environ.get("ZV_SIM_TOXIPROXY_LISTEN_PORT", "15432")
SIDECAR_IMAGE = os.environ.get("ZV_SIM_TOXIPROXY_IMAGE", "ghcr.io/shopify/toxiproxy:2.9.0")
SIDECAR_NAME = os.environ.get("ZV_SIM_TOXIPROXY_SIDECAR_NAME", "zv-sim-toxiproxy")
SIDECAR_LABEL = os.environ.get("ZV_SIM_TOXIPROXY_SIDECAR_LABEL", "zv-simulator.managed")
TOXIPROXY_COMPOSE_SERVICE = os.environ.get(  # matches zv-simulator/docker-compose.override.yml
    "ZV_SIM_TOXIPROXY_COMPOSE_SERVICE", "toxiproxy"
)

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
# Scratch table for slot-wal-retention-high's bulk WAL generator. Deliberately
# NOT part of the publication - the WAL piles up behind the pinned slot without
# ever being decoded or replicated.
WAL_BLOAT_TABLE = os.environ.get("ZV_SIM_WAL_BLOAT_TABLE", "zv_sim_wal_bloat")
