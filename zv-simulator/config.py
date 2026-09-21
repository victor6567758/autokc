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
TOXIPROXY_TIMEOUT = float(os.environ.get("ZV_SIM_TOXIPROXY_TIMEOUT", "5.0"))
TOXIPROXY_CONTROL_API_UP_TIMEOUT = float(os.environ.get("ZV_SIM_TOXIPROXY_CONTROL_API_TIMEOUT", "2.0"))
TOXIPROXY_WAIT_CONTROL_API_TIMEOUT = float(os.environ.get("ZV_SIM_TOXIPROXY_WAIT_CONTROL_API_TIMEOUT", "20.0"))

# Toxiproxy fixture (network-cut-source / network-latency-source): the
# hostname/port the source connector is repointed to during the fault
# window. The simulator sidecar attaches to the stack network with exactly
# this alias, and the optional zv-simulator/docker-compose.override.yml
# service is named so compose DNS resolves the same way.
TOXIPROXY_HOST = os.environ.get("ZV_SIM_TOXIPROXY_HOST", "toxiproxy")
TOXIPROXY_LISTEN_PORT = os.environ.get("ZV_SIM_TOXIPROXY_LISTEN_PORT", "15432")

TOXIPROXY_PG_SOURCE_PROXY_NAME = os.environ.get("ZV_SIM_TOXIPROXY_PG_SOURCE_NAME", "pg-source")
TOXIPROXY_PG_SOURCE_LISTEN = os.environ.get("ZV_SIM_TOXIPROXY_LISTEN", f"0.0.0.0:{TOXIPROXY_LISTEN_PORT}")
TOXIPROXY_PG_SOURCE_UPSTREAM = os.environ.get("ZV_SIM_TOXIPROXY_UPSTREAM", "postgres-source:5432")
TOXIPROXY_ADD_LATENCY_MS = int(os.environ.get("ZV_SIM_TOXIPROXY_LATENCY_MS", "3000"))
TOXIPROXY_ADD_LATENCY_JITTER_MS = int(os.environ.get("ZV_SIM_TOXIPROXY_JITTER_MS", "500"))
TOXIPROXY_ADD_TIMEOUT_MS = int(os.environ.get("ZV_SIM_TOXIPROXY_TIMEOUT_MS", "30000"))
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
CUT_MSG_LENGTH = 400

# REST client timeouts and retry policies
CONNECT_REST_TIMEOUT = 5.0  # per-request budget - fail fast, never hang a step
CONNECT_REST_RESTART_INCLUDE_TASKS = True
RETRIES_RESTART_CONNECTOR = 6
BACKOFF_RESTART_CONNECTOR = 5.0

RETRIES_DELETE_OFFSETS = 3
BACKOFF_DELETE_OFFSETS = 5.0

FLAP_INTERVAL = 2.0

WAIT_RUNNING_TIMEOUT = 90.0
WAIT_RUNNING_POLL = 2.0

WAIT_STOPPED_TIMEOUT = 30.0
WAIT_STOPPED_POLL = 1.0

# Verifier (verifier.py): uniformly scale every expectation's timeout_s -
# e.g. ZV_SIM_EXPECT_TIMEOUT_SCALE=0.5 halves all budgets for a quick pass.
EXPECT_TIMEOUT_SCALE = float(os.environ.get("ZV_SIM_EXPECT_TIMEOUT_SCALE", "1.0"))

SOURCE_CONNECTOR = os.environ.get("ZV_SIM_SOURCE_CONNECTOR", "inventory-source")
SINK_CONNECTOR = os.environ.get("ZV_SIM_SINK_CONNECTOR", "customers-sink")
REPLICATION_SLOT = os.environ.get("ZV_SIM_SLOT_NAME", "dbz_customers_slot")
PUBLICATION_NAME = os.environ.get("ZV_SIM_PUBLICATION", "dbz_publication")
PUBLICATION_TABLES = os.environ.get("ZV_SIM_PUBLICATION_TABLES", "public.customers, public.orders")
# Scratch table for slot-wal-retention-high's bulk WAL generator. Deliberately
# NOT part of the publication - the WAL piles up behind the pinned slot without
# ever being decoded or replicated.
WAL_BLOAT_TABLE = os.environ.get("ZV_SIM_WAL_BLOAT_TABLE", "zv_sim_wal_bloat")

# PgFaults defaults
PG_REPLICATION_SLOT_PLUGIN = os.environ.get("ZV_SIM_SLOT_PLUGIN", "pgoutput")
PG_WAIT_SLOT_ACTIVE_TIMEOUT_S = float(os.environ.get("ZV_SIM_WAIT_SLOT_ACTIVE_TIMEOUT", "120.0"))
PG_TERMINATE_BACKEND_ROLE = os.environ.get("ZV_SIM_TERMINATE_BACKEND_ROLE", "postgres")
PG_HOLD_LONG_TRANSACTION_SECONDS = int(os.environ.get("ZV_SIM_HOLD_TXN_SECONDS", "180"))
PG_BULK_GENERATE_WAL_SECONDS = int(os.environ.get("ZV_SIM_BULK_WAL_SECONDS", "300"))
PG_BULK_GENERATE_WAL_TARGET_BYTES = int(os.environ.get("ZV_SIM_BULK_WAL_TARGET_BYTES", str(2 * 1024**3)))
PG_CONNECT_AUTOCOMMIT = os.environ.get("ZV_SIM_PG_CONNECT_AUTOCOMMIT", "true").lower() == "true"
# psycopg2/libpq have NO default connect timeout: an unreachable postgres
# (stack down, paused container, leftover proxy cut from an aborted run)
# otherwise blocks setup/gate/cleanup for the OS TCP timeout (~2min+) with
# zero output - which reads as "the simulator hangs". Seconds; covers
# connection establishment only, never query runtime (hold_long_transaction
# and bulk_generate_wal deliberately run long statements).
PG_CONNECT_TIMEOUT_S = float(os.environ.get("ZV_SIM_PG_CONNECT_TIMEOUT", "5.0"))

# DockerCtl defaults
DOCKER_KILL_SIGNAL = os.environ.get("ZV_SIM_DOCKER_KILL_SIGNAL", "SIGKILL")
DOCKER_RESTART_TIMEOUT = int(os.environ.get("ZV_SIM_DOCKER_RESTART_TIMEOUT", "5"))
DOCKER_RESTART_LOOP_INTERVAL_S = float(os.environ.get("ZV_SIM_DOCKER_RESTART_LOOP_INTERVAL", "3.0"))
DOCKER_WAIT_RUNNING_TIMEOUT_S = float(os.environ.get("ZV_SIM_DOCKER_WAIT_RUNNING_TIMEOUT", "60.0"))
DOCKER_EXEC_RUN_USER = os.environ.get("ZV_SIM_DOCKER_EXEC_RUN_USER")
# Client-side ceiling for every docker API call (docker-py `timeout=`):
# bounds kill/restart/exec even against a wedged daemon or a container this
# very toolchain paused (pause() exists to freeze services - sockets stay
# open and nothing responds, exactly the case an unbounded API call hangs
# on). Generous enough for a first-time sidecar image pull; env-overridable.
DOCKER_API_TIMEOUT_S = float(os.environ.get("ZV_SIM_DOCKER_API_TIMEOUT", "30.0"))

# Simulator/Context defaults
RESET_SOURCE_DROP_SLOT = os.environ.get("ZV_SIM_RESET_SOURCE_DROP_SLOT", "true").lower() == "true"
RESET_SOURCE_TIMEOUT_S = float(os.environ.get("ZV_SIM_RESET_SOURCE_TIMEOUT", "300.0"))
WAIT_SLOT_ACTIVE_TIMEOUT_S = float(os.environ.get("ZV_SIM_WAIT_SLOT_ACTIVE_TIMEOUT", "300.0"))
WAIT_SLOT_ACTIVE_INTERVAL_S = float(os.environ.get("ZV_SIM_WAIT_SLOT_ACTIVE_INTERVAL", "3.0"))
WAIT_PIPELINE_HEALTHY_TIMEOUT_S = float(os.environ.get("ZV_SIM_WAIT_PIPELINE_HEALTHY_TIMEOUT", "240.0"))
