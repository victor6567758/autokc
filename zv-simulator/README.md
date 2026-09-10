# zv-simulator

Deterministic fault injector for the `autokc` / `zv-monitor` CDC pipeline.
Every scenario targets a failure class `zv-monitor` already knows how to
detect (see `zv-monitor/analysis/loki-log-patterns.yaml` and
`prometheus-metrics.yaml` in the autokc repo) - so a scenario run is also a
regression test: inject a known fault, poll Loki + Prometheus + zv-monitor's
own output, and report whether/how-fast detection fired.

This is deliberately the *deterministic* half of the plan (rule-based
injection, rule-based verification). It's designed to produce a labeled
event log an AI diagnosis module can later be evaluated against: run a
scenario, capture what zv-monitor emitted, check whether a diagnosis
model correctly names the injected fault from that emission alone.

## Prerequisites

- The `autokc` stack up: `make up` or `make up-dev` from the autokc repo root.
- Python 3.10+: `pip install -e .` from this directory (the package code lives
  in `zv_simulator/`; the console script `zv-simulator` lands on your PATH).
- Network access from wherever you run this to `localhost:8083` (Connect),
  `:9090` (Prometheus), `:3100` (Loki), `:5432`/`:5433` (Postgres source/sink).
  Works fine run directly on the host if the stack's ports are published as
  in the default `development/docker-compose.yml`.
- Docker socket access (docker_ctl resolves containers by compose labels via
  docker-py) for the infra-level scenarios (`kafka-broker-down`, ...).

Endpoints/names can be overridden with `ZV_SIM_*` env vars - see
`zv_simulator/__init__.py` (e.g. `ZV_SIM_CONNECT_URL`, `ZV_SIM_PG_SOURCE_PORT`,
`ZV_SIM_SLOT_NAME`, `ZV_SIM_SOURCE_CONNECTOR`).

## Quick start

```bash
pip install -e .
zv-simulator list
zv-simulator run replication-slot-issue
zv-simulator run-category replication
```

Each run prints per-expectation pass/fail with detection latency, and
exits non-zero if anything failed or cleanup didn't restore the stack.

## Scenario catalog (phase 1: connection + replication)

| id | category | fault mechanism | expects |
|---|---|---|---|
| `replication-slot-issue` | replication | drop the active replication slot | log: `replication-slot-issue`, metric: `source-disconnected` |
| `source-connection-terminated` | replication | kill the replication backend PID, slot untouched | log: `postgres-connection-terminated` |
| `source-disconnect-loop` | replication | kill the backend 3x, 20s apart | metric: `source-disconnect-loop` |
| `replication-privilege-revoked` | replication | `ALTER ROLE ... NOREPLICATION` mid-flight | log: `postgres-fatal` |
| `publication-dropped` | replication | drop the publication, slot stays | log: NPE / uncaught task exception |
| `slot-wal-retention-high` | replication | hold an open transaction while traffic flows | metric: `slot-wal-retention-high` (needs `scripts/simulate-changes.sh` running alongside, ~5-10min) |
| `network-cut-source` | connection | Toxiproxy hard cut, source path | log: `jdbc-connection-error`, metric: `source-disconnected` |
| `network-latency-source` | connection | Toxiproxy +3s latency, source path | metric: `source-stream-stalled` |
| `jdbc-connection-error` | connection | bad `connection.url` pushed to the sink connector | log: `jdbc-connection-error`, metric: `task-not-running` |
| `kafka-broker-down` | connection | kill the broker container | log: `kafka-broker-unreachable` |

Not yet built (see the design doc / next phases): resource exhaustion
(`oom`, real heap pressure), worker-rebalance faults, sink lag / snapshot
faults. `zv_simulator/scenarios/` is structured so each is its own module -
add `resource_faults.py`, `lag_faults.py`, `worker_faults.py` the same way.

## Network fault setup (Toxiproxy)

`network-cut-source` and `network-latency-source` need Debezium's
connection to `postgres-source` routed through Toxiproxy, since Toxiproxy
can only inject faults on traffic that actually passes through it.

1. Bring up the proxy (from the autokc repo root):
   ```bash
   docker compose -f development/docker-compose.yml \
                   -f zv-simulator/docker-compose.override.yml up -d toxiproxy
   ```
2. Point the source connector at it instead of `postgres-source` directly.
   Easiest: copy `development/kafka-connect/inventory-source.json` to
   `inventory-source-toxi.json` with:
   ```json
   "database.hostname": "toxiproxy",
   "database.port": "15432",
   ```
   and re-register that variant when you want to run these two scenarios;
   swap back to the original for everything else, since routing through
   an idle proxy adds a hop for every other scenario for no benefit.
3. `zv_simulator.toxiproxy_ctl.ensure_proxy()` creates the proxy mapping
   (`toxiproxy:15432 -> postgres-source:5432`) idempotently on first use,
   so you don't need to hit the Toxiproxy API by hand.

Every other scenario needs no Toxiproxy setup at all.

## Known findings (validated against the current stack)

- `source-connection-terminated` and `jdbc-connection-error` pass end-to-end
  against `make up` (fault -> Loki/metric match within seconds).
- `replication-slot-issue` currently FAILS - and that's a real finding, not a
  tool bug: the zv-debezium build in this repo uses offset mismatch strategy
  `no_validation` ("Slot recreation is not detected") and silently recreates a
  dropped slot within ~10s of the drop. Neither `logical replication slot ...
  does not exist` (log) nor `connected == 0` (metric) is observable long enough
  to fire, so zv-monitor's `replication-slot-issue` pattern can't trigger on
  this connector version for this fault. If you need that signature, drop the
  slot while the connector is paused, or drop + also remove the stored offset.

## Design notes

- **Loki stream label is `service`, not `container`.** Promtail labels every
  stream with the compose service name (`service="kafka-connect"`, `"postgres-source"`,
  ...); the auto-added `container` label holds the full docker name
  (`development-kafka-connect-1`) and must not be used in selectors. Scenario
  LogQL therefore uses `{service=~"kafka-connect|postgres-source"}` - exactly
  the selector zv-monitor's `LogPattern.toLogQl()` builds, so simulator and
  monitor can never drift apart.

- **Everything ties back to the existing catalog.** Expectation queries in
  `scenarios/*.py` are copied verbatim from `loki-log-patterns.yaml` /
  `prometheus-metrics.yaml` - if a scenario fails, that's either a bad
  injection or an actual zv-monitor regression, never a query typo living
  in two places.
- **Three independent checks, not one.** `verifier.py` can tell you "Loki
  saw it but zv-monitor never emitted an Event" - that's a real bug in
  zv-monitor's own detection, distinct from "the fault didn't reproduce
  at all."
- **Every scenario cleans up, even on failure.** Scenarios are written as
  generators (`yield` between inject and cleanup) specifically so
  `run_scenario()` can guarantee cleanup runs in a `finally`, whether
  detection passed, timed out, or the injection itself threw.
- **Faults come from three distinct layers on purpose**: `docker_ctl.py`
  (infrastructure), `pg_faults.py` (data-plane), `connect_rest.py`
  (config-plane), `toxiproxy_ctl.py` (network). Real incidents rarely
  announce which layer they came from, and each layer produces a
  meaningfully different signature - that's the point of keeping them
  separate rather than one grab-bag "break_things.py".
