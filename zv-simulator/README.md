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
- Python 3.10+: `make simulator-install` from the autokc repo root (creates/
  reuses `.venv` there and installs this package editable, putting the
  `zv-simulator` console script into `.venv/bin`). All commands below are
  wrapped as make targets, so you never need to activate the venv manually;
  for direct CLI use: `.venv/bin/zv-simulator ...`.
- Network access from wherever you run this to `localhost:8083` (Connect),
  `:9090` (Prometheus), `:3100` (Loki), `:5432`/`:5433` (Postgres source/sink).
  Works fine run directly on the host if the stack's ports are published as
  in the default `development/docker-compose.yml`.
- Docker socket access (docker_ctl resolves containers by compose labels via
  docker-py) for the infra-level scenarios (`kafka-broker-down`, ...).

Endpoints/names can be overridden via the autokc repo-root `.env` (copy
`zv-simulator/.env.example` there to get started) or plain `ZV_SIM_*` env
vars - see `config.py`. An exported env var always wins over `.env`, and `.env`
wins over the built-in defaults (e.g. `ZV_SIM_CONNECT_URL`,
`ZV_SIM_PROM_URL`, `ZV_SIM_LOKI_URL`, `ZV_SIM_TOXIPROXY_URL`,
`ZV_SIM_PG_SOURCE_PORT`, `ZV_SIM_SLOT_NAME`, `ZV_SIM_SOURCE_CONNECTOR`).

## Quick start

```bash
make simulator-install
make simulator-list
make simulator-run ID=replication-slot-issue
make simulator-category CAT=replication
```

Each run prints per-expectation pass/fail with detection latency, and
exits non-zero if anything failed or cleanup didn't restore the stack.

## Scenario catalog (phase 1: connection + replication)

| id | category | fault mechanism | expects |
|---|---|---|---|
| `replication-slot-issue` | replication | drop the active replication slot | log: `replication-slot-issue`, metric: `source-disconnected`, event: `log/replication-slot-issue` |
| `source-connection-terminated` | replication | kill the replication backend PID, slot untouched | log: `postgres-connection-terminated`, event: `log/postgres-connection-terminated` |
| `source-disconnect-loop` | replication | kill the backend 3x, 20s apart | metric: `source-disconnect-loop`, event: `metric/source-disconnect-loop` |
| `replication-privilege-revoked` | replication | `ALTER ROLE ... NOREPLICATION` mid-flight | log: `postgres-fatal`, event: `log/postgres-fatal` |
| `publication-dropped` | replication | drop the publication, slot stays | log: NPE / uncaught task exception, event: `log/npe\|task-uncaught-exception` |
| `slot-wal-retention-high` | replication | hold an open transaction while traffic flows | metric: `slot-wal-retention-high` (needs `scripts/simulate-changes.sh` running alongside, ~5-10min), event: `metric/slot-wal-retention-high` |
| `network-cut-source` | connection | Toxiproxy hard cut, source path | log: `jdbc-connection-error`, metric: `source-disconnected`, events: both |
| `network-latency-source` | connection | Toxiproxy +3s latency, source path | metric: `source-stream-stalled`, event: `metric/source-stream-stalled` |
| `jdbc-connection-error` | connection | bad `connection.url` pushed to the sink connector | log: `jdbc-connection-error`, metric: `task-not-running`, events: both |
| `kafka-broker-down` | connection | kill the broker container | log: `kafka-broker-unreachable`, event: `log/kafka-broker-unreachable` |

Not yet built (see the design doc / next phases): resource exhaustion
(`oom`, real heap pressure), worker-rebalance faults, sink lag / snapshot
faults. `scenarios/` is structured so each is its own module -
add `resource_faults.py`, `lag_faults.py`, `worker_faults.py` the same way.

## Network fault setup (Toxiproxy)

`network-cut-source` and `network-latency-source` need Debezium's
connection to `postgres-source` routed through Toxiproxy, since Toxiproxy
can only inject faults on traffic that actually passes through it.

**You set nothing up.** The scenarios self-provision, with escalating
least-intrusiveness:

1. a toxiproxy that is already running (however it got started) is reused
   untouched;
2. else the optional `docker-compose.override.yml` service container, if
   it exists but is stopped, is merely `docker start`ed - nothing is
   recreated, compose is never invoked;
3. else zv-simulator runs its own sidecar (`zv-sim-toxiproxy`) on the
   stack's own network, with the control API bound to `127.0.0.1:8474`.

The scenario then PUTs the source connector config over the Connect REST
API with `database.hostname=toxiproxy / database.port=15432`, waits for
the task to be RUNNING through the proxy (`wait_running`), and only then
injects the fault - so the fault always lands on a known-good path.
`toxiproxy_ctl.ensure_proxy()` creates the proxy mapping
(`toxiproxy:15432 -> postgres-source:5432`) idempotently at the start of
each run.

Cleanup restores the original connector config, restarts the connector
back onto `postgres-source` directly, and removes the fixture - but only
if the simulator started it (sidecar removed, a started service container
stopped again; a proxy you started yourself is left alone).

zv-simulator never runs `docker compose` and never restarts or recreates
stack services - the monitoring app and the rest of the stack are
untouched. Keeping `docker-compose.override.yml` around is purely
optional: start it with the two `-f` files from the repo root only if
you'd rather manage the proxy as part of the stack; the scenarios detect
and reuse it.

Every other scenario needs no Toxiproxy involvement at all.

## Known findings (validated against the current stack)

- `network-cut-source`: a hard network partition is INVISIBLE to
  zv-monitor's metric path. Debezium's streaming metrics bean is tied to a
  live streaming connection. Under a cut (verified across three live runs)
  the `connected{streaming}` gauge never goes to 0 - it holds 1 while
  Debezium retries in-task, then the whole series disappears from
  Prometheus once the streaming source gives up (anywhere from seconds to
  ~90s). `millisecondssincelastevent` stops being written the same way.
  zv-monitor's `source-disconnected` (`connected == 0`) and
  `source-stream-stalled` (`msSinceLastEvent > 60000`) rules are
  therefore structurally blind to partitions - `== 0` and threshold rules
  both are, because absent is neither (an instant query over a vanished
  series returns no data, which matches nothing). What a partition IS
  observable as: the log-based `jdbc-connection-error` (fires in seconds)
  and, for metric checks, `absent(connected{streaming})` - the scenario
  expects exactly those two shapes. If zv-monitor should catch partitions
  as CRITICAL, it needs `absent()`-aware rules or an error-log-driven
  disconnect rule.
- `network-latency-source` is expected to currently FAIL, and that is a
  finding about the same gap from the other side: with 3s latency TCP
  stays up and events keep arriving (delayed), so `msSinceLastEvent`
  never crosses 60s and no error logs appear - zv-monitor has no working
  slow-network detector on this stack. (Same treatment as
  `replication-slot-issue` below: keep the expectations, document the gap.)
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
  at all." Event expectations (`kind="event"`) name a zv-monitor pattern
  (`source` + `pattern` id from the same catalogs) and poll zv-monitor's
  JMX-exported `zv_event_counter_lastepochmillis` anchored on the
  injection timestamp - so events from *before* the run (bring-up chaos,
  connector-unhealthy flapping) can never satisfy them, and the reported
  latency is zv-monitor's own event time, not Prometheus scrape time.
  A `pattern` containing `|` is treated as a label regex
  (`"npe|task-uncaught-exception"`).
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
