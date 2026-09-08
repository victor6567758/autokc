# zv-monitor: event flow (metrics + logs)

## Goal

zv-monitor is a sidecar: a standalone process running next to the pipeline
that turns two sources of truth - **metrics** (already implemented: Connect
REST status -> JMX -> Prometheus) and **logs** (new) - into a single stream
of `Event`s. Everything downstream (today: logging + JMX counters; later: an
AI remediation module) consumes that one `Event` shape and doesn't care which
source produced it.

## Why logs go through Loki instead of zv-monitor tailing files directly

Two things wanted the same data: Grafana wanted to *show* logs, and
zv-monitor wanted to *filter* them into events. Rather than building two
pipelines (a file-tailer in zv-monitor + something else for Grafana), both
are served by one piece of infra:

- **Promtail** discovers every container via `docker.sock` and ships
  stdout/stderr to **Loki**, labelled with the docker-compose service name.
- **Grafana** queries Loki directly (Explore, log panels) - humans get raw
  logs for free, no extra code.
- **zv-monitor** also queries Loki, but narrowly: one LogQL query per
  `LogPattern` (NPE, Postgres FATAL, JDBC connection refused, ...), polled on
  an interval, only asking for the time window since the last successful
  poll. A hit becomes an `Event.log(...)`.

This keeps zv-monitor's own code free of file-tailing/rotation/offset
bookkeeping - that complexity already lives in Promtail/Loki, which are
purpose-built for it.

## Flow

```
 kafka-connect ─┐                         ┌─ Prometheus JMX exporter (5557) ─┐
 kafka          ├─ stdout/stderr ─ Promtail ─ Loki (3100) ─ Grafana (Explore)│
 postgres-*    ─┘                                │                          │
                                                  │ LogQL (per LogPattern)   │ scrape
                                                  ▼                          ▼
                                          LogEventPoller              (existing) ConnectorHealth
                                                  │                     (Connect REST poll)
                                                  ▼                          │
                                             Event(LOG, ...)          Event(METRIC, ...)
                                                  └───────────┬──────────────┘
                                                               ▼
                                                          EventBus.publish
                                                     ┌────────────┴────────────┐
                                                     ▼                         ▼
                                          LoggingEventHandler         EventCounterRegistry
                                          (zv-monitor's own logs)     (JMX -> Prometheus ->
                                                                       Grafana, one counter per
                                                                       (source, patternId))
                                                     │
                                                     ▼ (future)
                                          AI remediation consumer
                                          (diagnose + suggest/act,
                                           see RemediationHandler's javadoc)
```

## What exists after this change

- `com.zv.event` - `Event`, `EventBus`, `EventHandler`, `LoggingEventHandler`,
  `EventCounterRegistry` (+ MBean). Source-agnostic; the metrics path
  (`ConnectorStatusPoller`) and the log path (`LogEventPoller`) both just call
  `eventBus.publish(...)`.
- `com.zv.logs` - `LogPattern` (id, severity, container selector, regex),
  `LogPatternCatalog` (loads the filtered set at startup from
  `analysis/loki-log-patterns.yaml`: NPE, OOM, uncaught task exceptions,
  Postgres FATAL/disconnects, JDBC connection errors, broker unreachable,
  replication slot issues), `LokiClient` (query_range HTTP client),
  `LogEventPoller` (per-pattern watermarked polling loop). The metric side
  loads `analysis/prometheus-metrics.yaml` (`subscribe:` section) the same
  way via `com.zv.metrics.MetricPatternCatalog` - both catalogs are plain
  YAML in the analysis folder, editable without a rebuild (paths: config
  `logs.patternsFile` / `metrics.patternsFile`, env `LOG_PATTERNS_FILE` /
  `METRIC_PATTERNS_FILE`; relative paths resolve against the working
  directory with a `zv-monitor/` fallback, so the same defaults work in the
  container (CWD `/app`), module-dir runs and repo-root local runs).
- `connector_health_*` (existing) and `zv_event_counter_*` (new) Prometheus
  series, both via the same JMX-exporter-on-the-sidecar pattern already used
  for `ConnectorHealth`.
- Loki + Promtail added to both compose files; Grafana gets a Loki
  datasource alongside the existing Prometheus one.

## Shutdown

`ZvMonitorApp` parks on `GracefulShutdown.await()` (`com.zv.lifecycle`), not
`Thread.currentThread().join()`. The JVM shutdown hook - fired by SIGTERM
(`docker stop`, 20s grace in compose) or SIGINT (Ctrl-C) - runs the registered
steps in order: stop the shared poller scheduler (in-flight Loki/Prometheus/
Connect ticks get 10s to finish, then they are interrupted - `HttpClient.send`
is interruptible - and 5s more), then unregister the `ConnectorHealth` /
`EventCounter` MBeans. Each step is guarded, one failing step never blocks the
rest, and `initiate()` is idempotent, so a future admin endpoint can trigger
exactly the same path as a signal; once it completes, `main` returns and the
JVM exits normally.

## What's deliberately left as a stub / next step

- **Postgres-side stats** (replication lag, connection counts) currently
  only reach events via log lines (`postgres-fatal`,
  `postgres-connection-terminated`). A `pg_stat_activity` /
  `pg_stat_replication` poller would be a natural `EventHandler` producer to
  add next to `LogEventPoller` and the REST-status poller, following the same
  "poll -> compare -> `eventBus.publish(Event.metric(...))`" shape.
- **Reacting**, not just recording: `LoggingEventHandler` and
  `EventCounterRegistry` only observe. `RemediationHandler` already has the
  intended shape for closing the loop (diagnose via LLM, suggest or take
  action) - it would become another `EventHandler` implementation.
- The log-pattern set is intentionally short. Extend
  `analysis/loki-log-patterns.yaml` (and the metric side's `subscribe:`
  section) as real chaos experiments (Toxiproxy latency/timeout, container
  kill/restart, forced bad config, `INJECT_NPE`-style fault injection)
  surface new signatures worth naming - edit + restart, no rebuild.
