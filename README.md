# zv-monitor

A Java wrapper around Debezium/Kafka Connect that tracks connector health
during initialization and steady-state operation, exposes that health as JMX
metrics (visible in Grafana), and is meant to grow into a tool that can
diagnose - and eventually auto-fix - CDC pipeline failures using an AI module.

## Layout

```
zv-monitor/
├── zv-debezium/                    git submodule - the zv-debezium fork (Debezium 3.7.0-SNAPSHOT)
├── zv-debezium-connector-postgres/ ITs + assembly profile -> connector plugin tar.gz/zip (target/)
├── zv-debezium-connector-jdbc/     ITs + assembly profile -> JDBC sink plugin tar.gz/zip (target/)
├── zv-debezium-common/             shared code for both connectors: utilities + Kafka Connect
│                                   SMTs (see zv-debezium-common/README.md)
├── zv-monitor/                   Kafka Connector Monitor app: polls Connect REST API,
│                                 exposes connector health as JMX metrics (:5558)
├── utils/                          helper scripts (e.g. connector parameter generator)
├── lib/                            JMX exporter agent jar, fetched here by the maven build
│                                   (gitignored - see section 1)
├── docker/
│   ├── Dockerfile.postgres         packager image: zv-debezium Postgres connector + JMX agent
│   ├── Dockerfile.jdbc             packager image: zv-debezium JDBC sink connector + JMX agent
│   ├── Dockerfile.kafka            apache/kafka + JMX agent = broker image
│   └── Dockerfile.kc               apache/kafka + both connectors = Kafka Connect worker image
├── development/                    the local pipeline: docker-compose.yml, Postgres init SQL,
│                                   Kafka Connect worker properties, Prometheus + Grafana
│                                   provisioning, JMX exporter rule configs
├── Makefile                        thin wrapper - every target just calls a script
├── scripts/                        all pipeline logic (also usable directly)
│   ├── pipeline-up.sh              full stack: build, up, connectors, URLs
│   ├── pipeline-up-dev.sh          dev stack: no zv-monitor container
│   ├── pipeline-down.sh            stop (pass -v to also wipe the volumes)
│   ├── register-connectors.sh      (re-)register connectors, wait for RUNNING
│   ├── print-urls.sh               exposed service URLs (utils/compose_urls.py reads compose ports + x-url-info)
│   ├── simulate-changes.sh         continuous CDC traffic generator (Ctrl+C)
│   └── zv-debezium-track.sh        zv-debezium feature-vs-release comparison
└── .github/workflows/ci.yml        CI placeholder
```

## 1. Build the artifacts the images package

The Docker images don't build anything themselves - they package artifacts you
build once with Maven (a deliberate choice: it keeps the Docker build
contexts tiny, and zv-monitor's build needs python3 anyway):

```bash
# connector plugin distributions (tar.gz) -> zv-debezium-connector-*/target/
# (also fetches lib/jmx_prometheus_javaagent-0.20.0.jar for the packager images)
# zv-debezium-common must be in the same reactor (or `mvn install`ed once):
# both connectors ship its jar and depend on its test-jar for their ITs.
mvn package -Passembly -DskipTests \
  -pl zv-debezium-common,zv-debezium-connector-postgres,zv-debezium-connector-jdbc

# zv-monitor fat jar -> zv-monitor/target/zv-monitor.jar
# (regenerates zv-monitor/analysis/*.yaml via utils/gen_connector_params.py,
#  so python3 + tree-sitter-java + PyYAML must be installed - see utils/)
mvn package -DskipTests -pl zv-monitor
```

A full-reactor `mvn install` also builds the fork's `io.debezium` connector
modules (the root POM lists them so one command builds everything). Pass
`-Drevapi.skip=true` when you do: Debezium's revapi API-compatibility gate
(revapi-maven-plugin 0.15.1) dies locally with a log4j classpath error
(`LoggerAdapter` missing), and it is a release-time check that is irrelevant
for local builds. The `-pl` commands above never trigger it.

The JMX exporter agent jar in `lib/` comes with the first build above
(maven-dependency-plugin, version pinned in the parent POM; `lib/` is
gitignored). Every image that embeds it (both connector packager images and
zv-monitor) gets `lib/` as an extra build context and COPYs the jar from
it - so `docker build` never touches the network. `make up`
re-fetches it if `lib/` is empty.

### The images

- `docker/Dockerfile.postgres` / `.jdbc` are build-only "packager" images:
  their content is the payload (`/connectors/` - the connector plugin tree,
  ADD auto-extracts the tarball - plus `/jmx-exporter/` with the agent jar)
  and they are never started. `docker/Dockerfile.kc` COPYs both payloads
  into the Kafka Connect worker image, and `Dockerfile.kafka` takes the
  agent from the jdbc packager. Build order: packagers -> kafka-broker /
  kafka-connect -> zv-monitor (`make up` does this).
- Build contexts stay tiny (module dir / development/); the agent jar enters
  via the extra `lib` context: `additional_contexts: lib=../lib` in compose,
  or `--build-context lib=lib` for manual builds, e.g.:

  ```bash
  docker build -f docker/Dockerfile.postgres --build-context lib=lib \
    -t zv-debezium-connector-postgres:dev zv-debezium-connector-postgres
  ```

- The agent is COPY'd with `--chmod=0644` everywhere because the runtime
  images run as non-root `appuser`.
- The exporter rule configs are bind-mounted by compose, so changing what gets
  exposed is a container restart, not an image rebuild.

## 2. Run the pipeline

```bash
make up                          # builds images (ordered), starts the stack, registers connectors
```

This starts (no ZooKeeper - the Apache Kafka broker runs in KRaft mode):
Apache Kafka 3.7 -> Kafka Connect 3.7 with your zv-debezium Postgres source
connector (`postgres-source:sourcedb`, tables `customers` + `orders` -> topics
`sourcedb.public.customers` / `sourcedb.public.orders`) and the zv-debezium
JDBC sink connector (topics -> mirror tables in `sinkdb`, upsert mode +
deletes), a source Postgres (logical replication enabled, seeded `customers`
and `orders` tables), a sink Postgres, Apicurio Registry (Apache-2.0, in
kafkasql mode backed by the broker - schema storage for the topics,
Confluent-compatible REST at `localhost:8084/apis/ccompat/v7`; the worker
keeps JsonConverter as its default and pre-binds
`key/value.converter.schema.registry.url` to the in-stack registry), the
zv-monitor app, Prometheus,
Grafana and Kafka UI. The connectors (configs:
`development/kafka-connect/*.json`, one file per connector) are registered
idempotently by `make connectors` (PUT), which waits
for all of them to report RUNNING.

Then simulate source changes and watch them land in the sink:

```bash
make simulate                    # INSERT-heavy growing traffic on customers + orders, until Ctrl+C
```

Stop it again with:

```bash
make down                        # stop; ARGS=-v also wipes volumes (DBs, connector offsets)
```

### Run zv-monitor locally (dev stack)

To develop on the monitor itself, run the stack without the zv-monitor
container and start the app on your host instead:

```bash
make up-dev                     # same stack minus the zv-monitor container

CONNECT_REST_URL=http://localhost:8083 \
CONNECTOR_NAMES=inventory-source,customers-sink \
java -javaagent:lib/jmx_prometheus_javaagent-0.20.0.jar=5558:development/monitoring/jmx-exporter/zv-monitor-jmx.yml \
     -jar zv-monitor/target/zv-monitor.jar
```

Kafka (`localhost:9092`) and Kafka Connect (`localhost:8083`) are exposed to
the host, and Prometheus scrapes your local instance through
`host.docker.internal:5558`, so the Grafana dashboard works unchanged.
`make up` brings the container variant back.

### Connector status feeds: status topic + REST polling

zv-monitor reads connector/task statuses from the Connect worker's
status storage topic (`connect-status` - `status.storage.topic` in
`development/kafka-connect/connect-distributed.properties`): the same
compacted topic Connect itself persists to, so the monitor sees exactly what
Connect knows. Keys are `status-connector-<name>` and
`status-task-<connector>-<task>`; a null value (tombstone) means the
connector/task was deleted. Connectors are discovered from the topic itself -
`CONNECTOR_NAMES` does not apply to it.

On startup the consumer replays the compacted topic from the beginning - that
replay *is* the state snapshot (no REST bootstrap). The per-connector
`ConnectorHealth` JMX metrics are updated during the replay already, so the
Grafana dashboard works from the first seconds; `connector-unhealthy` /
`connector-recovered` events and remediation callbacks only fire for records
consumed after the replay caught up with the live head, on state transitions.

The REST poller (`GET /connectors/{name}/status`, driven by
`CONNECT_REST_URL` / `CONNECTOR_NAMES` / `POLL_INTERVAL_MS`) runs in parallel
with the same events + MBeans; `STATUS_TOPIC_ENABLED=false` disables the
topic feed, leaving REST only.

| variable | default (host run) | in compose | meaning |
|---|---|---|---|
| `STATUS_TOPIC_ENABLED` | `true` | `true` | topic feed on/off; REST polling always runs |
| `STATUS_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:29092` | bootstrap address of the Kafka cluster running Connect |
| `STATUS_TOPIC` | `connect-status` | `connect-status` | Connect `status.storage.topic` |
| `STATUS_GROUP_ID` | `zv-monitor` | `zv-monitor` | consumer group id - offsets are never committed, every start replays the full snapshot |

### Log levels

Every component's verbosity is a plain environment knob at bring-up time -
no rebuild, no image edits (a worker restart applies them):

```bash
ZV_LOG_LEVEL=DEBUG DEBEZIUM_LOG_LEVEL=DEBUG make up   # or make up-dev
```

| variable (`make up` / `up-dev`) | logger | default | what DEBUG adds |
|---|---|---|---|
| `ZV_LOG_LEVEL` | `com.zv.kcmanager.*` | INFO | the zv SQL executor's per-statement lines: `Executing zv SQL statement health.1 of connector inventory-source: SELECT pg_wal_lsn_diff(...)` + `executed in N ms` (every poll interval) |
| `DEBEZIUM_LOG_LEVEL` | `io.debezium.connector.postgresql` / `.jdbc` | INFO | Debezium snapshot/streaming internals, incl. the SQL Debezium itself runs |
| `KAFKA_CONNECT_LOG_LEVEL` | `org.apache.kafka.connect.*` | INFO | worker internals (REST, converters, rebalances) |
| `ZV_MONITOR_LOG_LEVEL` (zv-monitor, any run incl. IDEA) | zv-monitor (Logback) | info | per-poll connector state: `poll: connector '...' state=RUNNING tasks=1 failedTasks=0 healthy=1` (every poll interval); `warn`/`error` quiets the periodic INFO lines |

The worker levels are `-D` system properties in `KAFKA_OPTS` that fill the
`${...}` placeholders of `development/kafka-connect/connect-log4j.properties`
(bind-mounted over the stock worker file - stock content plus the
parameterized connector loggers at the bottom). Individual loggers can also
be flipped at runtime without any restart through the Connect Admin API
(changes survive until the worker restarts):

```bash
curl -X PUT localhost:8083/admin/v1/loggers/io.debezium.connector.postgresql \
     -H 'Content-Type: application/json' -d '{"level":"DEBUG"}'
#    ... same call with '{"level":null}' resets to the configured level
```

zv-monitor is the odd one out: it is a standalone app with its own logging
backend (Logback, not the worker's log4j), and `ZV_MONITOR_LOG_LEVEL` is read
natively from the environment by the `${...}` substitution in
`zv-monitor/src/main/resources/logback.xml` - no flags needed. Same in IDEA:
put `ZV_MONITOR_LOG_LEVEL=debug` into the run configuration's Environment
Variables. A fully custom setup (per-logger levels, file/JSON appenders) goes
in a separate file passed with `-Dlogback.configurationFile=/path/to/logback.xml`.

## 3. Look at the metrics

- Grafana: http://localhost:3000 (anonymous viewer access enabled; admin/admin
  if you need to edit) - two dashboards are pre-provisioned (provisioning in
  `development/monitoring/grafana/provisioning/`: Prometheus datasource, Loki
  datasource, dashboard provider): "Pipeline Health" (`/d/zv-monitor-health`)
  for connector/task health, throughput and zv-monitor events, and "Pipeline
  Logs" (`/d/zv-monitor-logs`) for container logs - volume, errors/warnings,
  and the raw log browser. The Service drop-down is single-select (default
  kafka-connect), so log streams of different services are never combined,
  and 'Line contains' is a substring filter. The time picker defaults to the
  last hour and accepts absolute From/To timestamps to jump to an exact
  window. The two dashboards link to each other in the top bar.
- Grafana -> Explore: pick the **Loki** datasource for container logs. Promtail
  ships every docker container's stdout/stderr (docker_sd over the docker
  socket, regardless of which compose project - or `docker run` - started it,
  so a locally-run zv-monitor shows up too). Useful queries:
  `{service="kafka-connect"}`, `{container=~".*zv-monitor.*"}`,
  `{service="kafka-connect"} |= "zv SQL statement"` (the SQL debug lines).
- Kafka UI: http://localhost:8080 - browse the `sourcedb.public.customers`
  and `sourcedb.public.orders` change-event topics.
- Postgres UI (pgweb): http://localhost:8081 - quick SQL access to both
  databases, no login: in the connection dialog pick the `sourcedb` /
  `sinkdb` bookmark and click Connect (bookmarks in `development/pgweb/`;
  reopen the dialog via the connection button to switch databases).
- Prometheus directly: http://localhost:9090
- Raw metrics endpoints: `localhost:5557/metrics` (Kafka Connect),
  `localhost:5558/metrics` (zv-monitor), `localhost:5559/metrics`
  (Kafka broker - kafka.server/kafka.controller MBeans via the same JMX
  exporter; Count -> counters, OneMinuteRate -> gauges; rules in
  development/monitoring/jmx-exporter/kafka-broker-jmx.yml)

The headline metric is `connector_health_healthy{connector="..."}` (1 = OK, 0
= unhealthy) - alert on that first, then drill into
`connector_health_failedtaskcount` and `kafka_connect_task_status` for detail.
Broker health is on the same endpoints: `kafka_server_replicamanager_
underreplicatedpartitions` must stay 0, `kafka_controller_kafkacontroller_
activecontrollercount` must stay 1, and
`kafka_server_brokertopicmetrics_*_oneminuterate` shows throughput.

## 4. Where the AI module plugs in

The Kafka Connector Monitor (`zv-monitor`) calls a `RemediationHandler`
(`zv-monitor/src/main/java/.../remediation/`) every time a connector goes
unhealthy or recovers. Right now `LoggingRemediationHandler` just logs. The
intended extension path:

1. **Manual-assist**: a new handler sends the failure trace + recent metrics
   to an LLM, gets back a diagnosis and suggested fix, and surfaces it
   (Slack message, dashboard annotation, whatever) for a human to act on.
2. **Automatic**: once you trust it for specific, well-understood failure
   modes, the handler calls `ConnectClient.restartConnector(...)` /
   `restartTask(...)` directly instead of just suggesting it.

`ConnectorStatus` already carries the connector/task state and error trace, so
the handler has what it needs without touching the polling loop.

## 5. CI

`.github/workflows/ci.yml` is currently a placeholder. The intended jobs,
mirroring the local workflow:

- **connector modules**: `mvn clean verify -Passembly,run-its -pl
  zv-debezium-connector-postgres,zv-debezium-connector-jdbc` (unit +
  Testcontainers integration tests). `run-its` is a root-pom opt-in profile;
  the zv-debezium fork does not inherit it, so Debezium's upstream IT suite
  stays skipped. Never substitute a global `-DskipITs=false` - it applies to
  every reactor module, enables the fork's ITs, and those expect their own
  docker-compose Postgres on localhost:5432 and fail wholesale.
- **zv-monitor**: `mvn verify`.
- **integration smoke**: `make up` +
  `timeout 90 scripts/simulate-changes.sh` - the generator runs until
  terminated, so CI bounds it with `timeout`; a real end-to-end check that
  connector changes didn't break the pipeline. Runs on push to `main`
  (not every PR, since it's the slowest job).

## Known gaps

- Storage is ephemeral: none of kafka/postgres-source/postgres-sink declare
  data volumes, so a full stack recreation (e.g. `docker compose up -d` after a
  compose edit) starts with empty broker topics and freshly seeded databases.
  That's deliberate for a dev pipeline - `make up` re-registers
  the connectors and Debezium re-snapshots; use `make down ARGS=-v` for a
  guaranteed clean slate.
- `LoggingRemediationHandler` is a stub - see section 4.
