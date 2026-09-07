# zv-debezium-connector-postgres

End-to-end integration tests for the [zv-debezium](../zv-debezium) fork's
PostgreSQL connector, modeled on streamkap's `streamkap-tests` /
`PostgresqlIT` + `KafkaFacade` layout.

## How it works

- `ZvDebeziumITBase` — the single shared IT base from `zv-debezium-common`'s
  test-jar, extended by both connector modules — spins the full development
  stack of `development/docker-compose.yml` on a private Docker network:
  an `apache/kafka:3.7.0` KRaft broker, a `postgres:16` with
  `wal_level=logical` (the stack's single database — the CDC origin for
  these source ITs) and a Kafka Connect worker whose image is built from
  **this module's own plugin tarball** (hence the `-Passembly` build).
- The tests (`ITPostgresSource`) deploy the fork's `PostgresConnector` through
  the Connect REST API, mutate the Postgres with plain SQL and assert the
  emitted change events on real Kafka topics — exactly the path the
  development docker-compose pipeline takes.

## Prerequisites

- Docker (running).
- The fork's snapshot artifacts in the local Maven repository:

      cd zv-debezium && mvn install -pl debezium-connector-postgres -am \
          -DskipTests -Dcheckstyle.skip=true -Dformat.skip=true -Drevapi.skip=true

## Running

From the repo root (the Connect worker image is built from the module's
plugin tarball, hence `-Passembly`):

    mvn verify -Passembly,run-its -pl zv-debezium-connector-postgres

or standalone (after the install above):

    cd zv-debezium-connector-postgres && mvn verify -Passembly,run-its

Failsafe picks up the `IT*`-prefixed classes in `src/integration-test/java`;
they run only when the `run-its` profile is active (defined in the root pom,
inherited by this module). Use `-Dit.test=ITPostgresSource` to run a single
class.

## zv wrapper connector

`ZvPostgresSourceConnector` extends the fork's `PostgresConnector` **without patching the
fork** and injects zv configuration checks into its validate pipeline (the protected
`validateAllFields` / `validateConnection` hooks). Kafka Connect calls
`PUT /connector-plugins/…/config/validate` with the full raw property map before a
connector is ever created — the wrapper pulls the configuration there:

- `zv.sql.resources` — comma-separated classpath resources packaged in the plugin
  archive (e.g. the shipped `zv-sql/health.sql`) with the SQL statements to run.
  Statements are split on `;` outside single quotes; line/block comments are ignored.
- Field level: every configured resource must resolve on the plugin classpath.
- Connection level (after the fork's own Postgres check): every statement is parsed
  by the server via `prepareStatement()` — never executed — and parse errors are
  reported on the `zv.sql.resources` field of the validate output.
- The JDBC parameters for that check are derived from the standard `database.*` keys:
  hostname/port/dbname go into the URL, user/password and any extra key (e.g.
  `database.sslmode`) become driver connection properties.

Runtime behaviour is still the stock connector's (task class/config inherited, zv
keys pass through to the task untouched); periodic execution + metrics wiring is the
next stage. `ZvSqlConfig` holds the parsing so the task will reuse it verbatim.

    connector.class=com.zv.kcmanager.source.postgresql.ZvPostgresSourceConnector
    zv.sql.resources=zv-sql/health.sql

## Layout

    src/main/java/com.zv.kcmanager/source/postgresql/
        ZvPostgresSourceConnector.java   zv wrapper connector (validate-time SQL checks)
        ZvSqlConfig.java                 zv.sql.* parsing, JDBC derivation, statement splitter
    src/main/resources/zv-sql/health.sql example SQL resource (shipped + parse-checked)
    src/test/java/com.zv.kcmanager/source/postgresql/
        ZvSqlConfigTest.java                       unit tests (no database, run automatically)
    src/integration-test/java/com.zv.kcmanager/source/postgresql/
        ITPostgresSource.java                      source-side ITs (stock connector + zv wrapper)

    shared test plumbing ships in zv-debezium-common's test-jar:
    com.zv.kcmanager.common.test.ZvDebeziumITBase   full docker stack (Kafka + Postgres + Connect worker) + REST/Kafka/SQL helpers
    com.zv.kcmanager.common.util.TestUtils          runSQL/querySQL/assertSQL/await
    com.zv.kcmanager.common.util.FixedPortPostgresContainer

## Connector plugin distribution

```bash
mvn clean package -Passembly -DskipTests -pl zv-debezium-connector-postgres
```

produces `target/zv-debezium-connector-postgres-3.7.0.tar.gz` (+ `.zip`) — the
`zv` prefix plus the fork's Debezium version (`debezium.version` with
`-SNAPSHOT` stripped), never the project's `0.1.0-SNAPSHOT` version.

## Docker image

Streamkap-style package image (tarball extracted at `/`), built from the
repository root after the assembly step:

    docker build -f docker/Dockerfile.postgres -t zv-debezium-postgres:3.7.0 .

## Notes / next steps

- Each test uses its own replication slot + publication (`zvit_slot_*`) and
  connector name, so tests never collide on slots or connectors.
- The sink-side counterpart (`ITPostgresSink`) lives in
  `zv-debezium-connector-jdbc` and extends the same shared `ZvDebeziumITBase`
  (its Postgres plays the CDC-target role instead).
