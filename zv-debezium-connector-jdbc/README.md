# zv-debezium-connector-jdbc

Build module for the [zv-debezium](../zv-debezium) fork's **JDBC sink connector**
(`io.debezium:debezium-connector-jdbc`): docker integration tests and the
`assembly`-profile connector plugin packaging.

The ITs run the fork's `JdbcSinkConnector` end-to-end through the shared
`ZvDebeziumITBase` stack (Kafka broker + Postgres + a Connect worker built
from **this module's own plugin tarball**, hence the `-Passembly` build) —
the mirror image of the source-side module: the test produces Debezium-shaped
change events (schema + payload) onto Kafka topics and asserts the replicated
rows in the Postgres, the stack's single database in its CDC-target role.

## Integration tests

```bash
mvn verify -Passembly,run-its -pl zv-debezium-connector-jdbc
```

- `ITPostgresSink` — produces Debezium change-event envelopes (create /
  update / delete + tombstone, plus auto-create, upsert and auto-evolve
  cases) onto Kafka topics and asserts the resulting rows via JDBC; the
  connector runs with `insert.mode=upsert`, `delete.enabled=true`,
  `primary.key.mode=record_key`, `auto.evolve=true`.
- All plumbing — the docker stack and the REST/Kafka/SQL helpers — is shared
  with the source side: both modules extend the single `ZvDebeziumITBase`
  from `zv-debezium-common`'s test-jar.
- Requires Docker (running); on Docker Engine 29+ hosts:
  `echo 'api.version=1.44' >> ~/.docker-java.properties`

## Building the fork connector

The connector itself comes from the fork (install into the local repo first):

```bash
cd zv-debezium && mvn install -pl debezium-connector-jdbc -am -DskipTests -Dcheckstyle.skip=true -Dformat.skip=true -Drevapi.skip=true
```

## Connector plugin distribution

```bash
mvn clean package -Passembly -DskipTests -pl zv-debezium-connector-jdbc
```

produces `target/zv-debezium-connector-jdbc-3.7.0.tar.gz` (+ `.zip`) — the
`zv` prefix plus the fork's Debezium version (`debezium.version` with
`-SNAPSHOT` stripped), never the project's `0.1.0-SNAPSHOT` version. The
archive holds the connector and its runtime dependency closure (Hibernate /
Agroal / all dialect JDBC drivers).

## Docker image

Streamkap-style package image (tarball extracted at `/`), built from the
repository root after the assembly step:

    docker build -f docker/Dockerfile.jdbc -t zv-debezium-jdbc:3.7.0 .

