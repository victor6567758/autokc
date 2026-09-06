# zv-debezium-common

Code shared by the two connector modules (`zv-debezium-connector-postgres`,
`zv-debezium-connector-jdbc`):

- `com.zv.kcmanager.common.smt` - Kafka Connect SMTs (single
  message transforms)
- `com.zv.kcmanager.common.test` - `ZvDebeziumITBase`, the single
  shared IT base both connector modules extend: it spins the full development
  stack (Kafka broker, one Postgres and a Kafka Connect worker built from the
  module's plugin tarball) on a private Docker network; that single Postgres
  plays whichever role the module tests (CDC origin for source ITs, CDC
  target for sink ITs)
- `com.zv.kcmanager.common.util` - IT helpers
  (`TestUtils`, `FixedPortPostgresContainer`)

`com.zv.kcmanager.common.test` and `.util` are published as the
`tests`-classifier test-jar (the two connectors depend on it `test`-scoped);
the base class itself is exercised end-to-end by the connector modules' ITs.

## Packaging

The main jar is a `runtime` dependency of both connectors, so their
`assembly` profile drops `zv-debezium-common-*.jar` into both plugin
tarballs, next to the connector jar. `org.apache.kafka:connect-api` is
`provided` here: the Connect worker supplies it at runtime and the
assemblies must not package it. Because of this dependency, builds that
target only the connector modules must keep this module in the same reactor
(`mvn ... -pl zv-debezium-common,zv-debezium-connector-postgres,...`) or use
`-am`; a standalone `mvn verify -pl zv-debezium-connector-postgres` needs
`mvn install -pl zv-debezium-common` first.

## Writing an SMT

Implement `org.apache.kafka.connect.transforms.Transformation` in
`...common.smt` and list the class in
`src/main/resources/META-INF/services/org.apache.kafka.connect.transforms.Transformation`
(so service-load plugin discovery finds it). Use it in a connector config:

```json
"transforms": "lowercase",
"transforms.lowercase.type": "com.zv.kcmanager.common.smt.LowerCaseTopic"
```

`LowerCaseTopic` is a minimal working example (topic lowercasing).
