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

## Remote debugging the Connect worker

The worker container accepts an IDE remote debugger so connector code can be
stepped through inside the real Connect runtime. Opt in per run (off by
default, tests behave identically without it):

    mvn verify -Passembly,run-its -pl zv-debezium-connector-postgres \
        -Dit.test=ITPostgresSource -Dzv.it.debug.connect=true

The worker JVM then starts with
`-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005`
(via `KAFKA_JVM_PERFORMANCE_OPTS`) and port 5005 is published to the host on
the same fixed port — attach a JDWP remote-debug config to `localhost:5005`.
Breakpoints can be set any time; add
`-Dzv.it.debug.connect.suspend=true` (or env `ZV_IT_DEBUG_CONNECT=1` /
`ZV_IT_DEBUG_CONNECT_SUSPEND=1`) to hold the worker at JVM startup until the
debugger attaches. Run one IT class per debug session — the host port is
fixed, so parallel classes would collide.

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
