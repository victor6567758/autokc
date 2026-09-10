"""Connection-class faults: network path and endpoint-config problems,
as opposed to replication_faults.py's data-plane (slot/backend/role)
faults on Postgres itself.
"""
from __future__ import annotations

from zv_simulator.scenarios import scenario, Context
from zv_simulator.verifier import Expectation
from zv_simulator import SINK_CONNECTOR, SOURCE_CONNECTOR


@scenario(
    id="network-cut-source",
    category="connection",
    description=(
        "Hard network partition between kafka-connect and postgres-source "
        "via Toxiproxy (proxy disabled - existing connections drop, new "
        "ones refuse immediately). REQUIRES inventory-source.json's "
        "database.hostname/port to target the toxiproxy listen address - "
        "see README. This is the 'dead' half of the dead-vs-slow pair; "
        "see network-latency-source for the other half."
    ),
    expects=[
        Expectation(
            id="metric:source-disconnected",
            kind="metric",
            query='debezium_postgres_connector_metrics_connected{context="streaming"} == 0',
            timeout_s=60,
        ),
        Expectation(
            id="log:jdbc-connection-error",
            kind="log",
            query='{service=~"kafka-connect"} '
            '|~ "(?i)PSQLException|[Cc]onnection to .* refused|the connection attempt failed"',
            timeout_s=60,
        ),
    ],
)
def run_network_cut(ctx: Context):
    ctx.toxiproxy.ensure_proxy()
    ctx.toxiproxy.cut()
    yield
    ctx.toxiproxy.restore()
    ctx.connect.restart_connector(SOURCE_CONNECTOR)


@scenario(
    id="network-latency-source",
    category="connection",
    description=(
        "Injects 3s +/- 500ms latency on the source path via Toxiproxy - "
        "TCP stays up, so this should read as 'stalled', not 'dead'. "
        "REQUIRES the same toxiproxy-fronted connector config as "
        "network-cut-source."
    ),
    expects=[
        Expectation(
            id="metric:source-stream-stalled",
            kind="metric",
            query='max_over_time(debezium_postgres_connector_metrics_millisecondssincelastevent'
            '{context="streaming"}[2m]) > 60000',
            timeout_s=150,
            poll_interval_s=5,
        ),
    ],
)
def run_network_latency(ctx: Context):
    ctx.toxiproxy.ensure_proxy()
    ctx.toxiproxy.add_latency(latency_ms=3000, jitter_ms=500)
    yield
    ctx.toxiproxy.clear_toxics()


@scenario(
    id="jdbc-connection-error",
    category="connection",
    description=(
        "Config-plane fault: PUTs a broken connection.url onto the JDBC "
        "sink connector (wrong port). No infrastructure touched at all - "
        "this isolates 'bad config someone pushed' from 'network actually "
        "broke', which is a meaningfully different on-call story."
    ),
    expects=[
        Expectation(
            id="log:jdbc-connection-error",
            kind="log",
            query='{service=~"kafka-connect"} '
            '|~ "(?i)PSQLException|[Cc]onnection to .* refused|the connection attempt failed"',
            timeout_s=45,
        ),
        Expectation(
            id="metric:task-not-running",
            kind="metric",
            query='kafka_connect_task_status{status!="running"} == 1',
            timeout_s=45,
        ),
    ],
)
def run_bad_jdbc_config(ctx: Context):
    good_config = ctx.connect.get_config(SINK_CONNECTOR)
    bad_config = dict(good_config)
    bad_config["connection.url"] = "jdbc:postgresql://postgres-sink:59999/sinkdb"
    ctx.connect.set_config(SINK_CONNECTOR, bad_config)
    yield
    ctx.connect.set_config(SINK_CONNECTOR, good_config)
    ctx.connect.restart_connector(SINK_CONNECTOR)


@scenario(
    id="kafka-broker-down",
    category="connection",
    description=(
        "Kills the kafka broker container outright - the connect worker "
        "loses its bootstrap connection entirely, distinct from a single "
        "connector's DB link failing. Both connectors should show it, not "
        "just one - a good check that zv-monitor's task-not-running signal "
        "isn't accidentally scoped to a single connector."
    ),
    expects=[
        Expectation(
            id="log:kafka-broker-unreachable",
            kind="log",
            query='{service=~"kafka-connect|kafka"} '
            '|~ "Connection to node .* could not be established|Broker may not be available|'
            'DisconnectException"',
            timeout_s=45,
        ),
    ],
)
def run_kafka_broker_down(ctx: Context):
    ctx.docker.kill("kafka")
    yield
    ctx.docker.restart("kafka")
    ctx.docker.wait_running("kafka", timeout_s=60)
    ctx.connect.restart_connector(SOURCE_CONNECTOR)
    ctx.connect.restart_connector(SINK_CONNECTOR)
