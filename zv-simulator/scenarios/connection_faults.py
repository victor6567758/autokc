"""Connection-class faults: network path and endpoint-config problems,
as opposed to replication_faults.py's data-plane (slot/backend/role)
faults on Postgres itself.
"""
from __future__ import annotations

import contextlib

from scenarios import scenario, Context
from verifier import Expectation
from config import SINK_CONNECTOR, SOURCE_CONNECTOR
from toxiproxy_ctl import (
    TOXIPROXY_HOST,
    TOXIPROXY_LISTEN_PORT,
    ensure_toxiproxy,
    stop_toxiproxy,
)


def _route_source_through_proxy(ctx: Context) -> tuple[dict, str | None]:
    """Setup shared by the network scenarios: guarantee a toxiproxy is up
    (ensure_toxiproxy - reuse > start stopped service > own sidecar; never
    a compose relaunch), then repoint the source connector at it over the
    Connect REST API and wait for the task to be healthy THROUGH the proxy,
    so the fault lands on a known-good path. Returns (original_config,
    fixture_token) for cleanup. If anything fails after the config swap,
    the swap and the fixture are undone before re-raising - a failed setup
    must leave no trace either."""
    started = ensure_toxiproxy(ctx.docker)
    # The proxy mapping must exist BEFORE the config swap: Debezium
    # validates the JDBC connection synchronously on PUT, so pointing the
    # connector at 15432 with no listener there fails the PUT outright.
    ctx.toxiproxy.ensure_proxy()
    original = ctx.connect.get_config(SOURCE_CONNECTOR)
    try:
        ctx.connect.set_config(SOURCE_CONNECTOR, {
            **original,
            "database.hostname": TOXIPROXY_HOST,
            "database.port": TOXIPROXY_LISTEN_PORT,
        })
        if not ctx.connect.wait_running(SOURCE_CONNECTOR, timeout_s=90):
            raise RuntimeError(
                f"{SOURCE_CONNECTOR} did not reach RUNNING through the toxiproxy - "
                "aborting before injecting the fault"
            )
    except BaseException:
        with contextlib.suppress(Exception):
            _unroute_source_from_proxy(ctx, original, started)
        raise
    return original, started


def _unroute_source_from_proxy(ctx: Context, original: dict, started: str | None):
    """Mirror of _route_source_through_proxy: restore the proxy, PUT the
    original connector config back (direct postgres-source connection),
    restart, and remove the toxiproxy fixture - but only if we started it."""
    ctx.toxiproxy.restore()
    ctx.toxiproxy.clear_toxics()
    ctx.connect.set_config(SOURCE_CONNECTOR, original)
    ctx.connect.restart_connector(SOURCE_CONNECTOR)
    stop_toxiproxy(ctx.docker, started)


@scenario(
    id="network-cut-source",
    category="connection",
    description=(
        "Hard network partition between kafka-connect and postgres-source "
        "via Toxiproxy (proxy disabled - existing connections drop, new "
        "ones refuse immediately). Self-provisioning: guarantees a "
        "toxiproxy (reuses a running one, docker-starts the optional "
        "override-file container if stopped, else runs a simulator-owned "
        "sidecar on the stack network - stack services are never "
        "recreated), temporarily repoints the source connector through it "
        "and restores everything at cleanup. NOTE: a partition does NOT "
        "flip Debezium's connected{streaming} gauge to 0 (task stays "
        "RUNNING in a retry loop), so the metric-based "
        "source-disconnected signal never fires for this fault - what is "
        "observable is the JDBC error-log storm plus the generic "
        "stream-stalled signature. See README Known findings. This is "
        "the 'dead' half of the dead-vs-slow pair."
    ),
    expects=[
        Expectation(
            id="log:jdbc-connection-error",
            kind="log",
            query='{service=~"kafka-connect"} '
            '|~ "(?i)PSQLException|[Cc]onnection to .* refused|the connection attempt failed"',
            timeout_s=60,
        ),
        # The REAL metric observable for a partition: Debezium's streaming
        # metrics bean is tied to a live streaming connection. Under a cut
        # (verified across three live runs) the connected gauge never goes
        # to 0 - it holds 1 while Debezium retries, then the whole series
        # disappears from Prometheus once the streaming source gives up
        # (within seconds to ~90s). zv-monitor's `connected == 0`
        # (source-disconnected) and `msSinceLastEvent > 60s`
        # (source-stream-stalled) rules therefore can NEVER fire for a
        # partition: 0-comparisons and thresholds are both blind to an
        # absent series. absent() is the query that actually observes it.
        # See README Known findings.
        Expectation(
            id="metric:source-stream-metrics-absent",
            kind="metric",
            query='absent(debezium_postgres_connector_metrics_connected{context="streaming"})',
            timeout_s=180,
            poll_interval_s=5,
        ),
        Expectation(
            id="event:jdbc-connection-error",
            kind="event",
            source="log",
            pattern="jdbc-connection-error",
            timeout_s=180,
        ),
    ],
)
def run_network_cut(ctx: Context):
    original, started = _route_source_through_proxy(ctx)
    try:
        ctx.toxiproxy.cut()
        yield
    finally:
        _unroute_source_from_proxy(ctx, original, started)


@scenario(
    id="network-latency-source",
    category="connection",
    description=(
        "Injects 3s +/- 500ms latency on the source path via Toxiproxy - "
        "TCP stays up, so this should read as 'stalled', not 'dead'. "
        "Self-provisioning exactly like network-cut-source: toxiproxy "
        "fixture + connector repoint handled automatically, restored at "
        "cleanup."
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
        Expectation(
            id="event:source-stream-stalled",
            kind="event",
            source="metric",
            pattern="source-stream-stalled",
            timeout_s=180,
        ),
    ],
)
def run_network_latency(ctx: Context):
    original, started = _route_source_through_proxy(ctx)
    try:
        ctx.toxiproxy.add_latency(latency_ms=3000, jitter_ms=500)
        yield
    finally:
        _unroute_source_from_proxy(ctx, original, started)


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
        Expectation(
            id="event:jdbc-connection-error",
            kind="event",
            source="log",
            pattern="jdbc-connection-error",
            timeout_s=90,
        ),
        Expectation(
            id="event:task-not-running",
            kind="event",
            source="metric",
            pattern="task-not-running",
            timeout_s=90,
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
        Expectation(
            id="event:kafka-broker-unreachable",
            kind="event",
            source="log",
            pattern="kafka-broker-unreachable",
            timeout_s=90,
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
