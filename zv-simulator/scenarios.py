"""All fault scenarios, as classes.

A scenario is: inject a fault, wait for a set of Expectations to be
satisfied (or time out), then clean up - always, even on failure/error,
since a scenario that leaves the stack broken poisons every scenario run
after it.

Each scenario's run() is a generator with exactly one `yield`: everything
before it is setup + injection, everything after it is cleanup. The
Simulator (simulator.py) drives that contract: it advances the generator
to the yield (fault goes live), polls the verifier, then advances it once
more in a finally block - so cleanup runs no matter what.

To add a scenario: subclass Scenario (or RoutedThroughProxy for the
network-through-toxiproxy ones), set id/category/description/expects,
implement run(), and add the class to SCENARIOS at the bottom.
"""
from __future__ import annotations

import contextlib
import threading
import time
from typing import Generator

from simulator import Context
from toxiproxy_ctl import ToxiproxyCtl, ToxiproxyFixture
from verifier import Expectation
from config import (
    BACKOFF_RESTART_CONNECTOR,
    CONNECT_REST_RESTART_INCLUDE_TASKS,
    DOCKER_KILL_SIGNAL,
    DOCKER_RESTART_TIMEOUT,
    PG_BULK_GENERATE_WAL_SECONDS,
    PG_BULK_GENERATE_WAL_TARGET_BYTES,
    PG_HOLD_LONG_TRANSACTION_SECONDS,
    PG_REPLICATION_SLOT_PLUGIN,
    PUBLICATION_NAME,
    PUBLICATION_TABLES,
    REPLICATION_SLOT,
    RESET_SOURCE_TIMEOUT_S,
    RETRIES_RESTART_CONNECTOR,
    SINK_CONNECTOR,
    SOURCE_CONNECTOR,
    TOXIPROXY_ADD_LATENCY_JITTER_MS,
    TOXIPROXY_ADD_LATENCY_MS,
    TOXIPROXY_HOST,
    TOXIPROXY_LISTEN_PORT,
    TOXIPROXY_PG_SOURCE_LISTEN,
    TOXIPROXY_PG_SOURCE_PROXY_NAME,
    TOXIPROXY_PG_SOURCE_UPSTREAM,
    WAIT_RUNNING_POLL,
)


class Scenario:
    """Base class for every fault scenario.

    Class attributes carry the scenario's identity and its detection
    contract (copied verbatim from zv-monitor's pattern catalogs); run()
    carries the injection/cleanup procedure.
    """

    id: str
    category: str
    description: str
    expects: list[Expectation]

    def run(self, ctx: Context) -> Generator:
        """Setup + inject, yield once (detection happens while paused),
        then clean up. Subclasses must implement this."""
        raise NotImplementedError(f"scenario {self.id!r} does not implement run()")
        yield  # pragma: no cover - makes the base run() a generator


# -- connection-class faults --------------------------------------------------
# Network path and endpoint-config problems, as opposed to the
# replication/data-plane (slot/backend/role) faults on Postgres itself.


class RoutedThroughProxy(Scenario):
    """Base for scenarios that need the source connector's traffic to flow
    through toxiproxy for the fault window: guarantees the fixture, repoints
    the connector at it, and undoes both at cleanup."""

    def route_source_through_proxy(self, ctx: Context) -> tuple[dict, str | None]:
        """Setup shared by the network scenarios: guarantee a toxiproxy is
        up (ToxiproxyFixture.ensure - reuse > start stopped service > own
        sidecar; never a compose relaunch), then repoint the source
        connector at it over the Connect REST API and wait for the task to
        be healthy THROUGH the proxy, so the fault lands on a known-good
        path. Returns (original_config, fixture_token) for cleanup. If
        anything fails after the config swap, the swap and the fixture are
        undone before re-raising - a failed setup must leave no trace
        either."""
        started = ToxiproxyFixture(ctx.docker).ensure()
        # The proxy mapping must exist BEFORE the config swap: Debezium
        # validates the JDBC connection synchronously on PUT, so pointing
        # the connector at 15432 with no listener there fails the PUT
        # outright.
        ctx.toxiproxy.ensure_proxy(
            name=TOXIPROXY_PG_SOURCE_PROXY_NAME,
            listen=TOXIPROXY_PG_SOURCE_LISTEN,
            upstream=TOXIPROXY_PG_SOURCE_UPSTREAM,
        )
        original = ctx.connect.get_config(SOURCE_CONNECTOR)
        try:
            ctx.connect.set_config(SOURCE_CONNECTOR, {
                **original,
                "database.hostname": TOXIPROXY_HOST,
                "database.port": TOXIPROXY_LISTEN_PORT,
            })
            if not ctx.connect.wait_running(
                SOURCE_CONNECTOR,
                timeout_s=90,
                poll_s=WAIT_RUNNING_POLL,
            ):
                raise RuntimeError(
                    f"{SOURCE_CONNECTOR} did not reach RUNNING through the toxiproxy - "
                    "aborting before injecting the fault"
                )
        except BaseException:
            with contextlib.suppress(Exception):
                self.unroute_source_from_proxy(ctx, original, started)
            raise
        return original, started

    def unroute_source_from_proxy(self, ctx: Context, original: dict, started: str | None):
        """Mirror of route_source_through_proxy: restore the proxy, PUT the
        original connector config back (direct postgres-source connection),
        restart, and remove the toxiproxy fixture - but only if we started
        it."""
        ctx.toxiproxy.restore(name=TOXIPROXY_PG_SOURCE_PROXY_NAME)
        ctx.toxiproxy.clear_toxics()
        ctx.connect.set_config(SOURCE_CONNECTOR, original)
        ctx.connect.restart_connector(
            SOURCE_CONNECTOR,
            include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
            retries=RETRIES_RESTART_CONNECTOR,
            backoff_s=BACKOFF_RESTART_CONNECTOR,
        )
        ToxiproxyFixture(ctx.docker).stop(started)


class NetworkCutSource(RoutedThroughProxy):
    id = "network-cut-source"
    category = "connection"
    description = (
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
    )
    expects = [
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
    ]

    def run(self, ctx: Context):
        original, started = self.route_source_through_proxy(ctx)
        try:
            ctx.toxiproxy.cut(name=TOXIPROXY_PG_SOURCE_PROXY_NAME)
            yield
        finally:
            self.unroute_source_from_proxy(ctx, original, started)


class NetworkLatencySource(RoutedThroughProxy):
    id = "network-latency-source"
    category = "connection"
    description = (
        "Injects 3s +/- 500ms latency on the source path via Toxiproxy - "
        "TCP stays up, so this should read as 'stalled', not 'dead'. "
        "Self-provisioning exactly like network-cut-source: toxiproxy "
        "fixture + connector repoint handled automatically, restored at "
        "cleanup."
    )
    expects = [
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
    ]

    def run(self, ctx: Context):
        original, started = self.route_source_through_proxy(ctx)
        try:
            ctx.toxiproxy.add_latency(
                name=TOXIPROXY_PG_SOURCE_PROXY_NAME,
                latency_ms=TOXIPROXY_ADD_LATENCY_MS,
                jitter_ms=TOXIPROXY_ADD_LATENCY_JITTER_MS,
            )
            yield
        finally:
            self.unroute_source_from_proxy(ctx, original, started)


class JdbcConnectionError(Scenario):
    id = "jdbc-connection-error"
    category = "connection"
    description = (
        "Config-plane fault: PUTs a broken connection.url onto the JDBC "
        "sink connector (wrong port). No infrastructure touched at all - "
        "this isolates 'bad config someone pushed' from 'network actually "
        "broke', which is a meaningfully different on-call story."
    )
    expects = [
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
    ]

    def run(self, ctx: Context):
        good_config = ctx.connect.get_config(SINK_CONNECTOR)
        bad_config = dict(good_config)
        bad_config["connection.url"] = "jdbc:postgresql://postgres-sink:59999/sinkdb"
        ctx.connect.set_config(SINK_CONNECTOR, bad_config)
        yield
        ctx.connect.set_config(SINK_CONNECTOR, good_config)
        ctx.connect.restart_connector(
            SINK_CONNECTOR,
            include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
            retries=RETRIES_RESTART_CONNECTOR,
            backoff_s=BACKOFF_RESTART_CONNECTOR,
        )


class KafkaBrokerDown(Scenario):
    id = "kafka-broker-down"
    category = "connection"
    description = (
        "Kills the kafka broker container outright - the connect worker "
        "loses its bootstrap connection entirely, distinct from a single "
        "connector's DB link failing. Both connectors should show it, not "
        "just one - a good check that zv-monitor's task-not-running signal "
        "isn't accidentally scoped to a single connector."
    )
    expects = [
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
    ]

    def run(self, ctx: Context):
        ctx.docker.kill("kafka", signal=DOCKER_KILL_SIGNAL)
        yield
        ctx.docker.restart("kafka", timeout=DOCKER_RESTART_TIMEOUT)
        ctx.docker.wait_running("kafka", timeout_s=60)
        ctx.connect.restart_connector(
            SOURCE_CONNECTOR,
            include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
            retries=RETRIES_RESTART_CONNECTOR,
            backoff_s=BACKOFF_RESTART_CONNECTOR,
        )
        ctx.connect.restart_connector(
            SINK_CONNECTOR,
            include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
            retries=RETRIES_RESTART_CONNECTOR,
            backoff_s=BACKOFF_RESTART_CONNECTOR,
        )


# -- replication-class faults -------------------------------------------------
# Everything that breaks the Debezium <-> Postgres logical replication link
# on the source side.
#
# Expectation queries are copied verbatim from zv-monitor/analysis/
# loki-log-patterns.yaml and prometheus-metrics.yaml so a scenario failing
# here means either the injection didn't reproduce the real condition, or
# zv-monitor's detection actually regressed - not a typo in this file.


class ReplicationSlotIssue(Scenario):
    id = "replication-slot-issue"
    category = "replication"
    description = (
        "Drops the active replication slot Debezium is streaming from. "
        "Debezium's next poll/reconnect finds the slot gone."
    )
    expects = [
        Expectation(
            id="log:replication-slot-issue",
            kind="log",
            query='{service=~"kafka-connect|postgres-source"} '
            '|~ "(?i)replication slot .* is active|logical replication slot .* does not exist|'
            'ReplicationSlotAlreadyExistsException"',
            timeout_s=60,
        ),
        Expectation(
            id="metric:source-disconnected",
            kind="metric",
            query='debezium_postgres_connector_metrics_connected{context="streaming"} == 0',
            timeout_s=60,
        ),
        Expectation(
            id="event:replication-slot-issue",
            kind="event",
            source="log",
            pattern="replication-slot-issue",
            timeout_s=90,
        ),
    ]

    def run(self, ctx: Context):
        ctx.pg.drop_replication_slot(slot_name=REPLICATION_SLOT)
        yield
        # Debezium will not recreate a slot it didn't drop itself - without
        # this the pipeline stays broken for every scenario run after this one.
        ctx.pg.recreate_replication_slot(
            slot_name=REPLICATION_SLOT,
            plugin=PG_REPLICATION_SLOT_PLUGIN,
        )
        # A freshly recreated slot's flush position is ahead of Debezium's
        # stored offset, which permanently wedges the task ("... this is no
        # longer available on the server" - no snapshot mode gets past that
        # guard). Drop the offsets so the restart re-snapshots from scratch
        # (the tables are tiny; the sink upserts by PK so it is idempotent),
        # then wait for streaming to re-attach to the new slot. The shared
        # reset helper tolerates individual step failures but always resumes,
        # so the sweep can never continue with a stopped connector.
        if not ctx.reset_source(drop_slot=True, timeout_s=RESET_SOURCE_TIMEOUT_S):
            raise RuntimeError("source connector did not resume streaming after offset reset")


class SourceConnectionTerminated(Scenario):
    id = "source-connection-terminated"
    category = "replication"
    description = (
        "Kills the backend PID behind the active replication slot without "
        "dropping the slot itself - a transient disconnect Debezium should "
        "recover from on its own by resuming from restart_lsn."
    )
    expects = [
        Expectation(
            id="log:postgres-connection-terminated",
            kind="log",
            query='{service=~"postgres-source|postgres-sink"} '
            '|~ "terminating connection|could not receive data from client|'
            'unexpected EOF on client connection|connection reset by peer"',
            timeout_s=30,
        ),
        Expectation(
            id="event:postgres-connection-terminated",
            kind="event",
            source="log",
            pattern="postgres-connection-terminated",
            timeout_s=60,
        ),
    ]

    def run(self, ctx: Context):
        ctx.pg.terminate_backend(pid=None, slot_name=REPLICATION_SLOT)
        yield
        # nothing to clean up - Debezium is expected to self-heal here; the
        # scenario is really testing that it does (watch source-disconnected
        # come back to 1 in Grafana after this run, not just that the log fired)


class SourceDisconnectLoop(Scenario):
    id = "source-disconnect-loop"
    category = "replication"
    description = (
        "Repeated backend termination - three kills 20s apart. Tests the "
        "increase()-over-5m metric, which needs >1 disconnect to fire, as "
        "opposed to source-connection-terminated's single blip."
    )
    expects = [
        Expectation(
            id="metric:source-disconnect-loop",
            kind="metric",
            query='increase(debezium_postgres_connector_metrics_numberofdisconnects'
            '{context="streaming"}[5m]) > 1',
            timeout_s=90,
        ),
        Expectation(
            id="event:source-disconnect-loop",
            kind="event",
            source="metric",
            pattern="source-disconnect-loop",
            timeout_s=120,
        ),
    ]

    def run(self, ctx: Context):
        ctx.pg.terminate_backend_loop(slot_name=REPLICATION_SLOT, times=3, interval_s=20.0)
        yield
        # self-healing expected, same as above


class ReplicationPrivilegeRevoked(Scenario):
    id = "replication-privilege-revoked"
    category = "replication"
    description = (
        "REVOKEs the REPLICATION privilege from the source user. Existing "
        "replication connections keep running (a subtle live/behavioral "
        "trap, only affects future sessions) - so this also kills the "
        "active backend to force an immediate reconnect, which is when "
        "the permission actually bites."
    )
    expects = [
        Expectation(
            id="log:replication-permission-denied",
            kind="log",
            query='{service=~"kafka-connect|postgres-source"} '
            '|~ "FATAL:  permission denied for database|must be superuser or replication role|'
            'no pg_hba.conf entry"',
            timeout_s=60,
        ),
        Expectation(
            id="event:replication-permission-denied",
            kind="event",
            source="log",
            pattern="replication-permission-denied",
            timeout_s=120,
        ),
    ]

    def run(self, ctx: Context):
        ctx.pg.revoke_replication("postgres")
        # only kicks in on a new connection, so terminate the streaming
        # backend to force an immediate reconnect attempt
        ctx.pg.terminate_backend(pid=None, slot_name=REPLICATION_SLOT)
        yield
        ctx.pg.restore_replication("postgres")
        ctx.connect.restart_connector(
            SOURCE_CONNECTOR,
            include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
            retries=RETRIES_RESTART_CONNECTOR,
            backoff_s=BACKOFF_RESTART_CONNECTOR,
        )


class PublicationDropped(Scenario):
    id = "publication-dropped"
    category = "replication"
    description = (
        "DROP PUBLICATION for the source - the replication slot survives "
        "but has nothing to read. Teaches me a distinct failure class: "
        "connection-level health checks all pass, but no data ever flows "
        "again."
    )
    expects = [
        Expectation(
            id="log:publication-dropped",
            kind="log",
            query='{service=~"kafka-connect|postgres-source"} '
            '|~ "publication .* does not exist|ERROR:  publication .* already exists"',
            timeout_s=60,
        ),
        Expectation(
            id="metric:source-stream-stalled",
            kind="metric",
            query='max_over_time(debezium_postgres_connector_metrics_millisecondssincelastevent'
            '{context="streaming"}[2m]) > 60000',
            timeout_s=150,
            poll_interval_s=5,
        ),
        Expectation(
            id="event:publication-dropped",
            kind="event",
            source="log",
            pattern="publication-dropped",
            timeout_s=90,
        ),
    ]

    def run(self, ctx: Context):
        ctx.pg.drop_publication(publication=PUBLICATION_NAME)
        # without the terminate Debezium doesn't even notice the drop until
        # its next connection - the terminate forces the issue (which is also
        # the real-world failure shape: an operator script that dropped the
        # publication AND reset connections)
        ctx.pg.terminate_backend(pid=None, slot_name=REPLICATION_SLOT)
        yield
        # Debezium never recreates a dropped publication either - without this,
        # the source stays silent forever and the metric expectation above
        # becomes meaningless noise for every scenario run after this one.
        ctx.pg.recreate_publication(publication=PUBLICATION_NAME, tables=PUBLICATION_TABLES)
        # the streaming position in the slot is past the publication drop, so
        # simply restarting leaves Debezium with no new events - drop the
        # offsets and re-snapshot (same reasoning as the slot-drop scenario)
        if not ctx.reset_source(drop_slot=True, timeout_s=RESET_SOURCE_TIMEOUT_S):
            raise RuntimeError("source connector did not resume streaming after publication reset")


class SlotWalRetentionHigh(Scenario):
    id = "slot-wal-retention-high"
    category = "replication"
    description = (
        "Holds a long transaction open against the source while generating "
        "bulk WAL (a big insert loop on a scratch table). The active "
        "replication slot cannot pass the open transaction's LSN, so WAL "
        "accumulates on-disk - retention grows without touching the "
        "connection at all. Scratched at cleanup."
    )
    expects = [
        Expectation(
            id="metric:slot-wal-retention-high",
            kind="metric",
            query='pg_replication_slots_pg_wal_lsn_diff > 100000000',
            timeout_s=180,
        ),
        Expectation(
            id="event:slot-wal-retention-high",
            kind="event",
            source="metric",
            pattern="slot-wal-retention-high",
            timeout_s=240,
        ),
    ]

    def run(self, ctx: Context):
        # The long transaction pins the WAL minimum that the slot must retain,
        # while bulk_generate_wal pushes megabytes of new WAL past it (the
        # thread joins in cleanup happen only after this scenario's
        # expectations are satisfied - the WAL keeps flowing while we poll).
        hold_seconds = 120
        t = threading.Thread(
            target=ctx.pg.hold_long_transaction,
            kwargs={"seconds": hold_seconds},
            daemon=True,
        )
        t.start()
        # The 2s sleep is a poor-man's ordering guarantee: make sure the
        # transaction is registered before the WAL generation begins, so
        # the WAL cannot possibly pass it.
        time.sleep(2.0)
        b = threading.Thread(
            target=ctx.pg.bulk_generate_wal,
            kwargs={
                "seconds": hold_seconds,
                "target_bytes": PG_BULK_GENERATE_WAL_TARGET_BYTES,
            },
            daemon=True,
        )
        b.start()
        yield
        # Keep the threads bounded (they self-terminate at hold_seconds
        # anyway; the join just guarantees they're done before we drop the
        # scratch table they may still be writing to).
        b.join(timeout=hold_seconds + 30)
        t.join(timeout=hold_seconds + 30)
        # the transactions end, retention drops back on its own - only the
        # scratch table needs dropping
        ctx.pg.drop_wal_bloat_table()


# The registry: id -> instance. Insertion order defines run-category order;
# run-all sorts by (category, id) itself.
SCENARIOS: dict[str, Scenario] = {
    cls.id: cls()
    for cls in (
        NetworkCutSource,
        NetworkLatencySource,
        JdbcConnectionError,
        KafkaBrokerDown,
        ReplicationSlotIssue,
        SourceConnectionTerminated,
        SourceDisconnectLoop,
        ReplicationPrivilegeRevoked,
        PublicationDropped,
        SlotWalRetentionHigh,
    )
}





