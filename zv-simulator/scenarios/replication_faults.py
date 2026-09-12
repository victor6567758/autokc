"""Replication-class faults: everything that breaks the Debezium <->
Postgres logical replication link on the source side.

Expectation queries are copied verbatim from zv-monitor/analysis/
loki-log-patterns.yaml and prometheus-metrics.yaml so a scenario failing
here means either the injection didn't reproduce the real condition, or
zv-monitor's detection actually regressed - not a typo in this file.
"""
from __future__ import annotations

import threading

from scenarios import scenario, Context
from verifier import Expectation
from config import REPLICATION_SLOT, PUBLICATION_NAME


@scenario(
    id="replication-slot-issue",
    category="replication",
    description=(
        "Drops the active replication slot Debezium is streaming from. "
        "Debezium's next poll/reconnect finds the slot gone."
    ),
    expects=[
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
    ],
)
def run_slot_drop(ctx: Context):
    ctx.pg.drop_replication_slot(REPLICATION_SLOT)
    yield
    # Debezium will not recreate a slot it didn't drop itself - without
    # this the pipeline stays broken for every scenario run after this one.
    ctx.pg.recreate_replication_slot(REPLICATION_SLOT)
    ctx.connect.restart_connector("inventory-source")


@scenario(
    id="source-connection-terminated",
    category="replication",
    description=(
        "Kills the backend PID behind the active replication slot without "
        "dropping the slot itself - a transient disconnect Debezium should "
        "recover from on its own by resuming from restart_lsn."
    ),
    expects=[
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
    ],
)
def run_terminate_backend(ctx: Context):
    ctx.pg.terminate_backend(slot_name=REPLICATION_SLOT)
    yield
    # nothing to clean up - Debezium is expected to self-heal here; the
    # scenario is really testing that it does (watch source-disconnected
    # come back to 1 in Grafana after this run, not just that the log fired)


@scenario(
    id="source-disconnect-loop",
    category="replication",
    description=(
        "Repeated backend termination - three kills 20s apart. Tests the "
        "increase()-over-5m metric, which needs >1 disconnect to fire, as "
        "opposed to source-connection-terminated's single blip."
    ),
    expects=[
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
    ],
)
def run_disconnect_loop(ctx: Context):
    ctx.pg.terminate_backend_loop(slot_name=REPLICATION_SLOT, times=3, interval_s=20.0)
    yield
    # self-healing expected, same as above


@scenario(
    id="replication-privilege-revoked",
    category="replication",
    description=(
        "Strips REPLICATION from the connecting role mid-flight. Unlike "
        "the slot/backend faults above, every reconnect attempt now fails "
        "auth, not transport - a different log signature worth telling "
        "apart from a dropped slot or a killed connection."
    ),
    expects=[
        Expectation(
            id="log:postgres-fatal",
            kind="log",
            query='{service=~"postgres-source|postgres-sink"} |~ "FATAL:"',
            timeout_s=60,
        ),
        Expectation(
            id="event:postgres-fatal",
            kind="event",
            source="log",
            pattern="postgres-fatal",
            timeout_s=90,
        ),
    ],
)
def run_revoke_replication(ctx: Context):
    ctx.pg.revoke_replication("postgres")
    yield
    ctx.pg.restore_replication("postgres")
    ctx.connect.restart_connector("inventory-source")


@scenario(
    id="publication-dropped",
    category="replication",
    description=(
        "Drops the publication backing the slot without touching the slot "
        "itself - config-drift fault distinct from replication-slot-issue: "
        "the slot exists but table resolution fails."
    ),
    expects=[
        Expectation(
            id="log:npe-or-uncaught",
            kind="log",
            query='{service=~"kafka-connect"} '
            '|~ "(?i)nullpointerexception|Task threw an uncaught and unrecoverable exception"',
            timeout_s=60,
        ),
        Expectation(
            # either zv-monitor pattern can fire depending on how Debezium
            # dies on a missing publication - "|" makes the label a regex.
            id="event:npe-or-uncaught",
            kind="event",
            source="log",
            pattern="npe|task-uncaught-exception",
            timeout_s=90,
        ),
    ],
)
def run_drop_publication(ctx: Context):
    ctx.pg.drop_publication(PUBLICATION_NAME)
    yield
    ctx.pg.recreate_publication(PUBLICATION_NAME)
    ctx.connect.restart_connector("inventory-source")


@scenario(
    id="slot-wal-retention-high",
    category="replication",
    description=(
        "Holds a long-lived open transaction on source while write traffic "
        "keeps flowing (run scripts/simulate-changes.sh alongside this), "
        "preventing the slot's restart_lsn from advancing. WAL piles up "
        "behind the slot. Needs sustained traffic to actually cross the "
        "1GiB threshold - budget several minutes, not seconds."
    ),
    expects=[
        Expectation(
            id="metric:slot-wal-retention-high",
            kind="metric",
            query="zv_sql_wal_lsn_diff > 1073741824",
            timeout_s=600,
            poll_interval_s=10,
        ),
        Expectation(
            id="event:slot-wal-retention-high",
            kind="event",
            source="metric",
            pattern="slot-wal-retention-high",
            timeout_s=720,
            poll_interval_s=10,
        ),
    ],
)
def run_wal_retention(ctx: Context):
    hold_seconds = 300
    t = threading.Thread(target=ctx.pg.hold_long_transaction, args=(hold_seconds,), daemon=True)
    t.start()
    yield
    t.join(timeout=hold_seconds + 30)
    # transaction rolls back and releases on its own inside
    # hold_long_transaction; nothing further to clean up
