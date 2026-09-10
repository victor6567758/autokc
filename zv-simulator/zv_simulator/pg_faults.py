"""Direct SQL fault injection against postgres-source / postgres-sink.

Every method here is a *data-plane* fault - it manipulates real Postgres
state (replication slots, backends, roles) rather than touching containers
or Connect config. This is the category most likely to reproduce
production incidents faithfully, since it's the same mechanism Postgres
itself would use to fail (a slot getting dropped by an operator, a
connection getting reaped by a pooler, a role losing a grant).
"""
from __future__ import annotations

import psycopg2
import psycopg2.extensions

from zv_simulator import PG_SOURCE, PG_SINK, REPLICATION_SLOT, PUBLICATION_NAME


def _connect(cfg: dict, autocommit: bool = True):
    conn = psycopg2.connect(**cfg)
    conn.autocommit = autocommit
    return conn


class PgFaults:
    def __init__(self, source_cfg: dict = PG_SOURCE, sink_cfg: dict = PG_SINK):
        self.source_cfg = source_cfg
        self.sink_cfg = sink_cfg

    # -- introspection --------------------------------------------------

    def list_replication_slots(self) -> list[dict]:
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT slot_name, active, active_pid, wal_status, "
                "restart_lsn FROM pg_replication_slots"
            )
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def replication_pid(self, slot_name: str = REPLICATION_SLOT) -> int | None:
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name,),
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    # -- faults -----------------------------------------------------------

    def drop_replication_slot(self, slot_name: str = REPLICATION_SLOT):
        """Drops the slot Debezium is streaming from. If the slot is
        currently active (attached), Postgres refuses the drop with an
        error unless the streaming connection is terminated first - so
        this pairs naturally with terminate_backend(). Expect:
          log:  replication-slot-issue
                ('logical replication slot ... does not exist' on
                 the connector's next reconnect attempt)
          metric: source-disconnected -> 1
        """
        pid = self.replication_pid(slot_name)
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            if pid:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))

    def recreate_replication_slot(
        self, slot_name: str = REPLICATION_SLOT, plugin: str = "pgoutput"
    ):
        """Cleanup for drop_replication_slot - without this the connector
        stays broken forever (Debezium does not auto-recreate a manually
        dropped slot)."""
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name,)
            )
            if cur.fetchone():
                return  # already exists, nothing to do
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, %s)",
                (slot_name, plugin),
            )

    def terminate_backend(self, pid: int | None = None, slot_name: str = REPLICATION_SLOT):
        """Kills the replication connection's backend without dropping the
        slot - Debezium should reconnect and resume from restart_lsn.
        Called once: a transient blip. Called repeatedly (see
        terminate_backend_loop): a disconnect storm.
          log:  postgres-connection-terminated
          metric: source-disconnect-loop (only fires on repeated calls,
                  needs 2+ within 5m per the promql's increase() window)
        """
        pid = pid or self.replication_pid(slot_name)
        if pid is None:
            raise RuntimeError(
                f"no active backend for slot '{slot_name}' - is the source "
                f"connector RUNNING and streaming?"
            )
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))

    def terminate_backend_loop(
        self, slot_name: str = REPLICATION_SLOT, times: int = 3, interval_s: float = 20.0
    ):
        import time

        for _ in range(times):
            try:
                self.terminate_backend(slot_name=slot_name)
            except RuntimeError:
                pass  # connector may still be reconnecting from the last kill
            time.sleep(interval_s)

    def revoke_replication(self, role: str = "postgres"):
        """Strips REPLICATION privilege from the role Debezium connects as.
        Unlike terminate_backend, this makes every reconnect attempt fail
        with an auth-flavored error rather than a transport one - a
        different failure signature worth distinguishing.
          log: postgres-fatal ('FATAL:' - insufficient privilege)
        NOTE: this also blocks normal (non-replication) connections as
        that role only if you also revoke LOGIN - we deliberately only
        touch REPLICATION so pg_isready/healthchecks keep passing and the
        fault stays isolated to the CDC path.
        """
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(f"ALTER ROLE {role} NOREPLICATION")

    def restore_replication(self, role: str = "postgres"):
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(f"ALTER ROLE {role} REPLICATION")

    def hold_long_transaction(self, seconds: int = 180):
        """Opens a transaction on source and holds it via pg_sleep(),
        without committing. A long-open transaction on a table covered by
        the publication prevents the slot's restart_lsn from advancing
        even as WAL keeps being generated by simulate-changes.sh traffic.
          metric: slot-wal-retention-high (zv_sql_wal_lsn_diff > 1GiB -
                  needs sustained write volume during the hold to actually
                  cross the threshold; run scripts/simulate-changes.sh
                  with a short INTERVAL alongside this)
        Blocking call - run in a background thread/process if you need
        the main flow to continue (see cli.py's --async flag).
        """
        conn = _connect(self.source_cfg, autocommit=False)
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT pg_sleep(%s)", (seconds,))
        conn.rollback()
        conn.close()

    def drop_publication(self, publication: str = PUBLICATION_NAME):
        """Removes the publication backing the slot - a config-drift fault
        distinct from dropping the slot itself: the slot still exists, but
        Debezium's next snapshot/streaming call fails resolving tables.
        """
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(f"DROP PUBLICATION IF EXISTS {publication}")

    def recreate_publication(
        self, publication: str = PUBLICATION_NAME, tables: str = "public.customers, public.orders"
    ):
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,)
            )
            if cur.fetchone():
                return
            cur.execute(f"CREATE PUBLICATION {publication} FOR TABLE {tables}")
