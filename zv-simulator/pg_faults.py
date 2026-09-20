"""Direct SQL fault injection against postgres-source / postgres-sink.

Every method here is a *data-plane* fault - it manipulates real Postgres
state (replication slots, backends, roles) rather than touching containers
or Connect config. This is the category most likely to reproduce
production incidents faithfully, since it's the same mechanism Postgres
itself would use to fail (a slot getting dropped by an operator, a
connection getting reaped by a pooler, a role losing a grant).
"""
from __future__ import annotations

import time

import psycopg2
import psycopg2.extensions

from config import (
    PG_SOURCE,
    PG_SINK,
    REPLICATION_SLOT,
    PUBLICATION_NAME,
    WAL_BLOAT_TABLE,
)


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

    def wait_slot_active(self, slot_name: str = REPLICATION_SLOT, timeout_s: float = 120.0) -> bool:
        """Wait until a walsender is attached to the slot again - the point
        where Debezium has finished (re-)snapshotting and gone back to
        streaming. Poll only, never touches server state."""
        import time

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.replication_pid(slot_name) is not None:
                return True
            time.sleep(2.0)
        return False

    # -- faults -----------------------------------------------------------

    def drop_replication_slot(self, slot_name: str = REPLICATION_SLOT):
        """Drops the slot Debezium is streaming from. If the slot is
        currently active (attached), Postgres refuses the drop with an
        error unless the streaming connection is terminated first - so
        this pairs naturally with terminate_backend(). Terminating a
        backend is asynchronous, so the drop is retried a few times
        (re-terminating each round) until Postgres releases the slot.
        Expect:
          log:  replication-slot-issue
                ('logical replication slot ... does not exist' on
                 the connector's next reconnect attempt)
          metric: source-disconnected -> 1
        """
        last_err: Exception | None = None
        for _ in range(5):
            pid = self.replication_pid(slot_name)
            with _connect(self.source_cfg) as conn, conn.cursor() as cur:
                if pid:
                    cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            time.sleep(1.0)  # let the walsender actually exit
            try:
                with _connect(self.source_cfg) as conn, conn.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
                return
            except psycopg2.errors.ObjectInUse as exc:
                last_err = exc
            except psycopg2.errors.UndefinedObject:
                return  # slot already gone - the desired end state
        raise RuntimeError(
            f"could not drop replication slot {slot_name!r}: {last_err}"
        )

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

    def bulk_generate_wal(self, seconds: int = 300, target_bytes: int = 2 * 1024**3):
        """Fills the WAL with bulk heap writes while hold_long_transaction()
        pins the slot's restart_lsn, so retention crosses any realistic
        threshold (zv-monitor fires at 1 GiB) in about a minute instead of
        hours. The writes go to a dedicated table that is NOT part of the
        publication: the physical WAL still piles up behind the pinned slot,
        but Debezium never decodes it and the sink never sees it - the fault
        stays isolated to the slot-retention metric it is meant to exercise.
        Blocking call - run in a background thread alongside the hold.
        """
        import time

        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {WAL_BLOAT_TABLE} (id int, pad text)")
            cur.execute(f"TRUNCATE {WAL_BLOAT_TABLE}")
            cur.execute("SELECT pg_current_wal_lsn()")
            start_lsn = cur.fetchone()[0]
            written = 0
            deadline = time.time() + seconds
            # ~5 MB of WAL per batch (2500 rows x ~2 KB) in autocommit mode,
            # so each batch is its own small transaction - restart_lsn stays
            # pinned behind the concurrent hold, not behind this writer.
            while time.time() < deadline and written < target_bytes:
                cur.execute(
                    f"INSERT INTO {WAL_BLOAT_TABLE} "
                    "SELECT g, repeat(md5(g::text), 60) FROM generate_series(1, 2500) g"
                )
                cur.execute(
                    # pg_wal_lsn_diff(end, start) - there is no one-argument
                    # pg_current_wal_lsn_diff() in Postgres; measuring the
                    # distance from the batch loop's starting LSN.
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)",
                    (start_lsn,),
                )
                written = int(cur.fetchone()[0])

    def drop_wal_bloat_table(self):
        with _connect(self.source_cfg) as conn, conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {WAL_BLOAT_TABLE}")

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
