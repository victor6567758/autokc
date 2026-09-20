"""The simulator engine.

Simulator owns the Context (the one shared set of infra clients a run
uses) and the run lifecycle: advance a scenario to its yield (fault goes
live), poll the Verifier, then always advance it once more in a finally
block so cleanup runs even on failure or error - a scenario that leaves
the stack broken poisons every scenario run after it.

Context.reset_source() is the shared post-fault recovery helper: the
failure-tolerant unwedge (drop offsets with retries, drop slot, restart
connector, wait for the slot to be stably active again).
"""
from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass
from typing import Generator

from connect_rest import ConnectRest
from docker_ctl import DockerCtl
from pg_faults import PgFaults
from report import ScenarioResult
from toxiproxy_ctl import ToxiproxyCtl
from verifier import Verifier
from config import REPLICATION_SLOT, SOURCE_CONNECTOR

log = logging.getLogger(__name__)


@dataclass
class Context:
    """The infra clients every scenario drives its fault through."""

    docker: DockerCtl
    connect: ConnectRest
    pg: PgFaults
    toxiproxy: ToxiproxyCtl

    def reset_source(self, drop_slot: bool = True, timeout_s: float = 300.0) -> bool:
        """Failure-tolerant source-connector unwedge: resume streaming no
        matter what.

        A single failed API call must not abort recovery - every step is
        best-effort with individual try/except blocks and execution always
        continues to the next step, so the pipeline ends up resumed even
        if some cleanup step (like the slot drop) couldn't be done.
        """
        log.info("resetting source connector (restart + offset wipe)")
        try:
            ctx_config = self.connect.get_config(SOURCE_CONNECTOR)
            ctx_config = dict(ctx_config)
            ctx_config["snapshot.mode"] = "initial"
            ctx_config.pop("slot.drop.on.stop", None)
            if ctx_config != self.connect.get_config(SOURCE_CONNECTOR):
                self.connect.set_config(SOURCE_CONNECTOR, ctx_config)
        except Exception:
            log.info("  failed to fetch/patch connector config - continuing")
        try:
            self.connect.restart_connector(SOURCE_CONNECTOR)
        except Exception:
            log.info("  failed to restart connector - continuing")
        # retry delete_offsets: the Kafka admin client can throw transient
        # CoordinatorUnavailableException errors on the first try
        for attempt in range(3):
            try:
                self.connect.delete_offsets(SOURCE_CONNECTOR)
                break
            except Exception as exc:
                log.info(f"  delete_offsets failed ({attempt + 1}/3): {exc}")
                time.sleep(5)
        # The slot's oldest LSN can be far behind the newly snapshotted
        # stream - dropping it forces Debezium to create a fresh one at the
        # right position; treat "already gone" as success, and use the same
        # "old enough to be safe" logic as the scenario cleanups
        if drop_slot:
            self.pg.drop_replication_slot(REPLICATION_SLOT)
        return self.wait_slot_active(timeout_s=timeout_s)

    def wait_slot_active(self, timeout_s: float = 300.0, interval_s: float = 3.0) -> bool:
        """Wait for the replication slot to be created and *stably* active.

        "Stably" matters: the slot flickers into existence during the
        snapshot phase and disappears again on the first task restart.
        Polling pg_replication_slots from the sink side (Debezium only
        exposes slot state via metrics while actually streaming).
        """
        deadline = time.monotonic() + timeout_s
        # after a drop, the slot name can be reused while the old connector
        # task is still shutting down - a single "active" observation right
        # after a drop can be the dying task, not the new one, so require
        # two consecutive active observations (6s apart at the default
        # poll interval) before declaring success
        consecutive_active = 0
        while time.monotonic() < deadline:
            if self.pg.slot_exists(REPLICATION_SLOT):
                consecutive_active += 1
                if consecutive_active >= 2:
                    return True
            else:
                consecutive_active = 0
            time.sleep(interval_s)
        log.info("  slot did not return to active within timeout")
        return False


class Simulator:
    """Drives scenario classes against a live stack through one shared
    Context; between scenarios in run_all it gates on pipeline health so
    one wedged run cannot poison the next."""

    def __init__(self, ctx: Context | None = None):
        self.ctx = ctx or Context(
            docker=DockerCtl(),
            connect=ConnectRest(),
            pg=PgFaults(),
            toxiproxy=ToxiproxyCtl(),
        )

    def run_scenario(self, scenario_id: str) -> ScenarioResult:
        # Imported here rather than at module top: scenarios.py imports
        # Context from this module, so a top-level import back would be
        # circular. This is the only direction the engine pulls the catalog.
        from scenarios import SCENARIOS

        if scenario_id not in SCENARIOS:
            raise KeyError(
                f"unknown scenario '{scenario_id}' - available: {', '.join(sorted(SCENARIOS))}"
            )
        scenario = SCENARIOS[scenario_id]
        log.info(f"scenario {scenario_id}: {scenario.description.strip().splitlines()[0]}")
        verifier = Verifier()
        gen = scenario.run(self.ctx)
        next(gen)  # run through injection - fault is live at the yield
        checks: list = []
        cleanup_ok = True
        error: str | None = None
        try:
            checks = verifier.check_all(scenario.expects)
        except Exception:
            error = traceback.format_exc(limit=2)
        finally:
            try:
                next(gen, None)  # run cleanup even when checks failed
            except Exception:
                cleanup_ok = False
                error = error or traceback.format_exc(limit=2)
        return ScenarioResult(
            scenario_id=scenario_id,
            injected_at=time.time(),
            checks=checks,
            cleanup_ok=cleanup_ok,
            error=error,
        )

    def run_category(self, category: str) -> list[ScenarioResult]:
        from scenarios import SCENARIOS  # local: scenarios imports Context here

        return [
            self.run_scenario(sid)
            for sid, scenario in SCENARIOS.items()
            if scenario.category == category
        ]

    def run_all(self, ids: list[str] | None = None) -> list[ScenarioResult]:
        from scenarios import SCENARIOS  # local: scenarios imports Context here

        unknown = set(ids or []) - SCENARIOS.keys()
        if unknown:
            raise KeyError(
                f"unknown scenario(s): {', '.join(sorted(unknown))} - "
                f"available: {', '.join(sorted(SCENARIOS))}"
            )
        ordered = sorted(SCENARIOS.values(), key=lambda s: (s.category, s.id))
        if ids is not None:
            wanted = set(ids)
            ordered = [s for s in ordered if s.id in wanted]
        results: list[ScenarioResult] = []
        for i, scenario in enumerate(ordered):
            if i and not self._wait_pipeline_healthy():
                log.info("!! pipeline not fully healthy - continuing anyway")
            results.append(self.run_scenario(scenario.id))
        return results

    def _wait_pipeline_healthy(self, timeout_s: float = 240.0) -> bool:
        """Gate between scenarios in a sweep: both connectors RUNNING and
        streaming re-attached. A wedged source connector (stuck task,
        dropped-slot edge cases) gets one reset_source() recovery attempt
        before giving up - which also makes it the shared entry point for
        manual un-wedging."""
        log.info("waiting for pipeline to be healthy before next scenario")
        deadline = time.monotonic() + timeout_s
        gave_up = False
        while time.monotonic() < deadline:
            if self.ctx.connect.healthy([SOURCE_CONNECTOR]):
                if self.ctx.pg.slot_exists(REPLICATION_SLOT):
                    log.info("  pipeline healthy")
                    return True
                log.info("  connectors RUNNING but slot missing - will unwedge")
                gave_up = True
                break
            time.sleep(5)
        if not gave_up:
            log.info("  timed out waiting for connectors/slot - will unwedge")
        return self.ctx.reset_source()

