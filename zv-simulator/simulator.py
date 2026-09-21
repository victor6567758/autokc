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

from connect_rest import ConnectRest
from docker_ctl import DockerCtl
from pg_faults import PgFaults
from report import ScenarioResult
from toxiproxy_ctl import ToxiproxyCtl
from verifier import Verifier
from config import (
    BACKOFF_DELETE_OFFSETS,
    BACKOFF_RESTART_CONNECTOR,
    COMPOSE_PROJECT,
    CONNECT_REST_RESTART_INCLUDE_TASKS,
    CONNECT_REST_TIMEOUT,
    CONNECT_REST_URL,
    LOKI_URL,
    PG_SINK,
    PG_SOURCE,
    PROMETHEUS_URL,
    RESET_SOURCE_DROP_SLOT,
    RESET_SOURCE_TIMEOUT_S,
    REPLICATION_SLOT,
    RETRIES_DELETE_OFFSETS,
    RETRIES_RESTART_CONNECTOR,
    SINK_CONNECTOR,
    SOURCE_CONNECTOR,
    TOXIPROXY_TIMEOUT,
    TOXIPROXY_URL,
    WAIT_PIPELINE_HEALTHY_TIMEOUT_S,
    WAIT_RUNNING_POLL,
    WAIT_SLOT_ACTIVE_INTERVAL_S,
    WAIT_STOPPED_POLL,
    WAIT_STOPPED_TIMEOUT,
)

log = logging.getLogger(__name__)


@dataclass
class Context:
    """The infra clients every scenario drives its fault through."""

    docker: DockerCtl
    connect: ConnectRest
    pg: PgFaults
    toxiproxy: ToxiproxyCtl

    def reset_source(self, drop_slot: bool, timeout_s: float) -> bool:
        """Failure-tolerant source-connector unwedge: resume streaming no
        matter what.

        A single failed API call must not abort recovery - every step is
        best-effort with individual try/except blocks and execution always
        continues to the next step, so the pipeline ends up resumed even
        if some cleanup step (like the slot drop) couldn't be done.
        """
        log.info(f"resetting source connector (drop_slot={drop_slot}, timeout={timeout_s}s)")
        log.info("  patching connector config")
        ctx_config: dict | None = None
        try:
            ctx_config = self.connect.get_config(SOURCE_CONNECTOR)
            ctx_config = dict(ctx_config)
            ctx_config["snapshot.mode"] = "initial"
            ctx_config.pop("slot.drop.on.stop", None)
            if ctx_config != self.connect.get_config(SOURCE_CONNECTOR):
                self.connect.set_config(SOURCE_CONNECTOR, ctx_config)
        except Exception:
            log.info("  failed to fetch/patch connector config - continuing")
        # Connect refuses offset deletes unless the connector is fully
        # STOPPED - a 400 "must be in the STOPPED state" is a state error,
        # not a transient one, so no amount of retrying cures it. stop() is
        # asynchronous (202 only means accepted), hence wait_stopped().
        try:
            log.info("  stopping connector")
            self.connect.stop(SOURCE_CONNECTOR)
            if not self.connect.wait_stopped(
                SOURCE_CONNECTOR,
                timeout_s=WAIT_STOPPED_TIMEOUT,
                poll_s=WAIT_STOPPED_POLL,
            ):
                log.info("  connector did not reach STOPPED in time - wiping anyway")
            else:
                log.info("  connector stopped")
        except Exception:
            log.info("  failed to stop connector - continuing")
        # retry delete_offsets: the Kafka admin client can throw transient
        # CoordinatorUnavailableException errors on the first try
        log.info("  deleting stored offsets (max 3 attempts)")
        for attempt in range(3):
            try:
                self.connect.delete_offsets(
                    SOURCE_CONNECTOR,
                    retries=RETRIES_DELETE_OFFSETS,
                    backoff_s=BACKOFF_DELETE_OFFSETS,
                )
                log.info("  offsets deleted")
                break
            except Exception as exc:
                log.info(f"  delete_offsets attempt {attempt + 1}/3 failed: {exc}")
                if attempt < 2:
                    time.sleep(5)
        # bring the connector back up; if a plain restart does not take
        # (older workers refuse to restart a STOPPED connector), re-PUT the
        # patched config - a config PUT is create-or-restart by definition.
        try:
            log.info("  restarting connector")
            self.connect.restart_connector(
                SOURCE_CONNECTOR,
                include_tasks=CONNECT_REST_RESTART_INCLUDE_TASKS,
                retries=RETRIES_RESTART_CONNECTOR,
                backoff_s=BACKOFF_RESTART_CONNECTOR,
            )
            log.info("  waiting for connector to reach RUNNING")
            if not self.connect.wait_running(
                SOURCE_CONNECTOR,
                timeout_s=90,
                poll_s=WAIT_RUNNING_POLL,
            ):
                raise RuntimeError("connector did not reach RUNNING after restart")
            log.info("  connector restarted and healthy")
        except Exception:
            log.info("  restart did not take - re-PUTting config as fallback")
            try:
                self.connect.set_config(SOURCE_CONNECTOR, ctx_config or {})
                log.info("  config restored")
            except Exception:
                log.info("  failed to revive connector - continuing")
        # The slot's oldest LSN can be far behind the newly snapshotted
        # stream - dropping it forces Debezium to create a fresh one at the
        # right position; treat "already gone" as success, and use the same
        # "old enough to be safe" logic as the scenario cleanups
        if drop_slot:
            log.info("  dropping replication slot")
            try:
                self.pg.drop_replication_slot(slot_name=REPLICATION_SLOT)
                log.info("  slot dropped")
            except Exception as e:
                log.info(f"  failed to drop slot: {e}")
        log.info(f"  waiting for slot to become active (timeout {timeout_s}s)")
        return self.wait_slot_active(timeout_s=timeout_s, interval_s=WAIT_SLOT_ACTIVE_INTERVAL_S)

    def wait_slot_active(self, timeout_s: float, interval_s: float) -> bool:
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
        checks = 0
        while time.monotonic() < deadline:
            if self.pg.slot_exists(REPLICATION_SLOT):
                consecutive_active += 1
                if consecutive_active >= 2:
                    log.info(f"  slot active after {checks} checks")
                    return True
            else:
                consecutive_active = 0
            checks += 1
            time.sleep(interval_s)
        log.info(f"  slot did not return to active within {timeout_s}s ({checks} checks)")
        return False


class Simulator:
    """Drives scenario classes against a live stack through one shared
    Context; between scenarios in run_all it gates on pipeline health so
    one wedged run cannot poison the next."""

    def __init__(self, ctx: Context | None):
        self.ctx = ctx or Context(
            docker=DockerCtl(compose_project=COMPOSE_PROJECT),
            connect=ConnectRest(
                base_url=CONNECT_REST_URL,
                timeout=CONNECT_REST_TIMEOUT,
            ),
            pg=PgFaults(source_cfg=PG_SOURCE, sink_cfg=PG_SINK),
            toxiproxy=ToxiproxyCtl(
                base_url=TOXIPROXY_URL,
                timeout=TOXIPROXY_TIMEOUT,
            ),
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
        verifier = Verifier(loki_url=LOKI_URL, prom_url=PROMETHEUS_URL)
        log.info(f"  [setup] initializing scenario")
        gen = scenario.run(self.ctx)
        log.info(f"  [inject] injecting fault")
        next(gen)  # run through injection - fault is live at the yield
        injected_at = time.time()  # expectations anchor to fault-live, not setup
        checks: list = []
        cleanup_ok = True
        error: str | None = None
        try:
            log.info(f"  [verify] polling for expectations")
            checks = verifier.check_all(scenario.expects, since_ts=injected_at)
        except Exception:
            error = traceback.format_exc(limit=2)
            # never swallow this silently - an aborted verify phase with no
            # middle logs is exactly what looks like a hang
            log.warning(f"  [verify] aborted by an error:\n{error}")
        finally:
            try:
                log.info(f"  [cleanup] running cleanup")
                next(gen, None)  # run cleanup even when checks failed
            except Exception:
                cleanup_ok = False
                tb = traceback.format_exc(limit=2)
                error = error or tb
                log.warning(f"  [cleanup] failed:\n{tb}")
        log.info(f"  [done] scenario complete")
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

    def run_all(self, ids: list[str] | None) -> list[ScenarioResult]:
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
            if i and not self._wait_pipeline_healthy(timeout_s=WAIT_PIPELINE_HEALTHY_TIMEOUT_S):
                log.info("!! pipeline not fully healthy - continuing anyway")
            results.append(self.run_scenario(scenario.id))
        return results

    def _wait_pipeline_healthy(self, timeout_s: float) -> bool:
        """Gate between scenarios in a sweep: both connectors RUNNING and
        streaming re-attached. A wedged source connector (stuck task,
        dropped-slot edge cases) gets one reset_source() recovery attempt
        before giving up - which also makes it the shared entry point for
        manual un-wedging."""
        log.info("waiting for pipeline to be healthy before next scenario")
        deadline = time.monotonic() + timeout_s
        checks = 0
        gave_up = False
        while time.monotonic() < deadline:
            checks += 1
            log.debug(f"  health check {checks}: connectors...")
            if self.ctx.connect.healthy([SOURCE_CONNECTOR, SINK_CONNECTOR]):
                log.debug(f"  health check {checks}: checking replication slot...")
                if self.ctx.pg.slot_exists(REPLICATION_SLOT):
                    log.info(f"  pipeline healthy (after {checks} checks)")
                    return True
                log.info("  connectors RUNNING but slot missing - will unwedge")
                gave_up = True
                break
            time.sleep(5)
        if not gave_up:
            log.info(f"  timed out waiting for pipeline after {checks} checks - will unwedge")
        return self.ctx.reset_source(drop_slot=RESET_SOURCE_DROP_SLOT, timeout_s=RESET_SOURCE_TIMEOUT_S)

