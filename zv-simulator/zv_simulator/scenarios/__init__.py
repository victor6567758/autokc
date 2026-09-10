"""Scenario registry.

A scenario is: inject a fault, wait for a set of Expectations to be
satisfied (or time out), then clean up - always, even on failure/error,
since a scenario that leaves the stack broken poisons every scenario run
after it.

Usage from a scenarios/*.py module:

    from zv_simulator.scenarios import scenario

    @scenario(
        id="replication-slot-issue",
        category="replication",
        expects=[...],
    )
    def run(ctx):
        ctx.pg.drop_replication_slot()
        yield  # detection happens here, verifier polls while we're paused
        ctx.pg.recreate_replication_slot()
"""
from __future__ import annotations

import time
import traceback
from dataclasses import dataclass
from typing import Callable, Generator

from zv_simulator.docker_ctl import DockerCtl
from zv_simulator.connect_rest import ConnectRest
from zv_simulator.pg_faults import PgFaults
from zv_simulator.toxiproxy_ctl import ToxiproxyCtl
from zv_simulator.verifier import Verifier, Expectation
from zv_simulator.report import ScenarioResult


@dataclass
class Context:
    docker: DockerCtl
    connect: ConnectRest
    pg: PgFaults
    toxiproxy: ToxiproxyCtl


@dataclass
class ScenarioDef:
    id: str
    category: str
    description: str
    expects: list[Expectation]
    fn: Callable[[Context], Generator]


REGISTRY: dict[str, ScenarioDef] = {}


def scenario(id: str, category: str, expects: list[Expectation], description: str = ""):
    def decorator(fn: Callable[[Context], Generator]):
        REGISTRY[id] = ScenarioDef(id, category, description or fn.__doc__ or "", expects, fn)
        return fn

    return decorator


def _make_context() -> Context:
    return Context(
        docker=DockerCtl(), connect=ConnectRest(), pg=PgFaults(), toxiproxy=ToxiproxyCtl()
    )


def run_scenario(scenario_id: str, ctx: Context | None = None) -> ScenarioResult:
    if scenario_id not in REGISTRY:
        raise KeyError(
            f"unknown scenario '{scenario_id}' - available: {', '.join(sorted(REGISTRY))}"
        )
    sd = REGISTRY[scenario_id]
    ctx = ctx or _make_context()
    gen = sd.fn(ctx)

    injected_at = time.time()
    error = None
    checks = []
    cleanup_ok = True

    try:
        next(gen)  # runs injection, pauses at `yield`
        verifier = Verifier()
        checks = verifier.check_all(sd.expects, since_ts=injected_at)
    except Exception:
        error = traceback.format_exc()
    finally:
        try:
            next(gen, None)  # runs cleanup (code after yield)
        except Exception:
            cleanup_ok = False
            error = (error or "") + "\ncleanup failed:\n" + traceback.format_exc()

    return ScenarioResult(scenario_id, injected_at, checks, cleanup_ok, error)


def run_category(category: str) -> list[ScenarioResult]:
    ctx = _make_context()
    return [
        run_scenario(sid, ctx) for sid, sd in REGISTRY.items() if sd.category == category
    ]


# Import scenario modules for their registration side-effects.
from zv_simulator.scenarios import connection_faults  # noqa: E402,F401
from zv_simulator.scenarios import replication_faults  # noqa: E402,F401
