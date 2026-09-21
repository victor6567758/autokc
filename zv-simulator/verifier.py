"""Closes the loop: after injecting a fault, poll the same read paths
zv-monitor itself uses, plus zv-monitor's own output, to confirm the
fault was actually detected - not just that it happened.

Three independent checks per scenario, each answering a different
question:
  1. Loki match       - was the raw signal even observable in logs?
  2. Prometheus match  - did the metric cross zv-monitor's threshold?
  3. zv-monitor event  - did zv-monitor itself produce an Event?
(1)/(2) can pass while (3) fails - that's a real bug in zv-monitor, not a
failed injection, and exactly the kind of gap this tool exists to catch.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from config import EXPECT_TIMEOUT_SCALE

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    matched: bool
    first_match_at: float | None = None  # unix ts, None if never matched
    detail: str = ""


@dataclass
class Expectation:
    """One thing a scenario should cause. `kind` selects which client polls
    it: "log" (Loki), "metric" (Prometheus) or "event" (zv-monitor's own
    output). `query` is a LogQL selector (log) or PromQL expression (metric);
    event expectations instead name the zv-monitor pattern - `source` is the
    event origin ("log" or "metric") and `pattern` a pattern id from the same
    catalogs the log/metric queries come from (loki-log-patterns.yaml /
    prometheus-metrics.yaml). A `pattern` containing "|" is treated as a
    label regex, e.g. "npe|task-uncaught-exception"."""

    id: str
    kind: str  # "log" | "metric" | "event"
    query: str = ""  # log/metric only
    source: str = "log"  # event only: zv-monitor event origin
    pattern: str = ""  # event only: pattern id, or "a|b" for a regex match
    timeout_s: float = 45.0
    poll_interval_s: float = 2.0

    def __post_init__(self):
        # EXPECT_TIMEOUT_SCALE (config.py: ZV_SIM_EXPECT_TIMEOUT_SCALE)
        # scales every budget uniformly - e.g. 0.5 halves them all for a
        # quick interactive pass - without rewriting the per-scenario
        # values, which encode real zv-monitor detection latencies.
        self.timeout_s *= EXPECT_TIMEOUT_SCALE


# The "fault live" anchor (injected_at) is captured when the injection
# generator yields - i.e. AFTER the fault call returned. Synchronous faults
# (e.g. pg_terminate_backend, which makes postgres write its FATAL line
# during the call) produce causal log lines stamped milliseconds BEFORE the
# anchor, so an exact `line_ts > anchor` / `start=anchor` comparison drops
# them. Poll with a small grace before the anchor; it must stay far below
# the poll intervals (2s) and zv-monitor's own scrape cycle (5s) so setup
# restart noise (the reason the anchor is post-injection) still can't
# satisfy an expectation.
ANCHOR_GRACE_S = 0.25

# Budget for one Loki/Prometheus HTTP call. Tight on purpose: a wedged
# query fails fast and visibly (check()'s RequestException handler logs it
# and keeps polling) instead of stalling the verify phase.
HTTP_TIMEOUT_S = 5.0

# INFO heartbeat cadence while polling - long budgets must never look hung.
HEARTBEAT_INTERVAL_S = 10.0


class LokiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def query_since(self, logql: str, since_ts: float) -> list[dict]:
        """Range query from since_ts to now. LogQL, e.g.:
        '{service=~"kafka-connect"} |~ "(?i)nullpointerexception"'
        (promtail labels streams with the compose service name, same label
        zv-monitor's LogPattern.containerSelectorRegex matches against)
        """
        params = {
            "query": logql,
            # ANCHOR_GRACE_S back so causal lines written during a synchronous
            # injection (stamped just before the anchor) are inside the range
            "start": str(int((since_ts - ANCHOR_GRACE_S) * 1e9)),
            "end": str(int(time.time() * 1e9)),
            "limit": 50,
        }
        r = requests.get(
            f"{self.base_url}/loki/api/v1/query_range", params=params, timeout=HTTP_TIMEOUT_S
        )
        r.raise_for_status()
        result = r.json().get("data", {}).get("result", [])
        entries = []
        for stream in result:
            for ts_ns, line in stream.get("values", []):
                entries.append({"ts": int(ts_ns) / 1e9, "line": line})
        return entries


class PrometheusClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def instant(self, promql: str) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/query", params={"query": promql}, timeout=HTTP_TIMEOUT_S
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("result", [])


class Verifier:
    def __init__(self, loki_url: str, prom_url: str):
        self.loki = LokiClient(base_url=loki_url)
        self.prom = PrometheusClient(base_url=prom_url)

    def check(self, expectation: Expectation, since_ts: float) -> CheckResult:
        started = time.time()
        last_beat = started
        deadline = started + expectation.timeout_s
        checks = 0
        log.info(f"    checking {expectation.kind} expectation '{expectation.id}' (timeout {expectation.timeout_s}s)")
        while time.time() < deadline:
            checks += 1
            try:
                if expectation.kind == "log":
                    entries = self.loki.query_since(expectation.query, since_ts)
                    if entries:
                        first = min(e["ts"] for e in entries)
                        log.info(
                            f"  + {expectation.id}: MATCH in Loki (check {checks}),"
                            f" fired after {max(0.0, first - since_ts):.1f}s"
                            f" ({len(entries)} matching line(s))"
                        )
                        return CheckResult(
                            expectation.id, True, first, f"{len(entries)} matching line(s)"
                        )
                elif expectation.kind == "metric":
                    result = self.prom.instant(expectation.query)
                    if result:
                        now = time.time()
                        log.info(
                            f"  + {expectation.id}: MATCH in Prometheus (check {checks}),"
                            f" fired after {now - since_ts:.1f}s"
                            f" ({len(result)} series matched)"
                        )
                        return CheckResult(
                            expectation.id, True, now, f"{len(result)} series matched"
                        )
                elif expectation.kind == "event":
                    fired_at = self.event_fired_since(expectation, since_ts)
                    if fired_at is not None:
                        log.info(
                            f"  + {expectation.id}: MATCH from zv-monitor (check {checks}),"
                            f" fired after {max(0.0, fired_at - since_ts):.1f}s"
                            f" (zv-monitor emitted {expectation.source}/{expectation.pattern})"
                        )
                        return CheckResult(
                            expectation.id,
                            True,
                            fired_at,
                            f"zv-monitor emitted {expectation.source}/{expectation.pattern}",
                        )
                else:
                    raise ValueError(f"unknown expectation kind: {expectation.kind!r}")
            except requests.RequestException as e:
                # A flaky/wedged Loki/Prometheus call must not abort the
                # whole verify phase silently - log it and keep polling
                # within the budget (HTTP_TIMEOUT_S bounds each attempt).
                log.warning(f"      {expectation.id}: query failed (check {checks}): {e}")
            now = time.time()
            # one DEBUG line per check (ZV_SIM_DEBUG=1) so progress - or the
            # absence of it - is observable on every poll, never just every 10th
            log.debug(
                f"      {expectation.id}: no match yet"
                f" (check {checks}, {now - started:.0f}s/{expectation.timeout_s:.0f}s)"
            )
            if now - last_beat >= HEARTBEAT_INTERVAL_S:  # heartbeat so long budgets don't look hung
                log.info(
                    f"  ... {expectation.id}: no match yet"
                    f" ({now - started:.0f}s/{expectation.timeout_s:.0f}s)"
                )
                last_beat = now
            time.sleep(expectation.poll_interval_s)
        log.info(f"  x {expectation.id} timed out after {expectation.timeout_s:.0f}s")
        return CheckResult(expectation.id, False, None, f"timed out after {expectation.timeout_s}s")

    # zv-monitor's own output, exported by its JMX sidecar
    # (development/monitoring/jmx-exporter/zv-monitor-jmx.yml): one series
    # per (source, pattern) event counter, value = epoch-ms of the last hit.
    EVENT_LAST_TS_METRIC = "zv_event_counter_lastepochmillis"

    def event_fired_since(self, expectation: Expectation, since_ts: float) -> float | None:
        """Unix ts of the freshest matching zv-monitor event fired after
        since_ts, or None if none did.

        Anchors on EventCounter's LastEpochMillis, which zv-monitor refreshes
        on every hit (EventCounter.recordHit) with the matched line's own
        timestamp, so events that fired *before* injection (stack bring-up
        chaos, connector-unhealthy flapping) can never satisfy the check - no
        baseline snapshot or increase() window arithmetic needed. The matched
        value is zv-monitor's own event timestamp, so the latency reported
        for an event check is the true detection latency, not Prometheus
        scrape time.
        """
        if not expectation.pattern:
            raise ValueError(f"event expectation {expectation.id!r} needs a pattern")
        op = "=~" if "|" in expectation.pattern else "="
        threshold_ms = int((since_ts - ANCHOR_GRACE_S) * 1000)
        promql = (
            f"max({self.EVENT_LAST_TS_METRIC}{{source=\"{expectation.source}\","
            f"pattern{op}\"{expectation.pattern}\"}}) > {threshold_ms}"
        )
        result = self.prom.instant(promql)
        if not result:
            return None
        return float(result[0]["value"][1]) / 1000.0

    def check_all(self, expectations: list[Expectation], since_ts: float) -> list[CheckResult]:
        # Sequential is fine here - scenarios run one at a time and each
        # expectation's own timeout bounds total wall time; parallelize
        # with a thread pool later if that gets slow.
        return [self.check(e, since_ts) for e in expectations]
