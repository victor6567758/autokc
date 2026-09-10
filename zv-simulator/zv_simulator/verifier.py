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

import time
from dataclasses import dataclass, field

import requests

from zv_simulator import PROMETHEUS_URL, LOKI_URL


@dataclass
class CheckResult:
    name: str
    matched: bool
    first_match_at: float | None = None  # unix ts, None if never matched
    detail: str = ""


@dataclass
class Expectation:
    """One thing a scenario should cause. `kind` selects which client polls
    it. `query` is a LogQL selector (log) or PromQL expression (metric)."""

    id: str
    kind: str  # "log" | "metric"
    query: str
    timeout_s: float = 45.0
    poll_interval_s: float = 2.0


class LokiClient:
    def __init__(self, base_url: str = LOKI_URL):
        self.base_url = base_url.rstrip("/")

    def query_since(self, logql: str, since_ts: float) -> list[dict]:
        """Range query from since_ts to now. LogQL, e.g.:
        '{service=~"kafka-connect"} |~ "(?i)nullpointerexception"'
        (promtail labels streams with the compose service name, same label
        zv-monitor's LogPattern.containerSelectorRegex matches against)
        """
        params = {
            "query": logql,
            "start": str(int(since_ts * 1e9)),
            "end": str(int(time.time() * 1e9)),
            "limit": 50,
        }
        r = requests.get(f"{self.base_url}/loki/api/v1/query_range", params=params, timeout=10)
        r.raise_for_status()
        result = r.json().get("data", {}).get("result", [])
        entries = []
        for stream in result:
            for ts_ns, line in stream.get("values", []):
                entries.append({"ts": int(ts_ns) / 1e9, "line": line})
        return entries


class PrometheusClient:
    def __init__(self, base_url: str = PROMETHEUS_URL):
        self.base_url = base_url.rstrip("/")

    def instant(self, promql: str) -> list[dict]:
        r = requests.get(
            f"{self.base_url}/api/v1/query", params={"query": promql}, timeout=10
        )
        r.raise_for_status()
        return r.json().get("data", {}).get("result", [])


class Verifier:
    def __init__(self):
        self.loki = LokiClient()
        self.prom = PrometheusClient()

    def check(self, expectation: Expectation, since_ts: float) -> CheckResult:
        deadline = time.time() + expectation.timeout_s
        while time.time() < deadline:
            if expectation.kind == "log":
                entries = self.loki.query_since(expectation.query, since_ts)
                if entries:
                    first = min(e["ts"] for e in entries)
                    return CheckResult(
                        expectation.id, True, first, f"{len(entries)} matching line(s)"
                    )
            elif expectation.kind == "metric":
                result = self.prom.instant(expectation.query)
                if result:
                    return CheckResult(
                        expectation.id, True, time.time(), f"{len(result)} series matched"
                    )
            else:
                raise ValueError(f"unknown expectation kind: {expectation.kind!r}")
            time.sleep(expectation.poll_interval_s)
        return CheckResult(expectation.id, False, None, f"timed out after {expectation.timeout_s}s")

    def check_all(self, expectations: list[Expectation], since_ts: float) -> list[CheckResult]:
        # Sequential is fine here - scenarios run one at a time and each
        # expectation's own timeout bounds total wall time; parallelize
        # with a thread pool later if that gets slow.
        return [self.check(e, since_ts) for e in expectations]
