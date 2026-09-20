from __future__ import annotations

import json
from dataclasses import dataclass, asdict

from verifier import CheckResult


@dataclass
class ScenarioResult:
    scenario_id: str
    injected_at: float
    checks: list[CheckResult]
    cleanup_ok: bool
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and self.cleanup_ok and all(c.matched for c in self.checks)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["passed"] = self.passed
        for c in d["checks"]:
            if c["first_match_at"] is not None:
                # clamp: causal lines written during a synchronous injection
                # can be stamped fractions of a ms before the anchor
                c["latency_s"] = round(max(0.0, c["first_match_at"] - self.injected_at), 2)
        return d


class ReportWriter:
    """Renders ScenarioResults: per-scenario detail, the sweep summary and
    the optional --json artifact."""

    def print_result(self, result: ScenarioResult):
        status = "PASS" if result.passed else "FAIL"
        print(f"\n[{status}] {result.scenario_id}")
        if result.error:
            print(f"  ERROR during injection/cleanup: {result.error}")
        for c in result.checks:
            if c.matched:
                latency = max(0.0, c.first_match_at - result.injected_at)
                print(f"  ✓ {c.name:<28} fired after {latency:6.1f}s  ({c.detail})")
            else:
                print(f"  ✗ {c.name:<28} never fired   ({c.detail})")
        print(f"  cleanup: {'ok' if result.cleanup_ok else 'FAILED - stack may still be broken'}")

    def print_summary(self, results: list[ScenarioResult]):
        passed = sum(r.passed for r in results)
        print(f"\n{'=' * 60}")
        print(f"{passed}/{len(results)} scenario(s) passed")
        for r in results:
            if not r.passed:
                print(f"  FAILED: {r.scenario_id}")

    def write_json(self, results: list[ScenarioResult], path: str):
        with open(path, "w") as f:
            json.dump([r.to_dict() for r in results], f, indent=2)

