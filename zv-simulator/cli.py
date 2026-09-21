"""zv-simulator - the single entry point.

Argparse only: every command builds a Simulator (simulator.py) around one
shared Context and renders results through a ReportWriter (report.py).
The scenario classes themselves live in scenarios.py.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from report import ReportWriter
from scenarios import SCENARIOS
from simulator import Simulator


def cmd_list(args):
    by_cat: dict[str, list] = {}
    for scenario in SCENARIOS.values():
        by_cat.setdefault(scenario.category, []).append(scenario)
    for cat in sorted(by_cat):
        print(f"\n{cat}:")
        for s in sorted(by_cat[cat], key=lambda s: s.id):
            print(f"  {s.id:<32} {s.description.strip().splitlines()[0]}")


def cmd_run(args):
    results = [Simulator(ctx=None).run_scenario(args.scenario_id)]
    writer = ReportWriter()
    for r in results:
        writer.print_result(r)
    writer.print_summary(results)
    if args.json:
        writer.write_json(results, args.json)
    sys.exit(0 if all(r.passed for r in results) else 1)


def cmd_run_category(args):
    results = Simulator(ctx=None).run_category(args.category)
    writer = ReportWriter()
    for r in results:
        writer.print_result(r)
    writer.print_summary(results)
    if args.json:
        writer.write_json(results, args.json)
    sys.exit(0 if all(r.passed for r in results) else 1)


def cmd_run_all(args):
    # Explicit positional ids select a subset; --skip filters (comma-separated,
    # repeatable) so CI can drop slow or known-gap scenarios from a sweep.
    skipped = {s.strip() for part in (args.skip or []) for s in part.split(",") if s.strip()}
    ids = list(args.ids) if args.ids else list(SCENARIOS)  # dict iterates ids
    results = Simulator(ctx=None).run_all([sid for sid in ids if sid not in skipped])
    writer = ReportWriter()
    for r in results:
        writer.print_result(r)
    writer.print_summary(results)
    if args.json:
        writer.write_json(results, args.json)
    sys.exit(0 if all(r.passed for r in results) else 1)


def main():
    # INFO lines go to stderr (report.py owns stdout, keeping --json/capture
    # clean); ZV_SIM_QUIET=1 drops the level to WARNING to silence them,
    # ZV_SIM_DEBUG=1 raises it to DEBUG (per-poll verifier progress lines).
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        level=(
            logging.DEBUG
            if os.environ.get("ZV_SIM_DEBUG")
            else logging.WARNING
            if os.environ.get("ZV_SIM_QUIET")
            else logging.INFO
        ),
    )
    parser = argparse.ArgumentParser(prog="zv-simulator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="list available scenarios")
    p_list.set_defaults(func=cmd_list)

    p_run = sub.add_parser("run", help="run a single scenario by id")
    p_run.add_argument("scenario_id")
    p_run.add_argument("--json", help="write result JSON to this path")
    p_run.set_defaults(func=cmd_run)

    p_cat = sub.add_parser("run-category", help="run every scenario in a category")
    p_cat.add_argument("category", choices=sorted({s.category for s in SCENARIOS.values()}))
    p_cat.add_argument("--json", help="write results JSON to this path")
    p_cat.set_defaults(func=cmd_run_category)

    p_all = sub.add_parser(
        "run-all",
        help="run every scenario sequentially (category-ordered); "
        "pass ids to run a subset, --skip to drop some from a full sweep",
    )
    p_all.add_argument("ids", nargs="*", help="optional: scenario ids to run (default: all)")
    p_all.add_argument(
        "--skip",
        action="append",
        default=[],
        metavar="IDS",
        help="scenario id(s) to skip, comma-separated; repeatable",
    )
    p_all.add_argument("--json", help="write results JSON to this path")
    p_all.set_defaults(func=cmd_run_all)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

