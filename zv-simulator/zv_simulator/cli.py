from __future__ import annotations

import argparse
import sys

from zv_simulator.scenarios import REGISTRY, run_scenario, run_category
from zv_simulator.report import print_result, print_summary, write_json


def cmd_list(args):
    by_cat: dict[str, list] = {}
    for sd in REGISTRY.values():
        by_cat.setdefault(sd.category, []).append(sd)
    for cat in sorted(by_cat):
        print(f"\n{cat}:")
        for sd in sorted(by_cat[cat], key=lambda s: s.id):
            print(f"  {sd.id:<32} {sd.description.strip().splitlines()[0]}")


def cmd_run(args):
    result = run_scenario(args.scenario_id)
    print_result(result)
    if args.json:
        write_json([result], args.json)
    sys.exit(0 if result.passed else 1)


def cmd_run_category(args):
    results = run_category(args.category)
    for r in results:
        print_result(r)
    print_summary(results)
    if args.json:
        write_json(results, args.json)
    sys.exit(0 if all(r.passed for r in results) else 1)


def main():
    parser = argparse.ArgumentParser(prog="zv-simulator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="list available scenarios")
    p_list.set_defaults(func=cmd_list)

    p_run = sub.add_parser("run", help="run a single scenario by id")
    p_run.add_argument("scenario_id")
    p_run.add_argument("--json", help="write result JSON to this path")
    p_run.set_defaults(func=cmd_run)

    p_cat = sub.add_parser("run-category", help="run every scenario in a category")
    p_cat.add_argument("category", choices=sorted({sd.category for sd in REGISTRY.values()}))
    p_cat.add_argument("--json", help="write results JSON to this path")
    p_cat.set_defaults(func=cmd_run_category)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
