#!/usr/bin/env python3
"""Snapshot a Prometheus /metrics endpoint, wait, snapshot again, and print
what changed. Useful for spotting which metrics actually move during a
failure-injection test vs which stay flat noise.

Usage:
    metrics_diff.py <url> [--wait SECONDS] [--top N]

Example:
    metrics_diff.py http://localhost:5557/metrics --wait 20 --top 30
    (kill a connector task, poison a message, etc. during the wait window)
"""
import argparse
import re
import sys
import time
import urllib.request

LINE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)\s*$')


def fetch(url: str) -> dict:
  with urllib.request.urlopen(url, timeout=10) as resp:
    text = resp.read().decode("utf-8")
  metrics = {}
  for line in text.splitlines():
    if not line or line.startswith("#"):
      continue
    m = LINE_RE.match(line)
    if not m:
      continue
    name, labels, value = m.groups()
    try:
      value = float(value)
    except ValueError:
      continue  # NaN / Inf / non-numeric - skip
    key = f"{name}{labels or ''}"
    metrics[key] = value
  return metrics


def main() -> None:
  ap = argparse.ArgumentParser(description=__doc__,
                               formatter_class=argparse.RawDescriptionHelpFormatter)
  ap.add_argument("url", help="e.g. http://localhost:5557/metrics")
  ap.add_argument("--wait", type=float, default=30,
                  help="seconds between snapshots (default: 30)")
  ap.add_argument("--top", type=int, default=40,
                  help="max number of changed metrics to print (default: 40)")
  args = ap.parse_args()

  print(f"Taking baseline snapshot from {args.url} ...", file=sys.stderr)
  before = fetch(args.url)

  print(f"Waiting {args.wait}s - trigger your event now ...", file=sys.stderr)
  time.sleep(args.wait)

  print("Taking second snapshot ...", file=sys.stderr)
  after = fetch(args.url)

  changed = []
  for key, after_val in after.items():
    before_val = before.get(key)
    if before_val is None:
      changed.append((key, None, after_val, after_val))  # new metric
    elif after_val != before_val:
      changed.append((key, before_val, after_val, after_val - before_val))

  for key in before:
    if key not in after:
      changed.append((key, before[key], None, -before[key]))  # disappeared

  changed.sort(key=lambda row: abs(row[3]), reverse=True)

  if not changed:
    print("No metrics changed between snapshots.")
    return

  print(f"\n{'metric':<80} {'before':>12} {'after':>12} {'delta':>12}")
  print("-" * 118)
  for key, before_val, after_val, delta in changed[: args.top]:
    b = "\u2014" if before_val is None else f"{before_val:.4g}"
    a = "\u2014" if after_val is None else f"{after_val:.4g}"
    print(f"{key[:80]:<80} {b:>12} {a:>12} {delta:>+12.4g}")


if __name__ == "__main__":
  main()
