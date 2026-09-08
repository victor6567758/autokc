#!/usr/bin/env python3
"""Parse docker-compose file(s) and print the exposed service URLs.

Helper for scripts/print-urls.sh - fully driven by the compose files, no
per-service knowledge in here. Port mappings come from each service's
`ports:` entries; presentation (labels, URL paths, notes) comes from the
top-level `x-url-info:` block in the same compose file:

    x-url-info:
      kafka:
        label: Kafka broker          # display name (default: service name)
        note: bootstrap server ...   # free-text hint shown in the table
        ports:
          9092: {kind: tcp}          # keyed on the CONTAINER port
          5559: {kind: metrics, path: /metrics}

kind: http (default - browsable URL) | metrics (Prometheus text endpoint,
rendered "<label> metrics" when the service publishes other ports too) |
tcp (non-HTTP wire protocol, printed without a scheme). path: URL path
appended for http/metrics (default "/"). A plain string entry
("5432: tcp") is accepted as shorthand for {kind: tcp}.

Callers:
  scripts/pipeline-up.sh      (full stack: development/docker-compose.yml)
  scripts/pipeline-up-dev.sh  (dev stack:  development/docker-compose.dev.yml)
  make urls                   (full stack)

A row is emitted for every service with published ports, in compose
declaration order; services without published ports are listed in a
compact footer. Services lacking an x-url-info entry fall back to a
generic http://localhost:<host-port> row, so new services show up
automatically. Several files may be given; later files win per service
(and per x-url-info entry). PyYAML is the only dependency.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.stderr.write("ERROR: PyYAML is required: python3 -m pip install --user pyyaml\n")
    sys.exit(2)

LABEL_W, URL_W = 22, 40


def info_ports(xurls: dict) -> dict:
    """Normalize an x-url-info 'ports' mapping to {int container_port: entry}."""
    out: dict = {}
    for key, entry in (xurls.get("ports") or {}).items():
        try:
            out[int(key)] = entry
        except (TypeError, ValueError):
            sys.stderr.write(f"WARNING: ignoring non-numeric x-url-info port key {key!r}\n")
    return out


def published_ports(service: dict) -> list[tuple[int, int]]:
    """Return [(host_port, container_port)] in declaration order."""
    out: list[tuple[int, int]] = []
    for entry in service.get("ports") or []:
        if isinstance(entry, dict):  # long syntax: {target, published, ...}
            target, published = entry.get("target"), entry.get("published")
            if target is None or published in (None, ""):
                continue  # not published to the host
            out.append((int(published), int(target)))
            continue
        # short syntax: [host_ip:]host_port:container_port[/protocol]
        spec = str(entry).split("/", 1)[0]
        parts = spec.split(":")
        if len(parts) == 1:  # container port only - not published
            continue
        host, container = parts[-2], parts[-1]
        if not host or not container:
            continue
        out.append((int(host), int(container)))
    return out


def load(paths: list[str]) -> tuple[dict, dict]:
    """Return (services, x-url-info) merged across files; later files win."""
    services: dict = {}
    url_info: dict = {}
    for p in paths:
        path = Path(p)
        if not path.is_file():
            sys.stderr.write(f"ERROR: compose file not found: {path}\n")
            sys.exit(2)
        doc = yaml.safe_load(path.read_text()) or {}
        svc = doc.get("services") or {}
        if not isinstance(svc, dict):
            sys.stderr.write(f"ERROR: no 'services:' mapping in {path}\n")
            sys.exit(2)
        services.update(svc)
        info = doc.get("x-url-info") or {}
        if not isinstance(info, dict):
            sys.stderr.write(f"WARNING: ignoring malformed x-url-info in {path}\n")
        else:
            url_info.update(info)
    return services, url_info


def build_rows(services: dict, url_info: dict) -> tuple[list[dict], list[str]]:
    rows: list[dict] = []
    internal: list[str] = []
    for name, svc in services.items():
        ports = published_ports(svc)
        if not ports:
            internal.append(name)
            continue
        xurls = url_info.get(name) or {}
        label = str(xurls.get("label") or name)
        note = str(xurls.get("note") or "")
        roles = info_ports(xurls)
        for i, (host, container) in enumerate(ports):
            entry = roles.get(container)
            if isinstance(entry, str):  # shorthand: "5432: tcp"
                kind, path = entry, "/"
            elif isinstance(entry, dict):
                kind = str(entry.get("kind") or "http")
                path = str(entry.get("path") or "/")
            else:  # published port with no x-url-info entry
                kind, path = "http", "/"
            if kind == "tcp":
                url = f"localhost:{host}"
            else:
                url = f"http://localhost:{host}" + ("" if path in ("", "/") else path)
            row_label = f"{label} metrics" if kind == "metrics" and len(ports) > 1 else label
            rows.append({
                "service": name,
                "label": row_label,
                "kind": kind,
                "host_port": host,
                "container_port": container,
                "url": url,
                "note": note if i == 0 else "",
            })
    return rows, internal


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("files", nargs="+", help="docker-compose file(s); later files win per service")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of the table")
    args = ap.parse_args()

    services, url_info = load(args.files)
    rows, internal = build_rows(services, url_info)

    if args.json:
        print(json.dumps({
            "compose_files": args.files,
            "urls": rows,
            "no_published_ports": internal,
        }, indent=2))
        return

    print()
    print("== Services (Ctrl+Click the URLs in GNOME Terminal) ==")
    print(f"   parsed from: {', '.join(args.files)}")
    print()
    for r in rows:
        print(f"  {r['label']:<{LABEL_W}} {r['url']:<{URL_W}} {r['note']}".rstrip())
    if internal:
        print()
        print(f"  (no published ports: {', '.join(internal)})")
    print()


if __name__ == "__main__":
    main()

