"""Toxiproxy control client.

Toxiproxy sits on the docker network between kafka-connect and
postgres-source (see docker-compose.override.yml). It gives graduated
network faults - latency, bandwidth caps, full cuts - that a raw
`docker kill`/`network disconnect` can't: those are binary (up/down),
but a lot of real incidents look like "slow", not "dead", and zv-monitor's
catalog explicitly distinguishes the two:
  - source-stream-stalled (WARNING): still connected, no events for 60s+
  - source-disconnected   (CRITICAL): connection itself is down
A hard network partition tends to produce the CRITICAL signature almost
immediately; injected latency/timeout toxics are what actually exercise
the WARNING path.

SETUP REQUIRED: postgres-source's Debezium connector config
(development/kafka-connect/inventory-source.json) must point
`database.hostname`/`database.port` at the toxiproxy listen address
instead of postgres-source directly, e.g. `toxiproxy:15432`. See README
for the docker-compose.override.yml + config variant. Faults injected
here are invisible to Debezium if it's still connecting straight to
postgres-source.
"""
from __future__ import annotations

import requests

from zv_simulator import TOXIPROXY_URL

PG_SOURCE_PROXY = "pg-source"


class ToxiproxyCtl:
    def __init__(self, base_url: str = TOXIPROXY_URL, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def ensure_proxy(
        self,
        name: str = PG_SOURCE_PROXY,
        listen: str = "0.0.0.0:15432",
        upstream: str = "postgres-source:5432",
    ):
        """Idempotently create the proxy. Safe to call at the start of
        every scenario run."""
        r = requests.get(self._url(f"/proxies/{name}"), timeout=self.timeout)
        if r.status_code == 200:
            return r.json()
        r = requests.post(
            self._url("/proxies"),
            json={"name": name, "listen": listen, "upstream": upstream, "enabled": True},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def cut(self, name: str = PG_SOURCE_PROXY):
        """Hard cut - disables the proxy entirely, new AND existing
        connections drop. Closest analog to a network partition."""
        requests.post(
            self._url(f"/proxies/{name}"), json={"enabled": False}, timeout=self.timeout
        ).raise_for_status()

    def restore(self, name: str = PG_SOURCE_PROXY):
        requests.post(
            self._url(f"/proxies/{name}"), json={"enabled": True}, timeout=self.timeout
        ).raise_for_status()

    def add_latency(
        self, name: str = PG_SOURCE_PROXY, latency_ms: int = 3000, jitter_ms: int = 500
    ):
        """Slow, not dead - the fault that should trigger
        source-stream-stalled (WARNING) rather than source-disconnected
        (CRITICAL), since TCP stays up."""
        r = requests.post(
            self._url(f"/proxies/{name}/toxics"),
            json={
                "name": "latency-down",
                "type": "latency",
                "stream": "downstream",
                "attributes": {"latency": latency_ms, "jitter": jitter_ms},
            },
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def add_timeout(self, name: str = PG_SOURCE_PROXY, timeout_ms: int = 30000):
        """Connection accepted, then goes silent - reproduces a hung
        replication stream rather than a refused/reset one."""
        r = requests.post(
            self._url(f"/proxies/{name}/toxics"),
            json={"name": "silent-timeout", "type": "timeout", "attributes": {"timeout": timeout_ms}},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def clear_toxics(self, name: str = PG_SOURCE_PROXY):
        r = requests.get(self._url(f"/proxies/{name}/toxics"), timeout=self.timeout)
        r.raise_for_status()
        for toxic in r.json():
            requests.delete(
                self._url(f"/proxies/{name}/toxics/{toxic['name']}"), timeout=self.timeout
            )
