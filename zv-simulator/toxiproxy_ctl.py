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

The scenarios that need the proxy call ensure_toxiproxy() themselves -
no manual docker-compose.override.yml step, no connector config file to
swap. See that function for the escalation order.
"""
from __future__ import annotations

import os
import time

import requests

from config import TOXIPROXY_URL

PG_SOURCE_PROXY = "pg-source"

# -- fixture lifecycle ------------------------------------------------------
# The network scenarios need a toxiproxy in the connection path. zv-simulator
# never (re)launches the compose stack - it only guarantees this ONE fixture
# is up, in the least invasive way possible:
#   1. an already-running toxiproxy (whoever started it) is reused untouched;
#   2. the optional docker-compose.override.yml service container, if it
#      exists but is stopped, is merely `docker start`ed - no recreate, no
#      compose invocation;
#   3. otherwise a simulator-owned sidecar (zv-sim-toxiproxy) is run on the
#      stack's own network, and removed again at cleanup.
# In every case the in-network hostname is "toxiproxy", so the connector
# config the scenarios PUT is identical no matter which path provided it.

TOXIPROXY_HOST = "toxiproxy"
TOXIPROXY_LISTEN_PORT = "15432"
SIDECAR_NAME = "zv-sim-toxiproxy"
SIDECAR_LABEL = "zv-simulator.managed"
SIDECAR_IMAGE = os.environ.get("ZV_SIM_TOXIPROXY_IMAGE", "ghcr.io/shopify/toxiproxy:2.9.0")
COMPOSE_SERVICE = "toxiproxy"  # matches zv-simulator/docker-compose.override.yml


def _find_container(docker, label_filters: list[str]):
    found = docker.client.containers.list(all=True, filters={"label": label_filters})
    return found[0] if found else None


def _control_api_up(base_url: str, timeout: float = 2.0) -> bool:
    try:
        return requests.get(f"{base_url.rstrip('/')}/proxies", timeout=timeout).status_code == 200
    except requests.RequestException:
        return False


def _wait_control_api(base_url: str, timeout_s: float = 20.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if _control_api_up(base_url):
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"toxiproxy control API at {base_url} did not come up within {timeout_s:.0f}s"
    )


def ensure_toxiproxy(docker, base_url: str = TOXIPROXY_URL) -> str | None:
    """Idempotently guarantee a running toxiproxy for the fault window.

    Returns a token for stop_toxiproxy(): None if one was already running
    (not ours to stop), "service" if we started the override-file's stopped
    container, "sidecar" if we ran our own. Never recreates, reconfigures
    or restarts any stack service - the monitoring app and everything else
    in the compose project are untouched.
    """
    if _control_api_up(base_url):
        # An orphaned sidecar from a crashed run is adopted AND owned again
        # (so this run's cleanup removes it); anything else that happens to
        # be running a toxiproxy is reused but never stopped by us.
        ours = _find_container(docker, [f"{SIDECAR_LABEL}=true"])
        return "sidecar" if ours is not None else None

    service = _find_container(
        docker,
        [
            f"com.docker.compose.project={docker.compose_project}",
            f"com.docker.compose.service={COMPOSE_SERVICE}",
        ],
    )
    if service is not None:
        service.start()
        _wait_control_api(base_url)
        return "service"

    stale = _find_container(docker, [f"{SIDECAR_LABEL}=true"])
    if stale is not None:
        stale.remove(force=True)  # dead leftover from a crashed run

    net = docker.compose_network()
    sidecar = docker.client.containers.run(
        SIDECAR_IMAGE,
        name=SIDECAR_NAME,
        detach=True,
        labels={SIDECAR_LABEL: "true"},
        # control API bound to loopback only - the host-side ToxiproxyCtl
        # talks to it; the proxied postgres port stays network-internal
        ports={"8474/tcp": ("127.0.0.1", 8474)},
    )
    net.connect(sidecar, aliases=[TOXIPROXY_HOST])
    _wait_control_api(base_url)
    return "sidecar"


def stop_toxiproxy(docker, started: str | None) -> None:
    """Undo exactly what ensure_toxiproxy() had to do - nothing more."""
    if started == "sidecar":
        c = _find_container(docker, [f"{SIDECAR_LABEL}=true"])
        if c is not None:
            c.remove(force=True)
    elif started == "service":
        service = _find_container(
            docker,
            [
                f"com.docker.compose.project={docker.compose_project}",
                f"com.docker.compose.service={COMPOSE_SERVICE}",
            ],
        )
        if service is not None:
            service.stop(timeout=5)


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
