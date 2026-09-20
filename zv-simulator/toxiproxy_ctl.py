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

The scenarios that need the proxy call ToxiproxyFixture(docker).ensure()
themselves - no manual docker-compose.override.yml step, no connector
config file to swap. See that method for the escalation order.
"""
from __future__ import annotations

import time

import docker.errors as docker_errors
import requests

from config import (
    SIDECAR_IMAGE,
    SIDECAR_LABEL,
    SIDECAR_NAME,
    TOXIPROXY_COMPOSE_SERVICE,
    TOXIPROXY_HOST,
    TOXIPROXY_LISTEN_PORT,
    TOXIPROXY_URL,
)

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
# In every case the in-network hostname is TOXIPROXY_HOST (config.py), so
# the connector config the scenarios PUT is identical no matter which path
# provided it.


class ToxiproxyFixture:
    """Guarantees a running toxiproxy for a fault window (and undoes
    exactly what it had to do to get one). Wraps one DockerCtl."""

    def __init__(self, docker, base_url: str = TOXIPROXY_URL):
        self.docker = docker
        self.base_url = base_url

    def _find_container(self, label_filters: list[str]):
        found = self.docker.client.containers.list(all=True, filters={"label": label_filters})
        return found[0] if found else None

    def _control_api_up(self, timeout: float = 2.0) -> bool:
        try:
            r = requests.get(f"{self.base_url.rstrip('/')}/proxies", timeout=timeout)
            return r.status_code == 200
        except requests.RequestException:
            return False

    def _wait_control_api(self, timeout_s: float = 20.0) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self._control_api_up():
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"toxiproxy control API at {self.base_url} did not come up within {timeout_s:.0f}s"
        )



    def ensure(self) -> str | None:
        """Idempotently guarantee a running toxiproxy for the fault window.

        Returns a token for stop(): None if one was already running
        (not ours to stop), "service" if we started the override-file's
        stopped container, "sidecar" if we ran our own. Never recreates,
        reconfigures or restarts any stack service - the monitoring app
        and everything else in the compose project are untouched.
        """
        if self._control_api_up():
            # An orphaned sidecar from a crashed run is adopted AND owned again
            # (so this run's cleanup removes it); anything else that happens to
            # be running a toxiproxy is reused but never stopped by us.
            ours = self._find_container([f"{SIDECAR_LABEL}=true"])
            return "sidecar" if ours is not None else None

        service = self._find_container(
            [
                f"com.docker.compose.project={self.docker.compose_project}",
                f"com.docker.compose.service={TOXIPROXY_COMPOSE_SERVICE}",
            ],
        )
        if service is not None:
            try:
                service.start()
            except docker_errors.APIError:
                # A stopped compose container whose network was recreated with the
                # stack (`make down && make up`) references a deleted network and
                # can never start again - remove the corpse and fall through to
                # the sidecar path. compose recreates its own container from the
                # override file the next time that stack variant comes up.
                service.remove(force=True)
            else:
                self._wait_control_api()
                return "service"

        stale = self._find_container([f"{SIDECAR_LABEL}=true"])
        if stale is not None:
            stale.remove(force=True)  # dead leftover from a crashed run

        net = self.docker.compose_network()
        sidecar = self.docker.client.containers.run(
            SIDECAR_IMAGE,
            name=SIDECAR_NAME,
            detach=True,
            labels={SIDECAR_LABEL: "true"},
            # control API bound to loopback only - the host-side ToxiproxyCtl
            # talks to it; the proxied postgres port stays network-internal
            ports={"8474/tcp": ("127.0.0.1", 8474)},
        )
        net.connect(sidecar, aliases=[TOXIPROXY_HOST])
        self._wait_control_api()
        return "sidecar"

    def stop(self, started: str | None) -> None:
        """Undo exactly what ensure() had to do - nothing more."""
        if started == "sidecar":
            c = self._find_container([f"{SIDECAR_LABEL}=true"])
            if c is not None:
                c.remove(force=True)
        elif started == "service":
            service = self._find_container(
                [
                    f"com.docker.compose.project={self.docker.compose_project}",
                    f"com.docker.compose.service={TOXIPROXY_COMPOSE_SERVICE}",
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
        listen: str = f"0.0.0.0:{TOXIPROXY_LISTEN_PORT}",
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
