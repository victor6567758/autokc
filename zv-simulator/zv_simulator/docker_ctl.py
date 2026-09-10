"""Thin wrapper over docker-py, scoped to one compose project.

Containers are resolved by the `com.docker.compose.service` label rather
than by guessing the `<project>-<service>-<n>` name, so this keeps working
regardless of compose naming/scaling.
"""
from __future__ import annotations

import time
import docker
from docker.errors import NotFound

from zv_simulator import COMPOSE_PROJECT


class DockerCtl:
    def __init__(self, compose_project: str = COMPOSE_PROJECT):
        self.client = docker.from_env()
        self.compose_project = compose_project

    def container(self, service: str):
        """Return the running container for a compose service name."""
        containers = self.client.containers.list(
            filters={
                "label": [
                    f"com.docker.compose.project={self.compose_project}",
                    f"com.docker.compose.service={service}",
                ]
            }
        )
        if not containers:
            raise NotFound(
                f"no running container for service '{service}' in project "
                f"'{self.compose_project}' - is the stack up? (`make up` / `make up-dev`)"
            )
        return containers[0]

    # -- lifecycle faults ---------------------------------------------------

    def kill(self, service: str, signal: str = "SIGKILL"):
        self.container(service).kill(signal=signal)

    def pause(self, service: str):
        """Freeze the process (SIGSTOP-equivalent) - unlike kill, sockets stay
        open but nothing responds. Good for simulating a hung dependency
        rather than a hard-down one."""
        self.container(service).pause()

    def unpause(self, service: str):
        self.container(service).unpause()

    def restart(self, service: str, timeout: int = 5):
        self.container(service).restart(timeout=timeout)

    def restart_loop(self, service: str, times: int, interval_s: float = 3.0):
        """Repeated restarts - the reliable way to trigger a Connect worker
        rebalance storm (maps to the worker-rebalance-loop metric)."""
        for _ in range(times):
            self.restart(service)
            time.sleep(interval_s)

    def is_running(self, service: str) -> bool:
        try:
            c = self.container(service)
        except NotFound:
            return False
        c.reload()
        return c.status == "running"

    def wait_running(self, service: str, timeout_s: float = 60.0) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.is_running(service):
                return True
            time.sleep(1)
        return False

    # -- network faults -------------------------------------------------
    # Prefer toxiproxy_ctl for graduated faults (latency, partial cuts).
    # These are the blunt, docker-native alternative when toxiproxy isn't
    # wired into a config's connection path.

    def network_disconnect(self, service: str, network: str):
        net = self.client.networks.get(network)
        net.disconnect(self.container(service), force=True)

    def network_connect(self, service: str, network: str):
        net = self.client.networks.get(network)
        net.connect(self.container(service))

    # -- exec ---------------------------------------------------------------

    def exec_run(self, service: str, cmd: str, user: str | None = None):
        return self.container(service).exec_run(cmd, user=user)
