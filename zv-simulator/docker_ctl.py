"""Thin wrapper over docker-py, scoped to one compose project.

Containers are resolved by the `com.docker.compose.service` label rather
than by guessing the `<project>-<service>-<n>` name, so this keeps working
regardless of compose naming/scaling.
"""
from __future__ import annotations

import time
import docker
from docker.errors import NotFound

from config import (
    COMPOSE_PROJECT,
    DOCKER_API_TIMEOUT_S,
    DOCKER_EXEC_RUN_USER,
    DOCKER_KILL_SIGNAL,
    DOCKER_RESTART_LOOP_INTERVAL_S,
    DOCKER_RESTART_TIMEOUT,
    DOCKER_WAIT_RUNNING_TIMEOUT_S,
)


class DockerCtl:
    def __init__(self, compose_project: str):
        # Explicit client-side ceiling on every docker API call. Without it,
        # exec/kill/restart against a paused or wedged container (pause() is
        # one of this tool's own faults, and a leftover from an aborted run
        # survives between runs) blocks far beyond any sane step budget.
        self.client = docker.from_env(timeout=DOCKER_API_TIMEOUT_S)
        self.compose_project = compose_project

    def container(self, service: str):
        """Return the container for a compose service name, running or not.

        `all=True` matters: containers.list() only returns *running*
        containers by default, so a cleanup like kill() -> restart() would
        otherwise raise NotFound for the freshly-killed container and leave
        the service down for every scenario that follows.
        """
        containers = self.client.containers.list(
            all=True,
            filters={
                "label": [
                    f"com.docker.compose.project={self.compose_project}",
                    f"com.docker.compose.service={service}",
                ]
            },
        )
        if not containers:
            raise NotFound(
                f"no running container for service '{service}' in project "
                f"'{self.compose_project}' - is the stack up? (`make up` / `make up-dev`)"
            )
        return containers[0]

    # -- lifecycle faults ---------------------------------------------------

    def kill(self, service: str, signal: str):
        self.container(service).kill(signal=signal)

    def pause(self, service: str):
        """Freeze the process (SIGSTOP-equivalent) - unlike kill, sockets stay
        open but nothing responds. Good for simulating a hung dependency
        rather than a hard-down one."""
        self.container(service).pause()

    def unpause(self, service: str):
        self.container(service).unpause()

    def restart(self, service: str, timeout: int):
        self.container(service).restart(timeout=timeout)

    def restart_loop(self, service: str, times: int, interval_s: float):
        """Repeated restarts - the reliable way to trigger a Connect worker
        rebalance storm (maps to the worker-rebalance-loop metric)."""
        for _ in range(times):
            self.restart(service, timeout=DOCKER_RESTART_TIMEOUT)
            time.sleep(interval_s)

    def is_running(self, service: str) -> bool:
        try:
            c = self.container(service)
        except NotFound:
            return False
        c.reload()
        return c.status == "running"

    def wait_running(self, service: str, timeout_s: float) -> bool:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            if self.is_running(service):
                return True
            time.sleep(1)
        return False

    def compose_network(self):
        """The stack's default docker network, so fixture containers we start
        (e.g. the toxiproxy sidecar) share DNS with the compose services.
        Never creates a network - only finds the one the stack runs on."""
        nets = self.client.networks.list(
            filters={"label": f"com.docker.compose.project={self.compose_project}"}
        )
        for n in nets:
            if n.name == f"{self.compose_project}_default":
                return n
        if nets:
            return nets[0]
        # label lookup came up empty - fall back to the network an actual
        # stack container is attached to
        for c in self.client.containers.list(
            filters={"label": f"com.docker.compose.project={self.compose_project}"}
        ):
            for name in c.attrs["NetworkSettings"]["Networks"]:
                return self.client.networks.get(name)
        raise NotFound(
            f"no network found for compose project '{self.compose_project}' - "
            "is the stack up? (`make up` / `make up-dev`)"
        )

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

    def exec_run(self, service: str, cmd: str, user: str | None):
        return self.container(service).exec_run(cmd, user=user)
