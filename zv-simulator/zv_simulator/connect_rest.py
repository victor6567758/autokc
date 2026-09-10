"""Kafka Connect REST client - the surface for config-injected faults
(bad JDBC URL, broken SMT, pause/resume flapping) as opposed to
infrastructure-level faults (docker_ctl) or data-level faults (pg_faults).
"""
from __future__ import annotations

import requests

from zv_simulator import CONNECT_REST_URL


class ConnectRest:
    def __init__(self, base_url: str = CONNECT_REST_URL, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get_config(self, connector: str) -> dict:
        r = requests.get(self._url(f"/connectors/{connector}/config"), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def set_config(self, connector: str, config: dict) -> dict:
        """PUT full config - Connect diffs and restarts tasks as needed.
        This is how a bad-config fault is injected: take a known-good
        config, mutate one field, PUT it, and (usually) revert it in
        cleanup()."""
        r = requests.put(
            self._url(f"/connectors/{connector}/config"),
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def status(self, connector: str) -> dict:
        r = requests.get(self._url(f"/connectors/{connector}/status"), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def pause(self, connector: str):
        requests.put(self._url(f"/connectors/{connector}/pause"), timeout=self.timeout).raise_for_status()

    def resume(self, connector: str):
        requests.put(self._url(f"/connectors/{connector}/resume"), timeout=self.timeout).raise_for_status()

    def restart_connector(self, connector: str, include_tasks: bool = True):
        params = {"includeTasks": "true"} if include_tasks else {}
        requests.post(
            self._url(f"/connectors/{connector}/restart"), params=params, timeout=self.timeout
        ).raise_for_status()

    def restart_task(self, connector: str, task_id: int):
        requests.post(
            self._url(f"/connectors/{connector}/tasks/{task_id}/restart"), timeout=self.timeout
        ).raise_for_status()

    def flap(self, connector: str, cycles: int, interval_s: float = 2.0):
        """Pause/resume in a tight loop - a config-plane way to make a
        connector's task status flap without touching infrastructure."""
        import time

        for _ in range(cycles):
            self.pause(connector)
            time.sleep(interval_s)
            self.resume(connector)
            time.sleep(interval_s)
