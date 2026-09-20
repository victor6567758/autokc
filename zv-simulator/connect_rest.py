"""Kafka Connect REST client - the surface for config-injected faults
(bad JDBC URL, broken SMT, pause/resume flapping) as opposed to
infrastructure-level faults (docker_ctl) or data-level faults (pg_faults).
"""
from __future__ import annotations

import time

import requests

from config import CONNECT_REST_URL


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

    def restart_connector(
        self,
        connector: str,
        include_tasks: bool = True,
        retries: int = 6,
        backoff_s: float = 5.0,
    ):
        """POST /restart, retrying transient transport failures. Right after
        an infrastructure recovery (broker container restarted) the worker's
        REST layer can accept the request but block applying it until its
        own Kafka producer reconnects - the client sees a read timeout even
        though the worker is otherwise alive. The restart is idempotent, so
        retrying until one goes through (or retries exhaust) is safe."""
        params = {"includeTasks": "true"} if include_tasks else {}
        last_exc: Exception | None = None
        for _ in range(max(1, retries)):
            try:
                requests.post(
                    self._url(f"/connectors/{connector}/restart"),
                    params=params,
                    timeout=self.timeout,
                ).raise_for_status()
                return
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                time.sleep(backoff_s)
        assert last_exc is not None
        raise last_exc

    def restart_task(self, connector: str, task_id: int):
        requests.post(
            self._url(f"/connectors/{connector}/tasks/{task_id}/restart"), timeout=self.timeout
        ).raise_for_status()

    def stop(self, connector: str):
        """PUT stop - fully stops the connector (stronger than pause: the
        task is torn down). Required before delete_offsets(); restart()
        brings a stopped connector back."""
        requests.put(self._url(f"/connectors/{connector}/stop"), timeout=self.timeout).raise_for_status()

    def delete_offsets(self, connector: str, retries: int = 3, backoff_s: float = 5.0):
        """Drop the connector's stored source offsets (Connect 3.5+).
        The connector must be STOPPED first (not merely paused), or the
        worker answers 400. With no offsets left, the connector restarts
        from its snapshot phase instead of trying to resume at a position
        that may no longer exist on the server. Retried on transport
        failures and on 4xx while the stop is still landing (409):
        right after an infrastructure recovery the worker can accept the
        request and then stall applying it - the caller sees a timeout
        even though the delete never ran."""
        last: Exception | None = None
        for _ in range(max(1, retries)):
            try:
                r = requests.delete(
                    self._url(f"/connectors/{connector}/offsets"), timeout=self.timeout
                )
                if r.status_code < 400:
                    return
                last = RuntimeError(f"delete_offsets failed: {r.status_code} {r.text[:200]}")
            except requests.exceptions.RequestException as exc:
                last = exc
            time.sleep(backoff_s)
        assert last is not None
        raise last

    def flap(self, connector: str, cycles: int, interval_s: float = 2.0):
        """Pause/resume in a tight loop - a config-plane way to make a
        connector's task status flap without touching infrastructure."""
        for _ in range(cycles):
            self.pause(connector)
            time.sleep(interval_s)
            self.resume(connector)
            time.sleep(interval_s)

    def wait_running(self, connector: str, timeout_s: float = 90.0, poll_s: float = 2.0) -> bool:
        """Wait until the connector AND all its tasks report RUNNING.
        Use after a config swap, so a fault lands on a pipeline that is
        healthy through the new path rather than mid-restart."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                st = self.status(connector)
                states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
                if states and all(s == "RUNNING" for s in states):
                    return True
            except requests.RequestException:
                pass  # worker rebalancing mid-restart - keep polling
            time.sleep(poll_s)
        return False

    def wait_stopped(self, connector: str, timeout_s: float = 30.0, poll_s: float = 1.0) -> bool:
        """Wait until the connector reports STOPPED. stop() is asynchronous
        (202 just means accepted) - calling delete_offsets() before the stop
        has landed answers 400 "must be in the STOPPED state"."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                st = self.status(connector)
                if st["connector"]["state"] == "STOPPED":
                    return True
            except requests.RequestException:
                pass
            time.sleep(poll_s)
        return False
