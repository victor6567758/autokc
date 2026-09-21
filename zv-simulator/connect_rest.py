"""Kafka Connect REST client - the surface for config-injected faults
(bad JDBC URL, broken SMT, pause/resume flapping) as opposed to
infrastructure-level faults (docker_ctl) or data-level faults (pg_faults).
"""
from __future__ import annotations

import time

import requests

from config import BACKOFF_DELETE_OFFSETS
from config import BACKOFF_RESTART_CONNECTOR
from config import CONNECT_REST_TIMEOUT
from config import CONNECT_REST_URL
from config import CONNECT_REST_RESTART_INCLUDE_TASKS
from config import CUT_MSG_LENGTH
from config import FLAP_INTERVAL
from config import RETRIES_DELETE_OFFSETS
from config import RETRIES_RESTART_CONNECTOR
from config import WAIT_RUNNING_POLL
from config import WAIT_RUNNING_TIMEOUT
from config import WAIT_STOPPED_POLL
from config import WAIT_STOPPED_TIMEOUT


class ConnectRest:
    def __init__(self, base_url: str, timeout: float):
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
        include_tasks: bool,
        retries: int,
        backoff_s: float,
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

    def delete_offsets(self, connector: str, retries: int, backoff_s: float):
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
                r = requests.delete(self._url(f"/connectors/{connector}/offsets"), timeout=self.timeout)
                if r.status_code < 400:
                    return
                last = RuntimeError(f"delete_offsets failed: {r.status_code} {r.text[:CUT_MSG_LENGTH]}")
            except requests.exceptions.RequestException as exc:
                last = exc
            time.sleep(backoff_s)
        assert last is not None
        raise last

    def flap(self, connector: str, cycles: int, interval_s: float):
        """Pause/resume in a tight loop - a config-plane way to make a
        connector's task status flap without touching infrastructure."""
        for _ in range(cycles):
            self.pause(connector)
            time.sleep(interval_s)
            self.resume(connector)
            time.sleep(interval_s)

    def healthy(self, connectors: list[str]) -> bool:
        """Instantaneous predicate: every named connector AND all its tasks
        report RUNNING (False on any other state or a transport error).
        For callers that own the polling loop, unlike wait_running()."""
        import logging
        log = logging.getLogger(__name__)
        for name in connectors:
            try:
                st = self.status(name)
                states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
                state_summary = f"connector={st['connector']['state']}, tasks={[t['state'] for t in st.get('tasks', [])]}"
                if not states or not all(s == "RUNNING" for s in states):
                    log.debug(f"    {name}: {state_summary} - NOT HEALTHY")
                    return False
                else:
                    log.debug(f"    {name}: {state_summary} - HEALTHY")
            except requests.RequestException as e:
                log.debug(f"    {name}: health check failed - {e.__class__.__name__}")
                return False
        return True

    def wait_running(self, connector: str, timeout_s: float, poll_s: float) -> bool:
        """Wait until the connector AND all its tasks report RUNNING.
        Use after a config swap, so a fault lands on a pipeline that is
        healthy through the new path rather than mid-restart."""
        deadline = time.time() + timeout_s
        checks = 0
        while time.time() < deadline:
            try:
                st = self.status(connector)
                states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
                checks += 1
                if states:
                    state_summary = f"connector={st['connector']['state']}, tasks={[t['state'] for t in st.get('tasks', [])]}"
                    if all(s == "RUNNING" for s in states):
                        import logging
                        logging.getLogger(__name__).debug(f"    {connector}: {state_summary} (check {checks}) - HEALTHY")
                        return True
                    else:
                        import logging
                        logging.getLogger(__name__).debug(f"    {connector}: {state_summary} (check {checks})")
            except requests.RequestException as e:
                import logging
                logging.getLogger(__name__).debug(f"    {connector}: status check failed - {e.__class__.__name__} (retrying)")
            time.sleep(poll_s)
        import logging
        logging.getLogger(__name__).warning(f"  {connector} did not reach RUNNING within {timeout_s}s ({checks} checks)")
        return False

    def wait_stopped(self, connector: str, timeout_s: float, poll_s: float) -> bool:
        """Wait until the connector reports STOPPED. stop() is asynchronous
        (202 just means accepted) - calling delete_offsets() before the stop
        has landed answers 400 "must be in the STOPPED state"."""
        deadline = time.time() + timeout_s
        checks = 0
        while time.time() < deadline:
            try:
                st = self.status(connector)
                checks += 1
                state = st["connector"]["state"]
                import logging
                log = logging.getLogger(__name__)
                if state == "STOPPED":
                    log.debug(f"    {connector}: state={state} (check {checks}) - STOPPED")
                    return True
                else:
                    log.debug(f"    {connector}: state={state} (check {checks})")
            except requests.RequestException as e:
                import logging
                logging.getLogger(__name__).debug(f"    {connector}: status check failed - {e.__class__.__name__} (retrying)")
            time.sleep(poll_s)
        import logging
        logging.getLogger(__name__).warning(f"  {connector} did not reach STOPPED within {timeout_s}s ({checks} checks)")
        return False
