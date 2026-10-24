"""Non-sensitive liveness telemetry for long-running workflow steps."""
from contextlib import contextmanager
import os
import threading
import time


@contextmanager
def heartbeat(label, interval=30, emit=print):
    """Report liveness, not successful progress; never emit payloads or secrets."""
    if interval <= 0:
        raise ValueError("interval must be positive")
    stopped = threading.Event()
    started = time.monotonic()

    def report():
        while not stopped.wait(interval):
            times = os.times()
            cpu = times.user + times.system + times.children_user + times.children_system
            emit(f"WORKFLOW_HEARTBEAT task={label} elapsed_s={int(time.monotonic() - started)} cpu_s={cpu:.1f}", flush=True)

    worker = threading.Thread(target=report, name="workflow-heartbeat", daemon=True)
    worker.start()
    try:
        yield
    finally:
        stopped.set()
        worker.join(timeout=2)
