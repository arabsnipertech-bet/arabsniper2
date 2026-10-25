"""Visible progress for long-running jobs; liveness never implies success."""
from datetime import datetime, timezone
from pathlib import Path
import signal
import sys
import threading
import time


def memory_usage():
    """Report current Linux RSS when available, without inspecting job secrets."""
    try:
        for line in Path('/proc/self/status').read_text().splitlines():
            if line.startswith('VmRSS:'):
                return f'; RSS={int(line.split()[1]) / 1024:.0f} MiB'
    except (OSError, ValueError, IndexError):
        pass
    return ''


class TaskProgress:
    def __init__(self, name, interval=45):
        if interval <= 0:
            raise ValueError('Positive heartbeat interval required')
        self.name, self.interval = name, interval
        self.started = time.monotonic()
        self.phase = 'avvio'
        self.lock = threading.RLock()
        self.done = threading.Event()
        self.thread = None
        self.handlers = {}

    def _emit(self, message):
        stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
        print(f'[{stamp}] {self.name}: {message}{memory_usage()}', flush=True)

    def update(self, phase):
        with self.lock:
            self.phase = phase
            self._emit(phase)

    def _heartbeat(self):
        while not self.done.wait(self.interval):
            with self.lock:
                self._emit(f'in esecuzione da {int(time.monotonic()-self.started)}s; '
                           f'fase: {self.phase} (non indica completamento)')

    def _stop(self, signum, _frame):
        # SystemExit propagates through the context manager and remains failure.
        self._emit(f'interruzione ricevuta: segnale {signum}; fase: {self.phase}')
        raise SystemExit(128 + signum)

    def __enter__(self):
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                self.handlers[signum] = signal.signal(signum, self._stop)
        self.update('avvio')
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, kind, value, traceback):
        self.done.set()
        if self.thread is not None:
            self.thread.join(timeout=2)
        for signum, handler in self.handlers.items():
            signal.signal(signum, handler)
        # Do not print exception values: upstream errors can contain credentials.
        succeeded = kind is None or (kind is SystemExit and value.code in (None, 0))
        self._emit('fase terminata correttamente' if succeeded else
                   f'fase non completata ({kind.__name__})')
        return False


def main():
    """Run another Python entry point under the same diagnostics."""
    import runpy
    if len(sys.argv) < 2:
        raise SystemExit('Usage: task_progress.py SCRIPT [ARGS...]')
    script = Path(sys.argv[1]).resolve()
    sys.argv = sys.argv[1:]
    sys.path.insert(0, str(script.parent))
    with TaskProgress(script.name) as progress:
        progress.update('esecuzione script')
        runpy.run_path(str(script), run_name='__main__')


if __name__ == '__main__':
    main()
