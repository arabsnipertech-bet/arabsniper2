"""Exercise liveness and cancellation with real, isolated child processes."""
import contextlib
import io
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from task_progress import TaskProgress


class TaskProgressTests(unittest.TestCase):
    def test_quiet_phase_emits_liveness_and_stops_its_thread(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with TaskProgress('Test', interval=.01) as progress:
                progress.update('lettura archivi')
                self.assertFalse(progress.done.wait(.05))
        self.assertIn('fase: lettura archivi (non indica completamento)', output.getvalue())
        self.assertIn('fase terminata correttamente', output.getvalue())
        self.assertFalse(progress.thread.is_alive())

    def test_errors_propagate_without_success_or_sensitive_error_text(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(ValueError, 'private-value'):
                with TaskProgress('Test', interval=.01):
                    raise ValueError('private-value')
        self.assertIn('fase non completata (ValueError)', output.getvalue())
        self.assertNotIn('private-value', output.getvalue())
        self.assertNotIn('terminata correttamente', output.getvalue())

    def test_script_wrapper_preserves_exit_code(self):
        for code in (0, 7):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'sample.py'
                path.write_text(f'import sys\nprint("SCRIPT_OUTPUT",flush=True)\nsys.exit({code})\n')
                proc = subprocess.run([sys.executable, str(ROOT/'src/task_progress.py'), str(path)],
                                      capture_output=True, text=True, timeout=5)
                self.assertEqual(proc.returncode, code)
                self.assertIn('SCRIPT_OUTPUT', proc.stdout)
                self.assertEqual('fase terminata correttamente' in proc.stdout, code == 0)

    @unittest.skipUnless(os.name == 'posix', 'POSIX signals')
    def test_shutdown_reports_current_phase_and_remains_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'output.log'
            code = (f'import sys,time\nsys.path.insert(0,{str(ROOT/"src")!r})\n'
                    'from task_progress import TaskProgress\n'
                    'with TaskProgress("Auditor",interval=.02) as progress:\n'
                    ' progress.update("verifica giornata")\n'
                    ' time.sleep(30)\n')
            with output.open('w') as handle:
                proc = subprocess.Popen([sys.executable, '-c', code], stdout=handle,
                                        stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 5
                    while 'verifica giornata' not in output.read_text() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertIn('verifica giornata', output.read_text())
                    self.assertIsNone(proc.poll())
                    proc.send_signal(signal.SIGTERM)
                    self.assertEqual(proc.wait(timeout=5), 143)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()
            text = output.read_text()
            self.assertIn('interruzione ricevuta: segnale 15; fase: verifica giornata', text)
            self.assertNotIn('fase terminata correttamente', text)


if __name__ == '__main__':
    unittest.main()
