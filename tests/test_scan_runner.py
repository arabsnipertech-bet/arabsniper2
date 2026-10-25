"""Exercise real child processes; no API calls or production data are used."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch


RUNNER_PATH = Path(__file__).resolve().parents[1] / "src/3appdays_runner.py"
spec = importlib.util.spec_from_file_location("scan_runner", RUNNER_PATH)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ScanRunnerTests(unittest.TestCase):
    def test_large_stdout_and_stderr_are_drained_with_bounded_tails(self):
        out, err = io.StringIO(), io.StringIO()
        code = ("import sys; "
                "sys.stdout.write('x'*100000+'OUT_END'); "
                "sys.stderr.write('y'*100000+'ERR_END'); sys.exit(7)")
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            result = runner.stream_process([sys.executable, "-c", code])
        self.assertEqual(result.returncode, 7)
        self.assertEqual(out.getvalue(), "x"*100000+"OUT_END")
        self.assertEqual(err.getvalue(), "y"*100000+"ERR_END")
        self.assertEqual(len(result.stdout), 4000)
        self.assertEqual(len(result.stderr), 4000)
        self.assertTrue(result.stdout.endswith("OUT_END"))
        self.assertTrue(result.stderr.endswith("ERR_END"))

    def test_quiet_child_has_heartbeat_and_success_is_preserved(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = runner.stream_process(
                [sys.executable, "-c", "import time; time.sleep(.3)"],
                heartbeat_seconds=.05,
            )
        self.assertEqual(result.returncode, 0)
        self.assertIn("Motore in esecuzione", output.getvalue())
        self.assertEqual(result.stdout, "")

    def test_messages_are_visible_before_child_finishes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gate, output_path = root / "release", root / "output"
            child = ("from pathlib import Path\nimport time\n"
                     "print('LIVE_STDOUT')\n"
                     f"while not Path({str(gate)!r}).exists(): time.sleep(.01)\n")
            harness = (
                "import importlib.util, sys\n"
                f"s=importlib.util.spec_from_file_location('r',{str(RUNNER_PATH)!r})\n"
                "r=importlib.util.module_from_spec(s); s.loader.exec_module(r)\n"
                f"sys.exit(r.stream_process([sys.executable,'-c',{child!r}]).returncode)\n"
            )
            with output_path.open("w") as output:
                proc = subprocess.Popen([sys.executable, "-c", harness], stdout=output,
                                        stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic()+5
                    while "LIVE_STDOUT" not in output_path.read_text() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertIn("LIVE_STDOUT", output_path.read_text())
                    self.assertIsNone(proc.poll())
                    gate.touch()
                    self.assertEqual(proc.wait(timeout=5), 0)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()

    def test_failure_is_recorded_and_never_marked_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            engine = root / "3appdays.py"
            engine.write_text("import sys\nprint('failure details',file=sys.stderr)\nsys.exit(9)\n")
            report = root / "run.json"
            with patch.object(runner, "ENGINE_CANDIDATES", (engine,)), \
                 patch.object(runner, "RUN_STATE_FILE", report), \
                 patch.object(runner, "PROJECT_ROOT", root), \
                 contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(runner.run("fast"), 9)
            state = json.loads(report.read_text())
            self.assertEqual(state["status"], "error")
            self.assertEqual(state["returncode"], 9)
            self.assertIn("failure details", state["stderr_tail"])

    @unittest.skipUnless(os.name == "posix", "POSIX cancellation")
    def test_sigterm_stops_child_and_remains_failure_even_if_child_exits_zero(self):
        for child_handler in ("lambda *args: sys.exit(0)", "signal.SIG_IGN"):
            with self.subTest(child_handler=child_handler), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                ready = root / "ready"
                child = ("import signal,sys,time,os\nfrom pathlib import Path\n"
                         f"signal.signal(signal.SIGTERM,{child_handler})\n"
                         f"Path({str(ready)!r}).write_text(str(os.getpid()))\n"
                         "time.sleep(30)\n")
                harness = (
                    "import importlib.util,sys\n"
                    f"s=importlib.util.spec_from_file_location('r',{str(RUNNER_PATH)!r})\n"
                    "r=importlib.util.module_from_spec(s);s.loader.exec_module(r)\n"
                    f"sys.exit(r.stream_process([sys.executable,'-c',{child!r}],"
                    "heartbeat_seconds=.05,termination_grace_seconds=.1).returncode)\n"
                )
                proc = subprocess.Popen([sys.executable, "-c", harness], stdout=subprocess.DEVNULL)
                try:
                    deadline = time.monotonic()+5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(ready.exists())
                    pid = int(ready.read_text())
                    proc.send_signal(signal.SIGTERM)
                    self.assertEqual(proc.wait(timeout=5), 143)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait()
                    if ready.exists():
                        try:
                            os.kill(int(ready.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass


if __name__ == "__main__":
    unittest.main()
