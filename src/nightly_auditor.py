"""Run the entire historical pipeline once, with a hard 01:50 Rome deadline."""
from datetime import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

from audit_storage import atomic_bytes
from auditor_schedule import ROME, MARKER, admission, already_completed


def run_command(root, command, deadline, clock=lambda: datetime.now(ROME)):
    remaining = (deadline - clock()).total_seconds()
    if remaining <= 0:
        raise TimeoutError('Auditor protected window ended; no further work')
    print('Nightly phase: ' + ' '.join(command), flush=True)
    process = subprocess.Popen(command, cwd=root, start_new_session=True)
    try:
        code = process.wait(timeout=remaining)
    except subprocess.TimeoutExpired:
        # Kill the whole phase, including grandchildren, before releasing the mutex.
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise TimeoutError('Auditor stopped at its protected deadline') from None
    if code:
        raise subprocess.CalledProcessError(code, command)


def run(root, clock=lambda: datetime.now(ROME), command_runner=run_command):
    root = Path(root)
    started = clock()
    gate = admission(started)
    if not gate['run']:
        print('Nightly Auditor skipped: ' + gate['reason'], flush=True)
        return gate['reason']
    if os.getenv('GOLD_PRIVATE') != 'true':
        raise ValueError('Nightly Auditor requires the authoritative private checkpoint')
    deadline = datetime.fromisoformat(gate['deadline'])

    def phase(*args):
        command_runner(root, [sys.executable, *args], deadline, clock)

    phase('src/gold_storage.py', 'restore')
    if already_completed(root, clock()):
        print('Nightly Auditor already published for this Rome date', flush=True)
        return 'already_completed'
    phase('src/audit_journal.py', 'ingest')
    phase('auditor/current_audit.py', 'settle')
    phase('src/gold_policy_audit.py')
    phase('src/task_progress.py', 'src/homepage_results.py')
    # Completion is durable only if the checkpoint's final pointer is published.
    marker = {'schema_version': 1, 'status': 'complete', 'date': gate['date'],
              'started_at': started.isoformat(), 'completed_at': clock().isoformat(),
              'run_id': os.getenv('GITHUB_RUN_ID')}
    atomic_bytes(root/MARKER, json.dumps(marker, separators=(',', ':')).encode()+b'\n')
    phase('src/gold_storage.py', 'publish')
    phase('src/private_publication.py')
    print('Nightly Auditor completed and published', flush=True)
    return 'complete'


def main():
    result = run(Path(__file__).resolve().parents[1])
    # The external watchdog must distinguish a skipped admission from a real
    # published completion. Emit proof only after the runner verified it.
    if os.getenv('GITHUB_OUTPUT'):
        with Path(os.environ['GITHUB_OUTPUT']).open('a') as out:
            out.write('completed=' + str(result in ('complete', 'already_completed')).lower() + '\n')


if __name__ == '__main__':
    main()
