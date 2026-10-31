"""Run the entire historical pipeline once, with a hard 01:50 Rome deadline."""
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

from audit_storage import atomic_bytes, read_store
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


PROGRESS = 'data/arab_quote_auditor_progress.json'


def saved_batch(root, today):
    checkpoint = read_store(root / PROGRESS)
    if checkpoint is None:
        return None
    if (not isinstance(checkpoint, dict) or checkpoint.get('version') != 1
            or checkpoint.get('stage') not in ('daily', 'history')):
        raise ValueError('Invalid nightly Auditor checkpoint')
    day = date.fromisoformat(checkpoint['work_date'])
    if day > today:
        raise ValueError('Future nightly Auditor checkpoint')
    return day, checkpoint['stage']


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
    today = date.fromisoformat(gate['date'])

    def phase(*args):
        command_runner(root, [sys.executable, *args], deadline, clock)

    def checkpoint(work_date, stage):
        value = {'version': 1, 'work_date': work_date.isoformat(), 'stage': stage}
        atomic_bytes(root / PROGRESS, json.dumps(value).encode())
        phase('src/gold_storage.py', 'publish')
        print(f'Auditor checkpoint saved: {work_date} {stage}', flush=True)

    phase('src/gold_storage.py', 'restore')
    pending = saved_batch(root, today)
    if pending is None and already_completed(root, clock()):
        print('Nightly Auditor already published for this Rome date', flush=True)
        return 'already_completed'

    # Scans append new immutable journals while an older batch is suspended.
    # Finish its frozen input before consuming those new journals.
    batches = [pending] if pending is not None else []
    if pending is None or pending[0] != today:
        batches.append((today, None))
    for work_date, stage in batches:
        report_date = (work_date - timedelta(days=1)).isoformat()
        if stage is None:
            phase('auditor/current_audit.py', 'prepare')
            phase('auditor/current_audit.py', 'daily', '--date', report_date)
            checkpoint(work_date, 'daily')
        # Public projections are not in R2. Rebuild on resume if git failed.
        phase('src/homepage_results.py')
        phase('src/private_publication.py', '--results-only')
        if stage != 'history':
            phase('auditor/current_audit.py', 'history', '--date', report_date)
            checkpoint(work_date, 'history')
        phase('src/gold_policy_audit.py')
        if work_date == today:
            phase('src/adaptive_calibration.py', '--as-of', today.isoformat())
        (root / PROGRESS).unlink(missing_ok=True)
        if work_date == today:
            marker = {'schema_version': 1, 'status': 'complete', 'date': gate['date'],
                      'started_at': started.isoformat(), 'completed_at': clock().isoformat(),
                      'run_id': os.getenv('GITHUB_RUN_ID')}
            atomic_bytes(root / MARKER, json.dumps(marker, separators=(',', ':')).encode()+b'\n')
        phase('src/gold_storage.py', 'publish')
        print(f'Nightly Auditor batch completed and published: {work_date}', flush=True)
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
