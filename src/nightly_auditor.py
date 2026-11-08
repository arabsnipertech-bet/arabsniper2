"""Resume bounded nighttime audit chunks under the shared data-writer mutex."""
from datetime import date, datetime, timedelta, time
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

from audit_storage import atomic_bytes, read_store
from auditor_schedule import ROME, MARKER, admission, recovery_admission, already_completed, has_daily_report


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
            or checkpoint.get('stage') not in ('preparing', 'daily', 'history')):
        raise ValueError('Invalid nightly Auditor checkpoint')
    day = date.fromisoformat(checkpoint['work_date'])
    if day > today:
        raise ValueError('Future nightly Auditor checkpoint')
    return day, checkpoint['stage']


def run(root, clock=lambda: datetime.now(ROME), command_runner=run_command, recovery_date=None):
    root = Path(root)
    started = clock()
    gate = recovery_admission(started, recovery_date) if recovery_date else admission(started)
    if not gate['run']:
        print('Nightly Auditor skipped: ' + gate['reason'], flush=True)
        return gate['reason']
    if os.getenv('GOLD_PRIVATE') != 'true':
        raise ValueError('Nightly Auditor requires the authoritative private checkpoint')
    deadline = datetime.fromisoformat(gate['deadline'])
    today = date.fromisoformat(gate['date'])

    def phase(*args):
        command_runner(root, [sys.executable, *args], deadline, clock)

    def calibrate():
        args = ['src/adaptive_calibration.py', '--as-of', today.isoformat()]
        if recovery_date:
            args.extend(['--recovery-date', recovery_date])
        phase(*args)

    def checkpoint(work_date, stage):
        value = {'version': 1, 'work_date': work_date.isoformat(), 'stage': stage}
        atomic_bytes(root / PROGRESS, json.dumps(value).encode())
        phase('src/gold_storage.py', 'checkpoint')
        print(f'Auditor checkpoint saved: {work_date} {stage}', flush=True)

    phase('src/gold_storage.py', 'restore')
    pending = saved_batch(root, today)
    if pending is not None and pending[1] != 'preparing' and not has_daily_report(root, pending[0]):
        pending = (pending[0], None)  # Rebuild an incomplete legacy checkpoint.
    if pending is None and already_completed(root, clock()):
        print('Nightly Auditor already published for this Rome date', flush=True)
        return 'already_completed'

    from auditor_daily_priority import ensure_priority
    ensure_priority(root, today, phase, calibrate)
    if deadline-clock() < timedelta(minutes=20):
        return 'paused'

    # Scans append new immutable journals while an older batch is suspended.
    # Finish its frozen input before consuming those new journals.
    batches = [pending] if pending is not None else []
    if pending is None or pending[0] != today:
        batches.append((today, None))
    for work_date, stage in batches:
        report_date = (work_date - timedelta(days=1)).isoformat()
        if stage in (None, 'preparing'):
            from audit_journal import pending_journals
            cutoff = datetime.combine(work_date, time(), ROME)
            phase('auditor/current_audit.py', 'prepare', '--journal-limit', '8', '--delta-archive',
                  '--journal-before', cutoff.isoformat())
            checkpoint(work_date, 'preparing')
            if pending_journals(root, cutoff):
                return 'paused'
            phase('auditor/current_audit.py', 'daily', '--date', report_date)
            stage = None
        if not has_daily_report(root, work_date):
            raise ValueError(f'Missing or mismatched daily Auditor report for batch {work_date}')
        # Save today's certified learning before the expensive historical pass.
        # Retain the daily progress marker so a timeout never claims full completion.
        if work_date == today and stage != 'history':
            calibrate()
        if stage is None or (work_date == today and stage != 'history'):
            checkpoint(work_date, 'daily')
        # Public projections are not in R2. Rebuild on resume if git failed.
        phase('src/homepage_results.py')
        phase('src/private_publication.py', '--results-only')
        if stage != 'history':
            from auditor_history_chunks import advance
            if not advance(root, work_date, phase, deadline, clock):
                return 'paused'
            checkpoint(work_date, 'history')
        phase('src/gold_policy_audit.py')
        if work_date == today:
            calibrate()
        (root / PROGRESS).unlink(missing_ok=True)
        if work_date == today:
            marker = {'schema_version': 1, 'status': 'complete', 'date': gate['date'],
                      'started_at': started.isoformat(), 'completed_at': clock().isoformat(),
                      'run_id': os.getenv('GITHUB_RUN_ID'),
                      'validation_cycle': 'validation_freeze_v5.1_2026-11-01',
                      'calibration_status': __import__('adaptive_calibration').calibration_status(
                          read_store(root/'data/arab_calibration_weights.json', {}), clock().isoformat())}
            atomic_bytes(root / MARKER, json.dumps(marker, separators=(',', ':')).encode()+b'\n')
        phase('src/gold_storage.py', 'checkpoint')
        print(f'Nightly Auditor batch completed and published: {work_date}', flush=True)
    return 'complete'


def main(argv=()):
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--recovery-date')
    args = parser.parse_args(argv)
    result = run(Path(__file__).resolve().parents[1], recovery_date=args.recovery_date)
    # The external watchdog must distinguish a skipped admission from a real
    # published completion. Emit proof only after the runner verified it.
    if result not in ('complete', 'already_completed', 'paused', 'outside_audit_window'):
        raise SystemExit(f'Auditor did not certify: {result}')
    if os.getenv('GITHUB_OUTPUT'):
        with Path(os.environ['GITHUB_OUTPUT']).open('a') as out:
            out.write('completed=' + str(result in ('complete', 'already_completed')).lower() + '\n')
            out.write('paused=' + str(result == 'paused').lower() + '\n')


if __name__ == '__main__':
    main(sys.argv[1:])
