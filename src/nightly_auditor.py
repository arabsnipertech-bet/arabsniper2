"""Resume bounded nighttime audit chunks under the shared data-writer mutex."""
from datetime import date, datetime, timedelta, time
import json
import os
from pathlib import Path
import signal
import subprocess
import sys

from audit_storage import atomic_bytes, read_store, write_store
from auditor_daily_priority import LEGACY_STATUS, ensure_priority, v7_ready
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

    def output(name, value):
        if os.getenv('GITHUB_OUTPUT'):
            with Path(os.environ['GITHUB_OUTPUT']).open('a') as out:
                out.write(f'{name}={str(value).lower()}\n')

    day = str(today-timedelta(days=1))

    def legacy_status(status, reason=None, evidence_saved=False):
        write_store(root/LEGACY_STATUS, dict(date=str(today), status=status, reason=reason,
                                           evidence_saved=evidence_saved))
        phase('src/gold_storage.py', 'checkpoint-day', '--date', day, '--legacy')
        output('v51_status', status)
        print(f'V5.1 audit status: {status}', flush=True)

    def calibrate_legacy():
        # V7 is already checkpointed. Leave time to persist the status after a
        # bounded V5.1 subprocess, including if a historical file is incomplete.
        end = min(clock()+timedelta(minutes=6), deadline-timedelta(minutes=3))
        args = ['src/adaptive_calibration.py', '--as-of', today.isoformat()]
        if recovery_date:
            args.extend(['--recovery-date', recovery_date])
        status = 'complete'
        reason = None
        evidence_saved = False
        try:
            command_runner(root, [sys.executable, 'auditor/current_audit.py',
                                  'legacy-evidence', '--date', day], end, clock)
            evidence_saved = True
            command_runner(root, [sys.executable, *args], end, clock)
        except (TimeoutError, subprocess.CalledProcessError) as error:
            status = 'partial_v51_skip'
            reason = 'deadline' if isinstance(error, TimeoutError) else f'exit_{error.returncode}'
        legacy_status(status, reason, evidence_saved)

    def checkpoint(work_date, stage):
        value = {'version': 1, 'work_date': work_date.isoformat(), 'stage': stage}
        atomic_bytes(root / PROGRESS, json.dumps(value).encode())
        phase('src/gold_storage.py', 'checkpoint')
        print(f'Auditor checkpoint saved: {work_date} {stage}', flush=True)

    phase('src/gold_storage.py', 'restore-v7-priority', '--date', day)
    if already_completed(root, clock()) and v7_ready(root, today):
        print('Nightly Auditor already published for this Rome date', flush=True)
        output('v7_completed', True)
        return 'already_completed'

    ensure_priority(root, today, phase)
    output('v7_completed', True)
    if deadline-clock() < timedelta(minutes=20):
        if deadline-clock() >= timedelta(minutes=3):
            legacy_status('partial_v51_skip', 'insufficient_window')
        return 'paused'

    # Historic state can be huge or incomplete. V7 already has its own
    # verified checkpoint, and this restore cannot hold the writer past cutoff.
    end = min(clock()+timedelta(minutes=15), deadline-timedelta(minutes=3))
    try:
        command_runner(root, [sys.executable, 'src/gold_storage.py', 'restore'], end, clock)
    except (TimeoutError, subprocess.CalledProcessError) as error:
        legacy_status('partial_v51_skip', 'restore_deadline' if isinstance(error, TimeoutError) else 'restore_failed')
        return 'paused'

    pending = saved_batch(root, today)
    if pending is not None and pending[1] != 'preparing' and not has_daily_report(root, pending[0]):
        pending = (pending[0], None)
    phase('src/homepage_results.py')
    phase('src/private_publication.py', '--results-only')
    if deadline-clock() >= timedelta(minutes=4):
        calibrate_legacy()
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
            if work_date != today or not v7_ready(root, today):
                phase('auditor/current_audit.py', 'daily', '--date', report_date)
            stage = None
        if not has_daily_report(root, work_date):
            raise ValueError(f'Missing or mismatched daily Auditor report for batch {work_date}')
        if stage is None:
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
