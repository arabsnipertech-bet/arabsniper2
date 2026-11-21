"""Daily V7 certification plus one independently saved, bounded results recovery."""
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import sys

from audit_daily_evidence import evidence_paths, read_receipt, receipt_path, verified_evidence
from audit_official_scores import LOOKBACK_DAYS
from audit_storage import read_store, write_store
from auditor_daily_priority import LEGACY_STATUS, ensure_priority, v7_ready
from auditor_schedule import EXCLUDED_RECOVERY_DAYS, MARKER, ROME
from gold_storage import previous_manifest, publish, restore_subset, restore_v7_priority

PROGRESS = 'data/arab_quote_auditor_light_progress.json'
WORK_BUDGET = timedelta(minutes=12)
SAVE_RESERVE = timedelta(seconds=90)
RESULT_IDS = 20
DAY_FILE = re.compile(r'auditordata/prematch_(\d{4}-\d{2}-\d{2})\.json')


def output(name, value):
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
            stream.write(f'{name}={str(value).lower()}\n')


def remote_receipt(index, receipt, day):
    """Trust a saved receipt only while all named evidence pointers still match."""
    hashes = (receipt or {}).get('sha256', {})
    return (isinstance(receipt, dict) and receipt.get('version') == 1
            and receipt.get('date') == str(day) and isinstance(hashes, dict)
            and hashes.keys() == evidence_paths(day)
            and all(index.get(path, {}).get('sha256') == digest for path, digest in hashes.items()))


def recoverable_dates(index, today):
    days = []
    for path in index:
        match = DAY_FILE.fullmatch(path)
        if match:
            day = date.fromisoformat(match[1])
            if 1 < (today-day).days <= LOOKBACK_DAYS and day not in EXCLUDED_RECOVERY_DAYS:
                days.append(day)
    return sorted(set(days), reverse=True)


def recovery_candidate(root, index, today, progress):
    candidates = []
    for day in recoverable_dates(index, today):
        saved = progress['days'].get(str(day), {})
        receipt = read_receipt(root, day)
        if remote_receipt(index, receipt, day):
            if receipt.get('pending_fixtures') == 0:
                continue
            checked = datetime.fromisoformat(receipt['checked_at'])
            if checked.astimezone(ROME).date() >= today:
                continue
        if saved.get('checked_on') == str(today):
            continue
        # Old failed blocks rotate behind dates not yet tried. Recent dates win ties.
        candidates.append((saved.get('checked_on', ''), -day.toordinal(), day))
    return min(candidates)[2] if candidates else None


def record_day(root, day, now, progress):
    receipt = verified_evidence(root, day)
    if receipt is None:
        raise ValueError('Cannot checkpoint an unverified recovery block')
    attempts = read_store(root/f'auditarchive/prematch_{day}_daily.json', {}).get('result_recovery', {})
    ids = attempts.get('attempted_fixture_ids', [])
    old = progress['days'].get(str(day), {})
    progress['days'][str(day)] = {
        'checked_on': str(now.astimezone(ROME).date()),
        'pending_fixtures': receipt['pending_fixtures'],
        'after_fixture_id': ids[-1] if ids else old.get('after_fixture_id'),
    }


def cycle(storage, bucket, root, started, deadline, command_runner, clock):
    """The parent subprocess enforces the total deadline, including storage reads."""
    root = Path(root)
    today = started.astimezone(ROME).date()
    yesterday = today-timedelta(days=1)
    restore_v7_priority(storage, bucket, root, yesterday)
    index = previous_manifest(storage, bucket)
    restore_subset(storage, bucket, root, index, {PROGRESS} & index.keys())
    progress = read_store(root/PROGRESS, {'version': 1, 'days': {}})
    if (not isinstance(progress, dict) or progress.get('version') != 1
            or not isinstance(progress.get('days'), dict)):
        raise ValueError('Invalid lightweight Auditor progress')
    marker = read_store(root/MARKER, {})
    if (marker.get('scope') == 'v7_daily' and marker.get('status') == 'complete'
            and marker.get('date') == str(today) and v7_ready(root, today)):
        output('v7_completed', True)
        return 'already_completed'

    def phase(*args, end=None):
        options = list(args)
        if options[0] == 'src/auditor_daily_priority.py':
            options.extend(['--checkpoint-freeze', '--max-result-ids', str(RESULT_IDS)])
        command_runner(root, [sys.executable, *options], min(end or deadline, deadline-SAVE_RESERVE), clock)

    def save_progress():
        write_store(root/PROGRESS, progress)
        publish(storage, bucket, root, checkpoint_only=True, selected_paths=[PROGRESS])

    ensure_priority(root, today, phase)
    if not v7_ready(root, today) or verified_evidence(root, yesterday) is None:
        raise ValueError('Daily V7 certification is incomplete')
    output('v7_completed', True)
    record_day(root, yesterday, started, progress)
    save_progress()

    # Only small receipts are inspected. No global forecast/history restore.
    receipt_paths = {receipt_path(day) for day in recoverable_dates(index, today)} & index.keys()
    restore_subset(storage, bucket, root, index, receipt_paths)
    candidate = recovery_candidate(root, index, today, progress)
    recovery_status = 'not_needed'
    if candidate is not None:
        recovery_status = 'deferred_budget'
        if deadline-clock() >= SAVE_RESERVE+timedelta(minutes=2):
            paths = (evidence_paths(candidate) | {receipt_path(candidate)}) & index.keys()
            restore_subset(storage, bucket, root, index, paths)
            previous = progress['days'].get(str(candidate), {})
            args = ['auditor/current_audit.py', 'daily', '--date', str(candidate),
                    '--no-legacy-evidence', '--max-result-ids', str(RESULT_IDS)]
            if previous.get('after_fixture_id'):
                args.extend(['--after-fixture-id', previous['after_fixture_id']])
            try:
                phase(*args)
                # Save this date before marking its cursor, independently of V7 learning.
                phase('src/gold_storage.py', 'checkpoint-results', '--date', str(candidate), end=deadline)
            except TimeoutError:
                recovery_status = 'deferred_budget'
                progress['days'][str(candidate)] = {**previous, 'checked_on': str(today), 'status': 'timeout'}
            else:
                record_day(root, candidate, started, progress)
                recovery_status = 'saved'
            save_progress()

    progress['cycle_date'] = str(today)
    progress['recovery'] = {'date': str(candidate) if candidate else None, 'status': recovery_status}
    # The retired V5.1 global rebuild is explicitly separate from daily V7 success.
    write_store(root/LEGACY_STATUS, {'date': str(today), 'status': 'partial_v51_skip',
                                   'reason': 'legacy_global_rebuild_deferred', 'evidence_saved': False})
    write_store(root/PROGRESS, progress)
    write_store(root/MARKER, {'schema_version': 1, 'status': 'complete', 'scope': 'v7_daily',
                            'date': str(today), 'started_at': started.isoformat(),
                            'completed_at': clock().isoformat(), 'run_id': os.getenv('GITHUB_RUN_ID'),
                            'pending_fixtures': progress['days'][str(yesterday)]['pending_fixtures'],
                            'recovery': progress['recovery'], 'legacy_status': 'deferred'})
    publish(storage, bucket, root, checkpoint_only=True, selected_paths=[PROGRESS, MARKER, LEGACY_STATUS])
    output('v51_status', 'partial_v51_skip')
    print('Daily V7 certified; incremental recovery: '+json.dumps(progress['recovery']), flush=True)
    return 'complete'


def main():
    from gold_storage import client
    from nightly_auditor import run_command
    from auditor_schedule import admission
    started = datetime.now(ROME)
    gate = admission(started)
    if not gate['run'] or os.getenv('GOLD_PRIVATE') != 'true':
        raise ValueError('Lightweight Auditor requires the protected private production window')
    deadline = min(datetime.fromisoformat(os.environ['AUDITOR_LIGHT_DEADLINE']),
                   datetime.fromisoformat(gate['deadline']), started+WORK_BUDGET)
    cycle(client(), os.environ['R2_BUCKET'], Path(__file__).resolve().parents[1],
          started, deadline, run_command, lambda: datetime.now(ROME))


if __name__ == '__main__':
    main()
