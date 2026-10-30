"""Admit the daily Auditor only in its protected Europe/Rome window."""
from datetime import datetime, time, timedelta
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import read_store

ROME = ZoneInfo('Europe/Rome')
MARKER = 'data/arab_quote_auditor_status.json'
START = time(0, 30)
LAST_START = time(1, 0)
STOP = time(1, 50)  # Ten minutes for cleanup before the 02:00 Night window.


def admission(now):
    if now.tzinfo is None:
        raise ValueError('Auditor clock must have a timezone')
    local = now.astimezone(ROME)
    deadline = datetime.combine(local.date(), STOP, ROME)
    allowed = START <= local.time().replace(tzinfo=None) < LAST_START
    return {'run': allowed, 'reason': 'night_window' if allowed else 'outside_audit_window',
            'date': local.date().isoformat(), 'deadline': deadline.isoformat()}


def already_completed(root, now):
    marker = read_store(Path(root)/MARKER, {})
    if not isinstance(marker, dict) or marker.get('schema_version') != 1:
        return False
    try:
        completed = datetime.fromisoformat(marker['completed_at'])
        started = datetime.fromisoformat(marker['started_at'])
        return (marker.get('status') == 'complete' and completed.tzinfo is not None
                and started.tzinfo is not None and started <= completed <= now
                and marker.get('date') == now.astimezone(ROME).date().isoformat()
                and started.astimezone(ROME).date() == completed.astimezone(ROME).date()
                == now.astimezone(ROME).date())
    except (KeyError, ValueError, TypeError):
        return False


def preflight(api, now, run_id):
    from fast_backup_dispatch import ACTIVE, WRITERS
    result = admission(now)
    if not result['run']:
        return result  # No provider/storage/GitHub access outside the window.
    for status in ACTIVE:
        for run in api.active_runs(status):
            if not isinstance(run, dict) or not isinstance(run.get('path'), str):
                raise ValueError('Invalid writer listing')
            if str(run.get('id')) != str(run_id) and run['path'].split('@')[0] in WRITERS:
                return {**result, 'run': False, 'reason': 'writer_busy', 'run_id': run.get('id')}
    return result


def main():
    from fast_backup_dispatch import GitHub, REPOSITORY
    if os.getenv('GITHUB_REPOSITORY') != REPOSITORY or os.getenv('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('Auditor can run only on production main')
    result = preflight(GitHub(os.getenv('GITHUB_TOKEN')), datetime.now(ROME), os.getenv('GITHUB_RUN_ID'))
    print(json.dumps(result))
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write('run=' + str(result['run']).lower() + '\n')
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('Nightly Auditor admission: **' + result['reason'] + '**\n')


if __name__ == '__main__':
    main()
