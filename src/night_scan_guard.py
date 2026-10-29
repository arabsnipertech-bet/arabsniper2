"""One completed five-day Night Scan per Rome date, across all triggers.

The marker travels with the private checkpoint. Since its pointer is published
last, an incomplete R2 publication cannot make a later workflow skip recovery.
This module never calls the football provider or changes prediction rules.
"""
import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import atomic_bytes, read_store

TZ = ZoneInfo('Europe/Rome')
MARKER = 'data/arab_quote_night_status.json'


def timestamp(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return result if result.tzinfo else None
    except (AttributeError, TypeError, ValueError):
        return None


def window(now):
    day = now.astimezone(TZ).date()
    return [(day + timedelta(days=i)).isoformat() for i in range(5)]


def valid_window(root, dates, started_at=None, completed_at=None):
    """Inspect actual payload dates, including checksummed multipart files."""
    for i, day in enumerate(dates, 1):
        payload = read_store(Path(root) / f'data/arab_quote_day{i}.json', {})
        if not isinstance(payload, dict) or payload.get('date') != day or not isinstance(payload.get('matches'), list):
            return False
        if started_at is not None:
            observed = timestamp(payload.get('updated_at') or payload.get('generated_at'))
            if (not observed or observed < started_at or observed > completed_at
                    or payload.get('refresh_status') != 'ok'):
                return False
    return True


def completed_today(root, now=None):
    now = now or datetime.now(TZ)
    marker = read_store(Path(root) / MARKER, {})
    if not isinstance(marker, dict) or marker.get('schema_version') != 1 or marker.get('status') != 'complete':
        return False
    dates = window(now)
    started = timestamp(marker.get('started_at'))
    completed = timestamp(marker.get('completed_at'))
    if (marker.get('dates') != dates or not started or not completed
            or not started <= completed <= now
            or started.astimezone(TZ).date().isoformat() != dates[0]
            or completed.astimezone(TZ).date().isoformat() != dates[0]):
        return False
    return valid_window(root, dates)


def record_completion(root, started_at, completed_at):
    started, completed = timestamp(started_at), timestamp(completed_at)
    if not started or not completed or completed < started or window(started) != window(completed):
        raise ValueError('Night Scan crossed the Rome date or has invalid timestamps')
    dates = window(started)
    if not valid_window(root, dates, started, completed):
        raise ValueError('Night Scan did not refresh all five expected dates')
    marker = {'schema_version': 1, 'status': 'complete', 'dates': dates,
              'started_at': started_at, 'completed_at': completed_at,
              'run_id': os.getenv('GITHUB_RUN_ID'), 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT')}
    atomic_bytes(Path(root) / MARKER, json.dumps(marker, separators=(',', ':')).encode() + b'\n')
    return marker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--force', action='store_true', help='Explicit operator recovery only')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    run = args.force or os.getenv('ARAB_FORCE_NIGHT') == 'true' or not completed_today(root)
    message = 'Night Scan required' if run else 'Night Scan already completed for this Rome date; no duplicate scan'
    print(message)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f"run={'true' if run else 'false'}\n")
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(message + '\n')


if __name__ == '__main__':
    main()
