"""Deduplicate full Fast scans using the successfully published checkpoint.

The marker is written only after a verified refresh of both current dates and
travels with the checkpoint's last-write pointer. A failed Gold publication
therefore leaves the previous marker authoritative. Prediction rules are not
changed. A completed scan covers its hourly slot, with a minimum interval across slots.
"""
import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from audit_storage import atomic_bytes, read_store
from night_scan_guard import TZ, timestamp, valid_window, window
from fast_backup_dispatch import fresh_for_hour

MARKER = 'data/arab_quote_fast_status.json'


def completed_recently(root, now=None):
    now = now or datetime.now(TZ)
    marker = read_store(Path(root) / MARKER, {})
    if not isinstance(marker, dict) or marker.get('schema_version') != 1 or marker.get('status') != 'complete':
        return False
    started, completed = timestamp(marker.get('started_at')), timestamp(marker.get('completed_at'))
    dates = window(now)[:2]
    if (not started or not completed or not started <= completed <= now
            or not fresh_for_hour(started, now) or marker.get('dates') != dates
            or window(started)[:2] != dates or window(completed)[:2] != dates):
        return False
    return valid_window(root, dates)


def record_completion(root, requested_at, now=None):
    now = now or datetime.now(TZ)
    requested = timestamp(requested_at)
    run = read_store(Path(root) / 'data/arab_quote_run_state.json', {})
    if not isinstance(run, dict):
        raise ValueError('Invalid Fast run state')
    started, finished = timestamp(run.get('started_at')), timestamp(run.get('generated_at'))
    if (not requested or not started or not finished or not requested <= started <= finished <= now
            or run.get('mode') != 'fast' or run.get('status') != 'ok' or run.get('returncode') != 0
            or window(started) != window(now)):
        raise ValueError('Fast did not complete in the requested Rome date')
    dates = window(started)[:2]
    if not valid_window(root, dates, started, now):
        raise ValueError('Fast did not refresh both expected dates')
    marker = {'schema_version': 1, 'status': 'complete', 'dates': dates,
              'started_at': started.isoformat(), 'completed_at': now.isoformat(),
              'run_id': os.getenv('GITHUB_RUN_ID'), 'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT')}
    atomic_bytes(Path(root) / MARKER, json.dumps(marker, separators=(',', ':')).encode() + b'\n')
    return marker


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    now = datetime.now(TZ).replace(microsecond=0)
    if args.record:
        record_completion(root, os.getenv('FAST_REQUESTED_AT'), now)
        print('Both Fast dates verified; completion becomes durable only with publication')
        return
    # Code releases must still refresh their policy even when prices are fresh.
    run = (os.getenv('EVENT_NAME') == 'push' or os.getenv('ARAB_FORCE_FAST') == 'true'
           or not completed_recently(root, now))
    message = 'Fast Scan required' if run else 'Fast already published for this hour or within the minimum interval; skipped'
    print(message)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f"run={'true' if run else 'false'}\nstarted_at={now.isoformat()}\n")
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(message + '\n')


if __name__ == '__main__':
    main()
