"""Certify each Fast scope against the data published with the checkpoint."""

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from audit_storage import atomic_bytes, read_store, write_store
from fast_schedule import (AFTERNOON, DAY_COUNT, EXTENDED, FORCED, HALF_HOUR, INTERVAL,
                           ON_HOUR, ORDINARY, explicit_slot, latest_slot, slots)
from night_scan_guard import TZ, timestamp, valid_window, window

MARKER = 'data/arab_quote_fast_status.json'


def scan_scope(requested=ORDINARY):
    return FORCED if os.getenv('ARAB_FORCE_FAST') == 'true' else requested


def fast_day_count(requested=ORDINARY):
    return DAY_COUNT[scan_scope(requested)]


def receipts(root):
    marker = read_store(Path(root) / MARKER, {})
    if isinstance(marker, dict) and marker.get('schema_version') == 2:
        saved = marker.get('receipts')
        if isinstance(saved, dict):
            return saved
    return {}


def completed_recently(root, now=None, scope=ORDINARY, slot=None):
    now = now or datetime.now(TZ)
    marker = receipts(root).get(scope, {})
    started, completed = timestamp(marker.get('started_at')), timestamp(marker.get('completed_at'))
    dates = window(now)[:DAY_COUNT[scope]]
    if (not started or not completed or not started <= completed <= now
            or marker.get('dates') != dates or window(started)[0] != dates[0]):
        return False
    if slot is not None:
        published_slot = timestamp(marker.get('slot'))
        if not published_slot or published_slot < slot or started < published_slot:
            return False
    elif now - started >= INTERVAL:
        return False
    return valid_window(root, dates)


def reuse_today(root, now):
    """Only the verified 15:00 ordinary scan can supply day 1 to the extended scan."""
    afternoon = slots(now.astimezone(TZ).date(), EXTENDED)[0]
    ordinary = afternoon - timedelta(minutes=30)
    if not afternoon <= now < ordinary + INTERVAL:
        return False
    if not completed_recently(root, now, ORDINARY, ordinary):
        return False
    marker = receipts(root)[ORDINARY]
    if timestamp(marker.get('slot')) != ordinary:
        return False
    started, completed = timestamp(marker['started_at']), timestamp(marker['completed_at'])
    if not valid_window(root, window(now)[:1], started, completed):
        return False
    day = read_store(Path(root) / 'data/arab_quote_day1.json')
    coverage = day.get('universal_coverage') or {}
    available = coverage.get('available_fixture_ids')
    return (coverage.get('coverage_basis') == 'provider_schedule' and type(available) is int
            and coverage.get('displayed_fixtures') == len(day['matches'])
            and len(day['matches']) >= available)


def align_fast_window(directory, dates):
    """Preserve saved boards and selection exports by Rome date when slots roll over."""
    directory = Path(directory)
    day_paths = [directory / f'arab_quote_day{i}.json' for i in range(1, len(dates) + 1)]
    old_days = [read_store(path, {}) for path in day_paths]
    if all(day.get('date') == expected for day, expected in zip(old_days, dates)):
        return
    exports = [directory / f'arab_quote_selections_day{i}.json' for i in range(1, len(dates) + 1)]
    old_exports = [read_store(path, {}) for path in exports]
    days_by_date = {day['date']: day for day in reversed(old_days)
                    if day.get('date') and isinstance(day.get('matches'), list)}
    exports_by_date = {entry['date']: entry for entry in reversed(old_exports) if entry.get('date')}
    for i, target in enumerate(dates, 1):
        if old_days[i - 1].get('date') != target:
            saved = days_by_date.get(target, {'date': target, 'matches': [], 'refresh_status': 'awaiting_night'})
            write_store(day_paths[i - 1], {**saved, 'day': i})
        if old_exports[i - 1].get('date') != target:
            saved = exports_by_date.get(target, {'date': target, 'selection_board': {}, 'rankings': {},
                                                  'probability_rankings': {}})
            write_store(exports[i - 1], saved)


def record_completion(root, requested_at, now=None):
    now = now or datetime.now(TZ)
    requested = timestamp(requested_at)
    run = read_store(Path(root) / 'data/arab_quote_run_state.json', {})
    started, finished = timestamp(run.get('started_at')), timestamp(run.get('generated_at'))
    scope = run.get('fast_scope')
    if (not requested or not started or not finished or not requested <= started <= finished <= now
            or run.get('mode') != 'fast' or run.get('status') != 'ok' or run.get('returncode') != 0
            or window(requested) != window(started) or window(started) != window(now)
            or scope not in DAY_COUNT or run.get('days') != DAY_COUNT[scope]):
        raise ValueError('Fast did not complete in the requested Rome date and scope')
    slot = explicit_slot(run['fast_slot'], scope, started) if run.get('fast_slot') else None
    dates = window(started)[:DAY_COUNT[scope]]
    result = read_store(Path(root) / 'data/arab_quote_dashboard.json', {}).get('fast_scan', {})
    collected = result.get('collected_dates')
    reused = result.get('reused_dates')
    planned = timestamp(result.get('planned_at'))
    if (result.get('scope') != scope or result.get('dates') != dates
            or not planned or not started <= planned <= finished
            or (collected, reused) not in [(dates, []), (dates[1:], dates[:1])]):
        raise ValueError('Fast did not produce the requested dates')
    if reused and (scope != EXTENDED or not reuse_today(root, planned)):
        raise ValueError('Fast day 1 reuse was not backed by a valid 15:00 scan')
    if not valid_window(root, collected, started, now, start_index=len(reused) + 1):
        raise ValueError('Fast did not refresh all expected dates')

    done = {'scope': scope, 'dates': dates, 'collected_dates': collected, 'reused_dates': reused,
            'slot': slot.isoformat() if slot else None, 'started_at': started.isoformat(),
            'completed_at': now.isoformat(), 'run_id': os.getenv('GITHUB_RUN_ID'),
            'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT')}
    marker = {'schema_version': 2, 'receipts': {**receipts(root), scope: done}}
    atomic_bytes(Path(root) / MARKER, json.dumps(marker, separators=(',', ':')).encode() + b'\n')
    return done


def plan(root, now, event, schedule, scope, requested_slot, *, created=None):
    slot = None
    if event == 'schedule':
        if schedule not in (ON_HOUR, HALF_HOUR, AFTERNOON) or created is None:
            raise ValueError('Invalid Fast schedule event')
        scope = EXTENDED if schedule == AFTERNOON else ORDINARY
        slot = latest_slot(created, scope, schedule)
        if slot is None or slot.date() != now.astimezone(TZ).date():
            return False, scope, None, 'scheduled_date_expired'
    elif requested_slot:
        slot = explicit_slot(requested_slot, scope, now)
    elif scope != FORCED:
        slot = latest_slot(now, scope)

    covered = scope != FORCED and completed_recently(root, now, scope, slot)
    run = event == 'push' or not covered
    return run, scope, slot, 'scan_due' if run else 'already_published'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    now = datetime.now(TZ).replace(microsecond=0)
    if args.record:
        marker = record_completion(root, os.getenv('FAST_REQUESTED_AT'), now)
        print('Verified Fast completion: ' + json.dumps(marker, sort_keys=True))
        return
    event = os.getenv('EVENT_NAME')
    created = None
    if event == 'schedule':
        from fast_backup_dispatch import GitHub, instant
        run_id = int(os.environ['GITHUB_RUN_ID'])
        metadata = GitHub(os.getenv('GITHUB_TOKEN')).request(f'runs/{run_id}')
        created = instant(metadata['created_at'])
    run, scope, slot, reason = plan(
        root, now, event, os.getenv('FAST_SCHEDULE'), scan_scope(os.getenv('FAST_SCOPE') or ORDINARY),
        os.getenv('FAST_BACKUP_SLOT'), created=created)
    message = f'Fast {scope}: {reason}, slot={slot.isoformat() if slot else "manual"}'
    print(message)
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f"run={'true' if run else 'false'}\nscope={scope}\n"
                      f"slot={slot.isoformat() if slot else ''}\nstarted_at={now.isoformat()}\n")
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write(message + '\n')


if __name__ == '__main__':
    main()
