"""Durable per-date audit receipts; bounded work before releasing the writer lock."""
from datetime import datetime, timedelta, date
from hashlib import sha256
from pathlib import Path

from audit_storage import read_store, write_store

CURSOR = 'data/arab_quote_auditor_history_progress.json'
MAX_DAYS = 2
RESERVE = timedelta(minutes=20)


def digest(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def receipt_path(root, day):
    return Path(root)/f'auditarchive/prematch_{day}_checkpoint.json'


def seal_day(root, day, now, pending, queried=False):
    root = Path(root)
    outputs = [f'auditarchive/prematch_{day}_{suffix}' for suffix in
               ('results.json', 'daily.json', 'daily.md')]
    calibration = f'auditarchive/prematch_{day}_calibration.json'
    if (root/calibration).exists():
        outputs.append(calibration)
    receipt = {'version': 1, 'day': str(day), 'checked_at': now.isoformat(),
               'pending': pending, 'results_queried': queried, 'input_sha256': digest(root/f'auditordata/prematch_{day}.json'),
               'outputs': {path: digest(root/path) for path in outputs}}
    write_store(receipt_path(root, day), receipt)


def reusable(root, day, as_of):
    root = Path(root)
    receipt = read_store(receipt_path(root, day), {})
    if not receipt:
        return False
    if receipt.get('version') != 1 or receipt.get('day') != str(day):
        raise ValueError('Invalid historical day receipt')
    if receipt['input_sha256'] != digest(root/f'auditordata/prematch_{day}.json'):
        return False  # Newly ingested evidence requires a new report.
    expected = {f'auditarchive/prematch_{day}_{suffix}' for suffix in
                ('results.json', 'daily.json', 'daily.md')}
    if not expected <= receipt['outputs'].keys():
        raise ValueError('Incomplete historical day receipt')
    for path, checksum in receipt['outputs'].items():
        if path not in expected | {f'auditarchive/prematch_{day}_calibration.json'}:
            raise ValueError('Invalid historical receipt output')
        if not (root/path).is_file() or digest(root/path) != checksum:
            raise ValueError('Historical checkpoint output changed or missing')
    checked = datetime.fromisoformat(receipt['checked_at'])
    # Unresolved outcomes remain pending. Retry them on the next audit date,
    # within the existing 14-day provider lookup window; never call them losses.
    return (not receipt['pending'] or checked.date() >= as_of.date()
            or (as_of.date()-day).days > 14)


def advance(root, work_date, phase, deadline, clock):
    root = Path(root)
    report_date = work_date-timedelta(days=1)
    cursor = read_store(root/CURSOR, {})
    if cursor.get('work_date') != str(work_date):
        cursor = {'version': 1, 'work_date': str(work_date), 'as_of': clock().isoformat()}
    if cursor.get('version') != 1:
        raise ValueError('Invalid history cursor')
    as_of = datetime.fromisoformat(cursor['as_of'])
    if as_of.tzinfo is None or as_of > clock():
        raise ValueError('Invalid history clock')
    days = [date.fromisoformat(p.stem.removeprefix('prematch_'))
            for p in sorted((root/'auditordata').glob('prematch_????-??-??.json'))]
    pending = [day for day in days if day < report_date and not reusable(root, day, as_of)]
    for day in pending[:MAX_DAYS]:
        if deadline-clock() < RESERVE:
            return False
        phase('auditor/current_audit.py', 'daily', '--date', str(day), '--as-of', as_of.isoformat())
        if not reusable(root, day, as_of):
            raise ValueError('Historical day did not produce a verified receipt')
        write_store(root/CURSOR, cursor)
        phase('src/gold_storage.py', 'publish')
        print(f'Historical day saved independently: {day}', flush=True)
    if len(pending) > MAX_DAYS or deadline-clock() < RESERVE:
        return False
    phase('auditor/current_audit.py', 'history', '--date', str(report_date),
          '--assemble-only', '--as-of', as_of.isoformat())
    return True
