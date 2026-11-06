"""Build yesterday from immutable dated evidence and still-pending scan journals."""
from datetime import datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
import sys

from audit_storage import read_store, write_store

STATUS = 'data/arab_quote_auditor_daily_priority.json'
WEIGHTS = 'data/arab_calibration_weights.json'


def signature(path):
    return sha256(Path(path).read_bytes()).hexdigest()


def ensure_priority(root, today, phase, calibrate):
    from auditor_schedule import has_daily_report
    root = Path(root)
    day = today-timedelta(days=1)
    report = root/f'auditarchive/prematch_{day}_daily.json'
    saved = read_store(root/STATUS, {})
    if (saved.get('date') == str(today) and has_daily_report(root, today)
            and saved.get('report_sha256') == signature(report)
            and (root/WEIGHTS).is_file()
            and saved.get('weights_sha256') == signature(root/WEIGHTS)
            and read_store(root/WEIGHTS, {}).get('as_of') == str(today)):
        return
    phase('src/auditor_daily_priority.py', '--date', str(day))
    if not has_daily_report(root, today):
        raise ValueError('Missing priority daily Auditor report')
    calibrate()
    if read_store(root/WEIGHTS, {}).get('as_of') != str(today):
        raise ValueError('Priority calibration was not updated')
    write_store(root/STATUS, {'version': 1, 'date': str(today), 'report_date': str(day),
                             'report_sha256': signature(report),
                             'weights_sha256': signature(root/WEIGHTS)})
    # This checkpoint preserves any older PROGRESS/cursor and unconsumed journals.
    phase('src/gold_storage.py', 'publish')
    phase('src/homepage_results.py')
    phase('src/private_publication.py', '--results-only')
    print(f'Priority daily report and calibration saved: {day}', flush=True)


def build_yesterday(root, day, now, fetcher=None):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'auditor'))
    from current_audit import forecast_day, settle_day, fetch_results, TZ
    from audit_journal import pending_journals, merge_forecast
    from audit_ledger import AuditLedger, entries
    if day != now.astimezone(TZ).date()-timedelta(days=1):
        raise ValueError('Priority phase is restricted to yesterday in Rome')
    root = Path(root)
    path = root/f'auditordata/prematch_{day}.json'
    cutoff = datetime.combine(day+timedelta(days=1), time(), TZ)
    journals = pending_journals(root, cutoff)
    # Match the ordinary ingestion order: forecasts first, then policies.
    forecasts = [p for p in journals if p.name.endswith('_forecasts.json')]
    policies = [p for p in journals if p.name.endswith('_policies.json')]
    if len(forecasts)+len(policies) != len(journals):
        raise ValueError('Unknown priority journal phase')
    with AuditLedger(path) as ledger:
        if path.exists() and ledger.metadata.get('audit_date') != str(day):
            raise ValueError('Priority archive date mismatch')
        ledger.metadata.update(schema_version=1, audit_date=str(day))
        ledger.add_collection('fixtures')
        for source in forecasts:
            kind = None
            for col, key, record in entries(source):
                if col is None:
                    if key == 'journal_type': kind = record
                elif key is not None and forecast_day(col, key, record, now) == str(day):
                    record.pop('settlement', None)
                    merge_forecast(ledger, col, key, record)
            if kind != 'forecasts':
                raise ValueError('Invalid forecast journal')
        for source in policies:
            payload = read_store(source)
            if payload.get('schema_version') != 1 or payload.get('journal_type') != 'policies':
                raise ValueError('Invalid policy journal')
            for col in ('analysis_updates', 'study_observations'):
                for key, record in payload.get(col, {}).items():
                    if forecast_day(col, key, record, now) == str(day):
                        record.pop('settlement', None)
                        merge_forecast(ledger, col, key, record)
        ledger.write(path)
    # The normal settlement, daily report and calibration evidence remain unchanged.
    return settle_day(root, day, now, fetcher=fetcher or fetch_results)


if __name__ == '__main__':
    import argparse
    from auditor_schedule import ROME, admission
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=lambda value: datetime.fromisoformat(value).date(), required=True)
    args = parser.parse_args()
    now = datetime.now(ROME)
    if not admission(now)['run']:
        parser.error('Priority audit requires the protected nighttime admission window')
    build_yesterday(Path(__file__).resolve().parents[1], args.date, now)
