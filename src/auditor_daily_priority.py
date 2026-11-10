"""Build yesterday from immutable dated evidence and still-pending scan journals."""
from datetime import datetime, time, timedelta
from pathlib import Path
import sys

from audit_storage import read_store, write_store
from auditor_history_chunks import digest as signature

STATUS = 'data/arab_quote_auditor_daily_priority.json'
WEIGHTS = 'data/arab_calibration_weights.json'
LEGACY_STATUS = 'data/arab_quote_auditor_v51_status.json'


def v7_paths(day):
    from v7_learning import STATE
    return {'daily_sha256': f'auditarchive/prematch_{day}_daily.json',
            'outcomes_sha256': f'auditarchive/prematch_{day}_v7.json',
            'learning_sha256': f'auditarchive/prematch_{day}_v7_learning.json',
            'weights_sha256': STATE}


def v7_ready(root, today):
    from v7_probability import VERSION
    root = Path(root)
    day = today-timedelta(days=1)
    saved = read_store(root/STATUS, {})
    paths = v7_paths(day)
    if (not isinstance(saved, dict) or saved.get('version') != 2 or saved.get('date') != str(today)
            or saved.get('report_date') != str(day)
            or any(not (root/path).is_file() or saved.get(key) != signature(root/path)
                   for key, path in paths.items())):
        return False
    daily, outcomes, learning, weights = [read_store(root/path, {}) for path in paths.values()]
    return (all(isinstance(value, dict) for value in (daily, outcomes, learning, weights))
            and daily.get('audit_date') == str(day) and outcomes.get('date') == str(day)
            and outcomes.get('version') == learning.get('version') == weights.get('version') == VERSION
            and learning.get('date') == str(today) and learning.get('state') == weights)


def ensure_priority(root, today, phase):
    from auditor_schedule import has_daily_report
    root = Path(root)
    day = today-timedelta(days=1)
    if not v7_ready(root, today):
        phase('src/auditor_daily_priority.py', '--date', str(day))
        if not has_daily_report(root, today):
            raise ValueError('Missing priority daily Auditor report')
        phase('auditor/current_audit.py', 'learn-v7')
        checksums = {key: signature(root/path) for key, path in v7_paths(day).items()}
        write_store(root/STATUS, {'version': 2, 'date': str(today), 'report_date': str(day), **checksums})
        phase('src/gold_storage.py', 'checkpoint-day', '--date', str(day))
    print(f'V7 results, report and learning checkpoint verified: {day}', flush=True)


def build_yesterday(root, day, now, fetcher=None):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'auditor'))
    from current_audit import forecast_day, settle_day, fetch_results, TZ
    from audit_journal import ForecastHistory, pending_journals, merge_forecast
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
        history = ForecastHistory(ledger)
        for source in forecasts:
            kind = None
            for col, key, record in entries(source, fixture_date=str(day)):
                if col is None:
                    if key == 'journal_type': kind = record
                elif key is not None and forecast_day(col, key, record, now) == str(day):
                    record.pop('settlement', None)
                    merge_forecast(ledger, col, key, record, history=history)
            if kind != 'forecasts':
                raise ValueError('Invalid forecast journal')
        for source in policies:
            metadata = {}
            for col, key, record in entries(source, fixture_date=str(day)):
                if col is None:
                    if key in ('schema_version','journal_type'): metadata[key] = record
                elif col in ('analysis_updates','study_observations') and key is not None:
                    if forecast_day(col, key, record, now) == str(day):
                        record.pop('settlement', None)
                        merge_forecast(ledger, col, key, record, history=history)
            if metadata != {'schema_version':1,'journal_type':'policies'}:
                raise ValueError('Invalid policy journal')
        ledger.write(path)
    return settle_day(root, day, now, fetcher=fetcher or fetch_results, legacy_evidence=False,
                      progress=lambda message: print('V7: '+message, flush=True))


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
