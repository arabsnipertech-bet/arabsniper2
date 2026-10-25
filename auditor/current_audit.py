"""Archive first-observed forecasts and settle them without rebuilding predictions."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from prematch_audit import build_report, settle_predictions
from audit_storage import read_store, write_store
from daily_audit import build_daily, markdown, fingerprint

TZ = ZoneInfo('Europe/Rome')
COLLECTIONS = ('fixtures', 'quality_evaluations', 'multimarket_evaluations',
               'signal_observations', 'analysis_updates')


def read(path, default):
    return read_store(path, default)


def write(path, value):
    from audit_storage import write_store, atomic_bytes
    if path.parent.name == "data" or path.name == "prematch_last_report.json":
        atomic_bytes(path, json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
    else:
        write_store(path, value)


def stamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed if parsed.tzinfo else None
    except (ValueError, TypeError):
        return None


def archive(root, now, progress=lambda message: None):
    from audit_ledger import AuditLedger
    source = root / 'data/arab_quote_prematch_audit.json'
    if not source.exists():
        raise FileNotFoundError(f'Archivio prepartita corrente mancante: {source}')
    days, rejected, added = set(), 0, 0
    with AuditLedger(source, progress=progress) as incoming:
        if 'fixtures' not in incoming.collections:
            raise ValueError('Archivio prepartita non valido: fixtures deve essere un oggetto')
        for collection, key, record in incoming.records():
            fixture = record.get('fixture') or {}
            kickoff, captured = stamp(fixture.get('kickoff')), stamp(record.get('captured_at'))
            expected_key = str(fixture.get('fixture_id'))
            if collection == 'quality_evaluations':
                profile = (record.get('first_half_assessment') or {}).get('profile_id')
                expected_key = f"{record.get('model_version')}::{profile}::{expected_key}"
            elif collection == 'multimarket_evaluations':
                expected_key = f"{record.get('model_version')}::{expected_key}"
            elif collection == 'analysis_updates':
                expected_key = f"{record.get('model_version')}::{expected_key}::{record.get('captured_at')}"
            elif collection == 'signal_observations':
                from signal_research import record_key
                expected_key = record_key(record)
            if (not kickoff or not captured or captured >= kickoff or captured > now
                    or expected_key != str(key)):
                rejected += 1
                incoming.db.execute('UPDATE records SET day=NULL WHERE collection=? AND key=?',
                                    (collection, key))
            else:
                days.add(kickoff.astimezone(TZ).date().isoformat())
        for date in sorted(days):
            progress(f'archiviazione {date}')
            path = root / f'auditordata/prematch_{date}.json'
            with AuditLedger(path, progress=progress) as day:
                if not path.exists():
                    day.metadata = {'schema_version': 1, 'audit_date': date,
                                    'source_file': str(source.relative_to(root))}
                    day.add_collection('fixtures')
                count = 0
                for collection, key, record in incoming.records(day=date):
                    record.pop('settlement', None)
                    count += day.put(collection, key, record, overwrite=False)
                if count:
                    progress(f'salvataggio archivio {date}: {count} nuove osservazioni')
                    day.write(path)
                    added += count
    return {'archived_dates': sorted(days), 'new_forecasts': added,
            'rejected_without_valid_prematch_evidence': rejected}


def fetch_results(date, key):
    request = Request('https://v3.football.api-sports.io/fixtures?date=' + date
                      + '&timezone=Europe%2FRome', headers={'x-apisports-key': key})
    with urlopen(request, timeout=45) as response:
        payload = json.load(response)
    if payload.get('errors'):
        raise RuntimeError(f'API risultati: richiesta rifiutata per {date}')
    if not isinstance(payload.get('response'), list):
        raise ValueError('Risposta API risultati non valida')
    matches = []
    for item in payload['response']:
        fixture = item.get('fixture') or {}
        score = item.get('score') or {}
        ht, ft = score.get('halftime') or {}, score.get('fulltime') or {}
        # FT markets are settled at 90 minutes, including matches with extra time.
        matches.append({'fixture': {'fixture_id': str(fixture.get('id')), 'result': {
            'status': (fixture.get('status') or {}).get('short'),
            'home_ht': ht.get('home'), 'away_ht': ht.get('away'),
            'home_ft': ft.get('home'), 'away_ft': ft.get('away')}}})
    return {'days': [{'matches': matches}]}


def settle(root, now, fetcher=fetch_results, progress=lambda message: None):
    from audit_ledger import AuditLedger, settle_ledger
    from multimarket_audit import build_report as multimarket_report
    from selection_studies import build_studies, study_markdown
    from audit_storage import atomic_bytes
    queried = []
    with AuditLedger(progress=progress) as combined:
        combined.metadata['schema_version'] = 1
        for collection in COLLECTIONS:
            combined.add_collection(collection)
        for path in sorted((root / 'auditordata').glob('prematch_????-??-??.json')):
            date = datetime.strptime(path.stem.removeprefix('prematch_'), '%Y-%m-%d').date()
            if date >= now.astimezone(TZ).date():
                continue
            progress(f'caricamento e verifica previsioni {date}')
            results_path = root / f'auditarchive/prematch_{date}_results.json'
            with AuditLedger(path, progress=progress) as day:
                if day.metadata['audit_date'] != str(date):
                    raise ValueError(f'Audit date does not match filename: {path.name}')
                with AuditLedger(results_path, progress=progress) as previous:
                    for collection, key, record in day.records():
                        old = previous.get(collection, {}).get(key)
                        if old is not None and fingerprint(old) != fingerprint(record):
                            raise ValueError(f'Frozen forecast mismatch: {date}/{collection}/{key}')
                        if (old or {}).get('settlement'):
                            record['settlement'] = old['settlement']
                            day.put(collection, key, record)
                pending = any(not (r.get('settlement') or {}).get('outcomes') or
                              any(v is None for v in r['settlement']['outcomes'].values())
                              for _, _, r in day.records())
                if pending and (now.astimezone(TZ).date() - date).days <= 14:
                    key = os.environ.get('API_SPORTS_KEY', '').strip()
                    if not key:
                        raise RuntimeError('API_SPORTS_KEY mancante per verificare i risultati')
                    progress(f'richiesta risultati API-Football {date}')
                    settle_ledger(day, fetcher(str(date), key))
                    queried.append(str(date))
                day.metadata = {'schema_version': 1}
                for collection in COLLECTIONS:
                    day.add_collection(collection)
                progress(f'salvataggio esiti {date}')
                day.write(results_path)
                progress(f'costruzione rapporto giornaliero {date}')
                daily = build_daily(day, str(date), now.isoformat(), os.environ.get('GITHUB_SHA'))
                daily['selection_studies'] = build_studies(daily)
                write(root / f'auditarchive/prematch_{date}_daily.json', daily)
                atomic_bytes(root / f'auditarchive/prematch_{date}_daily.md',
                             (markdown(daily)+study_markdown(daily['selection_studies'])).encode())
                del daily
                for collection, key, record in day.records():
                    combined.put(collection, key, record)
            progress(f'giornata {date} completata')
        progress('costruzione rapporto complessivo')
        report = build_report(combined)
        progress('confronto mercati: prima e ultima osservazione')
        report['multimarket'] = multimarket_report(combined)
        write(root / 'data/arab_quote_multimarket_report.json', report['multimarket'])
        report.update({'generated_at': now.isoformat(), 'results_dates_queried': queried,
                       'archived_forecasts': len(combined['fixtures']),
                       'archived_quality_evaluations': len(combined['quality_evaluations'])})
        progress('salvataggio rapporto complessivo')
        write(root / 'auditarchive/prematch_last_report.json', report)
    return {'results_dates_queried': queried, 'settled_report_groups': len(report['groups'])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['freeze', 'settle'])
    args = parser.parse_args()
    now = datetime.now(TZ)
    from task_progress import TaskProgress
    with TaskProgress('Auditor') as progress:
        summary = {'archive': archive(ROOT, now, progress=progress.update)}
        if args.mode == 'settle':
            summary['settlement'] = settle(ROOT, now, progress=progress.update)
    text = json.dumps(summary, ensure_ascii=False, separators=(",", ":"))
    print(text)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('### Audit prepartita corrente\n```json\n' + text + '\n```\n')


if __name__ == '__main__':
    main()


