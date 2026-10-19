"""Archive first-observed forecasts and settle them without rebuilding predictions."""
import argparse
from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from prematch_audit import build_report, settle_predictions

TZ = ZoneInfo('Europe/Rome')


def read(path, default):
    return json.loads(path.read_text()) if path.exists() else deepcopy(default)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temp.replace(path)


def stamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed if parsed.tzinfo else None
    except (ValueError, TypeError):
        return None


def archive(root, now):
    source = root / 'data/arab_quote_prematch_audit.json'
    if not source.exists():
        raise FileNotFoundError(f'Archivio prepartita corrente mancante: {source}')
    payload = read(source, {})
    records = payload.get('fixtures')
    if not isinstance(records, dict):
        raise ValueError('Archivio prepartita non valido: fixtures deve essere un oggetto')
    days, rejected, added = {}, 0, 0
    entries = [('fixtures', k, r) for k, r in records.items()]
    entries += [('quality_evaluations', k, r) for k, r in payload.get('quality_evaluations', {}).items()]
    for collection, key, record in entries:
        fixture = record.get('fixture') or {}
        kickoff, captured = stamp(fixture.get('kickoff')), stamp(record.get('captured_at'))
        expected_key = str(fixture.get('fixture_id'))
        if collection == 'quality_evaluations':
            profile = (record.get('first_half_assessment') or {}).get('profile_id')
            expected_key = f"{record.get('model_version')}::{profile}::{expected_key}"
        if (not kickoff or not captured or captured >= kickoff or captured > now
                or expected_key != str(key)):
            rejected += 1
            continue
        date = kickoff.astimezone(TZ).date().isoformat()
        path = root / f'auditordata/prematch_{date}.json'
        day = days.setdefault(date, read(path, {'schema_version': 1,
            'audit_date': date, 'source_file': str(source.relative_to(root)), 'fixtures': {}}))
        target = day.setdefault(collection, {})
        if str(key) not in target:
            frozen = deepcopy(record)
            frozen.pop('settlement', None)
            target[str(key)] = frozen
            added += 1
    for date, day in days.items():
        write(root / f'auditordata/prematch_{date}.json', day)
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


def settle(root, now, fetcher=fetch_results):
    combined = {'schema_version': 1, 'fixtures': {}, 'quality_evaluations': {}}
    queried = []
    for path in sorted((root / 'auditordata').glob('prematch_????-??-??.json')):
        day = read(path, {})
        date = datetime.strptime(day['audit_date'], '%Y-%m-%d').date()
        if date >= now.astimezone(TZ).date():
            continue
        results_path = root / f'auditarchive/prematch_{date}_results.json'
        previous = read(results_path, {'fixtures': {}})
        store = {'schema_version': 1}
        for collection in ('fixtures', 'quality_evaluations'):
            records = deepcopy(day.get(collection, {}))
            for key, record in records.items():
                saved = previous.get(collection, {}).get(key, {}).get('settlement')
                if saved:
                    record['settlement'] = saved
            store[collection] = records
        records = list(store['fixtures'].values()) + list(store['quality_evaluations'].values())
        pending = any(any((r.get('settlement') or {}).get('outcomes', {}).get(m) is None
                          for m in ('over15_ht', 'gg_ht', 'over25_ft')) for r in records)
        if pending and (now.astimezone(TZ).date() - date).days <= 14:
            key = os.environ.get('API_SPORTS_KEY', '').strip()
            if not key:
                raise RuntimeError('API_SPORTS_KEY mancante per verificare i risultati')
            settle_predictions(store, fetcher(str(date), key))
            queried.append(str(date))
        write(results_path, store)
        for collection in ('fixtures', 'quality_evaluations'):
            combined[collection].update(store[collection])
    report = build_report(combined)
    report.update({'generated_at': now.isoformat(), 'results_dates_queried': queried,
                   'archived_forecasts': len(combined['fixtures']),
                   'archived_quality_evaluations': len(combined['quality_evaluations'])})
    write(root / 'auditarchive/prematch_last_report.json', report)
    return {'results_dates_queried': queried, 'settled_report_groups': len(report['groups'])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['freeze', 'settle'])
    args = parser.parse_args()
    now = datetime.now(TZ)
    summary = {'archive': archive(ROOT, now)}
    if args.mode == 'settle':
        summary['settlement'] = settle(ROOT, now)
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    print(text)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as out:
            out.write('### Audit prepartita corrente\n```json\n' + text + '\n```\n')


if __name__ == '__main__':
    main()
