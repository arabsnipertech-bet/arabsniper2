"""Archive first-observed forecasts and settle them without rebuilding predictions."""
import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from prematch_audit import build_report, settle_predictions
from audit_storage import read_store, write_store
from daily_audit import build_daily, markdown, fingerprint
from auditor_schedule import EXCLUDED_RECOVERY_DAYS, ensure_recoverable_day

TZ = ZoneInfo('Europe/Rome')
RESULTS_MIN_REQUEST_GAP = 0.25
from audit_ledger import COLLECTIONS


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
    with AuditLedger(source, progress=progress) as incoming:
        return archive_ledger(root, now, incoming, progress)


def forecast_day(collection, key, record, now):
    fixture = record.get('fixture') or {}
    kickoff, captured = stamp(fixture.get('kickoff')), stamp(record.get('captured_at'))
    expected_key = str(fixture.get('fixture_id'))
    if collection == 'quality_evaluations':
        profile = (record.get('first_half_assessment') or {}).get('profile_id')
        expected_key = f"{record.get('model_version')}::{profile}::{expected_key}"
    elif collection == 'multimarket_evaluations':
        expected_key = f"{record.get('model_version')}::{expected_key}"
    elif collection == 'study_observations':
        expected_key = f"{expected_key}::{record.get('captured_at')}"
    elif collection == 'analysis_updates':
        stage = '::publication' if record.get('publication_snapshot') is True else ''
        expected_key = f"{record.get('model_version')}::{expected_key}{stage}::{record.get('captured_at')}"
    elif collection == 'signal_observations':
        from signal_research import record_key
        expected_key = record_key(record)
    if not kickoff or not captured or captured >= kickoff or captured > now or expected_key != str(key):
        return None
    return kickoff.astimezone(TZ).date().isoformat()


def archive_ledger(root, now, incoming, progress):
    from audit_ledger import AuditLedger
    days, rejected, added = set(), 0, 0
    if 'fixtures' not in incoming.collections:
        raise ValueError('Archivio prepartita non valido: fixtures deve essere un oggetto')
    for collection, key, record in incoming.records():
        day = forecast_day(collection, key, record, now)
        if day is None:
            rejected += 1
            incoming.db.execute('UPDATE records SET day=NULL WHERE collection=? AND key=?',
                                (collection, key))
        else:
            days.add(day)
    for date in sorted(days):
        progress(f'archiviazione {date}')
        path = root / f'auditordata/prematch_{date}.json'
        with AuditLedger(path, progress=progress) as day:
            if not path.exists():
                day.metadata = {'schema_version': 1, 'audit_date': date,
                                'source_file': 'data/arab_quote_prematch_audit.json'}
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


def fetch_results(date, key, fixture_ids=()):
    from v7_archive import FINISHED, VOID, checked_score, merge_verified_result

    last_request_at = None

    def fixtures(**params):
        nonlocal last_request_at
        query = urlencode({'timezone': 'Europe/Rome', **params})
        request = Request('https://v3.football.api-sports.io/fixtures?' + query, headers={'x-apisports-key': key})
        if last_request_at is not None:
            wait = RESULTS_MIN_REQUEST_GAP - (time.monotonic() - last_request_at)
            if wait > 0:
                time.sleep(wait)
        last_request_at = time.monotonic()
        with urlopen(request, timeout=30) as response:
            payload = {'response': []} if getattr(response, 'status', 200) == 204 else json.load(response)
            daily = response.headers.get('x-ratelimit-requests-remaining')
            limit = response.headers.get('x-ratelimit-requests-limit')
            minute = response.headers.get('x-ratelimit-remaining')
        if payload.get('errors'):
            raise RuntimeError(f'API risultati: richiesta rifiutata per {date}')
        if not isinstance(payload.get('response'), list):
            raise ValueError('Risposta API risultati non valida')
        reserve = max(1000, (int(limit) + 9) // 10) if limit and limit.isdecimal() else 1000
        return payload, bool(daily and daily.isdecimal() and int(daily) > reserve and minute not in ('0', '1'))

    payload, can_continue = fixtures(date=date)
    rows = list(payload['response'])
    pages = (payload.get('paging') or {}).get('total', 1)
    for page in range(2, min(pages, 3) + 1) if type(pages) is int else ():
        if not can_continue:
            break
        extra, can_continue = fixtures(date=date, page=page)
        rows.extend(extra['response'])
    def accept(item):
        fixture = item.get('fixture') or {}
        fid = fixture.get('id')
        if type(fid) is not int or fid <= 0:
            return
        score = item.get('score') or {}
        ht, ft = score.get('halftime') or {}, score.get('fulltime') or {}
        status = (fixture.get('status') or {}).get('short')
        if status == 'FT' and any(ft.get(side) is None for side in ('home', 'away')):
            ft = item.get('goals') or {}
        # FT markets are settled at 90 minutes, including matches with extra time.
        result = checked_score({
            'status': status,
            'home_ht': ht.get('home'), 'away_ht': ht.get('away'),
            'home_ft': ft.get('home'), 'away_ft': ft.get('away'),
            'extra_time': score.get('extratime'), 'penalties': score.get('penalty')})
        results[fid] = merge_verified_result(results.get(fid), result)
    results = {}
    for row in rows:
        accept(row)
    delayed = [fid for fid in dict.fromkeys(fixture_ids)
               if fid not in results or (results[fid].get('status') not in VOID | {'PST'}
               and (results[fid].get('status') not in FINISHED or any(results[fid].get(field) is None
                   for field in ('home_ft', 'away_ft', 'home_ht', 'away_ht'))))]
    if delayed and can_continue:
        selected = set(delayed[:20])
        try:
            details, _ = fixtures(ids='-'.join(str(fid) for fid in sorted(selected)))
        except (OSError, RuntimeError, ValueError) as error:
            print(f'API risultati {date}: recupero ID rinviato ({type(error).__name__})', flush=True)
        else:
            for row in details['response']:
                if (row.get('fixture') or {}).get('id') in selected:
                    accept(row)
    matches = [{'fixture': {'fixture_id': str(fid), 'result': result}} for fid, result in results.items()]
    return {'days': [{'matches': matches}]}


def settle_day(root, date, now, fetcher=fetch_results, progress=lambda message: None, combined=None,
               *, legacy_evidence=True, recheck_results=False):
    from audit_ledger import AuditLedger, settle_ledger
    from selection_studies import build_studies, study_markdown
    from audit_storage import atomic_bytes
    ensure_recoverable_day(date)
    if date >= now.astimezone(TZ).date():
        raise ValueError('Only closed Rome dates can be settled')
    path = root / f'auditordata/prematch_{date}.json'
    if not path.is_file():
        raise FileNotFoundError(f'No frozen archive for {date}')
    queried = False
    fetched = None
    progress(f'caricamento e verifica previsioni {date}')
    results_path = root / f'auditarchive/prematch_{date}_results.json'
    with AuditLedger(path, progress=progress) as day:
        if day.metadata['audit_date'] != str(date):
            raise ValueError(f'Audit date does not match filename: {path.name}')
        with AuditLedger(results_path, progress=progress) as previous:
            for collection, key, record in day.records():
                old = previous.get(collection, {}).get(key)
                from daily_audit import same_frozen_forecast
                if old is not None and not same_frozen_forecast(old, record):
                    raise ValueError(f'Frozen forecast mismatch: {date}/{collection}/{key}')
                if (old or {}).get('settlement'):
                    record['settlement'] = old['settlement']
                    day.put(collection, key, record)
        from audit_pending import unresolved
        pending_fixtures = {}
        pending = False
        for _, _, record in day.records():
            if not unresolved(record):
                continue
            pending = True
            fixture = record.get('fixture') or {}
            fid = str(fixture.get('fixture_id'))
            if fid.isascii() and fid.isdecimal():
                pending_fixtures[int(fid)] = max(pending_fixtures.get(int(fid), False),
                                                bool(record.get('publication_snapshot')))
        age = (now.astimezone(TZ).date() - date).days
        if age <= 14 and (pending or recheck_results):
            key = os.environ.get('API_SPORTS_KEY', '').strip()
            if not key:
                raise RuntimeError('API_SPORTS_KEY mancante per verificare i risultati')
            progress(f'richiesta risultati API-Football {date}')
            if fetcher is fetch_results:
                ordered = sorted(pending_fixtures, key=lambda fid: (not pending_fixtures[fid], fid))
                fetched = fetcher(str(date), key, ordered)
            else:
                fetched = fetcher(str(date), key)
            settle_ledger(day, fetched)
            queried = True
        day.metadata = {'schema_version': 1}
        for collection in COLLECTIONS:
            day.add_collection(collection)
        progress(f'salvataggio esiti {date}')
        day.write(results_path)
        from v7_archive import archive_day
        from v7_learning import signal_diagnostics
        v7_signals = signal_diagnostics(archive_day(root, day, date, now, fetched)['samples'])
        progress(f'costruzione rapporto giornaliero {date}')
        daily = build_daily(day, str(date), now.isoformat(), os.environ.get('GITHUB_SHA'))
        try:
            from promo_successes import build as build_promo
            promo = build_promo(day, str(date))
        except Exception as error:
            progress('Promotional projection unavailable: '+type(error).__name__)
            promo = dict(schema_version=1, date=str(date), ready=False, examples=[])
        write(root / f'auditarchive/prematch_{date}_promo.json', promo)
        from auditor_history_chunks import digest
        daily['evidence_provenance']['input_files'] = {
            path.relative_to(root).as_posix(): digest(path),
            results_path.relative_to(root).as_posix(): digest(results_path)}
        daily['selection_studies'] = build_studies(daily)
        daily['v7_signals'] = v7_signals
        write(root / f'auditarchive/prematch_{date}_daily.json', daily)
        atomic_bytes(root / f'auditarchive/prematch_{date}_daily.md',
                     (markdown(daily)+study_markdown(daily['selection_studies'])).encode())
        if legacy_evidence:
            seal_legacy_day(root, date, now, queried, ledger=day)
        del daily
        if combined is not None:
            for collection, key, record in day.records():
                combined.put(collection, key, record)

    return queried


def seal_legacy_day(root, date, now, queried=False, *, ledger=None):
    """Build V5.1 evidence from saved results without rewriting the V7 report."""
    from adaptive_calibration import Evidence
    from audit_ledger import AuditLedger
    from auditor_history_chunks import seal_day
    if ledger is None:
        with AuditLedger(root/f'auditarchive/prematch_{date}_results.json') as saved:
            return seal_legacy_day(root, date, now, queried, ledger=saved)
    evidence = Evidence()
    for collection in ('multimarket_evaluations', 'analysis_updates', 'study_observations'):
        for record in ledger.get(collection, {}).values():
            evidence.observe(record)
    write(root/f'auditarchive/prematch_{date}_calibration.json', evidence.samples())
    pending = read(root/f'auditarchive/prematch_{date}_daily.json', {})['pending_summary']
    seal_day(root, date, now, pending['pending'], queried, pending_summary=pending)


def settle_history_day(arguments):
    """Each worker owns one date and writes only that date's report and ledger."""
    from task_progress import TaskProgress
    root, day, now = arguments
    with TaskProgress(f'Auditor {day}') as progress:
        return settle_day(Path(root), day, now, progress=progress.update,
                          recheck_results=(now.astimezone(TZ).date() - day).days <= 3)


def settle(root, now, fetcher=fetch_results, progress=lambda message: None, *, prepared_date=None, workers=1, assemble_only=False):
    from audit_ledger import AuditLedger
    from multimarket_audit import build_report as multimarket_report
    queried = []
    if workers not in (1, 2):
        raise ValueError('Historical Auditor supports one or two workers')
    end = prepared_date + timedelta(days=1) if prepared_date else now.astimezone(TZ).date()
    dates = [datetime.strptime(path.stem.removeprefix('prematch_'), '%Y-%m-%d').date()
             for path in sorted((root / 'auditordata').glob('prematch_????-??-??.json'))]
    excluded = [str(day) for day in dates if day < end and day in EXCLUDED_RECOVERY_DAYS]
    dates = [day for day in dates if day < end and day not in EXCLUDED_RECOVERY_DAYS]
    if assemble_only:
        from auditor_history_chunks import receipt_path
        for day in dates:
            receipt = read_store(receipt_path(root, day), {})
            if receipt.get('checked_at') == now.isoformat() and receipt.get('results_queried'):
                queried.append(str(day))
    if workers == 2 and not assemble_only:
        if fetcher is not fetch_results:
            raise ValueError('Custom result providers require serial execution')
        from concurrent.futures import ProcessPoolExecutor
        from multiprocessing import get_context
        pending_dates = [day for day in dates if day != prepared_date]
        progress('rapporti giornalieri indipendenti: due processi, pubblicazione unica finale')
        with ProcessPoolExecutor(max_workers=2, mp_context=get_context('spawn')) as pool:
            results = pool.map(settle_history_day, ((str(root), day, now) for day in pending_dates))
            for day, queried_api in zip(pending_dates, results, strict=True):
                if queried_api:
                    queried.append(str(day))
    with AuditLedger(progress=progress) as combined:
        combined.metadata['schema_version'] = 1
        for collection in COLLECTIONS:
            combined.add_collection(collection)
        for date in dates:
            if date == prepared_date or workers == 2 or assemble_only:
                # The priority report and settled ledger were published together.
                result = root / f'auditarchive/prematch_{date}_results.json'
                if not result.is_file():
                    raise FileNotFoundError(f'Missing checkpointed outcomes for {date}')
                with AuditLedger(result, progress=progress) as saved:
                    for collection, key, record in saved.records():
                        combined.put(collection, key, record)
            elif settle_day(root, date, now, fetcher, progress, combined):
                queried.append(str(date))
            progress(f'giornata {date} completata')
        progress('costruzione rapporto complessivo')
        report = build_report(combined)
        progress('confronto mercati: prima e ultima osservazione')
        report['multimarket'] = multimarket_report(combined)
        write(root / 'data/arab_quote_multimarket_report.json', report['multimarket'])
        report.update({'generated_at': now.isoformat(), 'results_dates_queried': queried,
                       'archived_forecasts': len(combined['fixtures']),
                       'archived_quality_evaluations': len(combined['quality_evaluations'])})
        if excluded:
            report['excluded_recovery_dates'] = excluded
        progress('salvataggio rapporto complessivo')
        write_store(root / 'data/arab_quote_prematch_report.json', report)
        write(root / 'auditarchive/prematch_last_report.json', report)
    return {'results_dates_queried': queried, 'settled_report_groups': len(report['groups'])}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['freeze', 'settle', 'prepare', 'daily', 'history', 'learn-v7', 'legacy-evidence'])
    parser.add_argument('--date', type=lambda value: datetime.strptime(value, '%Y-%m-%d').date())
    parser.add_argument('--journal-before', type=datetime.fromisoformat)
    parser.add_argument('--journal-limit', type=int)
    parser.add_argument('--delta-archive', action='store_true')
    parser.add_argument('--assemble-only', action='store_true')
    parser.add_argument('--as-of', type=datetime.fromisoformat)
    parser.add_argument('--root', type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument('--results-queried', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    now = args.as_of or datetime.now(TZ)
    if now.tzinfo is None:
        parser.error('as-of requires a timezone')
    if args.mode == 'legacy-evidence':
        if args.date is None or args.date >= now.astimezone(TZ).date():
            parser.error('legacy-evidence requires a closed --date')
        seal_legacy_day(args.root, args.date, now, args.results_queried)
        return
    if args.mode == 'learn-v7':
        from v7_learning import run as learn_v7
        report = learn_v7(ROOT, now)
        print(f"V7 learning: {report['update']['reason']}; {report['verified_fixtures']} fixtures", flush=True)
        return
    from task_progress import TaskProgress
    with TaskProgress('Auditor') as progress:
        if args.mode == 'prepare':
            from audit_journal import ingest
            summary = {}
            def partition(ledger):
                summary['archive'] = archive_ledger(ROOT, now, ledger, progress.update)
            summary['journals'] = ingest(ROOT, after_forecasts=partition, limit=args.journal_limit,
                                         partition_delta=args.delta_archive, before=args.journal_before)
            if 'archive' not in summary and not args.delta_archive:
                summary['archive'] = archive(ROOT, now, progress=progress.update)
        elif args.mode == 'daily':
            if args.date is None:
                parser.error('daily requires --date')
            summary = {'daily': str(args.date), 'results_queried':
                       settle_day(ROOT, args.date, now, progress=progress.update)}
        elif args.mode == 'history':
            summary = {'settlement': settle(ROOT, now, progress=progress.update, prepared_date=args.date,
                                           workers=2, assemble_only=args.assemble_only)}
        else:
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


