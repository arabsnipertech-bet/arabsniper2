"""Recover a missing closed-day report from immutable prematch evidence.

Preparation uses an isolated runner. Publication alone takes the shared writer
lock and preserves the latest scan manifest even when V5.1 evidence is incomplete.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from audit_daily_evidence import evidence_paths, read_receipt, receipt_path as evidence_receipt_path, verified_evidence
from audit_journal import ForecastHistory, journal_fields, journals_for_day, merge_forecast
from audit_ledger import AuditLedger, entries
from audit_official_scores import LOOKBACK_DAYS
from audit_storage import FORMAT, atomic_bytes, read_store, write_store
from auditor_schedule import EXCLUDED_RECOVERY_DAYS, ensure_recoverable_day
from current_audit import TZ, fetch_results, forecast_day, settle_day
from daily_audit import markdown
from gold_storage import allowed, client, previous_manifest, sha, write_manifest
from gold_storage import restore_subset
from homepage_results import publish as publish_results
from selection_studies import study_markdown

PUBLIC_RESULTS = 'data/arab_quote_results_public.json'
FORECASTS = 'data/arab_quote_prematch_audit.json'


def report_path(day):
    return f'auditarchive/prematch_{day}_daily.json'


def complete_day(index, day):
    return all(f'auditarchive/prematch_{day}_{suffix}' in index for suffix in
               ('daily.json', 'daily.md', 'results.json', 'calibration.json', 'checkpoint.json'))


def recovery_status(day):
    return f'auditarchive/prematch_{day}_v51_status.json'


def recovery_core(files, day):
    return all(f'auditarchive/prematch_{day}_{suffix}' in files
               for suffix in ('daily.json', 'daily.md', 'results.json', 'v7.json'))


def recover_legacy(root, day, now, queried):
    from nightly_auditor import run_command
    args = [sys.executable, str(ROOT/'auditor/current_audit.py'), 'legacy-evidence',
            '--root', str(root.resolve()), '--date', str(day), '--as-of', now.isoformat()]
    if queried:
        args.append('--results-queried')
    clock = lambda: datetime.now(TZ)
    try:
        run_command(root, args, clock()+timedelta(minutes=6), clock)
    except (TimeoutError, subprocess.CalledProcessError) as error:
        return 'partial_v51_skip', 'deadline' if isinstance(error, TimeoutError) else f'exit_{error.returncode}'
    return 'complete', None


def source_set(index, day):
    """Observations first captured after this Rome date cannot be prematch."""
    dated = f'auditordata/prematch_{day}.json'
    names = {FORECASTS, dated, f'auditarchive/prematch_{day}_results.json', *journals_for_day(index, day)}
    return {p: index[p]['sha256'] for p in sorted(names) if p in index}


def checkpoint_sources(index, day):
    paths = (f'auditordata/prematch_{day}.json', f'auditarchive/prematch_{day}_results.json',
             f'auditarchive/prematch_{day}_daily.json', f'auditarchive/prematch_{day}_v7.json')
    if not all(p in index for p in paths):
        return {}
    receipt = evidence_receipt_path(day)
    return {p: index[p]['sha256'] for p in (*paths, receipt) if p in index}


def next_recovery_day(storage, bucket, root, now):
    """Pick one outstanding Rome date, rechecking scores after the live window."""
    index = previous_manifest(storage, bucket)
    today = now.astimezone(TZ).date()
    days = [today-timedelta(days=age) for age in range(1, LOOKBACK_DAYS+1)
            if today-timedelta(days=age) not in EXCLUDED_RECOVERY_DAYS]
    receipts = {evidence_receipt_path(day) for day in days}
    restore_subset(storage, bucket, root, index, receipts & index.keys())
    indexed_dates = {fields[1] for path in index if (fields := journal_fields(path)) and fields[1] is not None}
    missing, retry = [], []
    yesterday = today-timedelta(days=1)
    for day in days:
        receipt = read_receipt(root, day)
        if (receipt is None or receipt.get('sha256') != {
                path: index.get(path, {}).get('sha256') for path in evidence_paths(day)}):
            if (f'auditordata/prematch_{day}.json' in index or day.strftime('%Y%m%d') in indexed_dates
                    or day == yesterday and any(journals_for_day(index, day, legacy_days=1))):
                missing.append(day)
            continue
        if day == yesterday and receipt['fixtures'] and f'auditarchive/prematch_{day}_v7_learning.json' not in index:
            return day
        checked = datetime.fromisoformat(receipt['checked_at'])
        for_recent_corrections = (receipt['fixtures'] and 2 <= (today-day).days <= 4
                                  and checked.astimezone(TZ).date() < day+timedelta(days=2))
        if (receipt.get('pending_fixtures', 0) or for_recent_corrections) and now-checked >= timedelta(hours=20):
            retry.append((checked, day))
    if missing and (today-missing[0]).days <= 7:
        return missing[0]
    return min(retry)[1] if retry else (missing[0] if missing else None)


def recover_learning(storage, bucket, root, day, now):
    """Complete today's V7 learning when a recovered day reached durable storage."""
    from auditor_daily_priority import record_v7_status, v7_ready
    from gold_storage import checkpoint_day, restore_v7_priority
    from v7_learning import run as learn
    today = now.astimezone(TZ).date()
    if day != today-timedelta(days=1):
        return
    index = previous_manifest(storage, bucket)
    if evidence_receipt_path(day) not in index:
        return
    restore_v7_priority(storage, bucket, root, day)
    if v7_ready(root, today):
        return
    if verified_evidence(root, day) is None:
        raise ValueError('Learning requires the published daily evidence')
    learn(root, now)
    record_v7_status(root, today)
    checkpoint_day(storage, bucket, root, day)
    print(f'Verified V7 learning checkpoint after recovery: {day}', flush=True)


def recovery_files(root, paths):
    """Yield one verified stored object at a time; no full checkpoint in RAM."""
    pending = list(paths); seen = set()
    while pending:
        path = pending.pop()
        if path in seen: continue
        seen.add(path)
        raw = (root/path).read_bytes()
        yield path, raw
        if path.endswith('.json'):
            value = json.loads(raw)
            if isinstance(value,dict) and value.get('_storage') == FORMAT:
                pending.extend(str(Path(path).parent/'_audit_parts'/(part['sha256']+'.json.gz')) for part in value['parts'])


def prepare(storage, bucket, root, day, now, fetcher=fetch_results, *, refresh=False, recheck_results=False,
            existing_forecasts=False):
    ensure_recoverable_day(day)
    if not 1 <= (now.astimezone(TZ).date() - day).days <= 14:
        raise ValueError('Recovery requires a closed Rome date within 14 days')
    index = previous_manifest(storage, bucket)
    if complete_day(index, day) and not refresh:
        status = recovery_status(day)
        needed = [f'auditarchive/prematch_{day}_checkpoint.json']
        if status in index:
            needed.append(status)
        restore_subset(storage, bucket, root, index, needed)
        legacy = read_store(root/status, {}) if status in index else {}
        receipt = read_store(root/f'auditarchive/prematch_{day}_checkpoint.json')
        if receipt.get('pending') is False and legacy.get('status') != 'partial_v51_skip':
            return 'existing'
    checkpoint = checkpoint_sources(index, day) if existing_forecasts else {}
    dated = root / f'auditordata/prematch_{day}.json'
    if checkpoint:
        restore_subset(storage, bucket, root, index, checkpoint)
        prior_report = read_store(root/report_path(day), {})
        frozen_inputs = prior_report.get('evidence_provenance', {}).get('input_files', {})
        if frozen_inputs.get(dated.relative_to(root).as_posix()) != checkpoint[dated.relative_to(root).as_posix()]:
            checkpoint = {}  # The full archive now contains additional frozen observations.
    sources = checkpoint or source_set(index, day)
    if not checkpoint:
        restore_subset(storage, bucket, root, index, sources)
    certified = read_store(root/f'auditarchive/prematch_{day}_v7.json', {}) if checkpoint else {}
    if checkpoint and (certified.get('date') != str(day) or not isinstance(certified.get('fixtures'), dict)
                       or read_store(root/report_path(day), {}).get('audit_date') != str(day)):
        raise ValueError('Invalid saved closed-day certificate')
    frozen_dated = index[dated.relative_to(root).as_posix()]['sha256'] if checkpoint else None
    with AuditLedger(dated) as ledger:
        if dated.exists() and ledger.metadata.get('audit_date') != day.isoformat():
            raise ValueError('Private archive date mismatch')
        ledger.metadata.update(schema_version=1, audit_date=day.isoformat())
        for col, key, record in ledger.records():
            if forecast_day(col, key, record, now) != day.isoformat():
                raise ValueError('Invalid frozen archive entry')
        # Existing per-date archive wins, then the original master, then each
        # journal in observation order, as in normal archival/ingestion.
        pending = [p for p in sources if journal_fields(p)]
        # Match ordinary ingestion: forecast journals first, policy journals
        # second. Preserve policy publication markers and Study observations.
        pending.sort(key=lambda p:(p.endswith('_policies.json'), journal_fields(p)[0]))
        from daily_audit import fingerprint
        original = {(c,k): fingerprint(r) for c,k,r in ledger.records()}
        history = ForecastHistory(ledger)
        paths = ([FORECASTS] if FORECASTS in sources else []) + pending
        for path in paths:
            print('Verifying original source for '+str(day)+': '+path, flush=True)
            journal = path != FORECASTS
            policy = path.endswith('_policies.json')
            verified_type = not journal
            schema_version = None
            for collection, key, record in entries(root / path, fixture_date=day.isoformat()):
                if collection is None:
                    if journal and key == 'journal_type':
                        if record != ('policies' if policy else 'forecasts'):
                            raise ValueError('Incorrect journal type')
                        verified_type = True
                    if journal and key == 'schema_version': schema_version = record
                    continue
                if key is None or forecast_day(collection, key, record, now) != day.isoformat():
                    continue
                if policy and collection not in ('analysis_updates','study_observations'):
                    raise ValueError('Unexpected policy forecast collection')
                record.pop('settlement', None)
                if journal:
                    merge_forecast(ledger, collection, key, record, history=history)
                else:
                    inserted = ledger.put(collection, key, record, overwrite=False)
                    if inserted and collection == 'analysis_updates':
                        history.add(key, record)
            if not verified_type or (policy and schema_version != 1):
                raise ValueError('Missing forecast journal type')
        if not any(len(ledger.get(c, {})) for c in ('fixtures', 'multimarket_evaluations', 'analysis_updates')):
            raise ValueError('No eligible frozen observations for this date')
        for (c,k), checksum in original.items():
            if fingerprint(ledger[c][k]) != checksum:
                raise ValueError('Existing frozen prediction changed during recovery')
        if not dated.exists() or sum(len(ledger.get(c,{})) for c in ledger.collections) != len(original):
            ledger.write(dated)

    queried = settle_day(root, day, now, fetcher=fetcher, legacy_evidence=False, recheck_results=recheck_results,
                         progress=lambda message: print(f'Recovery {day}: {message}', flush=True))
    v7 = read_store(root/f'auditarchive/prematch_{day}_v7.json')
    if checkpoint:
        if sha(dated.read_bytes()) != frozen_dated:
            raise ValueError('Result refresh changed the frozen ledger')
        before = certified['fixtures']
        after = v7['fixtures']
        if before.keys() != after.keys() or any(before[fid]['forecast_sha256'] != after[fid]['forecast_sha256']
                                               for fid in before):
            raise ValueError('Result refresh changed an existing V7 forecast')
        print(json.dumps({'v7_result_refresh': v7['counts'], 'frozen_v7_fixtures': len(before)}), flush=True)
    if verified_evidence(root, day) is None:
        raise ValueError('No verified frozen report for this date')
    status, reason = recover_legacy(root, day, now, queried)
    write_store(root/recovery_status(day), {'date':str(day), 'status':status, 'reason':reason})
    print(f'Recovery {day}: V7 ready, V5.1 {status}', flush=True)
    # Stage the complete chain, including immutable parts. Publication runs
    # under the existing writer mutex and checks the original source set again.
    bundle = {}
    pointers = [f'auditordata/prematch_{day}.json', *[p.relative_to(root).as_posix()
                for p in (root/'auditarchive').glob(f'prematch_{day}_*') if p.is_file()]]
    if status == 'partial_v51_skip':
        optional = {f'auditarchive/prematch_{day}_{suffix}' for suffix in ('calibration.json', 'checkpoint.json')}
        pointers = [path for path in pointers if path not in optional]
    verified_objects = {item['sha256'] for item in index.values()}
    for path, raw_file in recovery_files(root, pointers):
        checksum = sha(raw_file)
        if checksum not in verified_objects:
            storage.put_object(Bucket=bucket, Key='state/objects/'+checksum, Body=raw_file)
            verified_objects.add(checksum)
        bundle[path] = {'sha256': checksum, 'bytes': len(raw_file)}
    staged = {'version': 4, 'date': day.isoformat(), 'sources': sources, 'files': bundle,
              'legacy_status': status}
    if checkpoint:
        staged['source_mode'] = 'closed_day'
    raw = gzip.compress(json.dumps(staged, ensure_ascii=False, allow_nan=False,
                                   separators=(',', ':')).encode(), mtime=0)
    digest = sha(raw)
    storage.put_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz', Body=raw)
    return digest


def install(storage, bucket, root, day, digest, now):
    """Called only by the short publication job holding the normal writer mutex."""
    ensure_recoverable_day(day)
    index = previous_manifest(storage, bucket)
    target = report_path(day)
    if digest != 'existing':
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('No verified prepared chain')
        raw = storage.get_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz')['Body'].read()
        if sha(raw) != digest:
            raise ValueError('Staged chain checksum mismatch')
        staged = json.loads(gzip.decompress(raw))
        source_mode = staged.get('source_mode')
        sources = checkpoint_sources(index, day) if source_mode == 'closed_day' else source_set(index, day)
        if staged.get('version') in (2, 3) and source_mode == 'closed_day':
            sources.pop(evidence_receipt_path(day), None)
        if (staged.get('version') not in (2, 3, 4) or staged.get('date') != day.isoformat()
                or source_mode not in (None, 'closed_day') or not sources or sources != staged.get('sources')):
            raise ValueError('Frozen sources changed; prepare the day again')
        additions = staged['files']
        partial = staged['version'] >= 3 and staged.get('legacy_status') == 'partial_v51_skip'
        if (staged['version'] >= 3 and not recovery_core(additions, day)
                or not partial and not complete_day(additions, day)):
            raise ValueError('Incomplete recovered chain')
        dated = f'auditordata/prematch_{day}.json'
        if dated not in additions:
            raise ValueError('Missing recovered frozen archive')
        for path in additions:
            if not (path == dated or path.startswith('auditordata/_audit_parts/')
                    or path.startswith(f'auditarchive/prematch_{day}_') or path.startswith('auditarchive/_audit_parts/')):
                raise ValueError('Unexpected recovery output')
        restore_subset(storage, bucket, root, additions, [p for p in additions if '/_audit_parts/' not in p])
        if staged['version'] >= 4 and verified_evidence(root, day) is None:
            raise ValueError('Recovered daily evidence failed verification')
        if dated in index and additions[dated] != index[dated]:
            # New original journal observations may fill gaps. Existing forecast
            # identities/hashes must survive unchanged in the augmented ledger.
            from daily_audit import fingerprint
            with TemporaryDirectory() as old_folder:
                old_root = Path(old_folder)
                restore_subset(storage, bucket, old_root, index, [dated])
                with AuditLedger(old_root/dated) as before, AuditLedger(root/dated) as after:
                    for collection, key, record in before.records():
                        updated = after.get(collection, {}).get(key)
                        if updated is None or fingerprint(updated) != fingerprint(record):
                            raise ValueError('Frozen archive changed')
        from auditor_history_chunks import verified_receipt
        if partial:
            from v7_probability import VERSION
            v7 = read_store(root/f'auditarchive/prematch_{day}_v7.json', {})
            status = read_store(root/recovery_status(day), {})
            daily = read_store(root/target, {})
            if (v7.get('version') != VERSION or v7.get('date') != str(day)
                    or status.get('status') != 'partial_v51_skip' or status.get('date') != str(day)
                    or daily.get('audit_date') != str(day)):
                raise ValueError('Unverified V7 recovery')
        # A pending result needs a new check the next day. It does not invalidate
        # the verified receipt if preparation and publication crossed midnight.
        elif verified_receipt(root, day) is None:
            raise ValueError('Recovered chain does not have a verified receipt')
        if previous_manifest(storage, bucket) != index:
            raise RuntimeError('Private checkpoint advanced during publication')
        index = {**index, **additions}
        write_manifest(storage, bucket, index)
    elif not complete_day(index, day):
        raise ValueError('Existing report lacks the learning chain')

    yesterday = now.astimezone(TZ).date() - timedelta(days=1)
    recent = [report_path(yesterday-timedelta(days=i)) for i in range(30)]
    restore_subset(storage, bucket, root, index, {target, *recent} & index.keys())
    verified = read_store(root / target) if day <= yesterday else None
    if not isinstance(verified, dict) or verified.get('audit_date') != day.isoformat():
        raise ValueError('Published daily report does not match its Rome date')
    return publish_results(root, now)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['plan','prepare','publish'])
    parser.add_argument('--date', type=date.fromisoformat)
    args = parser.parse_args()
    if (args.mode != 'plan') != (args.date is not None):
        parser.error('--date is required for prepare and publish')
    if os.getenv('GITHUB_REF') != 'refs/heads/main' or os.getenv('GOLD_PRIVATE') != 'true':
        raise ValueError('Private production workflow required')
    storage, bucket, now = client(), os.environ['R2_BUCKET'], datetime.now(TZ)
    with TemporaryDirectory() as folder:
        root = Path(folder)
        if args.mode == 'plan':
            day = next_recovery_day(storage, bucket, root, now)
            with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
                out.write(f'date={day or ""}\n')
            print(f'Next closed date due for recovery: {day or "none"}', flush=True)
        elif args.mode == 'prepare':
            digest = prepare(storage, bucket, root, args.date, now, refresh=os.getenv('RECOVERY_REFRESH') == 'true',
                             recheck_results=os.getenv('RECOVERY_RECHECK_RESULTS') == 'true', existing_forecasts=True)
            with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
                out.write('digest='+digest+'\n')
            from auditor_history_chunks import receipt_path
            print(json.dumps({'verified_receipt': read_store(receipt_path(root, args.date), {})}, sort_keys=True), flush=True)
            print(f'Verified recovery input ready for {args.date}', flush=True)
        else:
            from private_publication import publish
            report = install(storage, bucket, root, args.date, os.environ['REPORT_DIGEST'], now)
            from auditor_history_chunks import receipt_path
            print(json.dumps({'published_receipt': read_store(receipt_path(root, args.date), {})}, sort_keys=True), flush=True)
            atomic_bytes(ROOT / PUBLIC_RESULTS, (root / PUBLIC_RESULTS).read_bytes())
            try:
                publish(ROOT, {PUBLIC_RESULTS})
                print(f'Published verified results for {args.date}; homepage date {report["date"]}, '
                      f'available={report["available"]}, settled={report["daily"]["settled"]}', flush=True)
            finally:
                recover_learning(storage, bucket, root, args.date, datetime.now(TZ))


if __name__ == '__main__':
    main()
