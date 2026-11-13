"""Recover a missing closed-day report from immutable prematch evidence.

Preparation uses an isolated runner. Publication alone takes the shared writer
lock and preserves the latest scan manifest even when V5.1 evidence is incomplete.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
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
from audit_journal import PREFIX, ForecastHistory, merge_forecast
from audit_ledger import AuditLedger, entries
from audit_storage import FORMAT, atomic_bytes, read_store, write_store
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
    end = datetime.combine(day + timedelta(days=1), time.min, TZ)
    dated = f'auditordata/prematch_{day}.json'
    names = {dated, f'auditarchive/prematch_{day}_results.json'}
    names.add(FORECASTS)
    for path in index:
        if path.startswith('data/'+PREFIX) and path.endswith(('_forecasts.json','_policies.json')):
            value = path.removeprefix('data/'+PREFIX).rsplit('_',1)[0]
            at = datetime.strptime(value, '%Y%m%dT%H%M%S%f').replace(tzinfo=timezone.utc)
            if at < end:
                names.add(path)
    return {p: index[p]['sha256'] for p in sorted(names) if p in index}


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


def prepare(storage, bucket, root, day, now, fetcher=fetch_results, *, refresh=False, recheck_results=False):
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
    sources = source_set(index, day)
    restore_subset(storage, bucket, root, index, sources)
    dated = root / f'auditordata/prematch_{day}.json'
    with AuditLedger(dated) as ledger:
        if dated.exists() and ledger.metadata.get('audit_date') != day.isoformat():
            raise ValueError('Private archive date mismatch')
        ledger.metadata.update(schema_version=1, audit_date=day.isoformat())
        for col, key, record in ledger.records():
            if forecast_day(col, key, record, now) != day.isoformat():
                raise ValueError('Invalid frozen archive entry')
        # Existing per-date archive wins, then the original master, then each
        # journal in observation order, as in normal archival/ingestion.
        pending = [p for p in sorted(sources) if p.startswith('data/'+PREFIX)]
        # Match ordinary ingestion: forecast journals first, policy journals
        # second. Preserve policy publication markers and Study observations.
        pending.sort(key=lambda p:(p.endswith('_policies.json'),p))
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
        if not any(len(ledger.get(c, {})) for c in ('multimarket_evaluations', 'analysis_updates')):
            raise ValueError('No eligible frozen observations for this date')
        for (c,k), checksum in original.items():
            if fingerprint(ledger[c][k]) != checksum:
                raise ValueError('Existing frozen prediction changed during recovery')
        if not dated.exists() or sum(len(ledger.get(c,{})) for c in ledger.collections) != len(original):
            ledger.write(dated)

    queried = settle_day(root, day, now, fetcher=fetcher, legacy_evidence=False, recheck_results=recheck_results,
                         progress=lambda message: print(f'Recovery {day}: {message}', flush=True))
    report = read_store(root / report_path(day))
    v7 = read_store(root/f'auditarchive/prematch_{day}_v7.json')
    if not any(r.get('outcome') is not None for r in report['markets']) and not v7['counts'].get('settled'):
        raise ValueError('No verified outcomes in the recovered report')
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
    for path, raw_file in recovery_files(root, pointers):
        checksum = sha(raw_file)
        storage.put_object(Bucket=bucket, Key='state/objects/'+checksum, Body=raw_file)
        bundle[path] = {'sha256': checksum, 'bytes': len(raw_file)}
    staged = {'version': 3, 'date': day.isoformat(), 'sources': sources, 'files': bundle,
              'legacy_status': status}
    raw = gzip.compress(json.dumps(staged, ensure_ascii=False, allow_nan=False,
                                   separators=(',', ':')).encode(), mtime=0)
    digest = sha(raw)
    storage.put_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz', Body=raw)
    return digest


def install(storage, bucket, root, day, digest, now):
    """Called only by the short publication job holding the normal writer mutex."""
    index = previous_manifest(storage, bucket)
    target = report_path(day)
    if digest != 'existing':
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('No verified prepared chain')
        raw = storage.get_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz')['Body'].read()
        if sha(raw) != digest:
            raise ValueError('Staged chain checksum mismatch')
        staged = json.loads(gzip.decompress(raw))
        if (staged.get('version') not in (2, 3) or staged.get('date') != day.isoformat()
                or source_set(index, day) != staged.get('sources')):
            raise ValueError('Frozen sources changed; prepare the day again')
        additions = staged['files']
        partial = staged['version'] == 3 and staged.get('legacy_status') == 'partial_v51_skip'
        if (staged['version'] == 3 and not recovery_core(additions, day)
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
        from auditor_history_chunks import reusable
        if partial:
            from v7_probability import VERSION
            v7 = read_store(root/f'auditarchive/prematch_{day}_v7.json', {})
            status = read_store(root/recovery_status(day), {})
            daily = read_store(root/target, {})
            if (v7.get('version') != VERSION or v7.get('date') != str(day)
                    or status.get('status') != 'partial_v51_skip' or status.get('date') != str(day)
                    or daily.get('audit_date') != str(day)):
                raise ValueError('Unverified V7 recovery')
        elif not reusable(root, day, now):
            raise ValueError('Recovered chain does not have a verified receipt')
        if previous_manifest(storage, bucket) != index:
            raise RuntimeError('Private checkpoint advanced during publication')
        index = {**index, **additions}
        write_manifest(storage, bucket, index)
    elif not complete_day(index, day):
        raise ValueError('Existing report lacks the learning chain')

    yesterday = now.astimezone(TZ).date() - timedelta(days=1)
    recent = [report_path(yesterday-timedelta(days=i)) for i in range(30)]
    restore_subset(storage, bucket, root, index, [p for p in recent if p in index])
    verified = read_store(root / target) if day <= yesterday and (yesterday-day).days < 30 else None
    if not isinstance(verified, dict) or verified.get('audit_date') != day.isoformat():
        raise ValueError('Published daily report does not match its Rome date')
    return publish_results(root, now)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['prepare','publish'])
    parser.add_argument('--date', required=True, type=date.fromisoformat)
    args = parser.parse_args()
    if os.getenv('GITHUB_REF') != 'refs/heads/main' or os.getenv('GOLD_PRIVATE') != 'true':
        raise ValueError('Private production workflow required')
    storage, bucket, now = client(), os.environ['R2_BUCKET'], datetime.now(TZ)
    with TemporaryDirectory() as folder:
        root = Path(folder)
        if args.mode == 'prepare':
            digest = prepare(storage, bucket, root, args.date, now, refresh=os.getenv('RECOVERY_REFRESH') == 'true',
                             recheck_results=os.getenv('RECOVERY_RECHECK_RESULTS') == 'true')
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
            publish(ROOT, {PUBLIC_RESULTS})
            print(f'Published verified results for {args.date}; homepage date {report["date"]}, '
                  f'available={report["available"]}, settled={report["daily"]["settled"]}', flush=True)


if __name__ == '__main__':
    main()
