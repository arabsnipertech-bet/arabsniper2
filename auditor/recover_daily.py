"""Recover a missing closed-day report from immutable prematch evidence.

Preparation uses an isolated runner. Publication alone takes the shared writer
lock, imports the complete verified learning chain and preserves the latest scan manifest.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from audit_journal import PREFIX, merge_forecast
from audit_ledger import AuditLedger, entries
from audit_storage import FORMAT, atomic_bytes, read_store, write_store
from current_audit import TZ, fetch_results, forecast_day, settle_day
from daily_audit import markdown
from gold_storage import allowed, client, previous_manifest, sha, write_manifest
from homepage_results import publish as publish_results
from selection_studies import study_markdown

PUBLIC_RESULTS = 'data/arab_quote_results_public.json'
FORECASTS = 'data/arab_quote_prematch_audit.json'


def report_path(day):
    return f'auditarchive/prematch_{day}_daily.json'


def complete_day(index, day):
    return all(f'auditarchive/prematch_{day}_{suffix}' in index for suffix in
               ('daily.json', 'daily.md', 'results.json', 'calibration.json', 'checkpoint.json'))


def source_set(index, day):
    """Observations first captured after this Rome date cannot be prematch."""
    end = datetime.combine(day + timedelta(days=1), time.min, TZ)
    dated = f'auditordata/prematch_{day}.json'
    names = {dated, f'auditarchive/prematch_{day}_results.json'}
    if dated not in index:
        names.add(FORECASTS)
    for path in index:
        if path.startswith('data/'+PREFIX) and path.endswith('_forecasts.json'):
            value = path.removeprefix('data/'+PREFIX).removesuffix('_forecasts.json')
            at = datetime.strptime(value, '%Y%m%dT%H%M%S%f').replace(tzinfo=timezone.utc)
            if at < end:
                names.add(path)
    return {p: index[p]['sha256'] for p in sorted(names) if p in index}


def restore_subset(storage, bucket, root, index, paths):
    """Download only named pointers and their verified content-addressed chunks."""
    def download(path):
        info = index[path]
        digest = info['sha256']
        if not allowed(path) or not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('Invalid private object reference')
        raw = storage.get_object(Bucket=bucket, Key='state/objects/'+digest)['Body'].read()
        if len(raw) != info['bytes'] or sha(raw) != digest:
            raise ValueError('Private object checksum mismatch')
        target = root / path
        atomic_bytes(target, raw)
        if target.suffix == '.json' and len(raw) <= 1024*1024:
            value = json.loads(raw)
            if isinstance(value, dict) and value.get('_storage') == FORMAT:
                parts = []
                for item in value['parts']:
                    name = item['sha256']
                    if not re.fullmatch('[0-9a-f]{64}', name):
                        raise ValueError('Invalid private chunk')
                    parts.append(str(Path(path).parent/'_audit_parts'/(name+'.json.gz')))
                return parts
        return []

    with ThreadPoolExecutor(max_workers=8) as pool:
        chunks = {part for parts in pool.map(download, sorted(set(paths))) for part in parts}
        for _ in pool.map(download, sorted(chunks)):
            pass


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


def prepare(storage, bucket, root, day, now, fetcher=fetch_results):
    if not 1 <= (now.astimezone(TZ).date() - day).days <= 14:
        raise ValueError('Recovery requires a closed Rome date within 14 days')
    index = previous_manifest(storage, bucket)
    if complete_day(index, day):
        restore_subset(storage, bucket, root, index, [f'auditarchive/prematch_{day}_checkpoint.json'])
        receipt = read_store(root/f'auditarchive/prematch_{day}_checkpoint.json')
        if receipt.get('pending') is False:
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
        from daily_audit import fingerprint
        original = {(c,k): fingerprint(r) for c,k,r in ledger.records()}
        paths = ([FORECASTS] if FORECASTS in sources else []) + pending
        for path in paths:
            journal = path != FORECASTS
            verified_type = not journal
            for collection, key, record in entries(root / path):
                if collection is None:
                    if journal and key == 'journal_type':
                        if record != 'forecasts':
                            raise ValueError('Incorrect journal type')
                        verified_type = True
                    continue
                if key is None or forecast_day(collection, key, record, now) != day.isoformat():
                    continue
                record.pop('settlement', None)
                if journal:
                    merge_forecast(ledger, collection, key, record)
                else:
                    ledger.put(collection, key, record, overwrite=False)
            if not verified_type:
                raise ValueError('Missing forecast journal type')
        if not any(len(ledger.get(c, {})) for c in ('multimarket_evaluations', 'analysis_updates')):
            raise ValueError('No eligible frozen observations for this date')
        for (c,k), checksum in original.items():
            if fingerprint(ledger[c][k]) != checksum:
                raise ValueError('Existing frozen prediction changed during recovery')
        if not dated.exists() or sum(len(ledger.get(c,{})) for c in ledger.collections) != len(original):
            ledger.write(dated)

    settle_day(root, day, now, fetcher=fetcher)
    report = read_store(root / report_path(day))
    if not any(r.get('outcome') is not None for r in report['markets']):
        raise ValueError('No verified outcomes in the recovered report')
    # Stage the complete chain, including immutable parts. Publication runs
    # under the existing writer mutex and checks the original source set again.
    bundle = {}
    pointers = [f'auditordata/prematch_{day}.json', *[p.relative_to(root).as_posix()
                for p in (root/'auditarchive').glob(f'prematch_{day}_*') if p.is_file()]]
    for path, raw_file in recovery_files(root, pointers):
        checksum = sha(raw_file)
        storage.put_object(Bucket=bucket, Key='state/objects/'+checksum, Body=raw_file)
        bundle[path] = {'sha256': checksum, 'bytes': len(raw_file)}
    staged = {'version': 2, 'date': day.isoformat(), 'sources': sources, 'files': bundle}
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
        if (staged.get('version') != 2 or staged.get('date') != day.isoformat()
                or source_set(index, day) != staged.get('sources')):
            raise ValueError('Frozen sources changed; prepare the day again')
        additions = staged['files']
        if not complete_day(additions, day):
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
        if not reusable(root, day, now):
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
            digest = prepare(storage, bucket, root, args.date, now)
            with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
                out.write('digest='+digest+'\n')
            print(f'Verified recovery input ready for {args.date}', flush=True)
        else:
            from private_publication import publish
            report = install(storage, bucket, root, args.date, os.environ['REPORT_DIGEST'], now)
            atomic_bytes(ROOT / PUBLIC_RESULTS, (root / PUBLIC_RESULTS).read_bytes())
            publish(ROOT, {PUBLIC_RESULTS})
            print(f'Published verified results for {args.date}; homepage date {report["date"]}, '
                  f'available={report["available"]}, settled={report["daily"]["settled"]}', flush=True)


if __name__ == '__main__':
    main()
