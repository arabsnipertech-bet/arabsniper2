"""Recover a missing closed-day report from immutable prematch evidence.

Preparation uses an isolated runner. Publication alone takes the shared writer
lock, imports the two daily report files and preserves the latest scan manifest.
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
from gold_storage import allowed, client, collect, previous_manifest, sha, write_manifest
from homepage_results import publish as publish_results
from selection_studies import study_markdown

PUBLIC_RESULTS = 'data/arab_quote_results_public.json'
FORECASTS = 'data/arab_quote_prematch_audit.json'


def report_path(day):
    return f'auditarchive/prematch_{day}_daily.json'


def source_set(index, day):
    """Observations first captured after this Rome date cannot be prematch."""
    end = datetime.combine(day + timedelta(days=1), time.min, TZ)
    names = {FORECASTS, f'auditordata/prematch_{day}.json', f'auditarchive/prematch_{day}_results.json'}
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


def prepare(storage, bucket, root, day, now, fetcher=fetch_results):
    if not 1 <= (now.astimezone(TZ).date() - day).days <= 14:
        raise ValueError('Recovery requires a closed Rome date within 14 days')
    index = previous_manifest(storage, bucket)
    if report_path(day) in index:
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
        ledger.write(dated)

    settle_day(root, day, now, fetcher=fetcher)
    report = read_store(root / report_path(day))
    if not any(r.get('outcome') is not None for r in report['markets']):
        raise ValueError('No verified outcomes in the recovered report')
    staged = {'version': 1, 'date': day.isoformat(), 'sources': sources, 'report': report}
    raw = gzip.compress(json.dumps(staged, ensure_ascii=False, allow_nan=False,
                                   separators=(',', ':')).encode(), mtime=0)
    digest = sha(raw)
    storage.put_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz', Body=raw)
    return digest


def install(storage, bucket, root, day, digest, now):
    """Called only by the short publication job holding the normal writer mutex."""
    index = previous_manifest(storage, bucket)
    target = report_path(day)
    if target not in index:
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('No verified prepared report')
        raw = storage.get_object(Bucket=bucket, Key='audit-recovery/'+digest+'.json.gz')['Body'].read()
        if sha(raw) != digest:
            raise ValueError('Staged report checksum mismatch')
        staged = json.loads(gzip.decompress(raw))
        report = staged['report']
        if (staged.get('version') != 1 or staged.get('date') != day.isoformat()
                or report.get('audit_date') != day.isoformat()
                or source_set(index, day) != staged.get('sources')):
            raise ValueError('Frozen sources changed; prepare the day again')
        write_store(root / target, report)
        atomic_bytes((root / target).with_suffix('.md'),
                     (markdown(report)+study_markdown(report['selection_studies'])).encode())
        # Only report pointers and their immutable parts enter the latest state.
        additions = {}
        for path, data in collect(root).items():
            if path not in (target, str(Path(target).with_suffix('.md'))) and not path.startswith('auditarchive/_audit_parts/'):
                raise ValueError('Unexpected recovery output')
            computed = sha(data)
            storage.put_object(Bucket=bucket, Key='state/objects/'+computed, Body=data)
            additions[path] = {'sha256': computed, 'bytes': len(data)}
        if previous_manifest(storage, bucket) != index:
            raise RuntimeError('Private checkpoint advanced during publication')
        index = {**index, **additions}
        write_manifest(storage, bucket, index)

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
