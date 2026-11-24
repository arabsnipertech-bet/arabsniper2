"""Attach only saved numerical references to current Explorer rows."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from time import sleep

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from explorer_lease import writer
from gold_explorer import indexed_current, put, read, signature
from gold_storage import client
from multimarket_model import stamp
from selection_display_facts import selection_facts
from v7_decision import fingerprint


def kickoff(value):
    when = stamp(value)
    return when.astimezone(timezone.utc).isoformat() if when else None


def saved_facts(matches):
    result = {}
    for match in matches:
        meta = match.get('fixture') or {}
        snap = (((match.get('multimarket') or {}).get('matrix') or {}).get('v7') or {})
        digest = snap.get('sha256')
        if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
            continue
        if digest != fingerprint({k:v for k,v in snap.items() if k != 'sha256'}):
            continue
        for market, record in snap.get('markets', {}).items():
            spec = json.loads(market)
            key = (str(meta.get('fixture_id')), kickoff(snap.get('kickoff')), digest, market)
            result[key] = (tuple(record.get(k) for k in ('calibrated','odd','ev','ev_low')),
                           selection_facts(spec, record, snap))
    return result


def attach(rows, support):
    result = []
    for row in rows:
        key = (str(row['fixture_id']), kickoff(row['kickoff']), row['forecast_sha256'], row['market'])
        saved = support.get(key)
        if saved and saved[0] == tuple(row.get(k) for k in ('probability','odd','ev','ev_low')):
            row = {**row, 'selection_facts': saved[1]}
        result.append(row)
    return result


def core(rows):
    return signature(sorted(({k:v for k,v in row.items() if k != 'selection_facts'} for row in rows), key=lambda r:r['id']))


def enrich(storage, bucket):
    original = read(storage, bucket, 'explorer/current.json')
    if not original or len(original.get('pages', [])) > 1000:
        raise ValueError('Missing/oversized current view')
    catalog = json.loads(storage.get_object(Bucket=bucket,Key='arab_quote_model_research.json')['Body'].read())
    refs = {p['sha256']:p for day in (catalog.get('_gold_browser_days') or {}).values() for p in day.get('pages', [])}
    if not refs:
        raise ValueError('No verified Gold browser pages')
    def source(item):
        digest, ref = item
        if not re.fullmatch('[a-f0-9]{64}', digest):
            raise ValueError('Invalid Gold page reference')
        raw = gzip.decompress(storage.get_object(Bucket=bucket,Key='_audit_parts/'+digest+'.json.gz')['Body'].read())
        if len(raw) != ref['bytes'] or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('Gold page checksum mismatch')
        return saved_facts(json.loads(raw))
    with ThreadPoolExecutor(max_workers=4) as pool:
        support = {k:v for part in pool.map(source, refs.items()) for k,v in part.items()}
    def load(digest):
        return read(storage,bucket,'explorer/objects/'+digest+'.json')['rows']
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = [r for part in pool.map(load, original['pages']) for r in part]
    baseline = core(rows)
    enriched = attach(rows, support)
    with_lambda = sum(any(k.startswith('lambda_') for k in r.get('selection_facts', {})) for r in enriched)
    if rows and not with_lambda:
        raise ValueError('No matching saved statistical references')
    result = indexed_current(storage,bucket,enriched,original)
    with ThreadPoolExecutor(max_workers=8) as pool:
        verified = [r for part in pool.map(load, result['pages']) for r in part]
    if len(verified) != original['count'] or core(verified) != baseline:
        raise ValueError('Original row values changed; publication refused')
    if read(storage,bucket,'explorer/current.json') != original:
        raise RuntimeError('Current advanced; publication refused')
    put(storage,bucket,'explorer/current.json',result)
    if read(storage,bucket,'explorer/current.json') != result:
        raise RuntimeError('Current references not verified')
    print(json.dumps(dict(status='compact_facts_verified',rows=len(rows),rows_with_lambda=with_lambda,
        original_values_changed=0,gold_writes=0,historical_writes=0,football_requests=0)),flush=True)


def main():
    storage,bucket = client(fast_restore=True),os.environ['R2_BUCKET']
    for attempt in range(12):
        with writer(storage,bucket) as scoped:
            if scoped is not None:
                enrich(scoped,bucket)
                return
        if attempt < 11:
            sleep(10)
    raise RuntimeError('Explorer writer busy; no current pointer changed')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit('Compact references failed: '+type(error).__name__)
