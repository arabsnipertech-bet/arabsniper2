"""Read-only aggregate payload/timing profile. No account session or provider calls."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import gzip
import json
import os
from pathlib import Path
import sys
from time import monotonic

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from gold_storage import client


def main():
    storage, bucket = client(fast_restore=True), os.environ['R2_BUCKET']
    def read(key):
        if not (key == 'arab_quote_model_research.json' or key == 'explorer/current.json'
                or key.startswith(('explorer/objects/', '_audit_parts/'))):
            raise ValueError('Profile scope')
        started = monotonic()
        response = storage.get_object(Bucket=bucket, Key=key)
        with response['Body'] as body:
            raw = body.read()
        return raw, round(monotonic()-started, 3)

    raw, seconds = read('explorer/current.json')
    current = json.loads(raw)
    pages = current.get('pages', [])
    def size(digest):
        return storage.head_object(Bucket=bucket, Key='explorer/objects/'+digest+'.json')['ContentLength']
    with ThreadPoolExecutor(max_workers=8) as pool:
        sizes = list(pool.map(size, pages))
    samples = []
    for digest in pages[:3]+pages[-1:]:
        data, elapsed = read('explorer/objects/'+digest+'.json')
        rows = json.loads(data).get('rows', [])
        samples.append(dict(bytes=len(data), seconds=elapsed, rows=len(rows),
                            dates=dict(Counter(r.get('date') for r in rows)),
                            fixtures=len({r.get('fixture_id') for r in rows})))
    catalog_raw, catalog_seconds = read('arab_quote_model_research.json')
    catalog = json.loads(catalog_raw)
    gold = []
    for name, day in (catalog.get('_gold_browser_days') or {}).items():
        gold.append(dict(feed=name, date=day.get('date'), pages=len(day.get('pages', [])),
                         matches=day.get('match_count'), raw_bytes=sum(p['bytes'] for p in day.get('pages', []))))
    first = next(iter((catalog.get('_gold_browser_days') or {}).values()), {})
    sample = {}
    if first.get('pages'):
        part = first['pages'][0]
        data, elapsed = read('_audit_parts/'+part['sha256']+'.json.gz')
        decoded = gzip.decompress(data)
        sample = dict(compressed_bytes=len(data), raw_bytes=len(decoded), seconds=elapsed,
                      matches=len(json.loads(decoded)))
    report = dict(remote_writes=0, football_requests=0, timing_source='direct R2 reads, not mobile network',
                  explorer=dict(rows=current.get('count'), pages=len(pages), bytes=sum(sizes),
                                index_seconds=seconds, sample_pages=samples),
                  gold=dict(catalog_bytes=len(catalog_raw), catalog_seconds=catalog_seconds,
                            days=gold, first_page=sample))
    print(json.dumps(report, sort_keys=True), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit('Loading profile failed: '+type(error).__name__)
