"""Repack the existing current Explorer view; no Gold, history or model writes."""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from time import sleep
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from explorer_lease import writer
from gold_explorer import indexed_current, put, read, signature
from gold_storage import client


def report(current, status):
    today = datetime.now(ZoneInfo('Europe/Rome')).date().isoformat()
    headers = current.get('page_index', [])
    fixture_pages = Counter(fid for p in headers for fid in p['fixtures'])
    print(json.dumps(dict(status=status, rows=current['count'], pages=len(current['pages']),
                          today_pages=sum(today in p['dates'] for p in headers),
                          max_pages_per_fixture=max(fixture_pages.values(),default=0),
                          generated_at=current['generated_at'], row_values_changed=0,
                          gold_writes=0, historical_writes=0, football_requests=0)), flush=True)


def repack(storage, bucket):
    original = read(storage, bucket, 'explorer/current.json')
    if not original or len(original.get('pages', [])) > 1000:
        raise ValueError('Missing/oversized current view')
    def load(digest):
        return read(storage, bucket, 'explorer/objects/'+digest+'.json')['rows']
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = [r for page in pool.map(load, original['pages']) for r in page]
    baseline = signature(sorted(rows, key=lambda r: r['id']))
    result = indexed_current(storage, bucket, rows, original)
    with ThreadPoolExecutor(max_workers=8) as pool:
        verified = [r for page in pool.map(load, result['pages']) for r in page]
    if len(verified) != original['count'] or signature(sorted(verified, key=lambda r: r['id'])) != baseline:
        raise ValueError('Current row values changed; publication refused')
    if read(storage, bucket, 'explorer/current.json') != original:
        raise RuntimeError('Current advanced; publication refused')
    put(storage, bucket, 'explorer/current.json', result)
    if read(storage, bucket, 'explorer/current.json') != result:
        raise RuntimeError('Current index verification failed')
    report(result, 'indexed_verified')


def main():
    storage, bucket = client(fast_restore=True), os.environ['R2_BUCKET']
    current = read(storage, bucket, 'explorer/current.json') or {}
    if (current.get('page_index_version') == 'fixture-pages-v1'
            and len(current.get('page_index', [])) == len(current.get('pages', []))
            and sum(p['count'] for p in current['page_index']) == current.get('count')):
        report(current, 'already_indexed')
        return
    for attempt in range(6):
        with writer(storage, bucket) as scoped:
            if scoped is not None:
                repack(scoped, bucket)
                return
        if attempt < 5:
            sleep(10)
    raise RuntimeError('Explorer writer busy; no current pointer changed')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit('Current indexing failed: '+type(error).__name__)
