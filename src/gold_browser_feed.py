"""Bound browser parsing memory without changing any forecast or checkpoint.

Each immutable gzip object contains a complete JSON array, unlike byte slices
of a huge document. The authenticated Gold projection alone uses this index.
"""
import gzip
import hashlib
import json

FORMAT = 'arabsniper-gold-pages-v1'
PAGE_BYTES = 8 * 1024 * 1024


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def project(value, upload, page_bytes=PAGE_BYTES):
    if not isinstance(value, dict) or not isinstance(value.get('matches'), list):
        return value
    result = {k: v for k, v in value.items() if k != 'matches'}
    for key in ('previous_day', 'current_day_archive'):
        if isinstance(result.get(key), dict):
            result[key] = project(result[key], upload, page_bytes)
    pages, pending, size = [], [], 2

    def flush():
        nonlocal pending, size
        if not pending:
            return
        raw = b'[' + b','.join(pending) + b']'
        digest = hashlib.sha256(raw).hexdigest()
        upload('_audit_parts/' + digest + '.json.gz', gzip.compress(raw, mtime=0))
        pages.append({'sha256': digest, 'bytes': len(raw), 'count': len(pending)})
        pending, size = [], 2

    for match in value['matches']:
        raw = encoded(match)
        if pending and size + len(raw) + 1 > page_bytes:
            flush()
        pending.append(raw)
        size += len(raw) + 1
    flush()
    result.update(_gold_storage=FORMAT, pages=pages, match_count=len(value['matches']), matches=[])
    return result


def publish_catalog(s3, bucket):
    """Refresh only browser metadata/pages; leave daily/social feeds untouched."""
    key = 'arab_quote_model_research.json'
    source = s3.get_object(Bucket=bucket, Key=key)
    catalog = json.load(source['Body'])
    versions = {key: source['ETag']}
    boards = {}
    def upload(path, raw):
        s3.put_object(Bucket=bucket, Key=path, Body=raw,
                      ContentType='application/octet-stream', CacheControl='private, no-store')
    for day in range(1, 6):
        name = f'arab_quote_day{day}.json'
        response = s3.get_object(Bucket=bucket, Key=name)
        versions[name] = response['ETag']
        board = json.load(response['Body'])
        boards[name] = project(board, upload)
        print(f"Browser ready {name}: {boards[name].get('match_count', 0)} matches, {len(boards[name].get('pages', []))} pages", flush=True)
        del board
    name = 'arab_quote_prediction_audit.json'
    response = s3.get_object(Bucket=bucket, Key=name)
    versions[name] = response['ETag']
    audit = json.load(response['Body'])
    catalog['_gold_audit_summary'] = {key: audit[key] for key in ('summary', 'updated_at') if key in audit}
    del audit
    catalog['_gold_browser_days'] = boards
    # The workflow also holds the shared writer lock. Reject any external writer.
    for path, etag in versions.items():
        if s3.head_object(Bucket=bucket, Key=path)['ETag'] != etag:
            raise ValueError('Gold source changed during browser publication: retry')
    raw = encoded(catalog)
    s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json', CacheControl='private, no-store')
    print(f'Browser catalog published: {len(raw)} bytes; source feeds unchanged', flush=True)


if __name__ == '__main__':
    import os
    from gold_storage import client
    publish_catalog(client(), os.environ['R2_BUCKET'])
