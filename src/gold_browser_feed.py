"""Bound browser parsing memory without changing any forecast or checkpoint.

Each immutable gzip object contains a complete JSON array, unlike byte slices
of a huge document. The authenticated Gold projection alone uses this index.
"""
import gzip
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone

FORMAT = 'arabsniper-gold-pages-v1'
PAGE_BYTES = 8 * 1024 * 1024
INACTIVE = {'PST', 'CANC', 'ABD'}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def inactive_without_analysis(match):
    meta = match.get('fixture') or {}
    status = meta.get('status_short') or (meta.get('result') or {}).get('status')
    if status not in INACTIVE:
        return False
    analysis = match.get('multimarket') or {}
    matrix = analysis.get('matrix') or {}
    saved = any((analysis.get('markets'), matrix.get('rows'),
                 (matrix.get('v7') or {}).get('status') == 'published'))
    promoted = any(pick and pick.get('tier') in ('GOLD', 'PLATINUM')
                   for pick in (match.get('triad') or {}).values())
    return not (promoted or saved or (match.get('_gold_history') or {}).get('verified_prematch'))


def prematch_coverage(day, now=None):
    from multimarket_model import stamp
    from quote_cache import valid_number

    now = now or datetime.now(timezone.utc)
    summary = {'all': Counter(), 'national_teams': Counter(), 'japan_cups': Counter()}
    for match in day.get('matches', []):
        meta = match.get('fixture') or {}
        kickoff = stamp(meta.get('kickoff'))
        if not kickoff or kickoff <= now or meta.get('status_short') not in ('NS', 'TBD') or meta.get('result'):
            continue
        matrix = (match.get('multimarket') or {}).get('matrix') or {}
        prices = [r for r in matrix.get('rows', []) if r.get('quote_executable') is True
                  and r.get('quote_status') in ('recent', 'cached_stable') and valid_number(r.get('odd')) and r['odd'] > 1]
        signs = {spec.get('side') for r in prices if (spec := r.get('spec') or {}).get('kind') == 'result'
                 and spec.get('period') == 'ft'}
        result_pair = any(set(r.get('selected_market_odds') or {}) == {'1', 'x', '2'} and
                          (r.get('spec') or {}).get('kind') == 'result' and r['spec'].get('period') == 'ft' for r in prices)
        goal_pair = any(valid_number(r.get('selected_market_fair_pct')) and
                        valid_number(r.get('selected_margin_pct')) and 0 <= r['selected_margin_pct'] <= 30 and
                        (spec := r.get('spec') or {}).get('kind') in ('total', 'asian_total') and
                        spec.get('period') == 'ft' and spec.get('line') == 2.5 for r in prices)
        picks = [p for p in ((matrix.get('v7') or {}).get('triad') or {}).values() if p]
        coverage = dict(fixtures=1, quoted=bool(prices), one_x_two={'1', 'x', '2'} <= signs,
                        complete_1x2=result_pair, complete_over25=goal_pair, complete_primary=result_pair and goal_pair,
                        stable=any(r.get('prematch_stable') for r in prices),
                        full_triad=len(picks) == 3, empty_triad=not picks,
                        model_estimates=sum(p.get('quote_state') == 'model_estimate' for p in picks))
        summary['all'].update(coverage)
        scope = (match.get('gold_scope') or {}).get('tier')
        if scope == 'national_team':
            summary['national_teams'].update(coverage)
        if scope == 'national_cup' and meta.get('country') == 'Japan':
            summary['japan_cups'].update(coverage)
    return {'date': day.get('date'), **summary}


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

    for match in sorted(value['matches'], key=inactive_without_analysis):
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
