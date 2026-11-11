"""Light midnight anchor and bounded incremental prices under the existing writer lock."""
from datetime import datetime, time, timedelta, timezone
import importlib
import json
import os
from pathlib import Path

from api_usage import sanitize_status
from audit_storage import read_store, write_store
from fast_scan_guard import MARKER
from gold_storage import GOLD, client, previous_manifest, publish, restore_subset, sha, write_manifest
from matrix_quotes import attach
from multimarket_model import stamp
from proposal_stability import seed_stability
from quote_history import OPENING_SNAPSHOT, QuoteHistory, PREFIX as PRICE_HISTORY_PREFIX, restore_for_fixtures
from v7_archive import ROME
from v7_probability import utc

ROOT = Path(__file__).resolve().parents[1]


def local_now():
    return datetime.now(ROME)


def consolidate(storage, bucket, now):
    """Pin the preceding day's existing evidence without settling any fixture."""
    local = now.astimezone(ROME)
    if local.hour != 0 or not 1 <= local.minute < 25:
        return 'outside_midnight_window'
    day = local.date() - timedelta(days=1)
    path = f'data/arab_quote_v7_close_{day:%Y%m%d}.json'
    index = previous_manifest(storage, bucket)
    if not index:
        raise ValueError('Private checkpoint required for midnight consolidation')
    if path in index:
        return 'already_consolidated'
    source = json.dumps({'version': 1, 'files': index}, sort_keys=True, separators=(',', ':')).encode()
    source_hash = sha(source)
    sealed = storage.get_object(Bucket=bucket, Key=f'state/manifests/{source_hash}.json')['Body'].read()
    if sha(sealed) != source_hash:
        raise ValueError('Private checkpoint manifest checksum mismatch')
    payload = dict(version=1, date=str(day), status='pending_verification',
                   cutoff_utc=utc(datetime.combine(local.date(), time(), ROME)), captured_at=utc(now),
                   source_manifest_sha256=source_hash, settled_fixtures=0)
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
    digest = sha(raw)
    storage.put_object(Bucket=bucket, Key='state/objects/'+digest, Body=raw, Metadata={'sha256': digest})
    if previous_manifest(storage, bucket) != index:
        raise RuntimeError('Private checkpoint advanced during midnight consolidation')
    write_manifest(storage, bucket, {**index, path: {'sha256': digest, 'bytes': len(raw)}})
    return 'consolidated_pending_verification'


def price_window(root, now):
    local = now.astimezone(ROME)
    if not 12 <= local.hour <= 22 or not 30 <= local.minute < 53:
        return False
    marker = read_store(Path(root)/MARKER, {})
    began, ended = stamp(marker.get('started_at')), stamp(marker.get('completed_at'))
    hour = local.replace(minute=0, second=0, microsecond=0)
    return bool(marker.get('status') == 'complete' and began and ended
                and hour <= began <= ended <= now
                and now.astimezone(timezone.utc)-began.astimezone(timezone.utc) >= timedelta(minutes=24))


def upcoming_targets(day, now):
    from lateral_matrix import MODEL_ID
    eligible = []
    for match in day.get('matches', []):
        meta, model = match.get('fixture') or {}, match.get('multimarket') or {}
        kickoff = stamp(meta.get('kickoff'))
        if (kickoff and timedelta(minutes=5) <= kickoff-now <= timedelta(hours=2)
                and meta.get('status_short') in ('NS', 'TBD') and not meta.get('result')
                and model.get('model_id') == MODEL_ID and (model.get('matrix') or {}).get('rows')):
            eligible.append(match)
    return sorted(eligible, key=lambda m: (stamp(m['fixture']['kickoff']), str(m['fixture']['fixture_id'])))


def complete_price_pages(api, fid, can_request):
    payload = api.get('odds', {'fixture': fid, 'page': 1}, use_cache=False)
    if not payload or not payload.get('response'):
        return None
    paging = payload.get('paging') or {}
    pages = paging.get('total', 1)
    if type(pages) is not int or not 1 <= pages <= 6 or paging.get('current', 1) != 1:
        return None
    rows = list(payload['response'])
    for page in range(2, pages+1):
        if not can_request():
            return None
        next_page = api.get('odds', {'fixture': fid, 'page': page}, use_cache=False) or {}
        paging = next_page.get('paging') or {}
        if paging.get('current') != page or paging.get('total') != pages or not next_page.get('response'):
            return None
        rows.extend(next_page['response'])
    return {**payload, 'response': rows,
            'collection': {'complete': True, 'pages_fetched': pages, 'pages_total': pages}}


def incremental(root, api=None, clock=local_now, budget=65):
    root, now = Path(root), clock()
    if not price_window(root, now) or budget <= 0:
        return 0
    path = root/'data/arab_quote_day1.json'
    day = read_store(path, {})
    if day.get('date') != now.astimezone(ROME).date().isoformat():
        return 0
    targets = upcoming_targets(day, now)
    if not targets:
        return 0
    if api is None:
        api = importlib.import_module('3appdays').APIClient(os.environ['API_SPORTS_KEY'])
    try:
        quota = sanitize_status(api.get('status', {}, use_cache=False) or {})
    except ValueError:
        return 0
    if quota['active'] is not True:
        return 0
    reserve = max(500, quota['requests_limit_day'] // 20)
    allowance = min(budget, max(0, quota['requests_remaining'] - reserve))
    start = api.request_count
    deadline = now + timedelta(minutes=6)

    def can_request():
        return api.request_count - start + 4 <= allowance and clock() < deadline

    state_path = root/'data/arab_quote_state.json'
    state = read_store(state_path)
    if not isinstance(state, dict):
        raise ValueError('Missing private odds state')
    prices = QuoteHistory(root/'data')
    target_ids = {str(match['fixture']['fixture_id']) for match in targets}
    prices.seed_matrix({'matrix_quotes': {fid: value for fid, value in state.get('matrix_quotes', {}).items()
                                         if fid in target_ids}})
    refreshed = 0
    try:
        for match in targets:
            if not can_request():
                break
            meta, assessment = match['fixture'], match['multimarket']
            fid = int(meta['fixture_id'])
            payload = complete_price_pages(api, fid, can_request)
            at = clock()
            if not payload or at >= stamp(meta['kickoff']):
                continue
            from uuid import uuid4
            observed = utc(at)
            payload['collection']['acquisition_id'] = uuid4().hex
            for row in payload['response']:
                row['_observed_at'] = observed
            prices.observe(payload, observed, {'fixture': fid})
            prices.restore_openings(state, meta, observed)
            seed_stability(assessment, assessment)
            attach(assessment, payload, state, meta, observed)
            refreshed += 1
    finally:
        prices.flush()
    if refreshed:
        prices.save_openings(targets, utc(clock()))
        write_store(state_path, state)
        write_store(path, day)
    return refreshed


def price_pass(storage, bucket, root, budget, clock=local_now):
    # Restoring the entire archive took over 12 minutes in the production Fast.
    # The half-hour path restores only the live files and their verified chunks.
    root = Path(root)
    index = previous_manifest(storage, bucket)
    if MARKER not in index:
        return 0
    restore_subset(storage, bucket, root, index, [MARKER])
    if not price_window(root, clock()):
        return 0
    required = {'data/'+name for name in GOLD} | {
        'data/arab_quote_dashboard.json', 'data/arab_quote_state.json'}
    optional = {'data/arab_calibration_weights.json', 'data/arab_quote_v7_weights.json',
                'data/arab_quote_history.json', 'data/'+OPENING_SNAPSHOT}
    if not required.issubset(index):
        raise ValueError('Incomplete incremental Gold checkpoint')
    restore_subset(storage, bucket, root, index, required | (optional & index.keys()))
    current = read_store(root/'data/arab_quote_day1.json', {})
    restore_for_fixtures(storage, bucket, root, index,
                         [m['fixture']['fixture_id'] for m in upcoming_targets(current, clock())])
    count = incremental(root, clock=clock, budget=budget)
    if not count:
        return 0
    from refresh_opportunities import main as reprice
    from audit_journal import capture_policies
    reprice(root/'data')
    journal = capture_policies(root)
    if previous_manifest(storage, bucket) != index:
        raise RuntimeError('Private checkpoint advanced during the price refresh')
    paths = required | {'data/arab_quote_history.json', journal.relative_to(root).as_posix()}
    paths.update(p.relative_to(root).as_posix() for p in (root/'data').glob('arab_quote_selections_day[1-5].json'))
    paths.update(p.relative_to(root).as_posix() for p in (root/'data').glob(PRICE_HISTORY_PREFIX+'*.json'))
    paths.add('data/'+OPENING_SNAPSHOT)
    publish(storage, bucket, root, selected_paths=sorted(paths))
    return count


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('consolidate', 'prices'))
    args = parser.parse_args()
    if os.getenv('GOLD_PRIVATE') != 'true' or os.getenv('GITHUB_REF') != 'refs/heads/main':
        parser.error('V7 maintenance requires private production main')
    if args.command == 'consolidate':
        print(consolidate(client(), os.environ['R2_BUCKET'], local_now()))
    else:
        budget = max(0, min(100, int(os.getenv('ARAB_V7_PRICE_BUDGET', '65'))))
        count = price_pass(client(), os.environ['R2_BUCKET'], ROOT, budget)
        print(f'V7 price refresh: {count} fixtures')


if __name__ == '__main__':
    main()
