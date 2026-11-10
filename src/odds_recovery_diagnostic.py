"""Bounded read-only comparison of API-Football, private checkpoint and Gold.

Only aggregates and the two requested fixtures' public market prices are logged.
No production checkpoint, forecast, quote or credential is modified.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile

from audit_storage import read_store
from gold_storage import client as storage_client, previous_manifest, restore_subset
from matrix_quotes import collect
from multimarket_model import stamp
from odds_collection import OddsEnricher, coverage
from odds_probe import engine

TARGET_DATE = '2026-11-11'
TARGETS = (('kawasaki', 'kashiwa'), ('vissel', 'machida'))


def current_time():
    return datetime.now(timezone.utc).isoformat()


def named(meta):
    name = str(meta.get('match', '')).lower()
    return any(all(part in name for part in pair) for pair in TARGETS)


def prices(rows):
    return [{k: r.get(k) for k in ('id', 'odd', 'first_odd', 'first_observed_at',
             'quote_observed_at', 'provider_updated_at', 'bookmaker_id', 'bookmaker',
             'quote_status', 'delta_pct', 'oci')} for r in rows
            if (r.get('spec') or {}).get('kind') == 'result'
            and r['spec'].get('period') == 'ft']


def inventory(payload, fid, at):
    groups, rejected = collect(payload or {}, fid, at)
    return {'usable_market_books': len(coverage(payload, fid, at, False)),
            'complete_groups': sum(g['complete'] for g in groups.values()),
            'books': sorted({g['bookmaker_id'] for g in groups.values()}),
            'market_books': sorted([list(item) for item in coverage(payload, fid, at, False)]),
            'rejections': rejected,
            'snapshot_sha256': hashlib.sha256(json.dumps(payload or {}, sort_keys=True).encode()).hexdigest()}


def feed_coverage(days):
    rows = [m for day in days for m in day.get('matches', [])]
    result = {'fixtures': len(rows), '1x2_complete': 0, '1x2_any': 0,
              'goal_prices': 0, 'with_temporal_history': 0, 'models': {}}
    for m in rows:
        a = m.get('multimarket') or {}
        model = str((m.get('v7') or {}).get('model_version') or a.get('model_id'))
        result['models'][model] = result['models'].get(model, 0)+1
        matrix = (a.get('matrix') or {}).get('rows', [])
        available = [r for r in matrix if type(r.get('odd')) in (float, int)
                     and r['odd'] > 1 and r.get('quote_status') in ('recent', 'cached_stable')]
        outcomes = {(r.get('spec') or {}).get('side') for r in available
                    if (r.get('spec') or {}).get('kind') == 'result'
                    and r['spec'].get('period') == 'ft'}
        result['1x2_complete'] += {'1', 'x', '2'} <= outcomes
        result['1x2_any'] += bool(outcomes)
        result['goal_prices'] += sum((r.get('spec') or {}).get('kind') in ('total', 'btts') for r in available)
        result['with_temporal_history'] += any(r.get('oci') is not None for r in matrix)
    return result


def run():
    e = engine()
    api = e.APIClient(os.environ['API_SPORTS_KEY'])
    s3, bucket = storage_client(), os.environ['R2_BUCKET']
    index = previous_manifest(s3, bucket)
    selected = {'data/arab_quote_state.json', 'data/arab_quote_odds_collection.json',
                'data/arab_bet_catalog.json'}
    selected.update(f'data/arab_quote_day{i}.json' for i in range(1, 6))
    report = {'at': current_time(), 'revision': os.getenv('GITHUB_SHA'),
              'requested_date': TARGET_DATE, 'production_writes': 0,
              'checkpoint_files': len(index), 'cases': []}
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        restore_subset(s3, bucket, root, index, selected & index.keys())
        state = read_store(root/'data/arab_quote_state.json', {})
        saved = read_store(root/'data/arab_quote_odds_collection.json', {})
        catalog = read_store(root/'data/arab_bet_catalog.json', {})
        days = [read_store(root/f'data/arab_quote_day{i}.json', {}) for i in range(1, 6)]
        gold = {}
        for i in range(1, 6):
            try:
                gold[str(i)] = json.loads(s3.get_object(Bucket=bucket, Key=f'arab_quote_day{i}.json')['Body'].read())
            except Exception as error:
                report.setdefault('gold_errors', []).append(type(error).__name__)
        report['archive_coverage'] = feed_coverage(days)
        report['gold_coverage'] = feed_coverage(gold.values())
        report['checkpoint_identity'] = hashlib.sha256(json.dumps(index, sort_keys=True).encode()).hexdigest()
        quota, _ = api.get_bounded('status', {}, 1)
        limits = ((quota or {}).get('response') or {}).get('requests') or {}
        used, limit = limits.get('current'), limits.get('limit_day')
        if type(used) is not int or type(limit) is not int or limit-used <= max(7500, int(limit*.1))+150:
            raise RuntimeError('Verified provider quota unavailable or insufficient')
        report['quota_remaining_before'] = limit-used
        schedule, _ = api.get_bounded('fixtures', {'date': TARGET_DATE, 'timezone': 'Europe/Rome'}, 3)
        metas = [e.fixture_meta(row) for row in (schedule or {}).get('response', [])]
        requested = [m for m in metas if named(m)]
        report['api_target_fixtures_found'] = [{k: m.get(k) for k in
            ('fixture_id', 'match', 'kickoff', 'league', 'league_id', 'season', 'date', 'status_short')} for m in requested]
        archived = [m for day in days for m in day.get('matches', [])]
        gold_matches = [m for day in gold.values() for m in day.get('matches', [])]
        for match in archived:
            meta = match.get('fixture') or {}
            if named(meta) and not any(str(m['fixture_id']) == str(meta['fixture_id']) for m in requested):
                requested.append(meta)
        for pair in TARGETS:
            if not any(all(p in str(m.get('match', '')).lower() for p in pair) for m in requested):
                report.setdefault('missing_requested_fixtures', []).append(list(pair))
        sample = []
        seen = {str(m['fixture_id']) for m in requested}
        for match in archived:
            meta = match.get('fixture') or {}
            start = stamp(meta.get('kickoff'))
            if start and start > datetime.now(timezone.utc) and str(meta['fixture_id']) not in seen:
                sample.append(meta)
                seen.add(str(meta['fixture_id']))
            if len(sample) >= 4:
                break
        enricher = OddsEnricher(api, deepcopy(saved), max_calls=32, max_seconds=90,
                              market_catalog=catalog.get('bets') or saved.get('market_catalog', []))
        for meta in requested+sample:
            fid = meta['fixture_id']
            before_calls = api.request_count
            base, _ = api.get_bounded('odds', {'fixture': fid}, 12)
            at = current_time()
            row = {k: meta.get(k) for k in ('fixture_id', 'match', 'kickoff', 'date', 'league')}
            row['requested_case'] = named(meta)
            row['general'] = inventory(base, fid, at)
            enricher.register_fixture(meta)
            recovered = enricher.enrich(base, fid)
            row['collector'] = inventory(recovered, fid, current_time())
            if row['requested_case']:
                row['queries'] = []
                for bid in (1, 5, 6, 8, 34):
                    result, _ = api.get_bounded('odds', {'fixture': fid, 'bet': bid}, 8)
                    row['queries'].append({'bet': bid, **inventory(result, fid, current_time())})
                for key, matches in [('archive', archived), ('gold', gold_matches)]:
                    item = next((m for m in matches if str((m.get('fixture') or {}).get('fixture_id')) == str(fid)), {})
                    row[key] = {'present': bool(item), '1x2': prices(((item.get('multimarket') or {}).get('matrix') or {}).get('rows', [])),
                                'v7': (item.get('multimarket') or {}).get('v7', {}).get('version')}
                row['archive_groups'] = {k: {'count': len(points), 'first': points[0], 'last': points[-1]}
                    for k, points in state.get('matrix_quotes', {}).get(str(fid), {}).get('groups', {}).items()
                    if points and ('|result|ft|' in k or '|btts|ht|' in k)}
            row['http_calls'] = api.request_count-before_calls
            report['cases'].append(row)
        report['targeted_stats'] = enricher.stats
        report['http_calls'] = api.request_count
    destination = Path('odds-recovery-evidence')
    destination.mkdir(exist_ok=True)
    (destination/'diagnostic.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print('ODDS_RECOVERY_DIAGNOSTIC '+json.dumps(report, ensure_ascii=False, separators=(',', ':')), flush=True)
    return report


if __name__ == '__main__':
    try:
        run()
    except Exception as error:
        raise SystemExit('Odds recovery diagnostic failed: '+type(error).__name__)
