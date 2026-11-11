"""Bounded read-only comparison of API-Football, private checkpoint and Gold.

Only aggregates and the two requested fixtures' public market prices are logged.
No production checkpoint, forecast, quote or credential is modified.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import requests

from audit_storage import read_store
from gold_browser_feed import prematch_coverage
from gold_storage import client as storage_client, previous_manifest, restore_subset
from matrix_quotes import collect
from multimarket_model import stamp
from odds_collection import OddsEnricher, coverage, quota_reserve
from odds_probe import engine
from quote_history import QuoteHistory, PREFIX, INDEX

TARGET_DATE = '2026-11-11'
TARGETS = (('kawasaki', 'kashiwa'), ('vissel', 'machida'))
# Keep the pre-release comparison reproducible even if new quoted fixtures
# change the four dynamically selected API examples.
PERSISTENCE_PROOF_IDS = {'1647175', '1645407', '1649140', '1649141', '1648030', '1648031'}


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
    raw = [r for r in (payload or {}).get('response', []) if str((r.get('fixture') or {}).get('id')) == str(fid)]
    return {'raw_fixture_records': len(raw), 'api_results': (payload or {}).get('results'),
            'api_error_fields': list((payload or {}).get('errors') or []),
            'pagination': (payload or {}).get('collection'),
            'raw_bookmakers': len({b.get('id') for r in raw for b in r.get('bookmakers', [])}),
            'raw_bet_types': sorted({str(bet.get('id'))+':'+str(bet.get('name')) for r in raw
                for b in r.get('bookmakers', []) for bet in b.get('bets', [])}),
            'usable_market_books': len(coverage(payload, fid, at, False)),
            'complete_groups': sum(g['complete'] for g in groups.values()),
            'books': sorted({g['bookmaker_id'] for g in groups.values()}),
            'market_books': sorted([list(item) for item in coverage(payload, fid, at, False)]),
            'rejections': rejected,
            'snapshot_sha256': hashlib.sha256(json.dumps(payload or {}, sort_keys=True).encode()).hexdigest()}


def feed_coverage(days):
    rows = [m for day in days for m in day.get('matches', [])]
    result = {'fixtures': len(rows), '1x2_complete': 0, '1x2_any': 0,
              'goal_prices': 0, 'with_temporal_history': 0, 'models': {},
              'upcoming_fixtures': 0, 'upcoming_1x2_complete': 0, 'upcoming_with_goal_prices': 0}
    for m in rows:
        a = m.get('multimarket') or {}
        model = str((a.get('matrix') or {}).get('publication_model') or a.get('model_id'))
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
        start = stamp((m.get('fixture') or {}).get('kickoff'))
        if start and start > datetime.now(timezone.utc):
            result['upcoming_fixtures'] += 1
            result['upcoming_1x2_complete'] += {'1', 'x', '2'} <= outcomes
            result['upcoming_with_goal_prices'] += any((r.get('spec') or {}).get('kind') in ('total', 'btts') for r in available)
    return result


def v6_inventory(payload, fid, at):
    root = os.environ.get('BASELINE_ROOT')
    if not root:
        return {'available': False}
    program = """import json,sys
from matrix_quotes import collect
value=json.load(sys.stdin)
groups,rejected=collect(value['payload'],value['fid'],value['at'])
print(json.dumps({'complete_groups':sum(g['complete'] for g in groups.values()),
                 'groups':len(groups),'books':sorted({g['bookmaker_id'] for g in groups.values()}),
                 'rejections':rejected}))
"""
    env = {**os.environ, 'PYTHONPATH': str(Path(root)/'src')}
    result = subprocess.run([sys.executable, '-c', program], cwd=root, env=env,
        input=json.dumps({'payload': payload or {}, 'fid': fid, 'at': at}), text=True,
        capture_output=True, check=True, timeout=30)
    return json.loads(result.stdout)


def rendered_gold(match):
    program = """const a=require('./web/lateral-matrix.js');let s='';
process.stdin.on('data',b=>s+=b);process.stdin.on('end',()=>{
const html=a.card(JSON.parse(s),{});console.log(JSON.stringify({
has_1x2_board:html.includes('mx-compact-quotes'),
first_observed_label:html.includes('Prima osservata'),
movement_cells:(html.match(/data-movement=/g)||[]).length,
empty_slots:(html.match(/Nessun mercato idoneo/g)||[]).length,
model_estimates:(html.match(/Stima V7 · in attesa di quote bookmaker/g)||[]).length}));});"""
    result = subprocess.run(['node', '-e', program], cwd=Path(__file__).resolve().parents[1],
        input=json.dumps(match), text=True, capture_output=True, check=True, timeout=20)
    return json.loads(result.stdout)


def browser_matches(s3, bucket, catalog, day, cache):
    board = (catalog.get('_gold_browser_days') or {}).get(f'arab_quote_day{day}.json') or {}
    for item in board.get('pages', []):
        digest = item['sha256']
        if digest not in cache:
            raw = gzip.decompress(s3.get_object(Bucket=bucket, Key='_audit_parts/'+digest+'.json.gz')['Body'].read())
            if hashlib.sha256(raw).hexdigest() != digest or len(raw) != item['bytes']:
                raise ValueError('Published browser page checksum mismatch')
            matches = json.loads(raw)
            if not isinstance(matches, list) or len(matches) != item['count']:
                raise ValueError('Published browser page count mismatch')
            cache[digest] = matches
        yield from cache[digest]


def browser_match(s3, bucket, catalog, day, fid, cache):
    return next((m for m in browser_matches(s3, bucket, catalog, day, cache)
                 if str((m.get('fixture') or {}).get('fixture_id')) == str(fid)), None)


def rendered_browser_coverage(matches, as_of):
    """Run the shipped Gold renderer on every active browser fixture."""
    active = []
    for match in matches:
        fixture = match.get('fixture') or {}
        start = stamp(fixture.get('kickoff'))
        if not start or start <= as_of or fixture.get('status_short') not in ('NS', 'TBD') or fixture.get('result'):
            continue
        matrix = ((match.get('multimarket') or {}).get('matrix') or {})
        v7 = matrix.get('v7') or {}
        picks = [p for p in (v7.get('triad') or {}).values() if p]
        source_ids = {p.get('source_market_id') for p in picks}
        fields = ('id', 'spec', 'probability_model', 'quote_status', 'quote_executable', 'odd', 'prematch_stable',
                  'cache_expires_at', 'first_odd', 'quote_observed_at', 'provider_updated_at')
        active.append({'fixture': fixture, 'triad': match.get('triad'),
            'multimarket': {'matrix': {'publication_model': matrix.get('publication_model'),
                'rows': [{k: r[k] for k in fields if k in r} for r in matrix.get('rows', []) if r['id'] in source_ids],
                'v7': {**{k: v7.get(k) for k in ('version', 'status', 'fixture_id', 'kickoff', 'captured_at', 'tier', 'triad')},
                       'markets': {p['market_id']: (v7.get('markets') or {}).get(p['market_id'])
                                   for p in picks if p.get('market_id')}}}}})
    program = """const {matches,asOf}=JSON.parse(require('node:fs').readFileSync(0,'utf8'));
const a=require('./web/lateral-matrix.js');
const result={active_cards:matches.length,full_triads:0,cards_with_estimates:0,
 model_estimates:0,three_empty_slots:0,without_visible_proposal:0};
for(const m of matches){
 const view=a.publishedView(m,Date.parse(asOf));
 const html=view?.triad?a.triadPanel(view,true):'';
 const picks=Object.values(view?.triad||{}).filter(Boolean);
 const estimates=picks.filter(p=>p.signal.quote_state==='model_estimate').length;
 result.full_triads+=picks.length===3;
 result.cards_with_estimates+=estimates>0;
 result.model_estimates+=estimates;
 result.three_empty_slots+=(html.match(/Nessun mercato idoneo/g)||[]).length===3;
 result.without_visible_proposal+=!(picks.length||view?.primary);
}
console.log(JSON.stringify(result));"""
    result = subprocess.run(['node', '-e', program], cwd=Path(__file__).resolve().parents[1],
        input=json.dumps({'matches': active, 'asOf': as_of.isoformat()}), text=True,
        capture_output=True, check=True, timeout=30)
    return json.loads(result.stdout)


def deployed_assets():
    result = {}
    try:
        response = requests.get('https://arabsniperbet.com/gold-dashboard-live', timeout=20)
        result['gold_http_status'] = response.status_code
        match = re.search(r'<script src="(/lateral-matrix\.js[^\"]*)"', response.text)
        if match:
            script = requests.get('https://arabsniperbet.com'+match[1], timeout=20)
            result.update(script_url=match[1], script_http_status=script.status_code,
                matches_verified_source=script.content == (Path(__file__).resolve().parents[1]/'web/lateral-matrix.js').read_bytes())
    except Exception as error:
        result['verification_error'] = type(error).__name__
    return result


def run():
    e = engine()
    api = e.APIClient(os.environ['API_SPORTS_KEY'])
    s3, bucket = storage_client(), os.environ['R2_BUCKET']
    index = previous_manifest(s3, bucket)
    selected = {'data/arab_quote_state.json', 'data/arab_quote_odds_collection.json',
                'data/arab_bet_catalog.json'}
    selected.update(f'data/arab_quote_day{i}.json' for i in range(1, 6))
    history_paths = {path for path in index if path.startswith('data/'+PREFIX)}
    selected.update(history_paths)
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
        report['persistent_price_files'] = len(history_paths)
        report['persistent_price_groups'] = report['persistent_price_observations'] = 0
        report['persistent_price_sources'] = {'http': 0, 'original_ledger': 0, 'other': 0}
        report['persisted_1x2_series'] = {}
        for path in history_paths-{'data/'+INDEX}:
            recorded = read_store(root/path, {})
            for fid, fixture in recorded.get('fixtures', {}).items():
                report['persistent_price_groups'] += len(fixture.get('groups', {}))
                report['persistent_price_observations'] += sum(len(g['observations']) for g in fixture.get('groups', {}).values())
                for key, group in fixture.get('groups', {}).items():
                    observations = group['observations']
                    for point in observations:
                        source = point.get('source') or {}
                        kind = ('http' if source.get('provider') == 'API-Football' and source.get('acquisition_id')
                                else 'original_ledger' if source.get('endpoint') == 'legacy_matrix_ledger' else 'other')
                        report['persistent_price_sources'][kind] += 1
                    if str(fid) in PERSISTENCE_PROOF_IDS and '|result|ft|' in key and observations:
                        report['persisted_1x2_series'].setdefault(str(fid), {})[key] = {
                            'first': deepcopy(group['first']), 'observations': len(observations),
                            'http_observations': sum(bool((p.get('source') or {}).get('acquisition_id')) for p in observations),
                            'bookmaker': group.get('bookmaker'),
                            'last': {k: deepcopy(observations[-1].get(k)) for k in (
                                'observed_at', 'provider_updated_at', 'odds', 'delta_from_first_pct', 'source')}}
        # The above is captured before any local seeding or new API requests.
        # It proves what production writers actually committed to R2.
        journal = QuoteHistory(root/'data')
        journal.seed_matrix(state)
        api.odds_observer = journal
        gold = {}
        for i in range(1, 6):
            try:
                gold[str(i)] = json.loads(s3.get_object(Bucket=bucket, Key=f'arab_quote_day{i}.json')['Body'].read())
            except Exception as error:
                report.setdefault('gold_errors', []).append(type(error).__name__)
        browser_catalog = json.loads(s3.get_object(Bucket=bucket, Key='arab_quote_model_research.json')['Body'].read())
        browser_pages = {}
        report['archive_coverage'] = feed_coverage(days)
        report['gold_coverage'] = feed_coverage(gold.values())
        as_of = datetime.now(timezone.utc)
        report['archive_prematch_by_day'] = [prematch_coverage(day, as_of) for day in days]
        report['gold_prematch_by_day'] = [prematch_coverage(day, as_of) for day in gold.values()]
        report['browser_prematch_by_day'] = []
        for day_number, day in gold.items():
            browser = list(browser_matches(s3, bucket, browser_catalog, day_number, {}))
            report['browser_prematch_by_day'].append({
                'date': day.get('date'), **rendered_browser_coverage(browser, as_of)})
        report['checkpoint_identity'] = hashlib.sha256(json.dumps(index, sort_keys=True).encode()).hexdigest()
        manifest_bytes = json.dumps({'version': 1, 'files': index}, sort_keys=True, separators=(',', ':')).encode()
        manifest_id = hashlib.sha256(manifest_bytes).hexdigest()
        checkpoint = s3.get_object(Bucket=bucket, Key='state/manifests/'+manifest_id+'.json')['Body'].read()
        report['immutable_manifest_sha256'] = manifest_id
        report['immutable_manifest_verified'] = checkpoint == manifest_bytes
        quota, _ = api.get_bounded('status', {}, 1)
        limits = ((quota or {}).get('response') or {}).get('requests') or {}
        used, limit = limits.get('current'), limits.get('limit_day')
        if type(used) is not int or type(limit) is not int or limit-used <= quota_reserve(limit)+150:
            raise RuntimeError('Verified provider quota unavailable or insufficient')
        report['quota_remaining_before'] = limit-used
        schedule, _ = api.get_bounded('fixtures', {'date': TARGET_DATE, 'timezone': 'Europe/Rome'}, 3)
        metas = [e.fixture_meta(row) for row in (schedule or {}).get('response', [])]
        requested = [m for m in metas if named(m)]
        report['api_target_fixtures_found'] = [{k: m.get(k) for k in
            ('fixture_id', 'match', 'kickoff', 'league', 'league_id', 'season', 'date', 'status_short')} for m in requested]
        archived = [m for day in days for m in day.get('matches', [])]
        gold_matches = [m for day in gold.values() for m in day.get('matches', [])]
        def has_prices(match):
            return any(type(r.get('odd')) in (float, int) and r['odd'] > 1
                       for r in ((match.get('multimarket') or {}).get('matrix') or {}).get('rows', []))
        for match in sorted(archived, key=lambda m: (not has_prices(m), str((m.get('fixture') or {}).get('kickoff')))):
            meta = match.get('fixture') or {}
            if named(meta) and not any(str(m['fixture_id']) == str(meta['fixture_id']) for m in requested):
                requested.append(meta)
        for pair in TARGETS:
            if not any(all(p in str(m.get('match', '')).lower() for p in pair) for m in requested):
                report.setdefault('missing_requested_fixtures', []).append(list(pair))
        sample = []
        seen = {str(m['fixture_id']) for m in requested}
        for match in sorted(archived, key=lambda m: (not has_prices(m), str((m.get('fixture') or {}).get('kickoff')))):
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
            row['v6_same_response'] = v6_inventory(base, fid, at)
            enricher.register_fixture(meta)
            recovered = enricher.enrich(base, fid)
            row['collector'] = inventory(recovered, fid, current_time())
            row['collector_v6_same_response'] = v6_inventory(recovered, fid, current_time())
            anchors = {str(day)+':'+fid_+':'+key: deepcopy(g['first'])
                for day, stored in journal.days.items() for fid_, fixture in stored['fixtures'].items()
                for key, g in fixture['groups'].items()}
            second, _ = api.get_bounded('odds', {'fixture': fid}, 12)
            row['second_general'] = inventory(second, fid, current_time())
            row['first_observation_unchanged'] = all(journal.days[day]['fixtures'][fid_]['groups'][key]['first'] == first
                for identity, first in anchors.items() for day, fid_, key in [identity.split(':', 2)])
            if row['requested_case']:
                row['queries'] = []
                for bid in (1, 5, 6, 8, 34):
                    result, _ = api.get_bounded('odds', {'fixture': fid, 'bet': bid}, 8)
                    row['queries'].append({'bet': bid, **inventory(result, fid, current_time())})
                result, _ = api.get_bounded('odds', {'league': meta['league_id'], 'season': meta['season'],
                    'date': meta['date'], 'timezone': 'Europe/Rome'}, 10)
                row['queries'].append({'route': 'league_date', **inventory(result, fid, current_time())})
                row['archive_groups'] = {k: {'count': len(points), 'first': points[0], 'last': points[-1]}
                    for k, points in state.get('matrix_quotes', {}).get(str(fid), {}).get('groups', {}).items()
                    if points and ('|result|ft|' in k or '|btts|ht|' in k)}
            for key, matches in [('archive', archived), ('gold', gold_matches)]:
                item = next((m for m in matches if str((m.get('fixture') or {}).get('fixture_id')) == str(fid)), {})
                row[key] = {'present': bool(item), '1x2': prices(((item.get('multimarket') or {}).get('matrix') or {}).get('rows', [])),
                            'decision_model': ((item.get('multimarket') or {}).get('matrix') or {}).get('publication_model')}
                if key == 'gold' and item:
                    row['rendered_gold'] = rendered_gold(item)
                    day_number = next(i for i, day in gold.items() if any(
                        str((m.get('fixture') or {}).get('fixture_id')) == str(fid) for m in day.get('matches', [])))
                    browser = browser_match(s3, bucket, browser_catalog, day_number, fid, browser_pages)
                    row['browser_projection_present'] = browser is not None
                    row['browser_1x2_matches_gold'] = bool(browser is not None and
                        prices(((browser.get('multimarket') or {}).get('matrix') or {}).get('rows', [])) == row['gold']['1x2'])
            partition = journal.index['fixtures'].get(str(fid))
            recorded = journal.days.get(partition, {}).get('fixtures', {}).get(str(fid), {})
            row['actual_temporal_series'] = {key: {'observations': len(g['observations']), 'first': g['first'],
                'last_observed_at': g['observations'][-1]['observed_at'],
                'last_delta_from_first_pct': g['observations'][-1]['delta_from_first_pct']}
                for key, g in recorded.get('groups', {}).items() if '|result|ft|' in key}
            row['http_calls'] = api.request_count-before_calls
            report['cases'].append(row)
        report['targeted_stats'] = enricher.stats
        report['http_calls'] = api.request_count
        journal.flush()
        report['local_journal_observations'] = journal.observations
        report['new_http_observations'] = sum(
            str(p.get('source', {}).get('acquisition_id') or '').startswith(api.odds_acquisition_session+':')
            for day in journal.days.values() for fixture in day['fixtures'].values()
            for group in fixture['groups'].values() for p in group['observations'])
        report['cloudflare'] = deployed_assets()
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
