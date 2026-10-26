"""Prospective Gold policy experiment, using saved inputs/results only.

This module cannot promote a shadow selection or call a football provider.
Four goal rows per observation are frozen independently of later settlement.
"""
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import json
from math import log, sqrt
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import read_store, write_store
from gold_competitions import SCOPE_ID, output_allowed, norm
from lateral_matrix import MODEL_ID, settle_spec
from matrix_quotes import expire
from multimarket_model import stamp
from opportunity_policy import POLICY_ID, evaluate, market_tier, numeric, ranking_key

ROME = ZoneInfo('Europe/Rome')
STUDY_ID = 'gold-prospective-20261026-v1'
BASELINE, START, END = '2026-10-26', '2026-10-27', '2026-10-29'
STRATEGIES = ('published', 'shadow_reduced_cushion', 'shadow_sensitivity_minus3', 'without_pt_priority')
ROW_FIELDS = ('id', 'label', 'spec', 'family', 'period', 'quality', 'statistical_pct',
    'low_pct', 'high_pct', 'historical_pct', 'fair_odd', 'odd', 'first_odd', 'quote_status',
    'bookmaker', 'bookmaker_id', 'provider_updated_at', 'market_fair_pct',
    'selected_margin_pct', 'selected_margin_delta_pp', 'margin_pct', 'margin_delta_pp',
    'oci', 'oci_previous', 'paired_books', 'agreeing_books', 'selected_movement',
    'movement_confirmed', 'fair_direction_coherent', 'amplitude_pct', 'speed_pp_per_hour',
    'persistence', 'ev_pct', 'ev_low_pct', 'ev_high_pct', 'opportunity')


def key_for(row):
    s = row['spec']
    return 'gg_ht' if s['kind'] == 'btts' else ('over15_ht' if s['period'] == 'ht'
           else 'over25_ft' if s['line'] == 2.5 else 'over35_ft')


def policy_choices(rows, context):
    """Pre-register modest relaxations; all hard constraints remain unchanged."""
    choices = {}
    for strategy in STRATEGIES:
        candidates = []
        for original in rows:
            row = deepcopy(original)
            d = row['opportunity']
            if strategy == 'without_pt_priority':
                d = evaluate(row)
                # Never erase an independently detected probability incoherence.
                if 'coherence' in row['opportunity'].get('reasons', []):
                    d['eligible'] = False; d['reasons'].append('coherence')
                row['opportunity'] = d
            if strategy.startswith('shadow_'):
                hard = [r for r in d['reasons'] if r not in ('edge', 'sensitivity')]
                central, lower = row.get('ev_pct'), row.get('ev_low_pct')
                if strategy == 'shadow_reduced_cushion':
                    central_floor, lower_floor = max(2., d['min_ev_pct']/2), 0.
                    passes = numeric(lower) and lower > lower_floor
                else:
                    central_floor, lower_floor = d['min_ev_pct'], -3.
                    passes = numeric(lower) and lower >= lower_floor
                d.update(eligible=not hard and numeric(central) and central >= central_floor and passes,
                         min_ev_pct=central_floor, min_ev_low_pct=lower_floor)
                if d['eligible']:
                    d['playability_score'] = row['statistical_pct'] * max(0., lower-lower_floor)/(row['odd']-1)
            if d['eligible']: candidates.append(row)
        best = min(candidates, key=ranking_key, default=None)
        choices[strategy] = best['id'] if best else None
    return choices


def capture(store, match, observed_at):
    """Observe now, never backdate to the older statistical source timestamp."""
    meta, a = match.get('fixture') or {}, match.get('multimarket') or {}
    now, kickoff = stamp(observed_at), stamp(meta.get('kickoff'))
    if (not now or not kickoff or now >= kickoff or meta.get('result')
            or meta.get('status_short') not in ('NS', 'TBD') or not output_allowed(match)
            or a.get('model_id') != MODEL_ID): return False
    date = kickoff.astimezone(ROME).date().isoformat()
    if not BASELINE <= date <= END: return False
    # Freeze the decisions actually written by the scan. Do not reconstruct a
    # different "published" proposal retrospectively inside an audit.
    matrix = a.get('matrix') or {}
    if (matrix.get('opportunity_policy') or {}).get('id') != POLICY_ID: return False
    decision_at = stamp(matrix['opportunity_policy'].get('evaluated_at'))
    if not decision_at or decision_at > now or decision_at >= kickoff: return False
    rows = [{k: deepcopy(r.get(k)) for k in ROW_FIELDS} for r in matrix.get('rows', []) if market_tier(r) is not None]
    if not rows: return False
    variants = {r['market']: deepcopy(r.get('probabilities') or {}) for r in a.get('markets', [])}
    for row in rows: row['model_variants'] = variants.get(key_for(row), {})
    snapshot = {'observed_at': observed_at, 'decision_at': matrix['opportunity_policy']['evaluated_at'],
        'statistics_captured_at': a.get('captured_at'),
        'input_sha256': a.get('input_sha256'), 'model_id': MODEL_ID, 'policy_id': POLICY_ID,
        'scope_id': SCOPE_ID, 'kickoff': meta['kickoff'], 'rows': rows,
        'goal_context': deepcopy(matrix.get('goal_context') or {}),
        'profiles': deepcopy(a.get('profiles') or {}), 'data_quality': deepcopy(a.get('data_quality') or {})}
    snapshot['choices'] = policy_choices(rows, snapshot['goal_context'])
    if snapshot['choices']['published'] != matrix['opportunity_policy'].get('best_market_id'):
        raise ValueError('Gold policy audit proposal mismatch')
    # Decisions and provenance are fixed before any result is attached.
    digest = sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False).encode()).hexdigest()
    fid = str(meta['fixture_id'])
    fixture = store.setdefault('fixtures', {}).setdefault(fid, {'fixture': deepcopy(meta), 'date': date, 'snapshots': []})
    if any(s['observed_at'] == observed_at for s in fixture['snapshots']): return False
    snapshot['sha256'] = digest
    fixture['snapshots'].append(snapshot)
    fixture['snapshots'].sort(key=lambda s: stamp(s['observed_at']))
    return True


def valid_result(result):
    if not isinstance(result, dict) or result.get('status') not in ('FT', 'AET', 'PEN'): return False
    # Missing scores stay pending. Reject malformed/negative/non-integral goals.
    for key in ('home_ft', 'away_ft', 'home_ht', 'away_ht'):
        value = result.get(key)
        if value is not None and (not numeric(value) or value < 0 or value != int(value)): return False
    for side in ('home', 'away'):
        ht, ft = result.get(side+'_ht'), result.get(side+'_ft')
        if ht is not None and ft is not None and ht > ft: return False
    return True


def attach_result(store, fid, result, observed_at, source):
    entry = store.get('fixtures', {}).get(str(fid))
    if not entry or not valid_result(result): return False
    now = stamp(observed_at)
    if not now or any(stamp(s['kickoff']) > now for s in entry['snapshots']): return False
    old = (entry.get('settlement') or {}).get('result') or {}
    # Do not erase a known score with a later incomplete observation.
    merged = {**old, **{k: v for k, v in result.items() if v is not None}}
    if not valid_result(merged): return False
    if old != merged:
        entry.setdefault('result_history', []).append({'observed_at': observed_at, 'source': source, 'result': deepcopy(merged)})
        entry['settlement'] = {'observed_at': observed_at, 'source': source, 'result': deepcopy(merged)}
    return True


def settle_saved_archives(store, root, observed_at):
    """Consume normal Auditor results; no new provider requests or price lookups."""
    from audit_ledger import entries
    for date in sorted({f['date'] for f in store.get('fixtures', {}).values()}):
        path = root/'auditarchive'/f'prematch_{date}_results.json'
        if not path.exists(): continue
        # Hash pointer/inline file; unchanged archives need no repeated parsing.
        digest = sha256(path.read_bytes()).hexdigest()
        if store.setdefault('result_sources', {}).get(str(path.name)) == digest: continue
        for collection, key, record in entries(path):
            if not collection or not key: continue
            fid = str((record.get('fixture') or {}).get('fixture_id'))
            result = (record.get('settlement') or {}).get('result')
            if fid in store.get('fixtures', {}): attach_result(store, fid, result, observed_at, path.name)
        store['result_sources'][path.name] = digest


def outcome(row, result):
    if not valid_result(result): return None
    value = settle_spec(row['spec'], result)
    return None if value is None else bool(value[0])


def metrics(observations):
    """One fixture per strategy/view; missing outcomes are not losses."""
    done = [o for o in observations if o['outcome'] is not None]
    priced = [o for o in done if numeric(o.get('odd')) and o['odd'] > 1]
    model = [o for o in done if numeric(o.get('p')) and 0 <= o['p'] <= 1]
    book = [o for o in model if numeric(o.get('book_p')) and 0 <= o['book_p'] <= 1]
    n, wins = len(done), sum(o['outcome'] for o in done)
    interval = None
    if n:
        z, rate = 1.96, wins/n
        den = 1+z*z/n; center = (rate+z*z/(2*n))/den
        half = z*sqrt(rate*(1-rate)/n+z*z/(4*n*n))/den
        interval = [round(100*(center-half), 3), round(100*(center+half), 3)]
    returns = [(o['odd'] if o['outcome'] else 0)-1 for o in priced]
    clv = [100*(o['odd']/o['closing_odd']-1) for o in observations if numeric(o.get('closing_odd')) and numeric(o.get('odd'))]
    return {'observations': len(observations), 'distinct_fixtures': len({o['fixture_id'] for o in observations}),
        'settled': n, 'pending': len(observations)-n, 'wins': wins, 'losses': n-wins,
        'hit_rate_pct': round(100*wins/n, 3) if n else None, 'wilson_95_pct': interval,
        'priced_settled': len(priced), 'paper_profit_units': round(sum(returns), 4),
        'paper_roi_pct': round(100*sum(returns)/len(priced), 4) if priced else None,
        'brier': round(sum((o['p']-o['outcome'])**2 for o in model)/len(model), 6) if model else None,
        'log_loss': round(-sum(log(max(1e-12, min(1-1e-12, o['p'] if o['outcome'] else 1-o['p']))) for o in model)/len(model), 6) if model else None,
        'matched_book_n': len(book),
        'matched_model_brier': round(sum((o['p']-o['outcome'])**2 for o in book)/len(book), 6) if book else None,
        'matched_book_brier': round(sum((o['book_p']-o['outcome'])**2 for o in book)/len(book), 6) if book else None,
        'same_book_closing_pairs': len(clv), 'observed_clv_pct': round(sum(clv)/len(clv), 3) if clv else None}


def closing_price(fixture, row):
    """Last observed prematch price <=3 h from kickoff, exact same operator."""
    for s in reversed(fixture['snapshots']):
        minutes = (stamp(s['kickoff'])-stamp(s['observed_at'])).total_seconds()/60
        if not 0 < minutes <= 180: continue
        for candidate in s['rows']:
            updated = stamp(candidate.get('provider_updated_at'))
            fresh = updated and 0 <= (stamp(s['observed_at'])-updated).total_seconds() <= 3600
            if (candidate['id'] == row['id'] and row.get('bookmaker_id') is not None
                    and candidate.get('bookmaker_id') == row['bookmaker_id']
                    and candidate.get('quote_status') == 'recent' and fresh
                    and numeric(candidate.get('odd')) and candidate['odd'] > 1):
                return candidate['odd']
    return None


def observation(fid, fixture, snapshot, row):
    result = (fixture.get('settlement') or {}).get('result')
    p, q, margin = row.get('statistical_pct'), row.get('odd'), row.get('selected_margin_pct')
    book = 100/q/(1+margin/100) if numeric(q) and q > 1 and numeric(margin) and 0 <= margin <= 30 else None
    return {'fixture_id': fid, 'market': key_for(row), 'outcome': outcome(row, result),
        'p': p/100 if numeric(p) else None, 'book_p': book/100 if numeric(book) else None,
        'odd': row.get('odd') if row.get('quote_status') == 'recent' else None,
        'closing_odd': closing_price(fixture, row), 'snapshot': snapshot['observed_at']}


def group_dimensions(fixture, snapshot, row):
    context, d = snapshot['goal_context'], row['opportunity']
    delta, odd, p, oci = row.get('selected_margin_delta_pp'), row.get('odd'), row.get('statistical_pct'), row.get('oci')
    return {'market': key_for(row), 'signal': d.get('signal'), 'quality': row.get('quality'),
        'competition': fixture['fixture'].get('country', '')+' / '+fixture['fixture'].get('league', ''),
        'margin_band': d.get('margin_band'), 'margin_trend': 'unknown' if not numeric(delta) else 'compression' if delta <= -1 else 'expansion' if delta >= 1 else 'stable',
        'odds_band': 'missing' if not numeric(odd) else '<1.70' if odd < 1.7 else '1.70–2.20' if odd <= 2.2 else '>2.20',
        'probability_band': 'missing' if not numeric(p) else str(min(90, int(p//10)*10)),
        'oci_band': 'missing' if not numeric(oci) else 'positive_40_plus' if oci >= 40 else 'negative_40_plus' if oci <= -40 else 'neutral',
        'flow_confirmed': str(row.get('movement_confirmed') is True),
        'regime': (context.get('imbalance') or {}).get('regime', 'unconfirmed'),
        'drought_role': context.get('drought_role') or 'none',
        'pt_priority': str(context.get('pt_priority_active') is True)}


def build_report(store, observed_at):
    strategies, groups, rejection, variants = defaultdict(list), defaultdict(list), defaultdict(list), defaultdict(list)
    comparisons = defaultdict(lambda: defaultdict(list))
    for fid, fixture in store.get('fixtures', {}).items():
        for snapshot in fixture['snapshots']:
            raw = {k: v for k, v in snapshot.items() if k != 'sha256'}
            if sha256(json.dumps(raw, sort_keys=True, allow_nan=False).encode()).hexdigest() != snapshot['sha256']:
                raise ValueError('Frozen Gold policy snapshot mismatch')
        snapshots = [s for s in fixture['snapshots'] if stamp(s['observed_at']) < stamp(s['kickoff'])]
        if not snapshots: continue
        # Do not pool the baseline day into the preregistered next-three-day study.
        cohort = 'baseline_26' if fixture['date'] == BASELINE else 'study_27_29'
        for view, snapshot in (('first', snapshots[0]), ('latest', snapshots[-1])):
            indexed = {r['id']: r for r in snapshot['rows']}
            actual = snapshot['choices']['published']
            for strategy, selected in snapshot['choices'].items():
                if selected not in indexed: continue
                row = indexed[selected]; obs = observation(fid, fixture, snapshot, row)
                strategies[(cohort, view, strategy)].append(obs)
                comparisons[(cohort, view, strategy)]['extra' if actual is None else 'same' if actual == selected else 'changed'].append(obs)
                if strategy == 'published':
                    for dimension, value in group_dimensions(fixture, snapshot, row).items(): groups[(cohort, view, dimension, str(value))].append(obs)
            for row in snapshot['rows']:
                obs = observation(fid, fixture, snapshot, row)
                groups[(cohort, view, 'all_observed_market', key_for(row))].append(obs)
                for dimension in ('oci_band', 'flow_confirmed', 'margin_band', 'margin_trend', 'probability_band'):
                    value = group_dimensions(fixture, snapshot, row)[dimension]
                    groups[(cohort, view, 'all_'+key_for(row)+'_'+dimension, str(value))].append(obs)
                for reason in row['opportunity'].get('reasons', []): rejection[(cohort, view, key_for(row), reason)].append(obs)
                for variant, p in row.get('model_variants', {}).items():
                    if numeric(p) and 0 <= p <= 1: variants[(cohort, view, key_for(row), variant)].append({**obs, 'p': p})
    strategy_rows = []
    for cohort in ('baseline_26', 'study_27_29'):
        for view in ('first', 'latest'):
            for strategy in STRATEGIES:
                key = (cohort, view, strategy)
                strategy_rows.append({'cohort': cohort, 'snapshot': view, 'strategy': strategy,
                    **metrics(strategies[key]), 'relative_to_published': {k: metrics(comparisons[key][k]) for k in ('extra', 'same', 'changed')}})
    return {'study_id': STUDY_ID, 'generated_at': observed_at, 'baseline_date': BASELINE,
        'study_start': START, 'study_end': END, 'model_id': MODEL_ID, 'policy_id': POLICY_ID,
        'scope_id': SCOPE_ID, 'api_calls': 0, 'production_policy_changed': False,
        'frozen_fixtures': len(store.get('fixtures', {})),
        'snapshots': sum(len(f['snapshots']) for f in store.get('fixtures', {}).values()),
        'coverage': store.get('coverage', {}), 'strategies': strategy_rows,
        'groups': [dict(zip(('cohort', 'snapshot', 'dimension', 'value'), k), **metrics(v)) for k, v in sorted(groups.items())],
        'rejections': [dict(zip(('cohort', 'snapshot', 'market', 'reason'), k), **metrics(v)) for k, v in sorted(rejection.items())],
        'model_variants': [dict(zip(('cohort', 'snapshot', 'market', 'variant'), k), **metrics(v)) for k, v in sorted(variants.items())],
        'notes': ['Shadow strategies are frozen research, never Gold recommendations.',
            'One fixture per proposal strategy/view. First/latest are separate, not independent trials.',
            'Rows of different goal markets are correlated. Missing outcomes remain pending.',
            'Observed closing value uses the last saved same-book quote within three hours, not certified market close.',
            'ROI assumes one hypothetical unit per proposal, no execution, fees or staking optimization.',
            'Three days are exploratory evidence; no automatic threshold promotion or claim of calibrated certainty.']}


def requested_case(match, observed_at):
    """Bounded diagnostics for the user's named case, without exporting Gold rows."""
    meta = match.get('fixture') or {}
    names = [norm(meta.get(k)) for k in ('home', 'away')]
    if not any('odense' in n or n == 'ob' for n in names) and 'odense' not in norm(meta.get('match')): return None
    result = {'match': meta.get('match'), 'league': meta.get('league'), 'country': meta.get('country'),
              'scope_allowed': output_allowed(match), 'observed_at': observed_at, 'markets': []}
    a = deepcopy(match.get('multimarket') or {})
    if a.get('model_id') != MODEL_ID:
        result['status'] = 'current_matrix_unavailable'; return result
    expire(a, meta, observed_at)
    for row in a.get('matrix', {}).get('rows', []):
        if market_tier(row) is None: continue
        d = row.get('opportunity') or {}
        result['markets'].append({'market': key_for(row), 'eligible': d.get('eligible'), 'reasons': d.get('reasons'),
            'probability_min_met': numeric(row.get('statistical_pct')) and row['statistical_pct'] >= 35,
            'price_range_met': numeric(row.get('odd')) and 1.4 <= row['odd'] <= 3.5,
            'central_ev_positive': numeric(row.get('ev_pct')) and row['ev_pct'] > 0,
            'prudent_ev_positive': numeric(row.get('ev_low_pct')) and row['ev_low_pct'] > 0,
            'flow': d.get('signal')})
    return result


def main(root=Path('.'), observed_at=None):
    root = Path(root); now = observed_at or datetime.now(ROME).isoformat()
    path = root/'data/arab_quote_gold_policy_audit.json'
    store = read_store(path, {'study_id': STUDY_ID, 'policy_id': POLICY_ID, 'fixtures': {}})
    if store.get('study_id') != STUDY_ID or store.get('policy_id') != POLICY_ID: raise ValueError('Audit study identity mismatch')
    settle_saved_archives(store, root, now)
    coverage, seen, cases = defaultdict(Counter), set(), []
    for day_path in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        day = read_store(day_path, {})
        for match in day.get('matches', []):
            meta = match.get('fixture') or {}; fid = str(meta.get('fixture_id'))
            if fid in seen: continue
            seen.add(fid)
            kickoff = stamp(meta.get('kickoff'))
            if not kickoff: continue
            date = kickoff.astimezone(ROME).date().isoformat()
            if not BASELINE <= date <= END: continue
            case = requested_case(match, now)
            if case: cases.append(case)
            if output_allowed(match):
                coverage[date]['allowed_fixtures'] += 1
                if kickoff > stamp(now) and meta.get('status_short') in ('NS', 'TBD'):
                    coverage[date]['upcoming_fixtures'] += 1
                    if (match.get('multimarket') or {}).get('model_id') != MODEL_ID: coverage[date]['pending_current_engine'] += 1
                if capture(store, match, now): coverage[date]['snapshots_added'] += 1
            attach_result(store, fid, meta.get('result'), now, day_path.name)
    store['coverage'] = {k: dict(v) for k, v in sorted(coverage.items())}
    store['updated_at'] = now
    report = build_report(store, now)
    write_store(path, store)
    write_store(root/'data/arab_quote_gold_policy_report.json', report)
    # Aggregate results are useful in Actions; individual forecasts remain private.
    summary = {k: report[k] for k in ('study_id', 'generated_at', 'baseline_date', 'study_start', 'study_end',
        'frozen_fixtures', 'snapshots', 'coverage', 'api_calls', 'production_policy_changed', 'strategies')}
    print('GOLD_POLICY_AUDIT_SUMMARY='+json.dumps(summary, ensure_ascii=False, separators=(',', ':')), flush=True)
    print('REQUESTED_CASE_DIAGNOSTICS='+json.dumps(cases, ensure_ascii=False, separators=(',', ':')), flush=True)
    return report


if __name__ == '__main__': main()
