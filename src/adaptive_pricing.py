"""Read the overnight weights once. All following operations are local arithmetic."""
from collections import defaultdict
from adaptive_calibration import (VERSION, TRAPS, MIN_N, bound, cohort, load_weights,
                                  closing_prices, current_state, market_of, number, usable_entry)
from multimarket_model import stamp

BASE_FIELDS = ('statistical_pct', 'low_pct', 'high_pct', 'win_weight', 'loss_weight',
               'fair_odd', 'sensitivity_weights')


def threshold(row):
    data = row.get('calibration') or {}
    delta = data.get('threshold_delta_pp')
    if data.get('version') == VERSION and number(data.get('n')) and data['n'] >= MIN_N and number(delta):
        return bound(delta, -.5, 2.5)
    return 0.


def gate_passes(row, base_ev, base_edge=0.):
    delta = threshold(row)
    if not delta:
        return True
    ev, edge = row.get('ev_pct'), row.get('edge_pp')
    return (number(ev) and ev >= max(0., base_ev+delta) and (delta >= 0 or ev > 0)
            and number(edge) and edge >= base_edge+delta)


def blend(row, entry, bonus):
    base = (row.get('calibration_base') or row)['win_weight']
    weights = entry.get('weights') or {}
    empirical = (row.get('empirical') or {}).get('win_stake_pct')
    fair = row.get('market_fair_pct')
    left, p = .03, base
    for name, pct in [('empirical', empirical), ('fair', fair)]:
        weight = weights.get(name)
        if number(pct) and 0 <= pct <= 100 and number(weight):
            w = bound(weight, 0., left)
            p += w*(pct/100-base)
            left -= w
    return bound(p+bonus, .01, .99)


def set_probability(row, p):
    base = row.get('calibration_base') or {k: row[k] for k in BASE_FIELDS if k in row}
    row['calibration_base'] = base
    shift = p-base['win_weight']
    row.update(statistical_pct=round(p*100, 4), win_weight=p, loss_weight=1-p, fair_odd=1/p)
    row['sensitivity_weights'] = [[q, 1-q] for q in
        [bound(w+shift, .01, .99) for w, _ in base.get('sensitivity_weights', [[base['win_weight'], 1-base['win_weight']]])]]
    scenarios = [p, *(w for w, _ in row['sensitivity_weights'])]
    row['low_pct'] = round(100*min(scenarios), 4)
    row['high_pct'] = round(100*max(scenarios), 4)


def reprice(row):
    from quote_cache import recalculate
    from matrix_quotes import value_profile
    if row.get('quote_status') not in ('recent', 'cached_stable') or not recalculate(row):
        return
    fair = row.get('market_fair_pct')
    row['edge_pp'] = 100*row['win_weight']-fair if number(fair) else None
    oci = row.get('oci')
    strong = number(oci) and abs(oci) >= 40 and row.get('movement_confirmed') and row.get('fair_direction_coherent')
    behavior, stable, category, reason = value_profile(row, row['ev_pct']/100, row['ev_low_pct']/100,
                                                      strong, oci, row.get('quote_source') == 'synthetic_combo')
    if category == 'mature' and not gate_passes(row, 0.):
        category, reason = 'value_watch', 'Soglia di valore della coorte non raggiunta'
    row.update(market_behavior=behavior, stable_value=stable, category=category, category_reason=reason)


def apply_matrix(assessment, meta, as_of, state=None):
    matrix = assessment.get('matrix') or {}
    state = load_weights() if state is None else state
    if not state.get('cohorts') and not matrix.get('calibration'):
        return
    when, start = stamp(as_of), stamp(meta.get('kickoff'))
    if not when or not start or when >= start or meta.get('result'):
        return
    current = current_state(state, as_of)
    reference = matrix.get('calibration_reference')
    rows = [*matrix.get('rows', []), *([reference] if reference else [])]
    groups, partners, dirty = defaultdict(list), defaultdict(list), []
    params = {}
    for row in rows:
        spec = row.get('spec') or {}
        market = market_of(spec)
        if not market and spec.get('side') in ('under', 'no'):
            positive = {**spec, 'side': 'over' if spec['side'] == 'under' else 'yes'}
            other = market_of(positive)
            if other:
                partners[other].append(row)
        if row.pop('calibration', None) or row.get('calibration_base'):
            base = row.pop('calibration_base', None)
            if base:
                row.update(base)
            dirty.append(row)
        if not market or not number(row.get('win_weight')):
            continue
        name = (market, spec['side']) if market == '1x2_ft' else market
        groups[name].append(row)
        key = cohort(market, row.get('odd'), meta.get('competition_tier'))
        data = usable_entry(state, key) if current and key else {}
        if data and row.get('quote_status') in ('recent', 'cached_stable'):
            params[id(row)] = data
            row['calibration'] = {'version': VERSION, 'cohort': key, 'as_of': state['as_of'], 'n': data['n'],
                                  'threshold_delta_pp': data['threshold_delta_pp']}
            dirty.append(row)
    values, active = {}, set()
    pattern = assessment.get('favorite_trap_pattern') or {}
    for name, group in groups.items():
        src = min(group, key=lambda r: (id(r) not in params, r['spec']['kind'] == 'asian_total', str(r.get('id'))))
        entry = params.get(id(src))
        values[name] = src['win_weight']
        # A separately priced equivalent line with a neutral prior anchors this
        # shared event. Its odds still use its own cohort for the EV/edge gate.
        has_neutral_alias = any(r.get('quote_status') in ('recent', 'cached_stable') and id(r) not in params for r in group)
        if entry and not has_neutral_alias:
            bonus = 0.
            pattern_data = (entry.get('by_market_pattern') or {}).get(pattern.get('classification')) or {}
            multiplier = pattern_data.get('multiplier', 1.)
            baseline = (assessment.get('favorite_trap_base') or {}).get(name, 0.)
            if (pattern.get('classification') in TRAPS and pattern_data.get('n', 0) >= MIN_N
                    and number(multiplier) and number(baseline)):
                bonus = bound(baseline*bound(multiplier, .5, 1.25), 0., .036)
            values[name] = blend(src, entry, bonus)
            active.add(name)
    # Neutral neighbours anchor the nested binary events, including any-goal HT.
    for chain in [('gg_ht', 'over15_ht', 'over05_ht'), ('over35_ft', 'over25_ft')]:
        chain = [k for k in chain if k in values]
        for i, k in enumerate(chain):
            if k in active:
                lo = values[chain[i-1]] if i else .01
                hi = min((values[n] for n in chain[i+1:] if n not in active), default=.99)
                values[k] = bound(values[k], lo, hi)
    result = [('1x2_ft', side) for side in ('1', 'x', '2')]
    adjustable = [k for k in result if k in active]
    if adjustable and all(k in values for k in result):
        remaining = 1-sum(values[k] for k in result if k not in active)
        # Project the active components onto the bounded probability simplex.
        lo, hi = -1., 1.
        for _ in range(42):
            mid = (lo+hi)/2
            if sum(bound(values[k]+mid, .01, .99) for k in adjustable) > remaining:
                hi = mid
            else:
                lo = mid
        for k in adjustable:
            values[k] = bound(values[k]+(lo+hi)/2, .01, .99)
    for k in active:
        p = values[k]
        for row, new_p in [*((r, p) for r in groups[k]), *((r, 1-p) for r in partners[k])]:
            if number(row.get('win_weight')) and abs(new_p-row['win_weight']) > 1e-9:
                set_probability(row, new_p)
                dirty.append(row)
    for row in {id(r): r for r in dirty}.values():
        reprice(row)
    if params:
        matrix['calibration'] = {'version': VERSION, 'as_of': state['as_of'], 'active_events': len(active)}
    else:
        matrix.pop('calibration', None)


def ht_reference(assessment, offers, as_of, kickoff):
    """O0.5 HT belongs to the calibration ladder, outside the 58 dashboard rows."""
    from lateral_matrix import specification, empirical, quality
    from math import expm1
    from multimarket_model import fair
    from quote_sources import fresh
    rates = assessment.get('rates') or {}
    if not all(number(rates.get(k)) for k in ('home_ht', 'away_ht')):
        return None
    spec = specification('total', 'ht', 'over', .5)
    def chance(r):
        return -expm1(-(r['home_ht'] + r['away_ht']))
    p = chance(rates)
    scenarios = [p, *(chance(r) for r in assessment.get('sensitivity_rates', []) if 'home_ht' in r and 'away_ht' in r)]
    row = {'spec': spec, 'family': 'totals', 'period': 'ht', 'quality': quality(assessment, 'ht'),
           'statistical_pct': 100*p, 'win_weight': p, 'loss_weight': 1-p, 'fair_odd': 1/p if p else None,
           'sensitivity_weights': [[v, 1-v] for v in scenarios], 'low_pct': 100*min(scenarios),
           'high_pct': 100*max(scenarios), 'empirical': empirical(assessment, spec), 'quote_status': 'missing'}
    choices = [o for o in offers if o.get('complete') and set(o['odds']) == {'over', 'under'} and
               0 <= fair(o['odds'])[1] <= 9 and fresh(o.get('provider_updated_at'), as_of, kickoff)]
    if choices:
        closes = closing_prices(choices, 'over', as_of, kickoff)
        if closes:
            row['calibration_closes'] = closes
        best = max(choices, key=lambda o: (o['odds']['over'], o['provider_updated_at'], o['bookmaker_id']))
        fair_p, margin = fair(best['odds'])
        row.update(quote_status='recent', odd=best['odds']['over'], quote_source=best['quote_source'],
                   quote_executable=True, bookmaker_id=best['bookmaker_id'], bookmaker=best['bookmaker'],
                   provider_updated_at=best['provider_updated_at'], quote_observed_at=as_of,
                   selected_margin_pct=margin, market_fair_pct=100*fair_p['over'])
    return row


def freeze_pattern(ns, output, assessment, as_of):
    """Feed the existing favorite-trap detector one complete, paired 1X2 book."""
    from multimarket_model import fair
    rows = [r for r in assessment['matrix']['rows'] if market_of(r.get('spec', {})) == '1x2_ft'
            and r.get('quote_status') == 'recent' and r.get('opening_market_odds') and r.get('selected_market_odds')]
    if not rows:
        return
    ref = min(rows, key=lambda r: (-int(r.get('paired_books') or 0), str(r['bookmaker_id']), r['id']))
    before, current = ref['opening_market_odds'], ref['selected_market_odds']
    if set(before) != {'1','x','2'} or set(current) != set(before):
        return
    book = {}
    for phase, prices in [('open', before), ('current', current)]:
        book[phase] = {'odds': prices, 'math': {'fair_probabilities': fair(prices)[0]}}
    book['selections'] = {}
    for side in current:
        row = next((r for r in rows if r['bookmaker_id'] == ref['bookmaker_id'] and r['spec']['side'] == side), {})
        book['selections'][side] = {
            'delta_fair_pp': 100*(book['current']['math']['fair_probabilities'][side]-book['open']['math']['fair_probabilities'][side]),
            'oci_as': {'value': row.get('oci')},
            'bookmaker_consensus': {'ratio': (row.get('agreeing_books') or 0)/row['paired_books'] if row.get('paired_books') else 0}}
    pattern = ns['analyze_favorite_market_pattern']({**output, 'book': {'markets': {'1x2_ft': book}}})
    assessment['favorite_trap_pattern'] = {**pattern, 'captured_at': as_of, 'bookmaker_id': ref['bookmaker_id']}
    assessment['favorite_trap_base'] = ns['_apply_favorite_trap_adjustment']({}, pattern)
