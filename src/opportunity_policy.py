"""One versioned proposal policy; never a fixture or probability-matrix filter.

Thresholds are explicit product constraints, not calibrated win guarantees.
The browser consumes these decisions/ranks rather than reimplementing scoring.
"""
from collections import Counter
from math import isfinite, sqrt
from statistics import mean, median

POLICY_ID = 'gold-goals-v4'
SUPPORTED_POLICY_IDS = ('gold-goals-v2', 'gold-goals-v3', POLICY_ID)
ODDS_MIN, ODDS_MAX = 1.40, 3.50
GOALS = {('total', 'ht', 'over', 1.5): 'over15_ht', ('btts', 'ht', 'yes', None): 'gg_ht',
         ('total', 'ft', 'over', 2.5): 'over25_ft', ('total', 'ft', 'over', 3.5): 'over35_ft'}
ODDS_MAX_BY_MARKET = {'over15_ht': 3.50, 'over25_ft': 3.50, 'gg_ht': 5.50, 'over35_ft': 5.50}
LABELS = {'convergence': 'Convergenza di Trend',
          'contrarian': 'Contrarian sano',
          'unconfirmed': 'Valore statistico · flusso non confermato',
          'excluded': 'Fuori proposta'}
REASONS = {
    'market': 'Mercato escluso dal box: resta nella matrice',
    'probability': 'Probabilità inferiore alla soglia del mercato',
    'price': 'Quota fuori dall’intervallo consentito per il mercato',
    'stale': 'Quota recente non disponibile',
    'history': 'Storico statistico insufficiente',
    'adapter': 'Associazione della quota da aggiornare',
    'margin': 'Aggio anomalo: prezzo da verificare',
    'edge': 'Vantaggio insufficiente rispetto all’aggio',
    'sensitivity': 'Vantaggio insufficiente nello scenario prudente',
    'coherence': 'Probabilità dei mercati gol incoerenti',
}


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def goal_market(row):
    spec = row.get('spec') or {}
    if spec.get('extra') is not None: return None
    # Only identical half-line settlements are equivalent. Quarter/integer
    # Asian lines keep their existing reference role, not new proposal rights.
    kind = 'total' if spec.get('kind') == 'asian_total' else spec.get('kind')
    return GOALS.get((kind, spec.get('period'), spec.get('side'), spec.get('line')))


def odds_max(row):
    return ODDS_MAX_BY_MARKET.get(goal_market(row), ODDS_MAX)


def market_tier(row):
    return 0 if goal_market(row) is not None else None


def confirmed_flow(row):
    oci = row.get('oci')
    pairs, agreeing = row.get('paired_books', 0), row.get('agreeing_books', 0)
    selected = row.get('selected_movement') or {}
    return bool(numeric(oci) and abs(oci) >= 40 and
        row.get('movement_confirmed') is True and row.get('fair_direction_coherent') is True and
        numeric(pairs) and numeric(agreeing) and pairs >= 2 and agreeing >= 2 and agreeing > pairs/2 and
        numeric(selected.get('delta_pct')) and selected['delta_pct'] * oci < 0 and
        numeric(selected.get('fair_delta_pp')) and selected['fair_delta_pp'] * oci > 0 and
        numeric(selected.get('persistence')) and selected['persistence'] >= .5 and
        selected.get('reversal') is False)


def scoring_gap(history, period, recent_size=3, reference_minimum=3):
    field = 'team_goals_'+period
    rows = [r for r in history if numeric(r.get(field)) and r[field] >= 0 and
            (period == 'ft' or r.get('halftime_available') is True and
             numeric(r.get('team_goals_ft')) and r[field] <= r['team_goals_ft'])]
    # Inputs are the validated, deduplicated prematch history, newest first.
    values = [r[field] for r in rows[:8]]
    recent, reference = values[:recent_size], values[recent_size:]
    drought = 0
    for value in values:
        if value != 0: break
        drought += 1
    sufficient = len(recent) == recent_size and len(reference) >= reference_minimum
    # Reduce the effect of one old goleada on the reference; retain raw means.
    cap = median(reference)+2.5*sqrt(max(median(reference), .5)) if reference else None
    baseline = mean(min(v, cap) for v in reference) if reference else None
    recent_mean = mean(recent) if recent else None
    gap = baseline-recent_mean if baseline is not None and recent_mean is not None else None
    minimum = .5 if period == 'ft' else .25
    below = bool(sufficient and baseline >= minimum and gap >= minimum and recent_mean <= baseline*.7)
    fasting = bool(sufficient and drought >= 2 and baseline >= minimum)
    return {'active': below or fasting, 'sufficient': sufficient, 'drought_matches': drought,
        'recent_mean': recent_mean, 'recent_n': len(recent), 'reference_mean': baseline,
        'reference_raw_mean': mean(reference) if reference else None, 'reference_n': len(reference),
        'gap_goals': gap, 'relative_gap_pct': 100*gap/baseline if baseline else None,
        'basis': 'recent_vs_previous_available_matches', 'full_season_mean': False}


def goal_context(assessment, rows):
    histories = (assessment.get('inputs') or {}).get('histories') or {}
    trends = {side: {p: scoring_gap(histories.get(side, []), p) for p in ('ft', 'ht')}
              for side in ('home', 'away')}
    for side, trend in trends.items():
        venue_rows = [r for r in histories.get(side, []) if r.get('side') == side]
        trend['venue'] = {p: scoring_gap(venue_rows, p, 2, 2) for p in ('ft', 'ht')}
        trend['venue_side'] = side
        trend['venue_supported_gap'] = any(
            trend['venue'][p]['active'] or (trend[p]['active'] and trend['venue'][p]['sufficient'] and
                trend['venue'][p]['gap_goals'] > 0) for p in ('ft', 'ht'))
    flows = []
    for row in rows:
        spec = row.get('spec') or {}
        odds = row.get('selected_market_odds') or {}
        if (spec.get('kind') != 'result' or spec.get('period') != 'ft' or spec.get('side') not in ('1', '2') or
                row.get('quote_status') != 'recent' or not confirmed_flow(row) or
                set(odds) != {'1', 'x', '2'} or not all(numeric(q) and q > 1 for q in odds.values())): continue
        favorite = min(odds, key=odds.get)
        difference = abs(odds['1']-odds['2'])
        balanced = difference <= .4+1e-9
        margin, delta = row.get('selected_margin_pct'), row.get('selected_margin_delta_pp')
        fair_p = 100/odds[favorite]/sum(1/q for q in odds.values())
        if ((not balanced and (favorite not in ('1', '2') or odds[favorite] > 1.75 or fair_p < 55)) or
                not numeric(margin) or not 0 <= margin <= 9 or numeric(delta) and delta >= 1): continue
        flows.append({'regime': 'balanced' if balanced else 'clear_favorite',
            'home_away_odds_gap': difference, 'home_odd': odds['1'], 'away_odd': odds['2'],
            'favorite': None if balanced else favorite,
            'favorite_venue': None if balanced else 'home' if favorite == '1' else 'away',
            'favorite_odd': None if balanced else odds[favorite], 'favorite_fair_pct': None if balanced else fair_p,
            'observed_side': spec.get('side'), 'oci': row['oci'], 'margin_pct': margin,
            'margin_delta_pp': delta, 'bookmaker': row.get('bookmaker'),
            'source_market_id': row['id'], 'source_updated_at': row.get('provider_updated_at')})
    flow = max(flows, key=lambda f: abs(f['oci']), default=None)
    affected = [side for side in trends if trends[side]['venue_supported_gap']]
    preferred, role = None, None
    if flow and affected:
        if flow['regime'] == 'balanced': role = 'balanced_venue_gap'
        else:
            favorite_side = flow['favorite_venue']
            if favorite_side in affected:
                preferred, role = 'over15_ht', 'favorite_'+favorite_side
            else: preferred, role = 'gg_ht', 'underdog_'+affected[0]
    return {'trends': trends, 'imbalance': flow, 'drought_sides': affected,
        'pt_priority_active': bool(flow and affected), 'probability_bonus_pp': 0,
        'preferred_pt_market': preferred, 'drought_role': role,
        'reason': 'drought_and_comparable_1x2_flow' if flow and affected else
                  'no_comparable_imbalance' if not flow else 'no_supported_scoring_gap',
        'note': 'Priorità sperimentale, non recupero dei gol dovuto. Il flusso 1X2 non conferma da solo il mercato gol.'}


def signal_for(row, eligible):
    if not eligible: return 'excluded'
    margin, delta, oci = row.get('selected_margin_pct'), row.get('selected_margin_delta_pp'), row.get('oci')
    if numeric(margin) and confirmed_flow(row):
        if oci > 0: return 'convergence'
        if margin <= 9 and not (numeric(delta) and delta >= 1): return 'contrarian'
    return 'unconfirmed'


def evaluate(row, pt_priority=False, preferred_pt=None):
    tier = market_tier(row)
    minimum_p = 35
    if tier is not None and pt_priority:
        spec = row.get('spec') or {}
        if spec.get('period') != 'ht': tier = 2
        elif preferred_pt:
            key = 'gg_ht' if spec.get('kind') == 'btts' else 'over15_ht'
            tier = 0 if key == preferred_pt else 1
    p, odd = row.get('statistical_pct'), row.get('odd')
    # Only the complete partition of the operator offering this exact price.
    # A median of other operators must not validate an anomalous best price.
    margin = row.get('selected_margin_pct')
    delta = row.get('selected_margin_delta_pp')
    margin_known = numeric(margin)
    anomalous = margin_known and not 0 <= margin <= 30
    low_limit = 5 if (row.get('spec') or {}).get('period') == 'ft' else 7
    band = ('anomalous' if anomalous else 'unknown' if not margin_known else
            'low' if margin < low_limit else 'high' if margin > 9 else 'medium')
    min_ev, min_low = {'low': (2., 0.), 'medium': (4., 1.),
                       'high': (max(8., margin - 1), 2.) if margin_known else (8., 2.),
                       'unknown': (10., 3.), 'anomalous': (10., 3.)}[band]
    if numeric(delta) and delta >= 1:
        min_ev += 2
        min_low += 1
    reasons = []
    if tier is None: reasons.append('market')
    if not numeric(p) or not minimum_p <= p <= 100: reasons.append('probability')
    maximum_odd = odds_max(row)
    if not numeric(odd) or not ODDS_MIN <= odd <= maximum_odd: reasons.append('price')
    if row.get('quote_status') != 'recent': reasons.append('stale')
    if row.get('quality') != 'supported': reasons.append('history')
    if (row.get('spec') or {}).get('kind') == 'handicap' and row.get('quote_adapter_version') != 2:
        reasons.append('adapter')
    if anomalous: reasons.append('margin')
    ev, ev_low = row.get('ev_pct'), row.get('ev_low_pct')
    if not numeric(ev) or ev < min_ev: reasons.append('edge')
    if not numeric(ev_low) or ev_low <= 0 or ev_low < min_low: reasons.append('sensitivity')
    eligible = not reasons
    signal = signal_for(row, eligible)
    # An ordinal risk/value score, never a probability or suggested stake.
    score = p * max(0., ev_low-min_low)/(odd-1) if eligible else None
    return {'policy_id': POLICY_ID, 'eligible': eligible, 'strict_eligible': eligible, 'rank': None, 'priority': tier,
            'goal_market': goal_market(row), 'odds_min': ODDS_MIN, 'odds_max': maximum_odd,
            'min_probability_pct': minimum_p, 'min_ev_pct': min_ev,
            'entry_min_ev_pct': min_ev, 'entry_min_ev_low_pct': min_low,
            'min_ev_low_pct': min_low, 'margin_band': band,
            'signal': signal, 'label': LABELS[signal], 'reasons': reasons,
            'playability_score': score, 'pt_priority': pt_priority}


def ranking_key(row):
    d = row['opportunity']
    return (d['priority'], -d['playability_score'], -(row['ev_low_pct'] - d['min_ev_low_pct']),
            -row['ev_pct'], -row['statistical_pct'], str(row['id']))


def annotate(matrix, as_of=None, assessment=None):
    rows = matrix.get('rows', [])
    if assessment is not None: matrix['goal_context'] = goal_context(assessment, rows)
    context = matrix.get('goal_context') or {}
    for row in rows: row['opportunity'] = evaluate(row, context.get('pt_priority_active', False), context.get('preferred_pt_market'))
    goals = {key: [r for r in rows if goal_market(r) == key] for key in ODDS_MAX_BY_MARKET}
    for subset, superset in [('over35_ft', 'over25_ft'), ('gg_ht', 'over15_ht')]:
        for a in goals[subset]:
            for b in goals[superset]:
                if numeric(a.get('statistical_pct')) and numeric(b.get('statistical_pct')) and a['statistical_pct'] > b['statistical_pct']+.01:
                    for row in (a, b):
                        row['opportunity'].update(eligible=False, strict_eligible=False, rank=None, signal='excluded', label=LABELS['excluded'], playability_score=None)
                        if 'coherence' not in row['opportunity']['reasons']: row['opportunity']['reasons'].append('coherence')
    eligible = sorted((r for r in rows if r['opportunity']['eligible']), key=ranking_key)
    from proposal_stability import stabilize
    best = stabilize(matrix, as_of, eligible)
    eligible = sorted((r for r in rows if r['opportunity']['eligible']), key=ranking_key)
    if best: eligible = [best, *[r for r in eligible if r is not best]]
    for rank, row in enumerate(eligible, 1): row['opportunity']['rank'] = rank
    matrix['opportunity_policy'] = {'id': POLICY_ID, 'evaluated_at': as_of,
        'odds_min': ODDS_MIN, 'odds_max': max(ODDS_MAX_BY_MARKET.values()),
        'standard_odds_max': ODDS_MAX, 'odds_max_by_market': dict(ODDS_MAX_BY_MARKET),
        'asian_equivalence': 'identical_half_line_only', 'min_probability_pct': 35,
        'goal_markets_only': True, 'pt_priority_active': context.get('pt_priority_active', False),
        'eligible_markets': len(eligible),
        'excluded_reasons': dict(Counter(reason for r in rows for reason in r['opportunity']['reasons'])),
        'best_market_id': eligible[0]['id'] if eligible else None}
    return eligible[0] if eligible else None


def best_opportunity(rows, context=None):
    # Recompute on copies: public projections cannot trust stale saved ranks.
    matrix = {'rows': [dict(r) for r in rows], 'goal_context': context or {}}
    return annotate(matrix)
