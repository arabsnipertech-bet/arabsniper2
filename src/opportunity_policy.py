"""One versioned proposal policy; never a fixture or probability-matrix filter.

Thresholds are explicit product constraints, not calibrated win guarantees.
The browser consumes these decisions/ranks rather than reimplementing scoring.
"""
from collections import Counter
from math import isfinite, sqrt
from statistics import mean, median

POLICY_ID = 'gold-goals-v6.1'
SUPPORTED_POLICY_IDS = ('gold-goals-v2', 'gold-goals-v3', 'gold-goals-v4', 'gold-goals-v5', 'gold-goals-v6', POLICY_ID)
ODDS_MIN, ODDS_MAX = 1.85, 7.50
GOALS = {('total', 'ht', 'over', 1.5): 'over15_ht', ('btts', 'ht', 'yes', None): 'gg_ht',
         ('total', 'ht', 'over', 2.5): 'over25_ht',
         ('total', 'ft', 'over', 2.5): 'over25_ft', ('total', 'ft', 'over', 3.5): 'over35_ft'}
ODDS_MIN_BY_MARKET = {'over15_ht': 2.10, 'over25_ht': 4.50, 'over25_ft': 1.85, 'gg_ht': 3.50, 'over35_ft': 2.10}
ODDS_MAX_BY_MARKET = {'over15_ht': 3.50, 'over25_ht': 7.50, 'over25_ft': 2.60, 'gg_ht': 5.50, 'over35_ft': 4.50}
MIN_EV_LOW_BY_MARKET = {'over15_ht': 2., 'over25_ht': 0., 'over25_ft': 2., 'gg_ht': -1., 'over35_ft': 1.}
# Prudential admission rules, not fitted/calibrated probabilities. The model's
# independent 45-minute estimates remain untouched, including absent HT xG.
MIN_PROBABILITY_BY_MARKET = {'over15_ht': 40., 'over25_ht': 18., 'gg_ht': 26., 'over25_ft': 48., 'over35_ft': 35.}
HT_EVIDENCE_ID = 'observed-first-half-v1'
HT_MIN_MATCHES = 4
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
    'ht_history': 'Servono almeno quattro primi tempi osservati per ciascuna squadra',
    'adapter': 'Associazione della quota da aggiornare',
    'margin': 'Aggio anomalo: prezzo da verificare',
    'edge': 'Vantaggio insufficiente rispetto all’aggio',
    'sensitivity': 'Vantaggio insufficiente nello scenario prudente',
    'coherence': 'Probabilità dei mercati gol incoerenti',
    'yield_floor': 'Quota sotto soglia di rendimento',
    'oci_negative': 'OCI inferiore a -20: mercato contrario',
    'odds_rising': 'Salita quota superiore al 2% sullo stesso operatore',
    'suspicious_divergence': 'Divergenza sospetta / Mercato non concorde',
    'margin_missing': 'Aggio dello stesso operatore non disponibile',
    'ht_oci': 'Over 2,5 PT richiede OCI almeno +15',
    'structural_missing': 'Statistiche strutturali non disponibili',
    'isolated_peak': 'Media senza ultima inferiore all’80% della media combinata',
    'ht_symmetry': 'Simmetria delle due squadre nel PT insufficiente',
    'ht_expected': 'Gol attesi PT sotto soglia',
    'ht_frequency': 'Frequenza a segno PT inferiore al 75% per entrambe',
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


def odds_min(row):
    return ODDS_MIN_BY_MARKET.get(goal_market(row), ODDS_MIN)


def min_probability(row):
    return MIN_PROBABILITY_BY_MARKET.get(goal_market(row), 30.)


def first_half_evidence(assessment):
    """Summarize the same real HT window used by the model, never FT proxies.

    Missing xG/shots remain missing. They cannot be invented from FT totals or
    converted into a probability bonus. Record coverage for prospective review.
    """
    from statistics_first import CONFIG, period_rows, weighted
    histories = (assessment.get('inputs') or {}).get('histories') or {}
    teams = {}
    for side in ('home', 'away'):
        valid, seen = [], set()
        for row in histories.get(side, []):
            fid = row.get('fixture_id')
            if fid is None or str(fid) in seen: continue
            if not all(numeric(row.get(k)) and row[k] >= 0 for k in ('team_goals_ft', 'team_conceded_ft')): continue
            seen.add(str(fid)); valid.append(row)
        pool = period_rows(valid, 'ht')
        team = {'sample': len(pool), 'venue_sample': sum(r.get('side') == side for r in pool)}
        for label, field in [('scored', 'team_goals_ht'), ('conceded', 'team_conceded_ht'),
                ('xg_for', 'team_xg_for_ht'), ('xg_against', 'team_xg_against_ht'),
                ('shots_for', 'team_shots_on_target_for_ht'), ('shots_against', 'team_shots_on_target_against_ht')]:
            observed = [r for r in pool if numeric(r.get(field)) and r[field] >= 0]
            team[label+'_sample'] = len(observed)
            team[label+'_mean'] = weighted([r[field] for r in observed], observed, side) if observed else None
        teams[side] = team
    return {'id': HT_EVIDENCE_ID, 'period_minutes': 45, 'window_matches': CONFIG['history_ht'],
            'minimum_matches': HT_MIN_MATCHES, 'teams': teams,
            'xg_supported': all(t[k+'_sample'] >= CONFIG['minimum_xg'] for t in teams.values() for k in ('xg_for', 'xg_against')),
            'xg_source': 'observed_ht_only', 'validation_status': 'prospective_pending'}


def supported_first_half(evidence):
    return bool(isinstance(evidence, dict) and evidence.get('id') == HT_EVIDENCE_ID and
        all(numeric(((evidence.get('teams') or {}).get(side) or {}).get('sample')) and
            evidence['teams'][side]['sample'] >= HT_MIN_MATCHES for side in ('home', 'away')))


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
    from goal_evidence import structural_reasons
    market = goal_market(row)
    tier = market_tier(row)
    if tier is not None and pt_priority:
        tier = 2 if row.get('spec', {}).get('period') != 'ht' else 0 if market == preferred_pt else 1
    p, odd = row.get('statistical_pct'), row.get('odd')
    minimum_p, minimum_odd, maximum_odd = min_probability(row), ODDS_MIN_BY_MARKET.get(market, ODDS_MIN), odds_max(row)
    margin, oci = row.get('selected_margin_pct'), row.get('oci')
    margin_known = numeric(margin)
    band = 'unknown' if not margin_known else 'anomalous' if not 0 <= margin <= 8 else 'high' if margin >= 6.5 else 'low'
    min_ev = 6. if market == 'gg_ht' else 0.
    min_low = MIN_EV_LOW_BY_MARKET.get(market, 0.) + (1.5 if margin_known and 6.5 <= margin <= 8 else 0.)
    reasons = []
    if tier is None: reasons.append('market')
    if not numeric(p) or not minimum_p <= p <= 100: reasons.append('probability')
    if not numeric(odd) or not minimum_odd <= odd <= maximum_odd:
        reasons.append('yield_floor' if market == 'over25_ft' and numeric(odd) and odd < 1.85 else 'price')
    if row.get('quote_status') not in ('recent', 'cached_stable'): reasons.append('stale')
    if row.get('quality') != 'supported': reasons.append('history')
    if not margin_known: reasons.append('margin_missing')
    elif not 0 <= margin <= 8: reasons.append('margin')
    delta = row.get('delta_pct')
    if not numeric(delta): delta = (row.get('selected_movement') or {}).get('delta_pct')
    if numeric(oci) and oci < -20: reasons.append('oci_negative')
    if numeric(delta) and delta > 2.+1e-9: reasons.append('odds_rising')
    book = row.get('selected_market_fair_pct')
    if not numeric(book) and numeric(odd) and odd > 1 and margin_known and margin > -100:
        book = 100/odd/(1+margin/100)
    if not numeric(book): book = row.get('market_fair_pct')
    if numeric(p) and numeric(book) and p-book > 15.+1e-9 and not (numeric(delta) and delta < 0 and numeric(oci) and oci > 0):
        reasons.append('suspicious_divergence')
    if market == 'over25_ht' and (not numeric(oci) or oci < 15): reasons.append('ht_oci')
    if market in ('over15_ht', 'over25_ht', 'gg_ht') and not supported_first_half(row.get('ht_evidence')):
        reasons.append('ht_history')
    if market: reasons.extend(structural_reasons(row, market))
    ev, ev_low = row.get('ev_pct'), row.get('ev_low_pct')
    if not numeric(ev) or ev + 1e-9 < min_ev: reasons.append('edge')
    if not numeric(ev_low) or ev_low + 1e-9 < min_low: reasons.append('sensitivity')
    reasons = list(dict.fromkeys(reasons))
    eligible = not reasons
    signal = signal_for(row, eligible)
    score = p * max(0., ev_low-min_low)/(odd-1) if eligible else None
    return {'policy_id': POLICY_ID, 'eligible': eligible, 'strict_eligible': eligible, 'rank': None, 'priority': tier,
            'goal_market': market, 'odds_min': minimum_odd, 'odds_max': maximum_odd,
            'min_probability_pct': minimum_p, 'min_ev_pct': min_ev,
            'ht_history_supported': market not in ('over15_ht', 'over25_ht', 'gg_ht') or 'ht_history' not in reasons and 'structural_missing' not in reasons,
            'structural_supported': not any(r in reasons for r in ('structural_missing', 'isolated_peak', 'ht_symmetry', 'ht_expected', 'ht_frequency', 'ht_history')),
            'market_checks_passed': not any(r in reasons for r in ('margin', 'margin_missing', 'oci_negative', 'odds_rising', 'suspicious_divergence', 'ht_oci')),
            'threshold_status': 'user_defined_unvalidated',
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
    if assessment is not None and (assessment.get('inputs') or assessment.get('profiles')):
        matrix['goal_context'] = goal_context(assessment, rows)
        from goal_evidence import evidence as structural_evidence
        evidence = first_half_evidence(assessment)
        structure = structural_evidence(assessment)
        for row in rows:
            row['goal_evidence'] = structure
            if goal_market(row) in ('over15_ht', 'over25_ht', 'gg_ht'): row['ht_evidence'] = evidence
    context = matrix.get('goal_context') or {}
    for row in rows: row['opportunity'] = evaluate(row, context.get('pt_priority_active', False), context.get('preferred_pt_market'))
    goals = {key: [r for r in rows if goal_market(r) == key] for key in ODDS_MAX_BY_MARKET}
    for subset, superset in [('over35_ft', 'over25_ft'), ('gg_ht', 'over15_ht'), ('over25_ht', 'over15_ht')]:
        for a in goals[subset]:
            for b in goals[superset]:
                if numeric(a.get('statistical_pct')) and numeric(b.get('statistical_pct')) and a['statistical_pct'] > b['statistical_pct']+.01:
                    for row in (a, b):
                        row['opportunity'].update(eligible=False, strict_eligible=False, rank=None, signal='excluded', label=LABELS['excluded'], playability_score=None)
                        if 'coherence' not in row['opportunity']['reasons']: row['opportunity']['reasons'].append('coherence')
    from goal_evidence import combo_status
    for row in rows:
        if row.get('spec', {}).get('kind') == 'halves_total': row['combo'] = combo_status(row, rows)
    ft_prices = [r['odd'] for r in goals['over25_ft'] if r.get('quote_status') in ('recent', 'cached_stable') and numeric(r.get('odd'))]
    upgrade = bool(ft_prices and max(ft_prices) < 1.85)
    if upgrade:
        for row in rows:
            if row['opportunity']['eligible'] and goal_market(row) in ('over35_ft', 'over15_ht'):
                row['opportunity'].update(priority=-2 if goal_market(row) == 'over35_ft' else -1, automatic_upgrade=True)
    eligible = sorted((r for r in rows if r['opportunity']['eligible']), key=ranking_key)
    from proposal_stability import stabilize
    best = stabilize(matrix, as_of, eligible)
    eligible = sorted((r for r in rows if r['opportunity']['eligible']), key=ranking_key)
    if best: eligible = [best, *[r for r in eligible if r is not best]]
    for rank, row in enumerate(eligible, 1): row['opportunity']['rank'] = rank
    matrix['opportunity_policy'] = {'id': POLICY_ID, 'evaluated_at': as_of,
        'odds_min': ODDS_MIN, 'odds_max': max(ODDS_MAX_BY_MARKET.values()),
        'standard_odds_max': ODDS_MAX, 'odds_min_by_market': dict(ODDS_MIN_BY_MARKET), 'automatic_upgrade': upgrade, 'odds_max_by_market': dict(ODDS_MAX_BY_MARKET),
        'asian_equivalence': 'identical_half_line_only', 'min_probability_pct': min(MIN_PROBABILITY_BY_MARKET.values()),
        'min_probability_pct_by_market': dict(MIN_PROBABILITY_BY_MARKET),
        'ht_min_matches_per_team': HT_MIN_MATCHES, 'threshold_status': 'user_defined_unvalidated',
        'goal_markets_only': True, 'pt_priority_active': context.get('pt_priority_active', False),
        'eligible_markets': len(eligible),
        'excluded_reasons': dict(Counter(reason for r in rows for reason in r['opportunity']['reasons'])),
        'best_market_id': eligible[0]['id'] if eligible else None}
    return eligible[0] if eligible else None


def best_opportunity(rows, context=None):
    # Recompute on copies: public projections cannot trust stale saved ranks.
    matrix = {'rows': [dict(r) for r in rows], 'goal_context': context or {}}
    return annotate(matrix)

