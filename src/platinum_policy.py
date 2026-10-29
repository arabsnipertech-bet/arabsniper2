"""Independent price/flow tier. Never edits a Gold decision or probability."""
from math import isfinite
from opportunity_policy import goal_market
from multimarket_model import stamp

POLICY_ID = 'platinum-flow-v1'
LIMITS = {'min_oci': 30, 'max_delta_pct': -3, 'max_margin_pct': 9,
          'min_persistence': .5, 'min_agreeing_books': 2,
          'min_odd': 2.5, 'min_ev_pct': 10}


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def evaluate(row, as_of):
    """Missing statistics never veto the high-odds route. EV is optional."""
    reasons = []
    if goal_market(row) is None: reasons.append('market')
    odd, first = row.get('odd'), row.get('first_odd')
    if row.get('quote_status') not in ('recent', 'cached_stable') or not numeric(odd) or odd <= 1:
        reasons.append('price')
    if row.get('quote_status') == 'cached_stable':
        at, end, now = stamp(row.get('cache_source_at')), stamp(row.get('cache_expires_at')), stamp(as_of)
        if not at or not end or not now or (end-at).total_seconds() != 43200 or not at <= now <= end:
            reasons.append('cache_expired')
    oci, margin, delta = row.get('oci'), row.get('selected_margin_pct'), row.get('delta_pct')
    if not numeric(oci) or not LIMITS['min_oci'] <= oci <= 100: reasons.append('oci')
    if not numeric(margin) or not 0 <= margin <= LIMITS['max_margin_pct']: reasons.append('margin')
    if (not numeric(first) or first <= 1 or not numeric(odd) or not numeric(delta)
            or delta > LIMITS['max_delta_pct']+1e-9
            or numeric(first) and first > 1 and numeric(odd) and numeric(delta)
            and abs(delta-100*(odd/first-1)) > .01): reasons.append('price_drop')
    if not isinstance(row.get('bookmaker'), str) or not row['bookmaker'].strip(): reasons.append('bookmaker')
    paired, agreeing = row.get('paired_books'), row.get('agreeing_books')
    if (row.get('movement_confirmed') is not True or row.get('fair_direction_coherent') is not True
            or not numeric(paired) or not numeric(agreeing) or int(paired) != paired or int(agreeing) != agreeing
            or paired < 2 or agreeing < 2 or agreeing > paired or agreeing <= paired/2):
        reasons.append('operator_agreement')
    move = row.get('selected_movement') or {}
    if (not numeric(move.get('delta_pct')) or move['delta_pct'] > LIMITS['max_delta_pct']+1e-9
            or not numeric(move.get('fair_delta_pp')) or move['fair_delta_pp'] <= 0
            or not numeric(move.get('persistence')) or not .5 <= move['persistence'] <= 1
            or move.get('reversal') is not False): reasons.append('persistence')
    high_odds = numeric(odd) and odd >= LIMITS['min_odd']
    excellent_ev = numeric(row.get('ev_pct')) and row['ev_pct'] >= LIMITS['min_ev_pct']
    if not high_odds and not excellent_ev: reasons.append('odds_or_value')
    return {'policy_id': POLICY_ID, 'eligible': not reasons, 'rank': None,
            'evaluated_at': as_of, 'reasons': reasons,
            'qualification': 'odds_and_value' if high_odds and excellent_ev else 'high_odds' if high_odds else 'expected_value' if excellent_ev else None,
            'gold_eligible': (row.get('opportunity') or {}).get('eligible') is True}


def annotate(matrix, meta, as_of):
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    if (not now or not kickoff or now >= kickoff or meta.get('result')
            or meta.get('status_short') not in ('NS', 'TBD')): return
    candidates = []
    for row in matrix.get('rows', []):
        if goal_market(row) is None:
            continue
        row['platinum'] = evaluate(row, as_of)
        if row['platinum']['eligible']: candidates.append(row)
    candidates.sort(key=lambda r: (-r['oci'], r['delta_pct'], -r['odd'], str(r['id'])))
    for rank, row in enumerate(candidates, 1): row['platinum']['rank'] = rank
    matrix['platinum_policy'] = {'id': POLICY_ID, 'evaluated_at': as_of, 'limits': dict(LIMITS),
        'eligible_markets': len({goal_market(r) for r in candidates}),
        'best_market_id': candidates[0]['id'] if candidates else None,
        'gold_policy_changed': False}
