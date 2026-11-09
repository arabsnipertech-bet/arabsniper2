"""Independent price/flow tier. Never edits a Gold decision or probability."""
from opportunity_policy import RULES_REVISION, goal_market, numeric, probability_order
from multimarket_model import stamp

LEGACY_POLICY_ID = 'platinum-flow-v1'
POLICY_ID = 'platinum-flow-v1.1'
LIMITS = {'min_oci': 30, 'max_delta_pct': -3, 'max_margin_pct': 9,
          'min_persistence': .5, 'min_agreeing_books': 2,
          'min_ev_low_pct': 2., 'secondary_oci': 50}


def flow_rejections(row, as_of, max_drop):
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
            or delta > max_drop+1e-9
            or numeric(first) and first > 1 and numeric(odd) and numeric(delta)
            and abs(delta-100*(odd/first-1)) > .01): reasons.append('price_drop')
    if not isinstance(row.get('bookmaker'), str) or not row['bookmaker'].strip(): reasons.append('bookmaker')
    paired, agreeing = row.get('paired_books'), row.get('agreeing_books')
    if (row.get('movement_confirmed') is not True or row.get('fair_direction_coherent') is not True
            or not numeric(paired) or not numeric(agreeing) or int(paired) != paired or int(agreeing) != agreeing
            or paired < 2 or agreeing < 2 or agreeing > paired or agreeing <= paired/2):
        reasons.append('operator_agreement')
    move = row.get('selected_movement') or {}
    if (not numeric(move.get('delta_pct')) or move['delta_pct'] > max_drop+1e-9
            or not numeric(move.get('fair_delta_pp')) or move['fair_delta_pp'] <= 0
            or not numeric(move.get('persistence')) or not .5 <= move['persistence'] <= 1
            or move.get('reversal') is not False): reasons.append('persistence')
    return reasons


def evaluate(row, as_of):
    from adaptive_pricing import gate_passes

    reasons = flow_rejections(row, as_of, LIMITS['max_delta_pct'])
    ev, ev_low = row.get('ev_pct'), row.get('ev_low_pct')
    if not numeric(ev):
        reasons.append('ev_unavailable')
    elif ev <= 0:
        reasons.append('ev_floor')
    elif not gate_passes(row, 0.):
        reasons.append('edge')
    if not numeric(ev_low) or ev_low < LIMITS['min_ev_low_pct']:
        reasons.append('sensitivity')
    eligible = not reasons
    return {'policy_id': POLICY_ID, 'rules_revision': RULES_REVISION, 'eligible': eligible, 'rank': None,
            'evaluated_at': as_of, 'reasons': reasons,
            'value_cohort': 'platinum_positive_ev' if eligible else None,
            'positive_ev_shadow_eligible': eligible,
            'qualification': 'flow_and_value' if eligible else None,
            'secondary_to_gold': eligible and row['oci'] >= LIMITS['secondary_oci'],
            'gold_eligible': (row.get('opportunity') or {}).get('eligible') is True}


def annotate(matrix, meta, as_of, *, probability_model=None, quote_filter=None):
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    if (not now or not kickoff or now >= kickoff or meta.get('result')
            or meta.get('status_short') not in ('NS', 'TBD')): return
    candidates = []
    for row in matrix.get('rows', []):
        if goal_market(row) is None:
            continue
        row['platinum'] = evaluate(row, as_of)
        reason = quote_filter(row) if quote_filter else None
        if reason:
            d = row['platinum']
            d.update(eligible=False, reasons=[*d['reasons'], reason], qualification=None,
                     value_cohort=None, positive_ev_shadow_eligible=False, secondary_to_gold=False)
        if row['platinum']['eligible']: candidates.append(row)
    if probability_model:
        candidates.sort(key=lambda r: (r['platinum']['secondary_to_gold'], probability_order(r)))
    else:
        candidates.sort(key=lambda r: (r['platinum']['secondary_to_gold'], -r['oci'], r['delta_pct'], -r['odd'], str(r['id'])))
    for rank, row in enumerate(candidates, 1): row['platinum']['rank'] = rank
    matrix['platinum_policy'] = {'id': POLICY_ID, 'rules_revision': RULES_REVISION,
        'evaluated_at': as_of, 'limits': dict(LIMITS),
        'eligible_markets': len({goal_market(r) for r in candidates}),
        'best_market_id': candidates[0]['id'] if candidates else None,
        'gold_policy_changed': False}
    if probability_model:
        matrix['platinum_policy']['probability_model'] = probability_model
