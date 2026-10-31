"""Bounded hysteresis for the one previously published Gold proposal.

Entry always uses strict policy. A held decision never refreshes its anchor.
Hard probability, price, freshness, coverage and coherence gates cannot relax.
"""
from copy import deepcopy
from datetime import timedelta
from multimarket_model import stamp
from opportunity_policy import SUPPORTED_POLICY_IDS, evaluate, numeric, ranking_key, signal_for, LABELS

STABILITY_ID = 'gold-stability-v1'
LIMITS = {'max_hold_minutes': 90, 'odds_change_pct': 3., 'probability_change_pp': 1.,
          'margin_change_pp': .75, 'oci_change': 20., 'ev_tolerance_pp': 1.,
          'ev_low_floor_pct': -.5, 'switch_score_pct': 15., 'switch_score_points': 2.}
ANCHOR_FIELDS = ('odd', 'statistical_pct', 'selected_margin_pct', 'oci', 'bookmaker_id')


def seed_stability(previous, current):
    """Transfer only a saved published choice, never a fresh unpublished winner."""
    old = previous.get('matrix') or {}
    matrix = current.get('matrix') or {}
    # Display history is separate from eligibility and never refreshes quote age.
    if old.get('last_proposal'):
        matrix['last_proposal'] = deepcopy(old['last_proposal'])
    old_rows = {r['id']: r for r in old.get('rows', [])}
    old_best = old_rows.get((old.get('opportunity_policy') or {}).get('best_market_id'))
    if not old.get('last_proposal') and old_best and (old_best.get('opportunity') or {}).get('eligible'):
        matrix['last_proposal'] = {key: deepcopy(old_best.get(key)) for key in
                                   ('label', 'odd', 'bookmaker', 'provider_updated_at')}
        matrix['last_proposal'].update(market_id=old_best['id'], status='current',
            published_at=(old.get('opportunity_policy') or {}).get('evaluated_at'))
    from quote_cache import preserve_study_flow, remember
    for row in matrix.get('rows', []):
        prior = old_rows.get(row['id'], {})
        if row.get('quote_status') == 'missing' and (prior.get('odd') or prior.get('last_odd')) is not None:
            for key in ('first_odd', 'first_observed_at', 'bookmaker', 'bookmaker_id',
                        'provider_updated_at', 'quote_observed_at'):
                row[key] = deepcopy(prior.get(key))
            row.update(last_odd=prior.get('odd') or prior.get('last_odd'), quote_status='stale')
            saved = deepcopy(prior)
            remember(saved)
            if saved.get('cached_quote'): row['cached_quote'] = saved['cached_quote']
        preserve_study_flow(prior, row)
    matrix.pop('proposal_stability', None)
    state = old.get('proposal_stability') or {}
    if state.get('version') == STABILITY_ID and state.get('market_id'):
        matrix['proposal_stability'] = deepcopy(state)
        return
    policy = old.get('opportunity_policy') or {}
    if policy.get('id') not in SUPPORTED_POLICY_IDS or not stamp(policy.get('evaluated_at')): return
    row = next((r for r in old.get('rows', []) if r.get('id') == policy.get('best_market_id')), None)
    if row and (row.get('opportunity') or {}).get('eligible') is True and evaluate(row)['eligible']:
        matrix['proposal_stability'] = strict_state(row, policy['evaluated_at'])


def strict_state(row, as_of):
    return {'version': STABILITY_ID, 'market_id': row['id'], 'last_strict_at': as_of,
            'anchor': {key: deepcopy(row.get(key)) for key in ANCHOR_FIELDS}, 'status': 'strict'}


def retain(row, state, as_of):
    # v6.1 thresholds are mandatory, including GG PT's explicit negative floor.
    # Hysteresis may choose between eligible rows, never relax admission.
    if row.get('opportunity', {}).get('policy_id') == 'gold-goals-v6.1': return False
    now, start = stamp(as_of), stamp(state.get('last_strict_at'))
    d, anchor = row['opportunity'], state.get('anchor') or {}
    if not now or not start or not 0 <= (now-start).total_seconds() <= LIMITS['max_hold_minutes']*60: return False
    if not d.get('reasons') or set(d['reasons'])-{'edge', 'sensitivity'}: return False
    if anchor.get('bookmaker_id') is None or anchor.get('bookmaker_id') != row.get('bookmaker_id'): return False
    for key, limit in [('statistical_pct', LIMITS['probability_change_pp']), ('selected_margin_pct', LIMITS['margin_change_pp'])]:
        if not numeric(anchor.get(key)) or not numeric(row.get(key)) or abs(row[key]-anchor[key]) > limit+1e-9: return False
    if not numeric(anchor.get('odd')) or anchor['odd'] <= 1 or abs(row['odd']/anchor['odd']-1)*100 > LIMITS['odds_change_pct']+1e-9: return False
    before, current = anchor.get('oci'), row.get('oci')
    if before is not None or current is not None:
        if not numeric(before) or not numeric(current) or abs(current-before) > LIMITS['oci_change']: return False
    if (row.get('selected_movement') or {}).get('reversal') is True: return False
    ev_floor = max(.5, d['min_ev_pct']-LIMITS['ev_tolerance_pp'])
    low_floor = max(LIMITS['ev_low_floor_pct'], d['min_ev_low_pct']-LIMITS['ev_tolerance_pp'])
    if not numeric(row.get('ev_pct')) or row['ev_pct'] < ev_floor: return False
    if not numeric(row.get('ev_low_pct')) or row['ev_low_pct'] < low_floor: return False
    signal = signal_for(row, True)
    d.update(eligible=True, strict_eligible=False, strict_reasons=list(d['reasons']), reasons=[],
             min_ev_pct=ev_floor, min_ev_low_pct=low_floor, signal=signal, label=LABELS[signal],
             playability_score=row['statistical_pct']*max(0., row['ev_low_pct']-low_floor)/(row['odd']-1),
             stability={'version': STABILITY_ID, 'status': 'retained',
                        'last_strict_at': state['last_strict_at'],
                        'expires_at': (start+timedelta(minutes=LIMITS['max_hold_minutes'])).isoformat()})
    return True


def stabilize(matrix, as_of, strict):
    state = matrix.get('proposal_stability') or {}
    previous = next((r for r in matrix.get('rows', []) if r['id'] == state.get('market_id')), None)
    best = strict[0] if strict else None
    held = False
    if state.get('version') == STABILITY_ID and previous:
        held = not previous['opportunity']['eligible'] and retain(previous, state, as_of)
        if previous['opportunity']['eligible']:
            # Keep a valid incumbent when the alternative only improves its
            # score marginally. A newly superior PT priority still takes over.
            old = previous['opportunity']
            new = best['opportunity'] if best else None
            improvement = max(LIMITS['switch_score_points'], abs(old['playability_score'])*LIMITS['switch_score_pct']/100)
            if not best or best is previous or (old['priority'] <= new['priority'] and
                (old['priority'] < new['priority'] or new['playability_score'] < old['playability_score']+improvement)):
                best = previous
    if held and best is not previous:
        # Only the incumbent box choice receives tolerance, not a second pick.
        previous['opportunity'] = evaluate(previous, previous['opportunity']['pt_priority'],
            (matrix.get('goal_context') or {}).get('preferred_pt_market'))
    if best and stamp(as_of):
        if best['opportunity'].get('strict_eligible', True):
            matrix['proposal_stability'] = strict_state(best, as_of)
            best['opportunity']['stability'] = {'version': STABILITY_ID, 'status': 'strict'}
        else:
            matrix['proposal_stability'] = {**deepcopy(state), 'status': 'retained'}
        matrix['proposal_stability']['evaluated_at'] = as_of
    else:
        matrix['proposal_stability'] = {'version': STABILITY_ID, 'market_id': None,
            'status': 'released' if state.get('market_id') or state.get('status') == 'released' else 'none', 'evaluated_at': as_of}
    if best and stamp(as_of):
        matrix['last_proposal'] = {'market_id': best['id'], 'label': best.get('label'),
            'odd': best.get('odd'), 'bookmaker': best.get('bookmaker'),
            'provider_updated_at': best.get('provider_updated_at'), 'published_at': as_of,
            'status': 'current'}
    elif matrix.get('last_proposal'):
        history = matrix['last_proposal']
        current = next((r for r in matrix.get('rows', []) if r['id'] == history['market_id']), None)
        history['status'] = 'withdrawn' if history.get('status') == 'withdrawn' or current and current.get('quote_status') == 'recent' else 'awaiting_update'
    matrix['proposal_stability']['limits'] = dict(LIMITS)
    return best

