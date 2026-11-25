"""V7 prematch probabilities. All prices enter once, at the score-distribution level."""
from datetime import timezone
from math import exp, isclose, isfinite, log, sqrt

from lateral_matrix import ScoreModel, poisson
from multimarket_model import fair, stamp
from quote_cache import within_window
from quote_sources import fresh

VERSION = 'arabsniper-v7.0'
# Selected on 26 Oct–3 Nov from the predeclared grid.
# Preserve a statistical contribution even when fair is the stronger benchmark.
BASE_WEIGHT = .80
MIN_WEIGHT, MAX_WEIGHT = .50, .80

# Observation schema only. It is never read by calibration or routing.
MEASUREMENT_PROTOCOL = 'V7_RESEARCH_TRACE_20261125_V1'
MEASUREMENT_CONFIG = dict(gg_ft_weak_lambda=1., away_raw_gap=.10,
    lambda_origin='v7.rates', equivalent_update_tolerance_seconds=60,
    settlement='regulation_90_minutes', automatic_promotion=False)


def probability_origins(snapshot):
    """Copy three already computed distributions, with their original timestamps."""
    from copy import deepcopy
    return {name: dict(source=source, rates=deepcopy(snapshot.get(source)),
                       observed_at=snapshot.get('statistics_at') if source != 'rates'
                       else snapshot.get('captured_at'))
            for name, source in (('raw_poisson', 'raw_rates'),
                                 ('pre_market_statistics', 'statistics'), ('final_v7', 'rates'))}


def finite(value):
    return type(value) in (int, float) and isfinite(value)


def utc(value):
    when = stamp(value)
    return when.astimezone(timezone.utc).isoformat() if when else None


def price_usable(row, as_of, kickoff):
    """An executable observation from one operator is sufficient."""
    now, start = stamp(as_of), stamp(kickoff)
    observed, updated = (stamp(row.get(k)) for k in ('quote_observed_at', 'provider_updated_at'))
    odd = row.get('odd')
    if (not all((now, start, observed, updated)) or not updated <= observed <= now < start
            or not finite(odd) or odd <= 1 or row.get('quote_executable') is not True
            or row.get('bookmaker_id') is None):
        return False
    if row.get('quote_status') == 'recent':
        return fresh(row['provider_updated_at'], as_of, kickoff)
    return row.get('quote_status') == 'cached_stable' and within_window(row, as_of, kickoff)


def outcome_fair(row):
    """Only same-book fair from a complete, comparable partition."""
    value, margin = row.get('selected_market_fair_pct'), row.get('selected_margin_pct')
    if finite(value) and 0 < value < 100 and finite(margin) and 0 <= margin <= 30:
        return value / 100
    return None


def goal_structure(rates):
    """Describe the existing calibrated intensities; never add a model vote."""
    names = ('home_ht', 'away_ht', 'home_st', 'away_st')
    if not all(finite(rates.get(k)) and rates[k] >= 0 for k in names):
        return None
    home, away = rates['home_ht'], rates['away_ht']
    first, second = home+away, rates['home_st']+rates['away_st']
    total = first+second
    return dict(lambda_pt=first, lambda_st=second, lambda_ft=total,
                pt_share=first/total if total else None,
                # Normalized factors avoid an arbitrary epsilon and underflow
                # at tiny nonzero intensities. Zero volume has no symmetry.
                pt_symmetry=min(1., max(0., 4*(home/first)*(away/first))) if first else None,
                periods={period: dict(lambda_home=h, lambda_away=a, lambda_total=h+a,
                                      weaker_side_lambda=min(h, a),
                                      symmetry=min(1., max(0., 4*(h/(h+a))*(a/(h+a)))) if h+a else None)
                         for period, h, a in (('pt', home, away),
                                             ('st', rates['home_st'], rates['away_st']),
                                             ('ft', home+rates['home_st'], away+rates['away_st']))})


def value_relation(row, estimate):
    """Explain a binary price, without fitting weights or altering admission.

    Fair is proportional devig at one operator, not a certified true probability.
    A positive gap to fair can still fall short of covering that operator's margin.
    Refund/Asian settlements retain their existing payout-weight EV calculation.
    """
    if any(estimate.get(k, 0.) != 0 for k in
           ('push_probability', 'half_win_probability', 'half_loss_probability')):
        return dict(status='settlement_weights_only')
    odd, margin, fair_p = row.get('odd'), row.get('selected_margin_pct'), outcome_fair(row)
    p, loss = estimate.get('win_weight'), estimate.get('loss_weight')
    if fair_p is None or row.get('bookmaker_id') is None:
        return dict(status='same_book_fair_unavailable')
    if (not all(finite(v) for v in (odd, p, loss)) or odd <= 1 or not 0 <= p <= 1
            or not isclose(p+loss, 1., rel_tol=0, abs_tol=1e-10)
            or not isclose(fair_p, 1/odd/(1+margin/100), rel_tol=1e-9, abs_tol=1e-12)):
        return dict(status='inconsistent_price_reference')
    ev = p*(odd-1)-loss
    advantage = p/fair_p-1
    return dict(status='verified', basis='same_book_proportional_devig',
                bookmaker_id=row.get('bookmaker_id'), fair_probability=fair_p,
                break_even_probability=1/odd, model_probability=p,
                edge_fair_pp=100*(p-fair_p), relative_advantage_pct=100*advantage,
                margin_pct=margin, ev_pct=100*ev,
                fair_advantage_below_margin=bool(p > fair_p and ev < 0),
                identity_error_pp=100*(ev-(advantage-margin/100)/(1+margin/100)))


def movement_relation(row, as_of):
    """Decompose one comparable window, never a post-reversal leg or mixed book.

    References are preserved from already computed complete-partition movements.
    Missing/legacy evidence stays explicitly unavailable, without reconstruction
    from a median margin, a different operator, OCI or the current opening quote.
    """
    if row.get('prematch_stable') or row.get('quote_status') == 'cached_stable':
        return dict(status='stable_quote_no_live_flow')
    move = row.get('selected_movement') or {}
    ref = move.get('price_reference') or {}
    if not ref:
        return dict(status='comparable_reference_unavailable')
    if (ref.get('basis') != 'same_book_complete_partition' or row.get('bookmaker_id') is None
            or str(ref.get('bookmaker_id')) != str(row['bookmaker_id'])):
        return dict(status='inconsistent_bookmaker')
    start, end, seen, now = (stamp(v) for v in (move.get('observed_from'), move.get('observed_to'),
                                              row.get('quote_observed_at'), as_of))
    if not all((start, end, seen, now)) or not start < end <= seen <= now:
        return dict(status='inconsistent_observation_window')
    fields = ('odd_from', 'odd_to', 'fair_from', 'fair_to', 'margin_from_pct', 'margin_to_pct')
    if not all(finite(ref.get(k)) for k in fields):
        return dict(status='incomplete_price_reference')
    q0, q1, f0, f1, m0, m1 = (ref[k] for k in fields)
    if not (q0 > 1 and q1 > 1 and 0 < f0 < 1 and 0 < f1 < 1 and 0 <= m0 <= 30 and 0 <= m1 <= 30):
        return dict(status='inconsistent_price_reference')
    # The saved window can end earlier than a repeated observation, but its
    # complete current price reference must still match the selected quotation.
    current = row.get('odd'), outcome_fair(row), row.get('selected_margin_pct')
    if (not all(finite(v) for v in current)
            or not all(isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)
                       for a, b in zip((q1, f1, m1), current))
            or not all(isclose(f, 1/q/(1+m/100), rel_tol=1e-9, abs_tol=1e-12)
                       for q, f, m in ((q0, f0, m0), (q1, f1, m1)))):
        return dict(status='inconsistent_price_reference')
    implied, fair_move, margin_move = log(q0/q1), log(f1/f0), log((1+m1/100)/(1+m0/100))
    price_moves = not isclose(implied, 0., rel_tol=0, abs_tol=1e-12)
    return dict(status='verified', basis='same_book_proportional_devig',
                bookmaker_id=row['bookmaker_id'], observed_from=utc(start), observed_to=utc(end),
                net_delta_pct=100*(q1/q0-1), fair_delta_pp=100*(f1-f0), margin_delta_pp=m1-m0,
                implied_log_move=implied, fair_log_move=fair_move, margin_log_move=margin_move,
                margin_share_of_log_move=margin_move/implied if price_moves else None,
                fair_rises=bool(f1 > f0 and not isclose(f1, f0, rel_tol=0, abs_tol=1e-12)),
                identity_error=implied-fair_move-margin_move)


def pricing_evidence(row, estimate, raw_estimate, statistical_estimate, usable, *, as_of=None,
                     include_relations=True):
    """Trace alternative estimates on one real price; never change admission.

    EV always uses settlement weights, including pushes and partial payouts.
    The plain probability break-even is meaningful only for binary contracts.
    The related statistical/Poisson estimates are not independent confirmations.
    """
    odd = row.get('odd')
    if not usable or not finite(odd) or odd <= 1:
        return None
    fair_p, margin = outcome_fair(row), row.get('selected_margin_pct')
    win, loss = estimate['win_weight'], estimate['loss_weight']
    settled = win+loss
    binary = all(estimate.get(k, 0.) == 0 for k in
                 ('push_probability', 'half_win_probability', 'half_loss_probability'))
    evidence = dict(basis='binary' if binary else 'settlement_weights',
                break_even_probability=1/odd if binary else None,
                edge_fair_pp=100*(win/settled-fair_p) if fair_p is not None and settled else None,
                fair_identity_error_pp=100*(fair_p-1/odd/(1+margin/100)) if fair_p is not None else None,
                raw_ev=raw_estimate['win_weight']*(odd-1)-raw_estimate['loss_weight'],
                statistical_ev=statistical_estimate['win_weight']*(odd-1)-statistical_estimate['loss_weight'])
    if include_relations:
        evidence.update(relations_version='V7_MARKET_RELATION_V1', statistical_votes_independent=False,
                        value_relation=value_relation(row, estimate),
                        movement_relation=movement_relation(row, as_of) if binary else dict(status='settlement_weights_only'))
    return evidence


def poisson_over(volume, line):
    term = exp(-volume)
    under = term
    for n in range(1, int(line) + 1):
        term *= volume / n
        under += term
    return min(1., max(0., 1 - under))


def result_probabilities(volume, home_share):
    """Compute the complete 1X2 partition in linear time in the goal ceiling."""
    home, away = poisson(volume * home_share), poisson(volume * (1 - home_share))
    win, draw, away_below = 0., 0., 0.
    for goals, prob in enumerate(home):
        win += prob * away_below
        if goals < len(away):
            draw += prob * away[goals]
            away_below += away[goals]
    mass = sum(home) * sum(away)
    return win / mass, draw / mass, max(0., 1 - (win + draw) / mass)


def _fit_total(prior, totals):
    if not totals:
        return prior
    # Every line supplies a CDF target, so the signed discrepancy is monotone.
    low, high = .01, 12.
    for _ in range(30):
        volume = (low + high) / 2
        if sum(poisson_over(volume, line) - p for line, p in totals.items()) < 0:
            low = volume
        else:
            high = volume
    return (low + high) / 2


def _fit_fulltime(prior_volume, prior_share, totals, result):
    volume = _fit_total(prior_volume, totals)
    if not result:
        return volume, prior_share

    def loss(total, share):
        match_result = result_probabilities(total, share)
        # Equal weight to the two market families, regardless of line count.
        error = sum((p - q) ** 2 for p, q in zip(match_result, result))
        if totals:
            error += sum((poisson_over(total, line) - q) ** 2 for line, q in totals.items()) / len(totals)
        return error

    point = (volume, prior_share)
    error = loss(*point)
    for dv, ds in ((1., .18), (.5, .09), (.25, .045), (.12, .022), (.06, .011), (.03, .0055)):
        for _ in range(4):
            v, s = point
            for trial_volume, trial_share in ((v - dv, s), (v + dv, s), (v, s - ds), (v, s + ds)):
                trial = max(.05, min(12., trial_volume)), max(.005, min(.995, trial_share))
                trial_error = loss(*trial)
                if trial_error < error:
                    point, error = trial, trial_error
            if point == (v, s):
                break
    return point


def _shares(first, second, half_fraction, overall):
    """Apply one odds shift to the two period shares, preserving their ordering."""
    first = max(.001, min(.999, first))
    second = max(.001, min(.999, second))
    lo, hi = 0., 1e6
    for _ in range(44):
        factor = (lo + hi) / 2
        ht = factor * first / (1 - first + factor * first)
        st = factor * second / (1 - second + factor * second)
        if half_fraction * ht + (1 - half_fraction) * st < overall:
            lo = factor
        else:
            hi = factor
    return ht, st


def _rate_pair(ht, st, ht_share, st_share):
    home_ht, home_st = ht * ht_share, st * st_share
    away_ht, away_st = ht - home_ht, st - home_st
    return dict(home_ht=home_ht, away_ht=away_ht, home_st=home_st, away_st=away_st,
                home_ft=home_ht + home_st, away_ft=away_ht + away_st)


def raw_rates(assessment):
    source = assessment.get('pure_poisson_rates')
    if not source and not (assessment.get('matrix') or {}).get('lambda_shrinkage'):
        source = assessment.get('rates')
    if not source:
        return None
    fields = ('home_ht', 'away_ht', 'home_ft', 'away_ft')
    if not all(finite(source.get(k)) and 0 <= source[k] <= 12 for k in fields):
        return None
    if any(source[s+'_ht'] > source[s+'_ft'] for s in ('home', 'away')):
        return None
    return {**{k: source[k] for k in fields}, **{
        s+'_st': source[s+'_ft'] - source[s+'_ht'] for s in ('home', 'away')}}


def statistical_rates(raw, means):
    """Use paired observed ST scores when available, retaining the robust FT volume."""
    second = {}
    for team, opponent in (('home', 'away'), ('away', 'home')):
        attack = (means.get(team) or {}).get('st') or {}
        defence = (means.get(opponent) or {}).get('st') or {}
        count = min(attack.get('sample') or 0, defence.get('sample') or 0)
        observed = attack.get('scored'), defence.get('conceded')
        base = raw[team+'_st']
        if count and all(finite(v) and v >= 0 for v in observed):
            base = (4 * base + count * sqrt(observed[0] * observed[1])) / (4 + count)
        second[team] = base
    ft = raw['home_ft'] + raw['away_ft']
    ht = raw['home_ht'] + raw['away_ht']
    st = second['home'] + second['away']
    if not (ft and ht and st):
        return raw
    scale = ft / (ht + st)
    return _rate_pair(ht * scale, st * scale, raw['home_ht'] / ht, second['home'] / st)


def market_rates(statistics, totals, result=None):
    """Fit regulation volume/share and distribute it using verified period lines."""
    if not result and not any(totals.values()):
        return None
    home_ht, away_ht = statistics['home_ht'], statistics['away_ht']
    home_st, away_st = statistics['home_st'], statistics['away_st']
    ht_prior, st_prior = home_ht + away_ht, home_st + away_st
    ft_prior = ht_prior + st_prior
    if not ft_prior:
        return None
    fair_ft, share = _fit_fulltime(ft_prior, statistics['home_ft'] / ft_prior, totals.get('ft', {}), result)
    if totals.get('sh'):
        fair_st = _fit_total(st_prior, totals['sh'])
        if totals.get('ht'):
            fair_ht = _fit_total(ht_prior, totals['ht'])
            if not totals.get('ft') and not result:
                fair_ft = fair_ht + fair_st
            fair_ht = fair_ft * fair_ht / (fair_ht + fair_st)
        elif not totals.get('ft') and not result:
            fair_ht, fair_ft = ht_prior, ht_prior+fair_st
        else:
            fair_ht = max(.005, fair_ft-fair_st)
    elif totals.get('ht'):
        fair_ht = _fit_total(ht_prior, totals['ht'])
        if not totals.get('ft') and not result:
            fair_ft = fair_ht + st_prior
    else:
        fair_ht = fair_ft * ht_prior / ft_prior
    fair_ht = min(fair_ht, fair_ft - .005)
    if fair_ht < 0:
        return None
    ht_share, st_share = _shares(home_ht / ht_prior if ht_prior else share,
                                 home_st / st_prior if st_prior else share, fair_ht / fair_ft, share)
    return _rate_pair(fair_ht, fair_ft - fair_ht, ht_share, st_share)


def fair_inputs(rows):
    """Accept one fair price per line and one complete same-book 1X2 vector."""
    totals = {'ft': {}, 'ht': {}, 'sh': {}}
    result = None
    complete = (row for row in rows if outcome_fair(row) is not None)
    for row in sorted(complete, key=lambda r: (r.get('quote_status') != 'recent',
                                                r['selected_margin_pct'], str(r.get('bookmaker_id')))):
        spec = row['spec']
        if spec['kind'] == 'result' and spec['period'] == 'ft' and result is None:
            odds = row.get('selected_market_odds') or {}
            if set(odds) == {'1', 'x', '2'} and all(finite(v) and v > 1 for v in odds.values()):
                probabilities, _ = fair(odds)
                result = tuple(probabilities[s] for s in ('1', 'x', '2'))
        if spec['kind'] == 'total' and spec['side'] == 'over' and spec['period'] in totals:
            totals[spec['period']].setdefault(spec['line'], outcome_fair(row))
    return totals, result


def blend_rates(statistics, projection, weight):
    if projection is None:
        return statistics
    mixed = {key: statistics[key] + weight * (projection[key] - statistics[key])
             for key in ('home_ht', 'away_ht', 'home_st', 'away_st')}
    return {**mixed, **{s+'_ft': mixed[s+'_ht']+mixed[s+'_st'] for s in ('home', 'away')}}


def score_model(rates):
    return ScoreModel(rates, second_half_floor=0.)
