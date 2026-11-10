"""V7 prematch probabilities. All prices enter once, at the score-distribution level."""
from datetime import timezone
from math import exp, isfinite, sqrt

from lateral_matrix import ScoreModel, poisson
from multimarket_model import fair, stamp
from quote_cache import within_window
from quote_sources import fresh

VERSION = 'arabsniper-v7.0'
# Selected on 26 Oct–3 Nov from the predeclared grid.
# Preserve a statistical contribution even when fair is the stronger benchmark.
BASE_WEIGHT = .80
MIN_WEIGHT, MAX_WEIGHT = .50, .80


def finite(value):
    return type(value) in (int, float) and isfinite(value)


def utc(value):
    when = stamp(value)
    return when.astimezone(timezone.utc).isoformat() if when else None


def price_usable(row, as_of, kickoff):
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
    for row in sorted(rows, key=lambda r: (r.get('quote_status') != 'recent',
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
