"""Fixed Over 1.5 HT research signal, separate from price/value selection.

Thresholds were explored on 2026-10-18. Only full dates from 2026-10-20
are prospective evidence; no result is an input to this module.
"""
from copy import deepcopy
from math import isfinite

PROFILE_ID = 'over15_ht_convergence_v1'
DISCOVERY_THROUGH = '2026-10-18'
VALIDATION_FROM = '2026-10-20'
THRESHOLDS = {'book_fair_pct': 45.0, 'poisson_pct': 40.0,
              'empirical_pct': 40.0, 'reliability_pct': 60.0}


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def probability_pct(value, scale=1):
    if not finite(value) or not 0 <= value <= scale:
        return None
    return value / scale * 100


def valid_ht_row(row):
    if row.get('halftime_available') is False:
        return False
    keys = ('home_ht_goals', 'away_ht_goals')
    if not any(k in row for k in keys):
        keys = ('team_goals_ht', 'team_conceded_ht')
    return all(finite(row.get(k)) and row[k] >= 0 and int(row[k]) == row[k] for k in keys)


def ht_context_sample(profile):
    return profile.get('ht_sample_context', profile.get('sample_context', 0))


def assess(market, reliability, value_gate=None):
    """Accept a displayed/frozen market, without requiring positive model EV."""
    market = market or {}
    empirical = market.get('empirical_layer') or {}
    validation = market.get('market_validation') or {}
    over = (market.get('selections') or {}).get('over') or {}
    components = {
        'book_fair_pct': probability_pct(over.get('book_fair_probability_pct'), 100),
        'poisson_pct': probability_pct(empirical.get('poisson_positive')),
        'empirical_pct': probability_pct(empirical.get('empirical_positive')),
    }
    missing = [k for k, v in components.items() if v is None]
    reasons = ['missing_' + k for k in missing]
    for key, value in components.items():
        if value is not None and value < THRESHOLDS[key]:
            reasons.append(key + '_below_threshold')
    converges = not reasons
    if validation.get('usable_for_signals') is not True:
        reasons.append('market_not_verified')
    if not finite(reliability) or reliability < THRESHOLDS['reliability_pct']:
        reasons.append('data_reliability_below_60_or_missing')
    value_gate = value_gate or {}
    return {'profile_id': PROFILE_ID, 'experimental': True,
            'validation_from': VALIDATION_FROM, 'thresholds': dict(THRESHOLDS),
            'components': components, 'converges': converges,
            'eligible': not reasons,
            'status': 'unavailable' if missing else 'convergent' if not reasons else 'excluded',
            'reasons': reasons, 'reliability_pct': reliability,
            'value_eligible': value_gate.get('eligible') is True,
            'expected_value_pct': value_gate.get('expected_value_pct'),
            'note': 'Convergence is a research rule, not a calibrated success probability. '
                    'Poisson and empirical estimates share historical inputs.'}


def evidence(statistics, lambdas):
    """Keep the inputs needed to audit HT coverage, venue and goal estimates."""
    result = {'schema_version': 1, 'teams': {},
              'lambdas': {k: lambdas.get(k) for k in ('home_ht', 'away_ht', 'total_ht')},
              'league': {k: (statistics.get('league') or {}).get(k) for k in
                         ('ht_sample', 'ht_source', 'avg_total_ht', 'over15ht_rate')},
              'h2h_ht_sample': (statistics.get('h2h') or {}).get('ht_sample')}
    for side in ('home', 'away'):
        team = statistics.get(side) or {}
        first = deepcopy(team.get('first_half') or {})
        result['teams'][side] = {
            'sample_all': team.get('ht_sample_all'),
            'sample_context': team.get('ht_sample_context'),
            'missing_ht': team.get('ht_missing_count'),
            'coverage_pct': team.get('ht_coverage_pct'),
            'first_half': first,
            'history': [{k: row.get(k) for k in
                        ('fixture_id', 'date', 'side', 'halftime_available',
                         'home_ht_goals', 'away_ht_goals', 'team_goals_ht',
                         'team_conceded_ht', 'over15_ht', 'gg_ht')}
                       for row in team.get('last_matches', [])],
            'metrics': {k: (team.get('metrics') or {}).get(k) for k in
                        ('goals_scored_ht', 'goals_conceded_ht', 'over15ht_rate',
                         'gg_ht_rate', 'scored_ht_rate', 'conceded_ht_rate')},
        }
    counts = [r['sample_all'] for r in result['teams'].values()]
    result['coverage_status'] = ('unrecorded' if any(not finite(v) or v < 0 for v in counts)
                                 else 'available' if all(v > 0 for v in counts)
                                 else 'missing_team_ht')
    return result
