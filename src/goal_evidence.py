"""Observed structural evidence for Gold v6.1; no price-derived probabilities."""
from math import isfinite
from statistics import mean

EVIDENCE_ID = 'gold-structure-v6.1'


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def evidence(assessment):
    from statistics_first import period_rows
    profiles, rates = assessment.get('profiles') or {}, assessment.get('rates') or {}
    histories = (assessment.get('inputs') or {}).get('histories') or {}
    teams = {}
    for side in ('home', 'away'):
        team = {}
        for period in ('ft', 'ht'):
            profile = profiles.get(side, {})
            attack, defense = profile.get('for_'+period, {}), profile.get('against_'+period, {})
            rows = period_rows(histories.get(side, []), period)
            values = [r['team_goals_'+period] for r in rows if numeric(r.get('team_goals_'+period))]
            conceded = [r['team_conceded_'+period] for r in rows if numeric(r.get('team_conceded_'+period))]
            def observed(key, fallback):
                return attack[key] if numeric(attack.get(key)) else fallback
            team[period] = {
                'sample': observed('sample', len(values)),
                'mean': observed('mean', mean(values) if values else None),
                'without_last': observed('without_last', mean(values[1:]) if len(values) > 1 else None),
                'conceded_mean': defense.get('mean') if numeric(defense.get('mean')) else mean(conceded) if conceded else None,
                'frequency': observed('frequency', mean(v > 0 for v in values) if values else None),
                # Count the whole observed window, not only the current drought.
                'goalless_matches': sum(v == 0 for v in values) if values else attack.get('goalless_matches'),
                'expected_goals': rates.get(side+'_'+period),
            }
        teams[side] = team
    return {'id': EVIDENCE_ID, 'teams': teams, 'combined_basis': 'home_and_away_goals_scored_in_period'}


def structural_reasons(row, market):
    data = row.get('goal_evidence') or {}
    if data.get('id') != EVIDENCE_ID: return ['structural_missing']
    teams = data.get('teams') or {}
    period = 'ht' if market in ('over15_ht', 'over25_ht', 'gg_ht') else 'ft'
    pair = [(teams.get(s) or {}).get(period) or {} for s in ('home', 'away')]
    reasons = []
    if not all(numeric(t.get(k)) for t in pair for k in ('mean', 'without_last')):
        reasons.append('structural_missing')
    elif sum(t['without_last'] for t in pair) + 1e-9 < .8*sum(t['mean'] for t in pair):
        reasons.append('isolated_peak')
    if period == 'ht':
        if not all(numeric(t.get('sample')) and t['sample'] >= 4 for t in pair): reasons.append('ht_history')
        if market == 'gg_ht':
            if not all(numeric(t.get(k)) and t[k] + 1e-9 >= floor for t in pair
                       for k, floor in (('expected_goals', .55), ('mean', .5), ('conceded_mean', .5))):
                reasons.append('ht_symmetry')
        else:
            minimum = 1.70 if market == 'over25_ht' else 1.35
            if not all(numeric(t.get('expected_goals')) for t in pair) or sum(t['expected_goals'] for t in pair) + 1e-9 < minimum:
                reasons.append('ht_expected')
            if not any(numeric(t.get('frequency')) and t['frequency'] + 1e-9 >= .75 for t in pair):
                reasons.append('ht_frequency')
    return reasons


def combo_status(row, rows):
    data = row.get('goal_evidence') or {}
    pair = [(data.get('teams', {}).get(s) or {}) for s in ('home', 'away')]
    known = all(numeric(t.get(p, {}).get('expected_goals')) for t in pair for p in ('ft', 'ht'))
    ht = sum(t['ht']['expected_goals'] for t in pair) if known else None
    sh = max(.30, sum(t['ft']['expected_goals'] for t in pair)-ht) if known else None
    references = [r for r in rows if (r.get('spec') or {}).get('kind') in ('total', 'asian_total')
                  and r['spec'].get('period') == 'ft' and r['spec'].get('side') == 'over' and r['spec'].get('line') == 2.5]
    # Use the displayed ordinary market first; never cherry-pick a more positive OCI.
    ref = min(references, key=lambda r: (r['spec']['kind'] != 'total', str(r.get('id'))), default={})
    oci = ref.get('oci')
    reasons = []
    if not numeric(row.get('statistical_pct')) or row['statistical_pct'] + 1e-9 < 56: reasons.append('combo_probability')
    if not known or ht + 1e-9 < 1.15 or sh + 1e-9 < 1.40: reasons.append('combo_rates')
    if not all(numeric(t.get('ft', {}).get('goalless_matches')) and t['ft']['goalless_matches'] <= 1 for t in pair): reasons.append('combo_goalless')
    if not numeric(oci) or oci < -10: reasons.append('combo_oci')
    return {'active': not reasons, 'status': 'Combo Attiva' if not reasons else 'Sotto soglia',
            'reasons': reasons, 'lambda_ht': ht, 'lambda_sh': sh, 'over25_ft_oci': oci,
            'reference_market_id': ref.get('id'), 'price_available': False,
            'assumption': 'independent_disjoint_halves', 'policy_id': 'gold-goals-v6.1'}
