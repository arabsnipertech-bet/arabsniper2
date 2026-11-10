"""One bounded market universe for future Gold calculations and prices."""
from math import isfinite

MARKET_SCOPE_ID = 'v7-core-markets-v1'
HT_TOTALS = (.5, 1.5, 2.5, 3.5)
ST_TOTALS = (.5, 1.5, 2.5)
FT_TOTALS = (1.5, 2.5, 3.5)
TOTAL_LINES = {'ht': HT_TOTALS, 'sh': ST_TOTALS, 'ft': FT_TOTALS}
ASIAN_FT = (1.75, 2., 2.25, 2.75, 3., 3.25)
ASIAN_HT = (1., 1.25, 1.75, 2.)
HANDICAPS = (-1., -.75, -.5, -.25, 0., .25, .5, .75, 1.)
STUDY_RESULT_TOTAL_IDS = frozenset((25, 183))
CORE_BET_IDS = frozenset((1, 2, 4, 5, 6, 8, 26, 34, 35, 50, 72, 336))
COMBO_SPECS = (
    {'kind': 'halves_total', 'period': 'ft', 'side': 'over', 'line': .5, 'extra': .5},
    {'kind': 'pt_ft_total', 'period': 'ft', 'side': 'over', 'line': 1.5, 'extra': 2.5},
    {'kind': 'pt_btts_ft_total', 'period': 'ft', 'side': 'yes', 'line': 2.5},
    {'kind': 'both_halves', 'period': 'ft', 'side': 'both'},
)
JOINT_KINDS = frozenset(spec['kind'] for spec in COMBO_SPECS)


def core_market(spec):
    if not isinstance(spec, dict): return False
    kind, period, side, line = (spec.get(k) for k in ('kind', 'period', 'side', 'line'))
    if kind in JOINT_KINDS: return spec in COMBO_SPECS
    if period not in TOTAL_LINES or spec.get('extra') is not None: return False
    valid_line = isinstance(line, (int, float)) and not isinstance(line, bool) and isfinite(line)
    if kind == 'result': return period == 'ft' and side in ('1', 'x', '2') and line is None
    if kind == 'btts': return side in ('yes', 'no') and line is None
    if kind == 'dnb': return period == 'ft' and side in ('1', '2') and line is None
    if kind == 'total': return side in ('over', 'under') and valid_line and line in TOTAL_LINES[period]
    if kind == 'asian_total':
        # Half lines already modeled as ordinary totals remain available when
        # a provider uses its Asian catalogue ID for the same settlement.
        extra_lines = ASIAN_FT if period == 'ft' else ASIAN_HT if period == 'ht' else ()
        return side in ('over', 'under') and valid_line and line in extra_lines+TOTAL_LINES[period]
    if kind == 'handicap': return period == 'ft' and side in ('home', 'away') and valid_line and line in HANDICAPS
    return False


def trim_matrix(matrix):
    rows = matrix.get('rows', [])
    kept = [row for row in rows if core_market(row.get('spec'))]
    matrix['rows'] = kept
    matrix['market_scope'] = {'id': MARKET_SCOPE_ID, 'modeled_markets': len(kept)}
    matrix.pop('top_scores', None)
    matrix.setdefault('coverage', {})['modeled_markets'] = len(kept)
    return len(rows)-len(kept)

