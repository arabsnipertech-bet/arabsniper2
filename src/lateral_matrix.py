"""Independent, joint score probabilities for the entire available fixture list.

No quote, bookmaker, price movement or selection limit enters this module.
HT and second-half increments share the FT score; overlapping combo legs are
evaluated as one joint event, never multiplied as independent probabilities.
"""
from collections import defaultdict
from copy import deepcopy
from hashlib import sha256
from math import exp, floor
import json
from market_scope import MARKET_SCOPE_ID, TOTAL_LINES, ASIAN_FT, ASIAN_HT, HANDICAPS, COMBO_SPECS, JOINT_KINDS, core_market

MODEL_ID = 'lateral-matrix-v5.0'
CONFIG = {'tail_tolerance': 1e-11, 'score_ceiling': 48,
          'minimum_team_ft_for_value': 4, 'minimum_team_ht_for_value': 3,
          'maximum_selections': None, 'book_weight_in_statistics': 0,
          'probability_bonus_from_oci': 0, 'market_scope': MARKET_SCOPE_ID}
CONFIG_ID = sha256(json.dumps(CONFIG, sort_keys=True).encode()).hexdigest()[:16]
PERIODS = {'ft': 'FT', 'ht': 'PT', 'sh': 'ST'}
FAMILIES = {'result': '1x2', 'dc': '1x2', 'dnb': 'asian', 'total': 'totals',
            'team_total': 'totals', 'btts': 'btts', 'asian_total': 'asian',
            'handicap': 'asian', 'result_total': 'combo', 'dc_total': 'combo',
            'result_btts': 'combo', 'dc_btts': 'combo', 'btts_total': 'combo',
            'htft': 'combo', 'halves_total': 'combo', 'pt_ft_total': 'combo',
            'pt_btts_ft_total': 'combo', 'range': 'ranges',
            'score': 'scores', 'both_halves': 'combo'}


def specification(kind, period='ft', side='', line=None, extra=None):
    value = {'kind': kind, 'period': period, 'side': side}
    if line is not None:
        value['line'] = float(line)
    if extra is not None:
        value['extra'] = extra
    return value


def market_id(spec):
    return json.dumps(spec, sort_keys=True, separators=(',', ':'))


def label(spec):
    k, p, s = spec['kind'], PERIODS[spec['period']], spec['side']
    line = f"{spec.get('line', 0):g}".replace('.', ',')
    extra = spec.get('extra', '')
    over = 'Over' if s == 'over' else 'Under'
    if k == 'result': return f'{s.upper()} {p}'
    if k == 'dc': return f'{s.upper()} doppia chance {p}'
    if k == 'dnb': return f'{s} rimborso pareggio {p}'
    if k in ('total', 'asian_total'): return f'{over} {line} {p}' + (' asiatico' if k == 'asian_total' else '')
    if k == 'team_total': return f"{'Casa' if extra == 'home' else 'Ospite'} {over.lower()} {line} {p}"
    if k == 'btts': return f"{'GG' if s == 'yes' else 'No Goal'} {p}"
    if k == 'handicap': return f"{'Casa' if s == 'home' else 'Ospite'} AH {spec['line']:+g} {p}"
    if k in ('result_total', 'dc_total'): return f"{s.upper()} + {'Over' if extra == 'over' else 'Under'} {line} {p}"
    if k in ('result_btts', 'dc_btts'): return f"{s.upper()} + {'GG' if extra == 'yes' else 'No Goal'} {p}"
    if k == 'btts_total': return f"{'GG' if s == 'yes' else 'No Goal'} + {'Over' if extra == 'over' else 'Under'} {line} {p}"
    if k == 'htft': return f'{s.upper()}/{str(extra).upper()} parziale/finale'
    if k == 'halves_total': return f"Over {line} PT + Over {str(float(extra)).replace('.', ',')} ST"
    if k == 'pt_ft_total': return f"Over {line} PT + Over {str(float(extra)).replace('.', ',')} FT"
    if k == 'pt_btts_ft_total': return f'GG PT + Over {line} FT'
    if k == 'range': return f'Multigol {line}–{extra} {p}'
    if k == 'score': return f'Risultato {s} {p}'
    if k == 'both_halves': return 'GG PT + GG ST' if s == 'both' else f"{'Casa' if s == 'home' else 'Ospite'} a segno in entrambi i tempi"
    return k


def base_specs():
    rows = [specification('result', 'ft', side) for side in ('1', 'x', '2')]
    rows += [specification('dnb', 'ft', side) for side in ('1', '2')]
    for period, lines in TOTAL_LINES.items():
        rows += [specification('btts', period, side) for side in ('yes', 'no')]
        rows += [specification('total', period, side, line)
                 for line in lines for side in ('over', 'under')]
        rows += [specification('asian_total', period, side, line)
                 for line in (ASIAN_FT if period == 'ft' else ASIAN_HT if period == 'ht' else ()) for side in ('over', 'under')]
    rows += [specification('handicap', 'ft', side, line)
             for line in HANDICAPS for side in ('home', 'away')]
    rows.extend(dict(spec) for spec in COMBO_SPECS)
    return rows


def poisson(rate):
    values = [exp(-rate)]
    for n in range(1, CONFIG['score_ceiling'] + 1):
        values.append(values[-1] * rate / n)
        if n > rate and 1-sum(values) < CONFIG['tail_tolerance']:
            break
    return values


def result_side(h, a):
    return '1' if h > a else '2' if h < a else 'x'


def quarter_parts(line):
    twice = line*2
    if abs(twice-round(twice)) < 1e-8:
        return (line,)
    return (floor(twice)/2, (floor(twice)+1)/2)


def stakes(delta):
    return (1., 0.) if delta > 1e-8 else (0., 1.) if delta < -1e-8 else (0., 0.)


def payout_weights(spec, h, a, hh=None, ah=None):
    """Fractions of stake winning/losing; remaining fraction is refunded."""
    k, side = spec['kind'], spec['side']
    line, extra = spec.get('line'), spec.get('extra')
    result, total, gg = result_side(h, a), h+a, h > 0 and a > 0
    ou = lambda direction: total > line if direction == 'over' else total < line
    if k in ('asian_total', 'handicap'):
        values = [stakes((total-v)*(1 if side == 'over' else -1)) if k == 'asian_total'
                  else stakes((h-a if side == 'home' else a-h)+v) for v in quarter_parts(line)]
        return tuple(sum(x[i] for x in values)/len(values) for i in (0, 1))
    if k == 'dnb': return (0., 0.) if result == 'x' else (1., 0.) if result == side else (0., 1.)
    if k == 'result': yes = result == side
    elif k == 'dc': yes = result in side
    elif k == 'total': yes = ou(side)
    elif k == 'team_total': yes = (h if extra == 'home' else a) > line if side == 'over' else (h if extra == 'home' else a) < line
    elif k == 'btts': yes = gg == (side == 'yes')
    elif k in ('result_total', 'dc_total'): yes = result in side and ou(extra)
    elif k in ('result_btts', 'dc_btts'): yes = result in side and gg == (extra == 'yes')
    elif k == 'btts_total': yes = gg == (side == 'yes') and ou(extra)
    elif k == 'range': yes = line <= total <= extra
    elif k == 'score': yes = f'{h}:{a}' == side
    elif k == 'htft': yes = result_side(hh, ah) == side and result == extra
    elif k == 'halves_total': yes = hh+ah > line and h+a-hh-ah > extra
    elif k == 'pt_ft_total': yes = hh+ah > line and total > extra
    elif k == 'pt_btts_ft_total': yes = hh > 0 and ah > 0 and total > line
    elif k == 'both_halves':
        home, away = hh > 0 and h-hh > 0, ah > 0 and a-ah > 0
        yes = home if side == 'home' else away if side == 'away' else home and away
    else: raise ValueError('Unsupported score-market kind: '+k)
    return (1., 0.) if yes else (0., 1.)


class ScoreModel:
    def __init__(self, rates, *, second_half_floor=.30):
        self.rates = rates
        self.second_half_floor = second_half_floor
        self.grids = {}
        self.tail = {}
        for period in ('ft', 'ht', 'sh'):
            h = rates['home_'+period] if period != 'sh' else rates['home_ft']-rates['home_ht']
            a = rates['away_'+period] if period != 'sh' else rates['away_ft']-rates['away_ht']
            hp, ap = poisson(max(0., h)), poisson(max(0., a))
            self.grids[period] = [(i, j, x*y) for i, x in enumerate(hp) for j, y in enumerate(ap)]
            self.tail[period] = max(0., 1-sum(hp)*sum(ap))
        self.htft = None

    def settlement_distribution(self, spec):
        bins = defaultdict(float)
        kind, period = spec['kind'], spec['period']
        if kind == 'htft':
            if self.htft is None:
                self.htft = defaultdict(float)
                # Only 9 outcome cells are stored, not a 4-D grid per market.
                diffs = {period: defaultdict(float) for period in ('ht', 'sh')}
                for period in diffs:
                    for h, a, p in self.grids[period]: diffs[period][h-a] += p
                for first, p in diffs['ht'].items():
                    for second, q in diffs['sh'].items():
                        self.htft[(result_side(first, 0), result_side(first+second, 0))] += p*q
            chance = self.htft[(spec['side'], spec['extra'])]
            return {(1., 0.): chance, (0., 1.): 1-chance}
        if kind in ('pt_ft_total', 'pt_btts_ft_total'):
            def qualifies(h, a):
                return h+a > spec['line'] if kind == 'pt_ft_total' else h > 0 and a > 0
            target = spec['extra'] if kind == 'pt_ft_total' else spec['line']
            second = defaultdict(float)
            for h, a, p in self.grids['sh']:
                second[h+a] += p
            tails = {n: sum(p for goals, p in second.items() if goals > n)
                     for n in range(int(target)+1)}
            chance = sum(p * (1. if h+a > target else tails[int(target-h-a)])
                         for h, a, p in self.grids['ht'] if qualifies(h, a))
            return {(1., 0.): chance, (0., 1.): 1-chance}
        if kind in ('halves_total', 'both_halves'):
            if kind == 'halves_total':
                qualifies = lambda h, a: h+a > spec['line']
                second = sum(p for h, a, p in self.grids['sh'] if h+a > spec['extra'])
                if spec['extra'] == .5 and self.second_half_floor:
                    second = max(second, 1-exp(-self.second_half_floor))
            else:
                qualifies = lambda h, a: (h > 0 if spec['side'] == 'home' else a > 0
                                          if spec['side'] == 'away' else h > 0 and a > 0)
                second = sum(p for h, a, p in self.grids['sh'] if qualifies(h, a))
            chance = sum(p*second for h, a, p in self.grids['ht'] if qualifies(h, a))
            return {(1., 0.): chance, (0., 1.): 1-chance}
        for h, a, p in self.grids[period]:
            bins[payout_weights(spec, h, a)] += p
        return bins

    def estimate(self, spec):
        bins = self.settlement_distribution(spec)
        win = sum(w*p for (w, lost), p in bins.items())
        loss = sum(lost*p for (w, lost), p in bins.items())
        return {'p': sum(p for (w, lost), p in bins.items() if w > 0 and lost == 0),
                'win_weight': win, 'loss_weight': loss,
                'fair_odd': 1+loss/win if win > 1e-12 else None,
                'push_probability': bins.get((0., 0.), 0.),
                'half_win_probability': bins.get((.5, 0.), 0.),
                'half_loss_probability': bins.get((0., .5), 0.)}


def study_result_totals(model, side):
    double = {'1': '1x', '2': 'x2'}[side]
    for kind, leg in [('result_total', side), ('dc_total', double)]:
        spec = specification(kind, 'ft', leg, 2.5, 'over')
        yield spec, model.estimate(spec)['p']


def quality(assessment, period):
    if period == 'sh':
        teams = (assessment.get('observed_goal_means') or {}).get('teams') or {}
        counts = [teams.get(side, {}).get('st', {}).get('sample', 0) for side in ('home', 'away')]
    else:
        profiles = assessment.get('profiles') or {}
        counts = [profiles.get(side, {}).get('for_'+period, {}).get('sample', 0) for side in ('home', 'away')]
    minimum = min(counts)
    if not max(counts): return 'prior_only'
    if not minimum: return 'prior_dominant'
    return 'supported' if minimum >= (4 if period == 'ft' else 3) else 'limited'


def empirical(assessment, spec):
    """Descriptive past-event frequency; never another independent model vote."""
    observations = []
    seen = set()
    for rows in assessment.get('inputs', {}).get('histories', {}).values():
        for row in rows:
            fid = row.get('fixture_id')
            if fid in seen: continue
            seen.add(fid)
            gf, ga = row.get('team_goals_ft'), row.get('team_conceded_ft')
            ht, at = row.get('team_goals_ht'), row.get('team_conceded_ht')
            if row.get('side') == 'away': gf, ga, ht, at = ga, gf, at, ht
            result = {'status': 'FT', 'home_ft': gf, 'away_ft': ga, 'home_ht': ht, 'away_ht': at}
            settled = settle_spec(spec, result)
            if settled is not None: observations.append(settled[0])
    return {'n': len(observations), 'win_stake_pct': 100*sum(observations)/len(observations) if observations else None}


def settle_spec(spec, result):
    if result.get('status') not in ('FT', 'AET', 'PEN'): return None
    ft = [result.get('home_ft'), result.get('away_ft')]
    ht = [result.get('home_ht'), result.get('away_ht')]
    valid = lambda x: isinstance(x, (int, float)) and not isinstance(x, bool) and x >= 0
    need_ht = spec['period'] != 'ft' or spec['kind'] in JOINT_KINDS or spec['kind'] == 'htft'
    if not all(valid(x) for x in ft) or need_ht and not all(valid(x) for x in ht): return None
    if need_ht and any(h > f for h, f in zip(ht, ft)): return None
    h, a = ht if spec['period'] == 'ht' else [f-t for f, t in zip(ft, ht)] if spec['period'] == 'sh' else ft
    return payout_weights(spec, h, a, *ht)


def build_matrix(assessment, as_of, additional_specs=()):
    output = deepcopy(assessment)
    output.update(model_id=MODEL_ID, config_id=CONFIG_ID, captured_at=as_of)
    output['config'] = {**output.get('config', {}), **CONFIG, 'top_per_market': None}
    model = ScoreModel(output['rates'])
    scenarios = [ScoreModel(r) for r in output.get('sensitivity_rates', [])]
    specs = {market_id(s): s for s in [*base_specs(), *additional_specs] if core_market(s)}
    matrix = matrix_rows(output, specs, model, scenarios)
    output['matrix'] = {'schema_version': 1, 'as_of': as_of, 'rows': matrix,
        'coverage': {'modeled_markets': len(matrix), 'priced_markets': 0},
        'score_tail_probability': model.tail,
        'market_scope': {'id': MARKET_SCOPE_ID, 'modeled_markets': len(matrix)},
        'assumptions': ['Poisson per squadra con incrementi PT/ST indipendenti.',
            'Mercati PT, ST e FT, con quattro combinazioni di punteggio congiunto.',
            'Frequenze storiche descrittive; stime e rendimenti teorici ancora da validare.']}
    output['notes'] = [*output.get('notes', []), 'Tutte le partite disponibili vengono mostrate, anche con dati o quote insufficienti.']
    return output


def matrix_rows(assessment, specs, model=None, scenarios=None):
    model = model or ScoreModel(assessment['rates'])
    if scenarios is None:
        scenarios = [ScoreModel(r) for r in assessment.get('sensitivity_rates', [])]
    matrix = []
    for key, spec in specs.items():
        if not core_market(spec): continue
        estimate = model.estimate(spec)
        alternatives = [estimate, *[s.estimate(spec) for s in scenarios]]
        period = 'sh' if spec['kind'] in ('halves_total', 'both_halves') else 'ht' if spec['kind'] in JOINT_KINDS else spec['period']
        row = {'id': key, 'spec': spec, 'family': FAMILIES[spec['kind']], 'label': label(spec),
               'period': spec['period'], 'quality': quality(assessment, period),
               'statistical_pct': round(estimate['p']*100, 4),
               'low_pct': round(min(v['p'] for v in alternatives)*100, 4),
               'high_pct': round(max(v['p'] for v in alternatives)*100, 4),
               **{k: round(v, 8) if v is not None else None for k, v in estimate.items() if k != 'p'},
               # Settlement weights preserve quarter-line refunds in EV bounds.
               'sensitivity_weights': [list(v) for v in sorted({(round(v['win_weight'], 8), round(v['loss_weight'], 8)) for v in alternatives})],
               'empirical': empirical(assessment, spec), 'quote_status': 'missing', 'category': 'no_quote'}
        matrix.append(row)
    return matrix

