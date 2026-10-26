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

MODEL_ID = 'lateral-matrix-v5.0'
CONFIG = {'tail_tolerance': 1e-11, 'score_ceiling': 48,
          'minimum_team_ft_for_value': 4, 'minimum_team_ht_for_value': 3,
          'maximum_selections': None, 'book_weight_in_statistics': 0,
          'probability_bonus_from_oci': 0, 'experimental': True}
CONFIG_ID = sha256(json.dumps(CONFIG, sort_keys=True).encode()).hexdigest()[:16]
PERIODS = {'ft': 'FT', 'ht': 'PT', 'sh': 'ST'}
FAMILIES = {'result': '1x2', 'dc': '1x2', 'dnb': 'asian', 'total': 'totals',
            'team_total': 'totals', 'btts': 'btts', 'asian_total': 'asian',
            'handicap': 'asian', 'result_total': 'combo', 'dc_total': 'combo',
            'result_btts': 'combo', 'dc_btts': 'combo', 'btts_total': 'combo',
            'htft': 'combo', 'halves_total': 'combo', 'range': 'ranges',
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
    if k == 'halves_total': return f"Over {line} PT + Over {float(extra):g} ST"
    if k == 'range': return f'Multigol {line}–{extra} {p}'
    if k == 'score': return f'Risultato {s} {p}'
    if k == 'both_halves': return f"{'Casa' if s == 'home' else 'Ospite' if s == 'away' else 'Entrambe'} a segno in entrambi i tempi"
    return k


def base_specs():
    rows = []
    def add(kind, period, side, line=None, extra=None):
        rows.append(specification(kind, period, side, line, extra))
    for period in ('ft', 'ht', 'sh'):
        for side in ('1', 'x', '2'):
            add('result', period, side)
        for side in ('1x', 'x2', '12'):
            add('dc', period, side)
        for side in ('1', '2'):
            add('dnb', period, side)
        for side in ('yes', 'no'):
            add('btts', period, side)
        for line in ([.5, 1.5, 2.5, 3.5, 4.5, 5.5] if period == 'ft' else [.5, 1.5, 2.5, 3.5]):
            for side in ('over', 'under'):
                add('total', period, side, line)
        for line in ([1.75, 2, 2.25, 2.75, 3, 3.25] if period == 'ft' else [.75, 1, 1.25, 1.75, 2, 2.25]):
            for side in ('over', 'under'):
                add('asian_total', period, side, line)
        for team in ('home', 'away'):
            for line in ([.5, 1.5, 2.5] if period == 'ft' else [.5, 1.5]):
                for side in ('over', 'under'):
                    add('team_total', period, side, line, team)
        for line in (-1, -.75, -.25, .25, .75, 1):
            for team in ('home', 'away'):
                add('handicap', period, team, line)
    for period in ('ft', 'ht'):
        for line in ([1.5, 2.5, 3.5] if period == 'ft' else [.5, 1.5]):
            for side in ('1', 'x', '2', '1x', 'x2', '12'):
                for direction in ('over', 'under'):
                    add('dc_total' if len(side) == 2 else 'result_total', period, side, line, direction)
            for side in ('yes', 'no'):
                for direction in ('over', 'under'):
                    add('btts_total', period, side, line, direction)
        for side in ('1', 'x', '2', '1x', 'x2', '12'):
            for gg in ('yes', 'no'):
                add('dc_btts' if len(side) == 2 else 'result_btts', period, side, extra=gg)
        for low, high in ([(1, 3), (2, 4), (2, 5), (3, 5), (3, 6)] if period == 'ft' else [(1, 2), (1, 3), (2, 3)]):
            add('range', period, '', low, high)
    for ht in ('1', 'x', '2'):
        for ft in ('1', 'x', '2'):
            add('htft', 'ft', ht, extra=ft)
    for ht, sh in ((.5, .5), (1.5, .5), (1.5, 1.5)):
        add('halves_total', 'ft', '', ht, sh)
    for team in ('home', 'away', 'both'):
        add('both_halves', 'ft', team)
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
    elif k == 'both_halves':
        home, away = hh > 0 and h-hh > 0, ah > 0 and a-ah > 0
        yes = home if side == 'home' else away if side == 'away' else home and away
    else: raise ValueError('Unsupported score-market kind: '+k)
    return (1., 0.) if yes else (0., 1.)


class ScoreModel:
    def __init__(self, rates):
        self.rates = rates
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
        if kind in ('halves_total', 'both_halves'):
            if kind == 'halves_total':
                a = sum(p for h, away, p in self.grids['ht'] if h+away > spec['line'])
                b = sum(p for h, away, p in self.grids['sh'] if h+away > spec['extra'])
                chance = a*b  # Disjoint increments only.
            else:
                r = self.rates
                h = (1-exp(-r['home_ht']))*(1-exp(-(r['home_ft']-r['home_ht'])))
                a = (1-exp(-r['away_ht']))*(1-exp(-(r['away_ft']-r['away_ht'])))
                chance = h if spec['side'] == 'home' else a if spec['side'] == 'away' else h*a
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


def quality(assessment, period):
    period = 'ht' if period == 'sh' else period
    profiles = assessment.get('profiles') or {}
    counts = [profiles.get(side, {}).get('for_'+period, {}).get('sample', 0) for side in ('home', 'away')]
    minimum = min(counts)
    if not max(counts): return 'prior_only'
    if not minimum: return 'prior_dominant'
    return 'supported' if minimum >= CONFIG['minimum_team_'+period+'_for_value'] else 'limited'


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
    need_ht = spec['period'] != 'ft' or spec['kind'] in ('htft', 'halves_total', 'both_halves')
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
    specs = {market_id(s): s for s in [*base_specs(), *additional_specs]}
    # The most likely exact scores are useful summaries; any actually priced
    # exact score is also added by the quote adapter without a display cap.
    for period in ('ft', 'ht'):
        for h, a, _ in sorted(model.grids[period], key=lambda x: -x[2])[:6]:
            s = specification('score', period, f'{h}:{a}')
            specs[market_id(s)] = s
    matrix = matrix_rows(output, specs, model, scenarios)
    output['matrix'] = {'schema_version': 1, 'as_of': as_of, 'rows': matrix,
        'coverage': {'modeled_markets': len(matrix), 'priced_markets': 0},
        'score_tail_probability': model.tail, 'top_scores': {p: [
            {'score': f'{h}:{a}', 'probability_pct': round(prob*100, 4)}
            for h, a, prob in sorted(model.grids[p], key=lambda x: -x[2])[:6]] for p in ('ft', 'ht')},
        'assumptions': ['Poisson per squadra con incrementi PT/ST indipendenti.',
            'Combo sullo stesso punteggio congiunto; nessun prodotto delle quote delle singole gambe.',
            'Frequenze storiche descrittive; stime e rendimenti teorici ancora da validare.']}
    output['notes'] = [*output.get('notes', []), 'Tutte le partite disponibili vengono mostrate, anche con dati o quote insufficienti.']
    return output


def matrix_rows(assessment, specs, model=None, scenarios=None):
    model = model or ScoreModel(assessment['rates'])
    if scenarios is None:
        scenarios = [ScoreModel(r) for r in assessment.get('sensitivity_rates', [])]
    matrix = []
    for key, spec in specs.items():
        estimate = model.estimate(spec)
        alternatives = [estimate, *[s.estimate(spec) for s in scenarios]]
        row = {'id': key, 'spec': spec, 'family': FAMILIES[spec['kind']], 'label': label(spec),
               'period': spec['period'], 'quality': quality(assessment, 'ht' if spec['kind'] in ('htft', 'halves_total', 'both_halves') else spec['period']),
               'statistical_pct': round(estimate['p']*100, 4),
               'low_pct': round(min(v['p'] for v in alternatives)*100, 4),
               'high_pct': round(max(v['p'] for v in alternatives)*100, 4),
               **{k: round(v, 8) if v is not None else None for k, v in estimate.items() if k != 'p'},
               # Settlement weights preserve quarter-line refunds in EV bounds.
               'sensitivity_weights': [list(v) for v in sorted({(round(v['win_weight'], 8), round(v['loss_weight'], 8)) for v in alternatives})],
               'empirical': empirical(assessment, spec), 'quote_status': 'missing', 'category': 'no_quote'}
        matrix.append(row)
    return matrix
