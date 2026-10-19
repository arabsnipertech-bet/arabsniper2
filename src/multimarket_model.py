"""Experimental coherent pre-match goals model and operator-level quote ledger.

Parameters are fixed before prospective evaluation (20 October 2026 onward).
Market movements are recorded as features, never treated as independent money flows.
"""
from copy import deepcopy
from datetime import datetime
from math import exp, log, isfinite
from statistics import median
import re

MODEL_ID = 'coherent-markets-v2'
VALIDATION_FROM = '2026-10-20'
CONFIG = {'prior_penalty': .01, 'minimum_books': 2, 'max_quote_age_hours': 6,
          'minimum_reliability': 60, 'movement_probability_bonus': 0,
          'drought_probability_bonus': 0, 'validation_from': VALIDATION_FROM}

# IDs AND names AND exact lines must agree. Team totals are not match totals.
SPECS = {
    '1x2_ft': (1, 'Match Winner', None, ('1', 'x', '2'), 'Esito finale'),
    'over05_ht': (6, 'Goals Over/Under First Half', .5, ('over', 'under'), 'Over 0,5 PT'),
    'over15_ht': (6, 'Goals Over/Under First Half', 1.5, ('over', 'under'), 'Over 1,5 PT'),
    'over25_ht': (6, 'Goals Over/Under First Half', 2.5, ('over', 'under'), 'Over 2,5 PT'),
    'gg_ht': (34, 'Both Teams Score - First Half', None, ('yes', 'no'), 'GG PT sì'),
    'over15_ft': (5, 'Goals Over/Under', 1.5, ('over', 'under'), 'Over 1,5 finale'),
    'over25_ft': (5, 'Goals Over/Under', 2.5, ('over', 'under'), 'Over 2,5 finale'),
    'over35_ft': (5, 'Goals Over/Under', 3.5, ('over', 'under'), 'Over 3,5 finale'),
    'gg_ft': (8, 'Both Teams Score', None, ('yes', 'no'), 'GG finale sì'),
    'home15_ft': (16, 'Total - Home', 1.5, ('over', 'under'), 'Casa: almeno 2 gol'),
    'away15_ft': (17, 'Total - Away', 1.5, ('over', 'under'), 'Ospite: almeno 2 gol'),
    'over05_sh': (26, 'Goals Over/Under - Second Half', .5, ('over', 'under'), 'Over 0,5 ST'),
}
COMBO = 'over15_ht_over05_sh'
COMBOS = {
    'over05_ht_over05_sh': ('Over 0,5 PT + Over 0,5 ST', 1, 1),
    COMBO: ('Over 1,5 PT + Over 0,5 ST', 2, 1),
    'over15_ht_over15_sh': ('Over 1,5 PT + Over 1,5 ST', 2, 2),
}


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo else None
    except (ValueError, TypeError):
        return None


def number(value):
    try:
        value = float(value)
        return value if isfinite(value) else None
    except (ValueError, TypeError):
        return None


def normalized(value):
    return re.sub(r'[^a-z0-9]+', ' ', str(value).lower()).strip()


def fair(odds):
    total = sum(1 / v for v in odds.values())
    return {s: 1 / v / total for s, v in odds.items()}, (total - 1) * 100


def collect_quotes(payload, observed_at):
    """Use the existing odds response; never request additional odds endpoints."""
    now = stamp(observed_at)
    collected = {}
    if not now:
        return collected
    for response in payload.get('response', []):
        fid = str((response.get('fixture') or {}).get('id', ''))
        kickoff = stamp((response.get('fixture') or {}).get('date'))
        updated = stamp(response.get('update'))
        if not fid or updated and updated > now or kickoff and now >= kickoff:
            continue
        markets = collected.setdefault(fid, {})
        for book in response.get('bookmakers', []):
            if book.get('id') is None:
                continue
            for bet in book.get('bets', []):
                for key, (bid, name, line, sides, _label) in SPECS.items():
                    if str(bet.get('id')) != str(bid) or normalized(bet.get('name')) != normalized(name):
                        continue
                    odds = {}
                    ambiguous = False
                    for value in bet.get('values', []):
                        raw = str(value.get('value', '')).lower().strip().replace(',', '.')
                        if line is not None:
                            match = re.fullmatch(r'(over|under)\s+(\d+(?:\.\d+)?)', raw)
                            side = match.group(1) if match and float(match.group(2)) == line else None
                        else:
                            side = {'home': '1', 'draw': 'x', 'away': '2', 'yes': 'yes', 'no': 'no'}.get(raw)
                        odd = number(value.get('odd'))
                        if side in sides and odd and odd > 1:
                            if side in odds and odds[side] != odd:
                                ambiguous = True
                            odds[side] = odd
                    if ambiguous or set(odds) != set(sides):
                        continue
                    probabilities, overround = fair(odds)
                    row = {'bookmaker_id': str(book['id']), 'bookmaker': str(book.get('name') or book['id']),
                           'bet_id': bid, 'bet_name': name, 'line': line, 'odds': odds,
                           'fair': probabilities, 'overround_pct': overround,
                           'observed_at': observed_at, 'provider_updated_at': updated.isoformat() if updated else None}
                    markets.setdefault(key, {})[str(book['id'])] = row
    return collected


def update_ledger(state, fid, markets, observed_at, kickoff):
    now, start = stamp(observed_at), stamp(kickoff)
    if not now or not start or now >= start:
        return {}
    ledger = state.setdefault('multimarket_quotes', {}).setdefault(str(fid), {})
    result = {}
    for key, books in markets.items():
        result[key] = []
        for bid, row in books.items():
            history = ledger.setdefault(key, {}).setdefault(str(bid), [])
            if not history or (history[-1]['odds'], history[-1]['provider_updated_at']) != (row['odds'], row['provider_updated_at']):
                history.append(deepcopy(row))
            if len(history) > 100:
                history[:] = [history[0], *history[-99:]]
            first = history[0]
            paired = {'first_observed_at': first['observed_at'], 'first_odds': first['odds'],
                      'delta_fair_pp': {s: (row['fair'][s] - first['fair'][s]) * 100 for s in row['fair']},
                      'drop_pct': {s: (first['odds'][s] - row['odds'][s]) / first['odds'][s] * 100 for s in row['odds']},
                      'overround_delta_pp': row['overround_pct'] - first['overround_pct'],
                      'observations': len(history), 'opening_certified': False}
            result[key].append({**deepcopy(row), 'movement': paired})
    return result


def quote_benchmarks(quotes, as_of):
    now = stamp(as_of)
    result = {}
    for key, books in quotes.items():
        sides = SPECS[key][3]
        valid = []
        for b in books:
            observed = stamp(b.get('observed_at'))
            updated = stamp(b.get('provider_updated_at'))
            if (not now or not observed or observed > now or updated and updated > now
                    or not all(number(b.get('odds', {}).get(s)) and b['odds'][s] > 1 for s in sides)):
                continue
            valid.append(b)
        if not valid:
            continue
        # Normalize each complete operator first; never mix inverse median prices.
        values = {s: median(fair(b['odds'])[0][s] for b in valid) for s in sides}
        total = sum(values.values())
        result[key] = {'probabilities': {s: v / total for s, v in values.items()},
                       'books': valid, 'bookmakers_count': len(valid), 'source': 'operator_pairs'}
    return result


def legacy_benchmarks(match):
    """Historical aggregates are usable for research, never executable quotations."""
    result = {}
    for key, market in match.get('book', {}).get('markets', {}).items():
        if key not in SPECS:
            continue
        current = market.get('current', {})
        p = current.get('math', {}).get('fair_probabilities', {})
        sides = SPECS[key][3]
        if all(number(p.get(s)) is not None and 0 < p[s] < 1 for s in sides):
            total = sum(p[s] for s in sides)
            result[key] = {'probabilities': {s: p[s] / total for s in sides}, 'books': [],
                           'bookmakers_count': current.get('bookmakers_count', 0), 'source': 'historical_aggregate'}
    return result


def poisson(mean):
    values = [exp(-mean)]
    for n in range(1, 40):
        values.append(values[-1] * mean / n)
    return values


def goal_probabilities(rates):
    """Four independent half/team Poissons; all events share these same rates.

    The HT+SH conjunction is a joint event under this explicit increments model,
    not the product of overlapping marginal markets.
    """
    hh, ah, hs, ass = rates
    h, a, ht, sh = hh + hs, ah + ass, hh + ah, hs + ass
    hp, ap = poisson(h), poisson(a)
    draw = sum(x * y for x, y in zip(hp, ap))
    away_cdf, home_win = 0., 0.
    for i, value in enumerate(hp):
        home_win += value * away_cdf
        away_cdf += ap[i]
    one = lambda x: 1 - exp(-x)
    two = lambda x: 1 - exp(-x) * (1 + x)
    binary = lambda p, yes='over', no='under': {yes: p, no: 1 - p}
    return {'1x2_ft': {'1': home_win, 'x': draw, '2': max(0., 1 - draw - home_win)},
            'over05_ht': binary(one(ht)), 'over15_ht': binary(two(ht)),
            'over25_ht': binary(1-exp(-ht)*(1+ht+ht**2/2)),
            'over15_ft': binary(two(h+a)),
            'gg_ht': binary(one(hh) * one(ah), 'yes', 'no'),
            'over25_ft': binary(1 - exp(-(h+a)) * (1 + h+a + (h+a)**2 / 2)),
            'over35_ft': binary(1 - exp(-(h+a)) * (1 + h+a + (h+a)**2 / 2 + (h+a)**3 / 6)),
            'home15_ft': binary(two(h)), 'away15_ft': binary(two(a)),
            'gg_ft': binary(one(h) * one(a), 'yes', 'no'), 'over05_sh': binary(one(sh)),
            COMBO: binary(two(ht) * one(sh), 'yes', 'no'),
            'over05_ht_over05_sh': binary(one(ht)*one(sh), 'yes', 'no'),
            'over15_ht_over15_sh': binary(two(ht)*two(sh), 'yes', 'no')}


def fit_rates(prior, benchmarks, penalty):
    targets = {k: b['probabilities'] for k, b in benchmarks.items() if b['bookmakers_count'] >= CONFIG['minimum_books']}
    if not targets:
        return list(prior)
    initial = [log(max(.01, v)) for v in prior]
    def objective(point):
        probs = goal_probabilities([exp(v) for v in point])
        loss = sum(sum((probs[k][s]-p)**2 for s,p in t.items()) / len(t)
                   for k,t in targets.items()) / len(targets)
        return loss + penalty * sum((x-y)**2 for x,y in zip(point, initial)) / 4
    point = initial[:]
    best = objective(point)
    for step in (.5, .25, .125, .0625, .03125):
        for _ in range(3):
            changed = False
            for i in range(4):
                for sign in (-1, 1):
                    trial = point[:]
                    trial[i] = max(log(.01), min(log(6), trial[i] + sign * step))
                    score = objective(trial)
                    if score + 1e-12 < best:
                        point, best, changed = trial, score, True
            if not changed:
                break
    return [exp(v) for v in point]


def features(match):
    result = {}
    for side in ('home', 'away'):
        p = match.get('statistics', {}).get(side, {})
        ht = p.get('first_half', {})
        result[side] = {'sample': p.get('sample_all'), 'ht_sample': p.get('ht_sample_all'),
                        'metrics': deepcopy(p.get('metrics', {})),
                        'recent5_ht': deepcopy(ht.get('recent5', {})),
                        'trend_ht': deepcopy(ht.get('trend_recent5_vs_previous5', {})),
                        'drought': deepcopy(p.get('goal_drought', {}).get('all', {}))}
    result['legacy_1x2_pattern'] = deepcopy(match.get('model', {}).get('favorite_trap_pattern', {}))
    return result


def evaluate(match, as_of, quotes=None):
    model, meta = match.get('model', {}), match.get('fixture', {})
    layer = model.get('lambdas', {})
    base = layer.get('drought_layer', {}).get('baseline_lambdas') or layer
    values = [number(base.get(k)) for k in ('home_ht','away_ht','home_ft','away_ft')]
    if any(v is None or v < 0 for v in values):
        return {'model_id': MODEL_ID, 'status': 'unavailable', 'reason': 'lambda_assenti', 'markets': []}
    hh, ah, hf, af = values
    coherent_input = hh <= hf and ah <= af
    prior = [max(.01, min(hh,hf)), max(.01, min(ah,af)), max(.01,hf-hh), max(.01,af-ah)]
    benchmarks = quote_benchmarks(quotes or {}, as_of)
    if not quotes:
        benchmarks = legacy_benchmarks(match)
    rates = {'statistics': prior, 'market_projection': fit_rates(prior, benchmarks, .0001),
             'combined': fit_rates(prior, benchmarks, CONFIG['prior_penalty'])}
    probabilities = {name: goal_probabilities(r) for name,r in rates.items()}
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    is_prematch = bool(now and kickoff and now < kickoff and not meta.get('result'))
    ht_missing = any((match.get('statistics', {}).get(s, {}).get('ht_sample_all') or 0) < 6 for s in ('home','away'))
    markets = []
    for key in [*SPECS, *COMBOS]:
        sides = SPECS[key][3] if key in SPECS else ('yes','no')
        # Include all 1X2 outcomes, positive goals events and their complements.
        for side in sides:
            bench = benchmarks.get(key, {})
            p = probabilities['combined'][key][side]
            samples = [v[key][side] for v in probabilities.values()]
            low, high = min(samples), max(samples)
            books = bench.get('books', [])
            priced = sorted(books, key=lambda b: (b['odds'][side], b['bookmaker_id']))
            quote = deepcopy(priced[(len(priced)-1)//2]) if priced else None
            odd = quote['odds'][side] if quote else None
            updated = stamp(quote.get('provider_updated_at')) if quote else None
            age = (now-updated).total_seconds()/3600 if now and updated else None
            reasons = []
            if not is_prematch: reasons.append('partita_iniziata_o_orario_assente')
            if not coherent_input: reasons.append('lambda_tempi_incoerenti')
            if (model.get('reliability_pct') or 0) < CONFIG['minimum_reliability']: reasons.append('dati_insufficienti')
            if ('ht' in key or '_sh' in key) and ht_missing: reasons.append('campione_primo_tempo_insufficiente')
            if len(books) < CONFIG['minimum_books']: reasons.append('quote_operatori_insufficienti')
            if not quote: reasons.append('quota_specifica_assente')
            if age is None or age < 0 or age > CONFIG['max_quote_age_hours']: reasons.append('aggiornamento_quota_non_verificato')
            robust_ev = low*odd-1 if odd else None
            if robust_ev is not None and robust_ev <= 0: reasons.append('valore_non_concorde_tra_modelli')
            if high-low > .10: reasons.append('modelli_divergenti')
            label = SPECS[key][4] if key in SPECS else COMBOS[key][0]
            if key == '1x2_ft': label = {'1':'Vittoria casa', 'x':'Pareggio', '2':'Vittoria ospite'}[side]
            elif side in ('under','no'):
                label = label.replace('Over','Under').replace('sì','no').replace('almeno 2 gol','meno di 2 gol')
                if key in COMBOS: label = COMBOS[key][0] + ' — non realizzata'
            movements = [b.get('movement', {}) for b in books if b.get('movement', {}).get('observations', 0) >= 2]
            markets.append({'market':key, 'selection':side, 'label':label,
                'probability_pct':round(p*100,3), 'statistical_pct':round(probabilities['statistics'][key][side]*100,3),
                'market_projection_pct':round(probabilities['market_projection'][key][side]*100,3),
                'book_probability_pct':round(bench['probabilities'][side]*100,3) if bench else None,
                'model_range_pct':[round(low*100,3),round(high*100,3)],
                'range_note':'dispersione tra modelli; non intervallo di confidenza',
                'odd':odd,'quote':quote,'bookmakers_count':len(books),
                'quote_age_hours':round(age,3) if age is not None else None,
                'expected_value_pct':round((p*odd-1)*100,3) if odd else None,
                'conservative_ev_pct':round(robust_ev*100,3) if robust_ev is not None else None,
                'research_candidate':not reasons, 'reasons':reasons,
                'movement':{'paired_books':len(movements),
                    'median_drop_pct':median(m['drop_pct'][side] for m in movements) if movements else None,
                    'median_fair_delta_pp':median(m['delta_fair_pp'][side] for m in movements) if movements else None,
                    'median_overround_delta_pp':median(m['overround_delta_pp'] for m in movements) if movements else None},
                'legacy_oci':match.get('book',{}).get('markets',{}).get(key,{}).get('selections',{}).get(side,{}).get('oci_as',{}).get('value'),
                'probabilities':{name: round(v[key][side],8) for name,v in probabilities.items()}})
    candidates = [m for m in markets if m['research_candidate']]
    candidates.sort(key=lambda m: (-m['conservative_ev_pct'], -m['probability_pct'],m['market'],m['selection']))
    return {'model_id':MODEL_ID,'status':'experimental','captured_at':as_of,'config':dict(CONFIG),
            'validation_period':'prospective' if kickoff and kickoff.date().isoformat() >= VALIDATION_FROM else 'development',
            'coherent_input':coherent_input,'rates':rates,'features':features(match),
            'book_benchmarks': {k:{'probabilities':b['probabilities'],'source':b['source'],'bookmakers_count':b['bookmakers_count']} for k,b in benchmarks.items()},
            'quotes':deepcopy(quotes or {}),'markets':markets,'best_candidate':deepcopy(candidates[0]) if candidates else None,
            'notes':['Movimenti e digiuno registrati senza bonus automatici.',
                     'Stime sperimentali: accuratezza e rendimento da verificare su giornate successive.',
                     'Quote osservate presso operatori; disponibilità effettiva da confermare.']}


def freeze_evaluations(store, output, as_of):
    from signal_research import capture as capture_signals
    capture_signals(store, output, as_of)
    now = stamp(as_of)
    for day in output.get('days', []):
        for match in day.get('matches', []):
            meta = match.get('fixture', {})
            kickoff = stamp(meta.get('kickoff'))
            assessment = match.get('multimarket') or {}
            if not now or not kickoff or now >= kickoff or meta.get('result') or assessment.get('status') != 'experimental':
                continue
            key = f"{assessment['model_id']}::{meta['fixture_id']}"
            store.setdefault('multimarket_evaluations', {}).setdefault(key, deepcopy({
                'fixture':meta,'captured_at':as_of,'model_version':assessment['model_id'],
                'multimarket':assessment}))
    return store

