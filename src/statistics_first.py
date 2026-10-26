"""Statistics-first v4: robust goal potential, abstention, then quote confirmation.

The coefficients below are fixed engineering assumptions, not fitted accuracy.
No bookmaker value enters the statistical estimate, its sensitivity or ranking.
Goals droughts are observations, never a debt that the next match must repay.
"""
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from math import exp, sqrt, isfinite
from statistics import mean, median
import json

from multimarket_model import SPECS, stamp, goal_probabilities, quote_benchmarks
from context_analysis import movement

MODEL_ID = 'statistics-first-v4.0'
VALIDATION_FROM = '2026-10-26'
TARGETS = {'over25_ft': 'over', 'over15_ht': 'over',
           'over35_ft': 'over', 'gg_ht': 'yes'}
LABELS = {'over25_ft': 'Over 2,5 finale', 'over15_ht': 'Over 1,5 primo tempo',
          'over35_ft': 'Over 3,5 finale', 'gg_ht': 'Entrambe a segno nel primo tempo'}
CONFIG = {
    'history_ft': 8, 'history_ht': 4, 'minimum_ft': 6, 'minimum_ht': 4,
    'minimum_league_ft': 12, 'minimum_league_ht': 8, 'context_weight': .60,
    'prior_matches_ft': 4, 'prior_matches_ht': 3, 'xg_max_weight': .60,
    'minimum_xg': 3, 'robust_mad_scale': 2.5, 'h2h_max_age_days': 730,
    'h2h_max_rate_change': .08, 'minimum_uplift_pp': 5,
    'top_per_market': 5, 'minimum_drop_pct': 3,
    'max_quote_age_hours': 6, 'near_kickoff_quote_age_hours': 1,
    'book_weight_in_statistics': 0, 'movement_probability_bonus': 0,
    'drought_probability_bonus': 0, 'experimental': True,
}
CONFIG_ID = sha256(json.dumps(CONFIG, sort_keys=True).encode()).hexdigest()[:16]
HISTORY_FIELDS = ('fixture_id', 'kickoff', 'date', 'season', 'side', 'league_id',
                  'home_id', 'away_id', 'friendly', 'halftime_available',
                  'research_ft', 'team_goals_ft', 'team_conceded_ft',
                  'team_goals_ht', 'team_conceded_ht', 'team_xg_for',
                  'team_xg_against', 'team_shots_on_target_for',
                  'team_shots_on_target_against', 'team_xg_for_ht',
                  'team_xg_against_ht')


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def history(rows, as_of, season=None):
    """Only real pre-cutoff official records, with regulation-time scores."""
    now = stamp(as_of)
    selected = []
    if now is None:
        return []
    for item in rows:
        when = stamp(item.get('kickoff'))
        if not when or when >= now or item.get('fixture_id') is None:
            continue
        if item.get('friendly') or 'friendl' in str(item.get('league', '')).lower():
            continue
        if season is not None and str(item.get('season')) != str(season):
            continue
        ft = item.get('research_ft') or {}
        a, d = ft.get('scored'), ft.get('conceded')
        if not all(numeric(x) and x >= 0 for x in (a, d)):
            continue
        row = {k: deepcopy(item[k]) for k in HISTORY_FIELDS if k in item}
        row.update(team_goals_ft=a, team_conceded_ft=d)
        selected.append(row)
    selected.sort(key=lambda r: (stamp(r['kickoff']), str(r['fixture_id'])), reverse=True)
    unique = {}
    for row in selected:
        unique.setdefault(str(row['fixture_id']), row)
    return list(unique.values())[:CONFIG['history_ft']]


def period_rows(rows, period):
    if period == 'ft':
        return rows
    return [r for r in rows if r.get('halftime_available') is True and
            all(numeric(r.get(k)) and 0 <= r[k] <= r[k.replace('_ht', '_ft')] for k in
                ('team_goals_ht', 'team_conceded_ht'))][:CONFIG['history_ht']]


def league_prior(league, side, period):
    key = f'avg_{side}_goals' if period == 'ft' else f'avg_{side}_ht'
    fallback = {'home_ft': 1.45, 'away_ft': 1.15, 'home_ht': .68, 'away_ht': .52}
    value = league.get(key)
    count = league.get('sample' if period == 'ft' else 'ht_sample', 0)
    valid = numeric(value) and value >= 0 and numeric(count) and count > 0
    return (max(.05, value) if valid else fallback[f'{side}_{period}'],
            int(count) if valid else 0)


def weighted(values, rows, side):
    """60/40 home/away context only when both groups have real observations."""
    if not values:
        return None
    weights = [2 ** (-i / 4) for i in range(len(rows))]
    own = [i for i, r in enumerate(rows) if r.get('side') == side]
    other = [i for i in range(len(rows)) if i not in own]
    if own and other:
        for indices, share in ((own, CONFIG['context_weight']),
                               (other, 1-CONFIG['context_weight'])):
            total = sum(weights[i] for i in indices)
            for i in indices:
                weights[i] = share*weights[i]/total
    total = sum(weights)
    return sum(v*w for v, w in zip(values, weights))/total


def robust(values):
    """Limit exceptional influence; retain every original score in inputs."""
    if len(values) < 3:
        return list(values), None
    center = median(values)
    mad = median(abs(v-center) for v in values)*1.4826
    scale = max(mad, sqrt(max(center, .5)))
    cap = center+CONFIG['robust_mad_scale']*scale
    return [min(v, cap) for v in values], cap


def component(rows, side, direction, period, prior, use_xg=True, use_robust=True):
    pool = period_rows(rows, period)
    field = f'team_{"goals" if direction == "for" else "conceded"}_{period}'
    pool = [r for r in pool if numeric(r.get(field))]
    values = [float(r[field]) for r in pool]
    if not values:
        return {'rate': prior, 'sample': 0, 'context_sample': 0,
                'mean': None, 'last': None, 'without_last': None,
                'xg_sample': 0, 'xg_mean': None, 'xg_adjustment': 0.,
                'drought': 0, 'under_conversion': False, 'pressure': False,
                'peak_reduction': 0., 'last_state': 'unavailable', 'prior_weight': 1.}
    trimmed, cap = robust(values) if use_robust else (values, None)
    raw = weighted(values, pool, side)
    goal_rate = weighted(trimmed, pool, side)
    # Halftime accepts halftime xG only. Fulltime xG never generates an HT bonus.
    xfield = f'team_xg_{direction}'+('_ht' if period == 'ht' else '')
    paired = [r for r in pool if numeric(r.get(xfield)) and r[xfield] >= 0]
    xvalues, _ = robust([r[xfield] for r in paired])
    xmean = weighted(xvalues, paired, side) if paired else None
    xweight = (CONFIG['xg_max_weight']*len(paired)/len(pool)
               if use_xg and len(paired) >= CONFIG['minimum_xg'] else 0.)
    potential = (1-xweight)*goal_rate+xweight*xmean if xweight else goal_rate
    pseudo = CONFIG['prior_matches_'+period]
    prior_weight = pseudo/(len(pool)+pseudo)
    rate = (1-prior_weight)*potential+prior_weight*prior
    last = values[0]
    reference = mean(values[1:]) if len(values) > 1 else None
    deviation = last-reference if reference is not None else None
    drought = 0
    for value in values:
        if value != 0:
            break
        drought += 1
    paired_goal_mean = weighted([r[field] for r in paired], paired, side) if paired else None
    under = bool(xweight and xmean > paired_goal_mean+.25)
    # Shots measure supporting pressure, not interchangeable goal units.
    shots_field = f'team_shots_on_target_{direction}'+('_ht' if period == 'ht' else '')
    recent = pool[:3]
    shots = [r[shots_field] for r in recent if numeric(r.get(shots_field))]
    pressure = bool(under or len(shots) >= 2 and mean(shots) >= 4)
    return {'rate': rate, 'sample': len(pool),
            'context_sample': sum(r.get('side') == side for r in pool),
            'mean': mean(values), 'context_mean': mean([r[field] for r in pool if r.get('side') == side]) if any(r.get('side') == side for r in pool) else None,
            'weighted_mean': raw, 'robust_mean': goal_rate, 'robust_cap': cap,
            'last': last, 'without_last': reference, 'last_deviation': deviation,
            'last_state': 'above' if deviation is not None and deviation > .75 else
                          'below' if deviation is not None and deviation < -.75 else 'in_line',
            'peak_reduction': raw-goal_rate,
            'xg_mean': xmean, 'xg_sample': len(paired), 'xg_weight': xweight,
            'xg_adjustment': potential-goal_rate,
            'prior_weight': prior_weight, 'drought': drought,
            'under_conversion': under, 'pressure': pressure,
            'shots_mean': mean(shots) if shots else None,
            'frequency': mean(v > 0 for v in values)}


def valid_h2h(stats, meta, as_of, histories):
    now = stamp(as_of)
    seen = set()
    overlapping = {str(r['fixture_id']) for rows in histories.values() for r in rows}
    expected = {str(meta.get('home_id')), str(meta.get('away_id'))}
    eligible = []
    excluded_overlap = 0
    for row in (stats.get('h2h') or {}).get('matches', []):
        when, fid = stamp(row.get('kickoff')), str(row.get('fixture_id'))
        if (not now or not when or when >= now or now-when > timedelta(days=CONFIG['h2h_max_age_days'])
                or row.get('friendly') or row.get('fixture_id') is None or fid in seen):
            continue
        if None not in (meta.get('home_id'), meta.get('away_id')) and {str(row.get('home_id')), str(row.get('away_id'))} != expected:
            continue
        seen.add(fid)
        if fid in overlapping:
            excluded_overlap += 1
            continue
        if not numeric(row.get('total_ft')):
            continue
        eligible.append({k: row[k] for k in ('fixture_id', 'kickoff', 'home_id', 'away_id',
                         'total_ft', 'total_ht', 'halftime_available') if k in row})
    eligible.sort(key=lambda r: stamp(r['kickoff']), reverse=True)
    return eligible[:5], excluded_overlap


def rates_for(histories, league, h2h, as_of, use_xg=True, use_robust=True):
    profiles, rates = {}, {}
    for side in ('home', 'away'):
        other = 'away' if side == 'home' else 'home'
        profiles[side] = {}
        for period in ('ft', 'ht'):
            attack_prior, _ = league_prior(league, side, period)
            defense_prior, _ = league_prior(league, other, period)
            profiles[side]['for_'+period] = component(histories[side], side, 'for', period, attack_prior, use_xg, use_robust)
            profiles[side]['against_'+period] = component(histories[side], side, 'against', period, defense_prior, use_xg, use_robust)
    adjustments = {}
    for period in ('ft', 'ht'):
        for side, other in (('home', 'away'), ('away', 'home')):
            a = profiles[side]['for_'+period]['rate']
            d = profiles[other]['against_'+period]['rate']
            # Geometric pooling demands support from attack AND opposing defence.
            rates[side+'_'+period] = sqrt(max(.0025, a*d))
        pool = [r for r in h2h if numeric(r.get('total_'+period)) and
                (period == 'ft' or r.get('halftime_available') is True)]
        factor = 1.
        if len(pool) >= 2:
            observed = mean(r['total_'+period] for r in pool)
            expected = rates['home_'+period]+rates['away_'+period]
            freshness = mean(2**(-(stamp(as_of)-stamp(r['kickoff'])).days/365) for r in pool)
            weight = min(CONFIG['h2h_max_rate_change'], len(pool)/(len(pool)+10)*.15)*freshness
            factor += clamp(observed/max(expected, .1)-1, -1, 1)*weight
        adjustments[period] = {'sample': len(pool), 'relative_change': factor-1}
        for side in ('home', 'away'):
            rates[side+'_'+period] *= factor
    for side in ('home', 'away'):
        rates[side+'_ht'] = min(rates[side+'_ht'], rates[side+'_ft'])
    return rates, profiles, adjustments


def probabilities(rates):
    return goal_probabilities([rates['home_ht'], rates['away_ht'],
                               rates['home_ft']-rates['home_ht'],
                               rates['away_ft']-rates['away_ht']])


def statistical_analysis(match, as_of):
    stats, meta = match.get('statistics') or {}, match.get('fixture') or {}
    histories = {s: history((stats.get(s) or {}).get('last_matches', []), as_of, meta.get('season'))
                 for s in ('home', 'away')}
    league = deepcopy(stats.get('league') or {})
    h2h, overlaps = valid_h2h(stats, meta, as_of, histories)
    rates, profiles, h2h_effect = rates_for(histories, league, h2h, as_of)
    central = probabilities(rates)
    no_xg = probabilities(rates_for(histories, league, h2h, as_of, use_xg=False)[0])
    no_peak_control = probabilities(rates_for(histories, league, h2h, as_of, use_robust=False)[0])
    raw = probabilities(rates_for(histories, league, [], as_of, use_xg=False, use_robust=False)[0])
    no_h2h = probabilities(rates_for(histories, league, [], as_of)[0])
    scenarios = [central, no_h2h]
    # Delete one historical match at a time, not the match being predicted.
    for side in ('home', 'away'):
        for index in range(len(histories[side])):
            trial = {**histories, side: histories[side][:index]+histories[side][index+1:]}
            scenarios.append(probabilities(rates_for(trial, league, h2h, as_of)[0]))
    base_rates = {s+'_'+p: league_prior(league, s, p)[0]
                  for s in ('home', 'away') for p in ('ft', 'ht')}
    for side in ('home', 'away'):
        base_rates[side+'_ht'] = min(base_rates[side+'_ht'], base_rates[side+'_ft'])
    reference = probabilities(base_rates)
    rows = []
    for market, selection in TARGETS.items():
        period = 'ht' if market.endswith('_ht') else 'ft'
        sample = min(profiles[s]['for_'+period]['sample'] for s in profiles)
        context_sample = min(profiles[s]['for_'+period]['context_sample'] for s in profiles)
        league_n = min(league_prior(league, s, period)[1] for s in profiles)
        p = central[market][selection] if sample else None
        low = min(v[market][selection] for v in scenarios) if p is not None else None
        high = max(v[market][selection] for v in scenarios) if p is not None else None
        baseline = reference[market][selection]
        reasons, pros, cons = [], [], []
        if sample < CONFIG['minimum_'+period]:
            reasons.append(f"Servono almeno {CONFIG['minimum_'+period]} precedenti {period.upper()} validi per entrambe le squadre.")
        if context_sample < 2:
            reasons.append('Meno di due precedenti nel contesto casa/trasferta per una squadra.')
        if league_n < CONFIG['minimum_league_'+period]:
            reasons.append('Riferimento del campionato insufficiente.')
        if low is None or low < baseline+CONFIG['minimum_uplift_pp']/100:
            reasons.append('Il vantaggio statistico sul riferimento del campionato non regge le variazioni del campione.')
        if market == 'gg_ht' and any(rates[s+'_ht'] < base_rates[s+'_ht'] for s in profiles):
            reasons.append('Il potenziale del primo tempo non è sostenuto da entrambe le squadre.')
        for side, title in (('home', 'Casa'), ('away', 'Ospite')):
            profile = profiles[side]['for_'+period]
            other = 'away' if side == 'home' else 'home'
            defense = profiles[other]['against_'+period]
            pros.append(f"{title}: potenziale {profile['rate']:.2f} gol contro una difesa da {defense['rate']:.2f} gol concessi ({period.upper()}).")
            if profile['under_conversion']:
                pros.append(f"{title}: occasioni xG superiori ai gol realizzati; recupero parziale del potenziale nel calcolo.")
            if profile['peak_reduction'] > .15:
                cons.append(f"{title}: la gara più prolifica gonfia la media; il suo peso è ridotto.")
            if profile['drought']:
                (pros if profile['under_conversion'] else cons).append(
                    f"{title}: {profile['drought']} gare senza segnare, "+
                    ('con potenziale xG inespresso.' if profile['under_conversion'] else 'senza bonus automatico di rientro.'))
        eligible = not reasons
        rows.append({'market': market, 'selection': selection, 'label': LABELS[market],
                     'probability_pct': p*100 if p is not None else None,
                     'statistical_pct': p*100 if p is not None else None,
                     'probabilities': {'statistics': p, 'selection_reference': p,
                         'without_opportunities': no_xg[market][selection] if sample else None,
                         'without_peak_control': no_peak_control[market][selection] if sample else None,
                         'raw_goals': raw[market][selection] if sample else None,
                         'without_h2h': no_h2h[market][selection] if sample else None},
                     'sensitivity': {'low_pct': low*100 if low is not None else None,
                                     'high_pct': high*100 if high is not None else None,
                                     'method': 'leave_one_history_out_and_no_h2h',
                                     'is_confidence_interval': False},
                     'league_probability_pct': baseline*100,
                     'statistical_candidate': eligible, 'primary_eligible': eligible,
                     'statistical_reliability': 'sufficient_history' if eligible else 'not_selected',
                     'statistical_score': low*100 if low is not None else None,
                     'coverage': sample/CONFIG['history_'+period],
                     'source': 'analisi statistica', 'pros': pros, 'cons': cons,
                     'missing': reasons, 'selection_reasons': reasons,
                     'published_selection': False, 'published_rank': None})
    compact_league = {k: league[k] for k in ('sample', 'ht_sample', 'avg_home_goals',
                    'avg_away_goals', 'avg_home_ht', 'avg_away_ht', 'source', 'ht_source', 'as_of')
                    if k in league}
    inputs = {'histories': histories, 'league': compact_league, 'h2h': h2h}
    return {'model_id': MODEL_ID, 'config_id': CONFIG_ID, 'config': deepcopy(CONFIG),
            'captured_at': as_of, 'status': 'experimental', 'validation_from': VALIDATION_FROM,
            'validation_status': 'prospective_pending', 'rates': rates, 'profiles': profiles,
            'inputs': inputs, 'input_sha256': sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest(),
            'h2h': {'used': len(h2h), 'overlapping_excluded': overlaps, 'effects': h2h_effect},
            'data_quality': {s: {'sample': profiles[s]['for_ft']['sample'],
                'ht_sample': profiles[s]['for_ht']['sample'], 'xg_for': profiles[s]['for_ft']['xg_sample'],
                'opponent_strength_adjusted': False} for s in profiles},
            'markets': rows, 'best_candidate': None, 'selected_markets': [],
            'selection_diagnostic': {'status': 'statistics_first', 'allow_abstention': True,
                'opponent_strength_adjusted': False, 'book_weight_in_statistics': 0},
            'notes': ['Le stime non sono percentuali di successo già validate.',
                      'Digiuni e goleade non rendono dovuto il risultato opposto.',
                      'La forza dei singoli avversari storici non è normalizzata: il riferimento è il campionato.']}


def quote_row(market, side, fresh, available):
    bench = fresh.get(market) or {}
    books = bench.get('books') or []
    priced = sorted(books, key=lambda b: (b['odds'][side], str(b['bookmaker_id'])))
    quote = deepcopy(priced[(len(priced)-1)//2]) if priced else None
    fallback = sorted((available.get(market) or {}).get('books') or [],
                      key=lambda b: (b.get('provider_updated_at') or '', str(b.get('bookmaker_id'))), reverse=True)
    display = deepcopy(quote or (fallback[0] if fallback else None))
    for value in (quote, display):
        if value:
            value.pop('trace', None)
    return {'odd': quote['odds'][side] if quote else None, 'quote': quote,
            'display_odd': display['odds'][side] if display else None, 'display_quote': display,
            'quote_status': 'recent' if quote else 'stale' if display else 'missing',
            'book_probability_pct': (bench['probabilities'][side]*100) if bench else None,
            'bookmakers_count': len(books), 'movement': movement(books, side)}


def coherent_drop(move):
    return (move.get('confirmed') is True and move.get('agreeing_books', 0) >= 2 and
            move.get('agreeing_books', 0) > move.get('paired_books', 0)/2 and
            move.get('direction') == 'down' and move.get('fair_direction_coherent') is True and
            numeric(move.get('oci')) and move['oci'] > 0 and
            move.get('amplitude_pct', 0) >= CONFIG['minimum_drop_pct'])


def confirm_markets(assessment, quotes, meta, as_of):
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    age = CONFIG['near_kickoff_quote_age_hours'] if now and kickoff and 0 < (kickoff-now).total_seconds() <= 10800 else CONFIG['max_quote_age_hours']
    observed = {key: [b for b in books if stamp(b.get('provider_updated_at'))]
                for key, books in (quotes or {}).items() if key in SPECS}
    recent = {key: [b for b in books if now and
                   0 <= (now-stamp(b['provider_updated_at'])).total_seconds() <= age*3600]
              for key, books in observed.items()}
    fresh, available = quote_benchmarks(recent, as_of), quote_benchmarks(observed, as_of)
    for row in assessment['markets']:
        row.update(quote_row(row['market'], row['selection'], fresh, available))
        p, odd = row['statistical_pct'], row['odd']
        ratio = p/100*odd if p is not None and odd is not None else None
        row['price_assessment'] = {'reference_source': 'statistics_only',
            'reference_probability_pct': p, 'reference_fair_odd': 100/p if p else None,
            'break_even_probability_pct': 100/odd if odd else None,
            'return_index': ratio, 'estimated_return_pct': (ratio-1)*100 if ratio is not None else None,
            'status': 'unvalidated_positive_estimate' if ratio is not None and ratio > 1 else 'no_positive_estimate' if ratio is not None else 'missing_price',
            'validated_advantage': False, 'note': 'Rendimento stimato dal modello, non validato.'}
        move = row['movement']
        confirmation = 'not_evaluated' if not row['statistical_candidate'] else 'unavailable' if not odd else 'neutral'
        if row['statistical_candidate'] and odd:
            if coherent_drop(move):
                confirmation = 'confirmed'
            elif move.get('confirmed') and move.get('direction') == 'up' and move.get('fair_direction_coherent'):
                confirmation = 'contrary'
        patterns = []
        if confirmation == 'confirmed' and row['market'] == 'over25_ft':
            ops = move.get('operators') or []
            above = [o for o in ops if numeric(o.get('initial')) and o['initial'] > 2 and
                     o.get('delta_pct', 0) < 0 and o.get('leg_fair_delta_pp', 0) > 0]
            if len(above) >= 2 and len(above) > len(ops)/2:
                patterns.append('over25_from_above_2')
        row['market_confirmation'] = {'status': confirmation, 'scope': 'same_market',
            'probability_bonus': 0, 'patterns': patterns,
            'margin_change_pp': move.get('median_overround_delta_pp')}
    one_x_two = []
    for side in ('1', 'x', '2'):
        one_x_two.append({'market': '1x2_ft', 'selection': side,
                         'label': {'1': 'Casa', 'x': 'Pareggio', '2': 'Ospite'}[side],
                         **quote_row('1x2_ft', side, fresh, available)})
    probabilities_1x2 = (fresh.get('1x2_ft') or {}).get('probabilities') or {}
    favorite = max(probabilities_1x2, key=probabilities_1x2.get) if probabilities_1x2 else None
    fav = next((r for r in one_x_two if r['selection'] == favorite), None)
    gg = next(r for r in assessment['markets'] if r['market'] == 'gg_ht')
    pattern = {'active': False, 'scope': '1x2_context', 'probability_bonus': 0}
    if favorite in ('1', '2') and fav and fav['odd'] and 1.5 <= fav['odd'] <= 1.7:
        underdog = 'away' if favorite == '1' else 'home'
        profile = assessment['profiles'][underdog]['for_ft']
        active = bool(gg['statistical_candidate'] and coherent_drop(fav['movement']) and
                      profile['drought'] >= 1 and profile['under_conversion'])
        pattern.update(active=active, favorite=favorite, favorite_odd=fav['odd'],
                       opponent_drought=profile['drought'], opponent_under_conversion=profile['under_conversion'])
        if active:
            gg['market_confirmation']['patterns'].append('favorite_drop_opponent_under_conversion')
            if gg['market_confirmation']['status'] in ('neutral', 'unavailable'):
                gg['market_confirmation'].update(status='context', scope='1x2_context')
    assessment['book_context'] = {'favorite_drought': pattern, 'markets': one_x_two}
    assessment['quote_max_age_hours'] = age
    return assessment


def evaluate(match, as_of, quotes=None):
    # Deliberate stage boundary: no quote object is passed to statistical_analysis.
    result = statistical_analysis(match, as_of)
    return confirm_markets(result, quotes or {}, match.get('fixture') or {}, as_of)


def expire_preserved_prices(assessment, meta, as_of):
    """An API outage must not keep an old blue confirmation apparently current."""
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    age = CONFIG['near_kickoff_quote_age_hours'] if now and kickoff and 0 < (kickoff-now).total_seconds() <= 10800 else CONFIG['max_quote_age_hours']
    expired_context = False
    main = assessment.get('markets', [])
    for row in [*main, *(assessment.get('book_context') or {}).get('markets', [])]:
        quote = row.get('quote')
        if not quote:
            continue
        at = stamp(quote.get('provider_updated_at'))
        if now and at and 0 <= (now-at).total_seconds() <= age*3600:
            continue
        row.update(display_quote=quote, display_odd=row.get('odd'), quote=None, odd=None,
                   quote_status='stale', book_probability_pct=None)
        if row.get('market') == '1x2_ft':
            expired_context = True
        if row.get('market_confirmation'):
            row['market_confirmation'].update(status='unavailable' if row.get('statistical_candidate') else 'not_evaluated', patterns=[])
        if row.get('price_assessment'):
            row['price_assessment'].update(status='missing_price', break_even_probability_pct=None,
                                            return_index=None, estimated_return_pct=None)
    if expired_context:
        assessment.get('book_context', {}).get('favorite_drought', {}).update(active=False)
        for row in main:
            if row.get('market_confirmation', {}).get('status') == 'context':
                row['market_confirmation'].update(status='unavailable' if not row.get('odd') else 'neutral', patterns=[])


def build_selection_board(day, as_of):
    """Publish up to five per market; statistics determine admission and order."""
    now = stamp(as_of)
    ranking = {market: [] for market in TARGETS}
    locked = {market: set() for market in TARGETS}
    seen = set()
    for match in day.get('matches', []):
        assessment, meta = match.get('multimarket') or {}, match.get('fixture') or {}
        kickoff = stamp(meta.get('kickoff'))
        fid = str(meta.get('fixture_id'))
        if assessment.get('model_id') != MODEL_ID or not now or not kickoff or fid in seen:
            continue
        seen.add(fid)
        if kickoff <= now or meta.get('result'):
            for row in assessment.get('markets', []):
                if row.get('published_selection') and row.get('market') in locked:
                    locked[row['market']].add(fid)
            continue
        assessment['best_candidate'] = None
        expire_preserved_prices(assessment, meta, as_of)
        assessment['selected_markets'] = []
        assessment['selection_as_of'] = as_of
        for row in assessment.get('markets', []):
            row.update(published_selection=False, published_rank=None)
            if row.get('statistical_candidate') and row['market'] in TARGETS:
                ranking[row['market']].append((match, row))
    board = {}
    for market, pairs in ranking.items():
        pairs.sort(key=lambda item: (-item[1]['statistical_score'],
                                     -item[1]['coverage'], item[0]['fixture']['kickoff'],
                                     str(item[0]['fixture']['fixture_id'])))
        remaining = max(0, CONFIG['top_per_market']-len(locked[market]))
        picked = pairs[:remaining]
        board[market] = {'label': LABELS[market], 'eligible': len(pairs), 'selected': [],
                         'started_selections': len(locked[market]), 'remaining_slots': remaining}
        for rank, (match, row) in enumerate(picked, 1):
            meta, assessment = match['fixture'], match['multimarket']
            row.update(published_selection=True, published_rank=rank)
            assessment['selected_markets'].append(market)
            if assessment['best_candidate'] is None or row['statistical_score'] > assessment['best_candidate']['statistical_score']:
                assessment['best_candidate'] = row
            board[market]['selected'].append({'fixture_id': str(meta['fixture_id']),
                'market': market, 'rank': rank, 'kickoff': meta['kickoff'],
                'statistical_pct': row['statistical_pct'], 'statistical_score': row['statistical_score'],
                'confirmation': row['market_confirmation']['status']})
    day['selection_board'] = {'model_id': MODEL_ID, 'config_id': CONFIG_ID,
        'as_of': as_of, 'max_per_market': CONFIG['top_per_market'], 'markets': board,
        'ranking': 'statistical_sensitivity_floor', 'book_weight_in_ranking': 0}
    return day['selection_board']
