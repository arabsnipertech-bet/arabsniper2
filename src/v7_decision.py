"""The prematch V7 decision, with one frozen triad per fixture."""
from collections import defaultdict
from functools import lru_cache
from hashlib import sha256
from itertools import product
import json
from zoneinfo import ZoneInfo

from adaptive_pricing import BASE_FIELDS, reprice
from context_analysis import congruence_triggers
from gold_competitions import national_competition
from lateral_matrix import label, market_id, payout_weights, quality, specification
from market_scope import COMBO_SPECS, JOINT_KINDS
from matrix_quotes import safe_price_row
from multimarket_model import fair, stamp
from opportunity_policy import annotate as annotate_gold
from platinum_policy import annotate as annotate_platinum
from v7_probability import (BASE_WEIGHT, VERSION, blend_rates, fair_inputs, finite,
                            market_rates, outcome_fair, price_usable, raw_rates,
                            score_model, statistical_rates, utc)

GOAL = {
    name: market_id(specification('total', period, 'over', line))
    for name, period, line in (('over15_ht', 'ht', 1.5), ('over25_ht', 'ht', 2.5),
                              ('over35_ht', 'ht', 3.5), ('over25_ft', 'ft', 2.5),
                              ('over35_ft', 'ft', 3.5), ('over15_sh', 'sh', 1.5),
                              ('over25_sh', 'sh', 2.5))
}
GG, GG_SH, GG_FT = (market_id(specification('btts', p, 'yes')) for p in ('ht', 'sh', 'ft'))
RESULTS = {side: market_id(specification('result', 'ft', side)) for side in ('1', 'x', '2')}
AWAY = RESULTS['2']
COMBOS = COMBO_SPECS
BOTH_GOALS, OVER_CARRY, GG_CARRY, GG_BOTH = map(market_id, COMBOS)
COMBO_LEGS = {
    BOTH_GOALS: tuple(market_id(specification('total', period, 'over', .5)) for period in ('ht', 'sh')),
    OVER_CARRY: (GOAL['over15_ht'], GOAL['over25_ft']),
    GG_CARRY: (GG, GOAL['over25_ft']),
    GG_BOTH: (GG, GG_SH),
}
TRIAD_MARKETS = {
    'most_probable': {GOAL['over25_ft'], GOAL['over15_sh'], GG_FT, BOTH_GOALS, *RESULTS.values()},
    'recommended_alternative': {GOAL['over15_ht'], GOAL['over35_ft'], GG_SH, OVER_CARRY},
    'high_odds_shot': {GG, GOAL['over25_ht'], GG_BOTH, GG_CARRY, GOAL['over25_sh'], AWAY},
}
TRIAD_LIMITS = {'most_probable': (.45, 1.55, 2.15),
                'recommended_alternative': (.32, 2.15, 3.50),
                'high_odds_shot': (.16, 3.50, float('inf'))}
STUDY_LIMITS = {**TRIAD_LIMITS, 'most_probable': (.45, 1.50, 2.15)}
STUDY_PREFERRED = dict(zip(TRIAD_MARKETS, (GOAL['over25_ft'], GOAL['over15_ht'], GG)))
SIGNAL_ROLES = dict(zip(TRIAD_MARKETS, ('primary', 'consigliata', 'colpaccio')))
BADGES = {'BLU': '◆ BLU', 'GOLD': '● GOLD', 'PLATINUM': '★ PLATINUM'}
TIER_RANK = {tier: rank for rank, tier in enumerate(BADGES)}
HORIZONS = {'ht': 'PT', 'sh': 'ST', 'ft': 'FT'}
TAGS = {'PT': '[1° TEMPO]', 'ST': '[2° TEMPO]', 'FT': "[FINALE 90']",
        'PT+ST': '[COMBO 1°T+2°T]'}
NICHE_LABELS = {
    'fav_home_pt_vulnerable': 'Favorita PT',
    'balanced_1x2_inversion': 'Inversione 1X2',
    'bookmaker_shift_rebound': 'Rimbalzo PT',
    'pt_st_symmetry_11_22': 'Simmetria 1°+2°T',
}
PAYOUT_FIELDS = ('push_probability', 'half_win_probability', 'half_loss_probability')


def fingerprint(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                             ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def canonical(spec):
    if spec['kind'] == 'asian_total' and finite(spec.get('line')) and spec['line'] % 1 == .5:
        return dict(spec, kind='total')
    return spec


def quote_rows(rows, as_of, kickoff):
    groups = defaultdict(list)
    for original in rows:
        if not original.get('spec'):
            continue
        row = safe_price_row(original)
        groups[market_id(canonical(row['spec']))].append(row)
    chosen, inputs = {}, []
    for key, group in groups.items():
        usable = [r for r in group if price_usable(r, as_of, kickoff)]
        pick = max(usable, key=lambda r: (r['odd'], stamp(r['quote_observed_at']), r['id'] == key, r['id']),
                   default=next((r for r in group if r['id'] == key), group[0]))
        chosen[key] = pick, bool(usable)
        inputs.extend(dict(r, spec=canonical(r['spec'])) for r in usable if outcome_fair(r) is not None)
    return chosen, inputs


def features(row, usable, as_of):
    if not usable:
        return {}
    move = row.get('selected_movement') or {}
    from_at, to_at, now = (stamp(t) for t in (move.get('observed_from'), move.get('observed_to'), as_of))
    repeated = bool(from_at and to_at and from_at < to_at <= now)
    first, odd = row.get('first_odd'), row['odd']
    first_at, last_at = (stamp(row.get(k)) for k in ('first_observed_at', 'quote_observed_at'))
    opening_delta = 100*(odd/first-1) if first_at and last_at and first_at < last_at <= now and finite(first) and first > 1 else None
    net = 0. if row.get('prematch_stable') else opening_delta
    oci = None if row.get('historical_oci') else row.get('oci')
    return dict(delta_pct=move.get('delta_pct') if repeated else None, net_delta_pct=net,
                opening_delta_pct=opening_delta,
                oci=oci if finite(oci) else None, oci_method=row.get('oci_method'),
                oci_band=('below_-50' if oci < -50 else '-50_to_0' if oci < 0 else
                          '0_to_50' if oci < 50 else '50_plus') if finite(oci) else None,
                persistence=move.get('persistence') if repeated else None,
                reversal=move.get('reversal') if repeated else None,
                margin_pct=row.get('selected_margin_pct'),
                paired_books=row.get('paired_books'), agreeing_books=row.get('agreeing_books'))


def bounded_probability(model, spec):
    return min(1., max(0., model.estimate(spec)['p']))


def niche_evidence(records, assessment, rates, statistics, totals, weight, fair_rows, means):
    result = next((r for r in sorted(fair_rows, key=lambda r: (r['selected_margin_pct'], str(r.get('bookmaker_id'))))
                   if r['spec']['kind'] == 'result' and r['spec']['period'] == 'ft'
                   and set(r.get('selected_market_odds') or {}) == set(RESULTS)
                   and all(finite(v) and v > 1 for v in r['selected_market_odds'].values())), {})
    opening = result.get('opening_market_odds') or {}
    first_at, last_at = (stamp(result.get(k)) for k in ('first_observed_at', 'quote_observed_at'))
    opening_share = None
    if (first_at and last_at and first_at < last_at and set(opening) == set(RESULTS)
            and all(finite(v) and v > 1 for v in opening.values())):
        vector, _ = fair(opening)
        projected = market_rates(statistics, totals, tuple(vector[key] for key in RESULTS))
        before = blend_rates(statistics, projected, weight)
        volume = before['home_ht']+before['away_ht']
        opening_share = before['home_ht']/volume if volume else None
    else:
        opening = {}
    keys = {'home': RESULTS['1'], 'gg_ht': GG, 'over25_ft': GOAL['over25_ft'], 'over15_ht': GOAL['over15_ht']}
    observed = congruence_triggers(assessment.get('profiles') or {}, means, rates,
                                   {name: records.get(key, {}) for name, key in keys.items()},
                                   opening, result.get('selected_market_odds') or {}, opening_share)
    niches = {}
    if observed['fav_home_pt_vulnerable']:
        niches['fav_home_pt_vulnerable'] = {GOAL['over15_ht'], GG, GG_CARRY}
    if observed['balanced_1x2_inversion']:
        niches['balanced_1x2_inversion'] = {BOTH_GOALS, GOAL['over25_ft'], GOAL['over15_ht'], GG}
    rebound = set()
    if observed['conversion']:
        rebound.add(GOAL['over25_ft'])
    if observed['rebound']:
        rebound.add(GOAL['over15_ht'])
    if observed['rebound_gg']:
        rebound.add(GG)
    if rebound:
        niches['bookmaker_shift_rebound'] = rebound
    if observed['pt_st_symmetry_11_22']:
        niches['pt_st_symmetry_11_22'] = {GOAL['over15_sh'], GOAL['over25_ft'], GOAL['over35_ft'], GG_SH, GG_BOTH}
    return niches, observed


def recheck_policies(assessment, meta, as_of, rates):
    def quote_filter(row):
        if rates is None:
            return 'v7_unavailable'
        if not row.get('v7_supported'):
            return 'history'
        if row.get('prematch_stable') or not price_usable(row, as_of, meta['kickoff']):
            return 'v7_quote_unusable'

    matrix = assessment['matrix']
    view = {**assessment, 'rates': rates} if rates is not None else None
    annotate_gold(matrix, as_of, view, probability_model=VERSION, quote_filter=quote_filter)
    annotate_platinum(matrix, meta, as_of, probability_model=VERSION, quote_filter=quote_filter)


def publish_prices(assessment, meta, as_of, model, projection, weight, means):
    scenarios = assessment.get('pure_poisson_sensitivity_rates', assessment.get('sensitivity_rates', []))
    models = [model, *(score_model(blend_rates(statistical_rates(rates, means), projection, weight))
                       for scenario in scenarios or [] if (rates := raw_rates({'rates': scenario})) is not None)]
    supported = all(quality(assessment, period) == 'supported' for period in ('ft', 'ht'))
    for row in assessment['matrix']['rows']:
        if not row.get('spec'):
            continue
        row.setdefault('v51_pricing', {key: row[key] for key in (*BASE_FIELDS, *PAYOUT_FIELDS) if key in row})
        row.pop('calibration', None)
        if row.get('quote_source') == 'synthetic_combo':
            row.update(odd=None, quote_status='missing', ev_pct=None, ev_low_pct=None,
                       ev_high_pct=None, category='no_quote', stable_value=False, theoretical_value=False)
        estimates = [m.estimate(row['spec']) for m in models]
        estimate = estimates[0]
        row.update(probability_model=VERSION, v7_supported=supported and row.get('quality') == 'supported',
                   statistical_pct=round(100*estimate['p'], 4),
                   low_pct=round(100*min(v['p'] for v in estimates), 4),
                   high_pct=round(100*max(v['p'] for v in estimates), 4),
                   sensitivity_weights=[list(pair) for pair in sorted({
                       (v['win_weight'], v['loss_weight']) for v in estimates})],
                   **{key: estimate[key] for key in ('win_weight', 'loss_weight', 'fair_odd', *PAYOUT_FIELDS)})
        reprice(row)
    recheck_policies(assessment, meta, as_of, model.rates)


@lru_cache(maxsize=128)
def winning_scores(key):
    spec = json.loads(key)
    mask = 0
    for i, (home, away, home_st, away_st) in enumerate(product(range(5), repeat=4)):
        ht = home, away
        scores = {'ht': ht, 'sh': (home_st, away_st), 'ft': (home+home_st, away+away_st)}
        if payout_weights(spec, *scores[spec['period']], *ht)[0] == 1.:
            mask |= 1 << i
    return mask


def compatible(first, *others):
    """The selected markets must share a joint winning PT and ST score."""
    mask = winning_scores(first)
    for key in others:
        mask &= winning_scores(key)
    return bool(mask)


def signal(key, row, reason):
    return dict(market_id=key, source_market_id=row['source_market_id'], label=row['label'],
                odd=row['odd'], probability=row['calibrated'], ev=row['ev'], ev_low=row['ev_low'],
                model_fair_odd=row.get('model_fair_odd'), quote_state=row.get('quote_state'),
                tier=row['tier'], badge=BADGES.get(row['tier']), horizon=row['horizon'],
                horizon_label=TAGS[row['horizon']], niche_id=row['niche_id'],
                niche_label=NICHE_LABELS.get(row['niche_id']), reasons=[reason])


def fill_study_slots(triad, records, allow_model_estimates, allow_unpriced):
    model_only = not any(finite(row['odd']) for row in records.values())
    if model_only and not allow_unpriced:
        return
    fixed = [pick['market_id'] for pick in triad.values() if pick]
    options = {}
    for role, pick in triad.items():
        if pick:
            continue
        floor, minimum, maximum = STUDY_LIMITS[role]
        candidates = []
        for key, row in records.items():
            if model_only:
                value = row['model_fair_odd']
                if (key in TRIAD_MARKETS[role] and 0 < row['calibrated'] < 1
                        and finite(value) and value > 1):
                    candidates.append(key)
                continue
            if (key in fixed or not row.get('study_supported') or row['calibrated'] < floor
                    or row['spec_kind'] not in ('result', 'total', 'btts', *JOINT_KINDS)):
                continue
            is_estimate = not finite(row['odd'])
            if is_estimate and (not allow_model_estimates or row['horizon'] not in ('PT', 'ST', 'PT+ST')):
                continue
            value = row['model_fair_odd'] if is_estimate else row['odd']
            if finite(value) and minimum <= value <= maximum and compatible(key, *fixed):
                candidates.append(key)
        options[role] = [None, *candidates]
    if not options:
        return

    def score(picks):
        ranks = [(k == STUDY_PREFERRED[role] if model_only else finite(records[k]['odd']),
                  records[k]['calibrated'], records[k]['ev'] or 0., k)
                 if k is not None else (False, 0., 0., '') for role, k in zip(options, picks)]
        return sum(k is not None for k in picks), sum(rank[0] for rank in ranks), ranks

    def valid(picks):
        extra = [key for key in picks if key is not None]
        return not extra or len(set(extra)) == len(extra) and compatible(extra[0], *extra[1:], *fixed)

    best = max(filter(valid, product(*options.values())), key=score)
    for role, key in zip(options, best):
        if key is not None:
            row = records[key]
            estimate = row['odd'] is None
            triad[role] = {**signal(key, {**row, 'tier': 'BLU'}, 'stima_modello' if estimate else 'solo_studio'),
                           'study_only': True, 'quote_state': 'model_estimate' if estimate else row['quote_state']}


def route(records, observed, *, study=False, allow_model_estimates=False, allow_unpriced=False):
    triad = dict.fromkeys(TRIAD_MARKETS)
    chosen = []
    for role, markets in TRIAD_MARKETS.items():
        floor, minimum, maximum = TRIAD_LIMITS[role]
        candidates = []
        for key in markets & records.keys():
            row = records[key]
            odd, ev, low = row['odd'], row['ev'], row['ev_low']
            if not (row['tier'] and finite(odd) and minimum <= odd <= maximum
                    and row['calibrated'] >= floor and finite(ev) and ev > 0):
                continue
            if (role != 'high_odds_shot' or row['tier'] == 'BLU') and (not finite(low) or low < 0):
                continue
            if role == 'recommended_alternative' and chosen and odd <= records[chosen[0]]['odd']:
                continue
            delta = row['features'].get('net_delta_pct')
            if role == 'high_odds_shot' and key == AWAY and (odd < 4.50 or not finite(delta) or delta <= 0):
                continue
            if key not in chosen and all(compatible(other, key) for other in chosen):
                candidates.append(key)
        if not candidates:
            continue
        if role == 'most_probable':
            key = max(candidates, key=lambda k: (records[k]['calibrated'], records[k]['ev_low'], records[k]['ev'], k))
        else:
            key = max(candidates, key=lambda k: (bool(records[k]['niche_id']), records[k]['ev'], records[k]['calibrated'], k))
        triad[role] = signal(key, records[key], role)
        chosen.append(key)

    signals = {name: triad[role] for role, name in SIGNAL_ROLES.items()}
    for tier in ('GOLD', 'PLATINUM'):
        signals[tier.lower()] = next((pick for pick in triad.values() if pick and pick['tier'] == tier), None)
    over = GOAL['over25_ft']
    signals['radar_conversione'] = signal(over, records[over], 'gg_pt_sale_over25_ft_scende') if observed['conversion'] else None
    surprises = [key for key in (GG, AWAY) if key in records and finite(records[key]['odd'])
                 and (observed['rebound_gg'] if key == GG else records[key]['odd'] >= 4.5
                      and (records[key]['features'].get('net_delta_pct') or 0) > 0)]
    best = max(surprises, key=lambda k: (records[k]['calibrated'], k), default=None)
    signals['radar_sorprese'] = signal(best, records[best], 'quota_alta_in_salita') if best else None
    signals['observations'] = observed
    if study:
        fill_study_slots(triad, records, allow_model_estimates, allow_unpriced)
    return triad, signals


def capture(assessment, meta, as_of, weight=BASE_WEIGHT, calibration=None):
    at, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    if (not at or not kickoff or at >= kickoff or meta.get('result') or
            meta.get('status_short') not in ('NS', 'TBD')):
        return None
    matrix = assessment.get('matrix') or {}
    raw = raw_rates(assessment)
    original_rows = matrix.get('rows', [])
    if raw is None or not original_rows:
        return None
    chosen, fair_rows = quote_rows(original_rows, as_of, meta['kickoff'])
    observed = assessment.get('observed_goal_means') or {}
    statistics_at = stamp(assessment.get('statistics_as_of') or observed.get('captured_at'))
    if statistics_at and statistics_at > at:
        return None
    means = observed.get('teams') or {}
    stats = statistical_rates(raw, means)
    totals, fair_result = fair_inputs(fair_rows)
    projected = market_rates(stats, totals, fair_result)
    corrected = blend_rates(stats, projected, BASE_WEIGHT)
    calibrated = blend_rates(stats, projected, weight)
    raw_model, stat_model, fixed_model = (score_model(r) for r in (raw, stats, corrected))
    learned_model = fixed_model if calibrated == corrected else score_model(calibrated)
    baseline = {r['id']: (r.get('v51_pricing') or r).get('statistical_pct') for r in original_rows}
    publish_prices(assessment, meta, as_of, learned_model, projected, weight, means)
    records = {}
    sources = {r['id']: r for r in original_rows}
    profiles = assessment.get('profiles') or {}
    limited_national_history = national_competition(meta.get('league')) and all(
        profiles.get(side, {}).get('for_'+period, {}).get('sample', 0) >= minimum
        for side in ('home', 'away') for period, minimum in (('ft', 3), ('ht', 2)))
    for key, (row, usable) in chosen.items():
        source = sources[row['id']]
        spec = canonical(row['spec'])
        fair_p = outcome_fair(row) if usable else None
        original_p = baseline.get(row.get('id'))
        odd = row.get('odd') if usable else None
        estimate = learned_model.estimate(spec)
        ev = estimate['win_weight']*(odd-1)-estimate['loss_weight'] if odd else None
        ev_low = source.get('ev_low_pct')
        supported = source.get('v7_supported') is True
        exclusions = []
        if not usable:
            exclusions.append('quota_non_eseguibile')
        if fair_p is None:
            exclusions.append('fair_non_disponibile')
        if not supported:
            exclusions.append('statistica_insufficiente')
        if ev is not None and ev <= 0:
            exclusions.append('ev_non_positivo')
        quote = ({**{k: row.get(k) for k in ('first_odd', 'opening_odd', 'bookmaker', 'bookmaker_id', 'quote_status')},
                  **{k: utc(row.get(k)) for k in ('first_observed_at', 'quote_observed_at',
                                                  'provider_updated_at')}} if usable else None)
        evidence = features(row, usable, as_of)
        quote_state = ('stable' if usable and (row['quote_status'] == 'cached_stable' or row.get('first_odd') == odd)
                       else 'moving' if evidence.get('net_delta_pct') else 'observed' if usable else 'missing')
        records[key] = dict(source_market_id=row['id'], label=label(spec), spec_kind=spec['kind'], side=spec['side'],
                            horizon='PT+ST' if spec['kind'] in JOINT_KINDS else HORIZONS[spec['period']],
                            raw=round(bounded_probability(raw_model, spec), 8),
                            statistics=round(bounded_probability(stat_model, spec), 8),
                            corrected=round(bounded_probability(fixed_model, spec), 8),
                            calibrated=round(min(1., max(0., estimate['p'])), 8), fair=fair_p,
                            v51=original_p/100 if finite(original_p) else None, odd=odd, ev=ev, supported=supported,
                            study_supported=bool(supported or limited_national_history), quote_state=quote_state,
                            ev_low=ev_low/100 if usable and finite(ev_low) else None,
                            model_fair_odd=estimate['fair_odd'], quote=quote, features=evidence,
                            edge_exclusions=exclusions, policy_exclusions=(source.get('opportunity') or {}).get('reasons') or [],
                            platinum_exclusions=(source.get('platinum') or {}).get('reasons') or [])
    niches, observed_patterns = niche_evidence(records, assessment, calibrated, stats, totals, weight,
                                               [r for r in fair_rows if r['quote_status'] == 'recent'], means)
    for key, row in records.items():
        row['niche_id'] = next((name for name, keys in niches.items() if key in keys), None)
        source = sources[row['source_market_id']]
        if (source.get('platinum') or {}).get('eligible') is True:
            row['tier'] = 'PLATINUM'
        elif (source.get('opportunity') or {}).get('eligible') is True:
            row['tier'] = 'GOLD'
        elif row['niche_id'] or (row['supported'] and finite(row['ev']) and row['ev'] > 0
                                and finite(row['ev_low']) and row['ev_low'] >= 0):
            row['tier'] = 'BLU'
        else:
            row['tier'] = None
        legs = COMBO_LEGS.get(key)
        if legs and row['odd'] is None and all(finite(records.get(leg, {}).get('odd')) for leg in legs):
            row['reference_legs'] = [dict(market_id=leg, label=records[leg]['label'], odd=records[leg]['odd'],
                                          quote=records[leg]['quote']) for leg in legs]

    rome = ZoneInfo('Europe/Rome')
    days_ahead = (kickoff.astimezone(rome).date()-at.astimezone(rome).date()).days
    team_history = any(profiles.get(side, {}).get('for_'+period, {}).get('sample', 0) > 0
                       for side in ('home', 'away') for period in ('ft', 'ht'))
    triad, signals = route(records, observed_patterns, study=True,
                           allow_model_estimates=1 <= days_ahead <= 4, allow_unpriced=team_history)
    first = next((pick for pick in triad.values() if pick), None)
    tier = max((pick['tier'] for pick in triad.values() if pick), key=TIER_RANK.__getitem__,
               default='BLU' if niches or any(row['tier'] for row in records.values()) else None)
    payload = dict(version=VERSION, status='published', fixture_id=str(meta['fixture_id']),
                   captured_at=utc(at), kickoff=utc(kickoff), statistics_at=utc(statistics_at),
                   statistics=stats, raw_rates=raw, market_projection=projected, rates=calibrated,
                   weight=weight if projected else 0., calibration=calibration or 'baseline',
                   fair_inputs=dict(totals={p: {str(k): v for k, v in lines.items()} for p, lines in totals.items()},
                                    result=fair_result),
                   data_status='team_history' if any(r['supported'] for r in records.values()) else 'limited',
                   baseline_model=assessment.get('model_id'), means=means, markets=records,
                   signals=signals, triad=triad, tier=tier, badge=BADGES.get(tier),
                   horizon=first['horizon'] if first else None,
                   niche_id=(first and first['niche_id']) or next(iter(niches), None),
                   niches={name: sorted(keys) for name, keys in niches.items()})
    return {**payload, 'sha256': fingerprint(payload)}


def stored_offers(assessment, fixture, as_of):
    """Read the published slots and their executable quotes without re-ranking."""
    matrix = assessment.get('matrix') or {}
    snapshot = matrix.get('v7') or {}
    triad = snapshot.get('triad') or {}
    at, captured, kickoff = (stamp(value) for value in
                             (as_of, snapshot.get('captured_at'), fixture.get('kickoff')))
    if (snapshot.get('version') != VERSION or snapshot.get('status') != 'published'
            or snapshot.get('fixture_id') != str(fixture.get('fixture_id'))
            or kickoff != stamp(snapshot.get('kickoff'))
            or not (at and captured and kickoff and captured <= at < kickoff)):
        return
    rows = {row['id']: row for row in matrix.get('rows', [])}
    for role in TRIAD_MARKETS:
        pick = triad.get(role) or {}
        source = rows.get(pick.get('source_market_id')) or {}
        market = (snapshot.get('markets') or {}).get(pick.get('market_id')) or {}
        if (not pick.get('study_only') and pick.get('tier') in TIER_RANK and source.get('probability_model') == VERSION
                and (not source.get('prematch_stable') or pick['tier'] == 'BLU')
                and source['id'] == market.get('source_market_id')
                and pick.get('odd') == source.get('odd') == market.get('odd')
                and finite(pick.get('probability')) and pick['probability'] == market.get('calibrated')
                and price_usable(source, as_of, fixture['kickoff'])
                and stamp(source['quote_observed_at']) <= captured):
            yield role, pick, source
