"""Build and record the prematch V7 triad."""
from collections import defaultdict
from datetime import timedelta
from functools import lru_cache
from hashlib import sha256
from itertools import product
import json
from math import isclose
from zoneinfo import ZoneInfo

from adaptive_pricing import BASE_FIELDS, reprice
from context_analysis import congruence_triggers
from gold_competitions import national_competition
from lateral_matrix import label, market_id, payout_weights, quality, specification
from market_scope import COMBO_SPECS, JOINT_KINDS, canonical_market as canonical
from matrix_quotes import safe_price_row, upgrade_equivalent_prices
from multimarket_model import fair, stamp
from opportunity_policy import annotate as annotate_gold, operational_role_allowed, margin_friction
from statistics_first import market_lambda_profile
from platinum_policy import annotate as annotate_platinum
from v7_probability import (BASE_WEIGHT, VERSION, blend_rates, fair_inputs, finite,
                            goal_structure, market_rates, outcome_fair, price_usable, pricing_evidence, raw_rates,
                            score_model, statistical_rates, utc, price_identity)
from v7_recent_selection import POLICY_ID as LATEST_PREMATCH_POLICY, save as save_latest_selection
from v7_over15_deficit import single_deficit, priority_bonus, explain as explain_deficit
from v7_confirmation import confirmation, oci_context

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
STUDY_SELECTION_POLICY = 'TRIAD_ROLE_COHERENCE_V1'
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
# Independent role switches. Roll back this gate without changing prices or models.
EV_RECOVERY_AGGIO_V1 = {
    'enabled': True, 'min_ev': -.10, 'roles': tuple(TRIAD_MARKETS),
    'price_support_roles': ('recommended_alternative', 'high_odds_shot'),
}
TRIAD_QUALIFIED_REFRESH_V1 = {
    'enabled': True, 'alternative_roles': ('recommended_alternative',),
    'repair_commitments': True,
}
POSITIVE_ALTERNATIVES_POLICY = 'TRIAD_POSITIVE_ALTERNATIVES_V2'
DECISION_QUALITY_POLICY = 'V7_SURGICAL_20261126_V1'
PRICE_UPGRADE_FIELDS = ('price_upgrade_source', 'price_upgrade_control_market',
    'price_upgrade_control_odd', 'price_upgrade_same_operator', 'price_upgrade_probability_difference',
    'price_upgrade_ev_gain')
OPPOSITE_FIELDS = ('opposite_market', 'opposite_odd', 'opposite_ev', 'opposite_margin_pct',
    'opposite_same_operator', 'cross_same_operator', 'opposite_conflict_flag', 'opposite_quote_observed_at')
BASE_MARKETS = {GOAL['over25_ft'], GOAL['over15_sh'], GG_FT, BOTH_GOALS, RESULTS['1'], RESULTS['2']}


def fingerprint(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                             ensure_ascii=False, allow_nan=False).encode()).hexdigest()


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
    from_at, to_at, now = (stamp(t) for t in (move.get('leg_observed_from',move.get('observed_from')),
                                           move.get('leg_observed_to',move.get('observed_to')), as_of))
    repeated = bool(from_at and to_at and from_at < to_at <= now)
    first, odd = row.get('first_odd'), row['odd']
    first_at, last_at = (stamp(row.get(k)) for k in ('first_observed_at', 'quote_observed_at'))
    opening_delta = 100*(odd/first-1) if first_at and last_at and first_at < last_at <= now and finite(first) and first > 1 else None
    net = 0. if row.get('prematch_stable') else opening_delta
    context = oci_context(row)
    oci = context['oci']
    return dict(delta_pct=move.get('delta_pct') if repeated else None, net_delta_pct=net,
                opening_delta_pct=opening_delta,
                delta_basis='last_leg', oci_state=context['oci_state'],
                delta_observed_from=utc(from_at) if repeated else None,
                delta_observed_to=utc(to_at) if repeated else None,
                last_confirmed_oci=context['last_confirmed_oci'],
                oci=oci if finite(oci) else None, oci_method=row.get('oci_method'),
                oci_band=('below_-50' if oci < -50 else '-50_to_0' if oci < 0 else
                          'zero' if oci == 0 else '0_to_50' if oci < 50 else '50_plus') if finite(oci) else None,
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


def scenario_means(assessment, scenario, index, as_of):
    """Rebuild ST observations from the same history as a verified rate probe."""
    from statistics_first import frozen_goal_observations

    sources = assessment.get('sensitivity_sources') or []
    histories = (assessment.get('inputs') or {}).get('histories') or {}
    if (not isinstance(sources, list) or index >= len(sources) or not all(isinstance(histories.get(s), list)
                                       for s in ('home', 'away'))):
        return None
    source = sources[index]
    if (not isinstance(source, dict) or source.get('rates') != scenario
            or 'omitted' not in source):
        return None
    omitted = source['omitted']
    if omitted is not None:
        if not isinstance(omitted, dict) or omitted.get('side') not in ('home', 'away'):
            return None
        side, fid = omitted['side'], omitted.get('fixture_id')
        rows = histories[side]
        if fid is None or sum(str(r.get('fixture_id')) == str(fid) for r in rows) != 1:
            return None
        histories = {**histories, side: [r for r in rows if str(r.get('fixture_id')) != str(fid)]}
    return frozen_goal_observations(histories, {}, as_of)[0]['teams']


def publish_prices(assessment, meta, as_of, model, projection, weight, means, *,
                   fair_totals=None, fair_result=None):
    scenarios = assessment.get('pure_poisson_sensitivity_rates', assessment.get('sensitivity_rates', []))
    models, verified = [model], 0
    for index, scenario in enumerate(scenarios or []):
        rates = raw_rates({'rates': scenario})
        if rates is None:
            continue
        trial_means = scenario_means(assessment, scenario, index, as_of)
        complete = trial_means is not None and fair_totals is not None
        trial_stats = statistical_rates(rates, trial_means if complete else means)
        # Prices and the calibrated weight stay fixed. Refit the same model:
        # its period shares also depend on the omitted statistical observation.
        trial_projection = market_rates(trial_stats, fair_totals, fair_result) if complete else projection
        models.append(score_model(blend_rates(trial_stats, trial_projection, weight)))
        verified += complete
    assessment['matrix']['v7_sensitivity'] = dict(
        method='leave_one_history_out_full_estimator_v1',
        scenarios=len(models)-1, verified_contexts=verified,
        complete=bool(verified and verified == len(models)-1),
        legacy_fallback_contexts=len(models)-1-verified)
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


def confirmed_triad_coherent(triad):
    """Fail closed for mutually exclusive confirmed offers; never rewrite history."""
    if not isinstance(triad, dict):
        return False
    keys = [pick.get('market_id') for role, pick in triad.items()
            if role in TRIAD_MARKETS and pick and not pick.get('study_only')
            and not pick.get('observation_only') and pick.get('tier') in TIER_RANK]
    try:
        return len(set(keys)) == len(keys) and (not keys or compatible(keys[0], *keys[1:]))
    except (TypeError, ValueError, KeyError):
        return False


def selection_motivation(row, reason, relaxed):
    quote, move = row.get('quote') or {}, row['features']
    fair, first, odd = row.get('fair'), quote.get('first_odd'), row['odd']
    margin, oci = move.get('margin_pct'), move.get('oci')
    summary = {
        'PLATINUM': 'Criteri Gold e flusso Platinum confermati',
        'GOLD': 'Criteri Gold superati',
        'BLU': 'Spunto Blu',
    }.get(row['tier'], 'Osservazione di mercato')
    if reason in ('solo_studio', 'stima_modello'):
        summary = ('Studio · scelta compatibile per probabilità V7' if relaxed else
                   'Studio · probabilità e fascia quota del ruolo')
    if reason == 'ev_recovery_aggio_v1':
        summary = f"Rivalutazione statistica · EV originale {100*row['ev']:+.1f}%"
    if reason == 'alternativa_qualificata':
        summary = 'Alternativa con valore e margine prudente confermati'
    if reason == 'solo_osservazione':
        summary = 'Osservazione · qualifica non confermata al prezzo osservato'
    parts = [summary, f"Probabilità V7 {100*row['calibrated']:.1f}%",
             f'Fair bookmaker {100*fair:.1f}%' if finite(fair) else 'Fair bookmaker non disponibile']
    recovery = row.get('ev_recovery') or {}
    if reason == 'ev_recovery_aggio_v1' and recovery.get('break_even_probability') is not None:
        parts.extend((f"Poisson grezzo {100*recovery['poisson_probability']:.1f}%",
                      f"Statistiche {100*recovery['statistical_probability']:.1f}%",
                      f"Pareggio alla quota {100*recovery['break_even_probability']:.1f}%"))
    if finite(odd):
        parts.append(f'Prima osservata {first:.2f}' if finite(first) else 'Prima osservata non disponibile')
        state = 'Ultima osservata stabile' if row.get('quote_state') == 'stable' else 'Quota osservata'
        parts.append(f'{state} {odd:.2f}')
        delta = move.get('opening_delta_pct')
        if finite(delta):
            parts.append(f'Variazione da apertura {delta:+.1f}%')
    else:
        parts.append('Prima osservata e quota attuale in attesa di quotazione')
        if finite(row.get('model_fair_odd')):
            parts.append(f"Quota equa V7 {row['model_fair_odd']:.2f}")
    niche = NICHE_LABELS.get(row['niche_id'])
    if niche:
        parts.append('Contesto tattico '+niche)
    parts.append(f'Aggio {margin:.1f}%' if finite(margin) else 'Aggio non disponibile')
    if move.get('paired_books') == 1:
        parts.append('OCI a singolo bookmaker, in attesa di conferma')
    else:
        parts.append('OCI 0 · neutro' if oci == 0 else f'OCI {oci:+.1f}' if finite(oci) else 'OCI non disponibile')
    return ' · '.join(parts).replace('.', ',')


def origin_number(value, digits=2, signed=False):
    if not finite(value):
        return '—'
    return (('+' if signed and value > 0 else '')+f'{value:.{digits}f}').replace('.', ',')


def market_origin(spec, rates, record, *, conversion=False):
    """Explain the saved probability and price without changing admission or ranking."""
    kind, period = spec['kind'], spec['period']
    home_ht, away_ht, home_st, away_st, home_ft, away_ft = (
        rates[key] for key in ('home_ht', 'away_ht', 'home_st', 'away_st', 'home_ft', 'away_ft'))
    ht, st, ft = (origin_number(total) for total in (home_ht+away_ht, home_st+away_st, home_ft+away_ft))
    prob = origin_number(100*record['calibrated'], 1)
    fair_odd = origin_number(record['model_fair_odd'])
    comparison = f'Prob. V7 {prob}% → Quota equa {fair_odd}.'
    evidence = record.get('pricing_evidence') or {}
    value, movement = evidence.get('value_relation') or {}, evidence.get('movement_relation') or {}
    if value.get('status') == 'verified':
        comparison += (f" Fair stesso operatore {origin_number(100*value['fair_probability'], 1)}%"
                       f" · Pareggio economico {origin_number(100*value['break_even_probability'], 1)}%.")
        if value['fair_advantage_below_margin']:
            comparison += ' Il vantaggio sul fair non copre interamente l’aggio; EV originale negativo.'
    if movement.get('status') == 'verified':
        comparison += (f" Finestra stesso operatore: quota {origin_number(movement['net_delta_pct'], 1, True)}%"
                       f" · fair {origin_number(movement['fair_delta_pp'], 1, True)} pp"
                       f" · aggio {origin_number(movement['margin_delta_pp'], 1, True)} pp.")
    qualification = record.get('confirmation') or {}
    if qualification.get('label'):
        return explain_deficit(dict(origin_badge=qualification['label'],
            origin_tooltip=f"{qualification['detail']} {comparison}"),market_id(spec),record)
    niche = record['niche_id']
    if niche == 'fav_home_pt_vulnerable':
        badge = NICHE_LABELS[niche]
        text = f"Favorita casa con spinta precoce (λ 1°T casa {origin_number(home_ht)}) e difesa vulnerabile nei primi 45′ contro un ospite incisivo."
    elif niche == 'balanced_1x2_inversion':
        badge = NICHE_LABELS[niche]
        share = 100*home_ht/(home_ht+away_ht) if home_ht+away_ht else None
        text = f"Quote 1 e 2 in equilibrio con movimento incrociato dall’apertura. Peso gol 1°T della casa {origin_number(share, 1)}%."
    elif niche == 'bookmaker_shift_rebound':
        badge = NICHE_LABELS[niche]
        text = (f"Mercato 1°T in risalita su base offensiva solida (λ 1°T totale {ht}) e compressione sul finale."
                if conversion else
                f"Mercato 1°T osservato per il rimbalzo su base offensiva solida (λ 1°T totale {ht}) e il confronto con il finale.")
    elif niche == 'pt_st_symmetry_11_22':
        badge = NICHE_LABELS[niche]
        text = f"Alto volume atteso (λ 90′ {ft}) distribuito tra 1° tempo (λ {ht}) e 2° tempo (λ {st})."
    else:
        badge, text = None, None
    if badge:
        return explain_deficit(dict(origin_badge=badge, origin_tooltip=f'{text} {comparison}'), market_id(spec), record)

    odd, ev, low = (record[key] for key in ('odd', 'ev', 'ev_low'))
    move, quote = record['features'], record.get('quote') or {}
    first = quote.get('first_odd') or quote.get('opening_odd')
    delta, oci, margin = (move.get(key) for key in ('opening_delta_pct', 'oci', 'margin_pct'))
    if finite(odd) and odd > 1:
        if not finite(delta) and finite(first) and first > 1:
            delta = 100*(odd/first-1)
        shown_odd, opening = origin_number(odd), origin_number(first)
        change, flow = origin_number(delta, 1, True), origin_number(oci, 1, True)
        ev_pct = origin_number(100*ev, 1, True)+'%' if finite(ev) else '—'
        low_pct = origin_number(100*low, 1, True)+'%' if finite(low) else '—'
        if (finite(oci) and oci > 0 and (not qualification or qualification.get('flow_confirmed'))
                and (record['tier'] == 'PLATINUM' or oci >= 30 and finite(delta) and delta <= -3+1e-9)):
            badge = f'Flusso OCI {flow}'
            books = '/'.join(origin_number(move.get(key), 0) for key in ('agreeing_books', 'paired_books'))
            change_pct = change+'%' if finite(delta) else '—'
            text = f"Calo quota {change_pct} dall’apertura ({opening} → {shown_odd}). Operatori concordi {books} · EV {ev_pct}."
        elif finite(ev) and (record['tier'] == 'GOLD' or ev > 0 and finite(low) and low >= 0):
            if finite(oci) and oci < 0:
                badge = 'Valore Contrarian'
                text = f'Quota bookmaker {shown_odd} · EV {ev_pct} · EV prudente {low_pct} · OCI {flow}.'
            else:
                badge = f'Valore +EV ({origin_number(100*ev, 1)}%)'
                aggio = origin_number(margin, 1)+'%' if finite(margin) else 'non disponibile'
                text = f'Quota bookmaker {shown_odd} sopra la quota equa V7 · Aggio {aggio}.'
        elif finite(delta) and abs(delta) >= 2.5-1e-9:
            badge = 'Rialzo senza conferma' if delta > 0 else 'Quota in calo, conferma insufficiente'
            text = f"Variazione dalla prima osservata {opening} a {shown_odd} ({change}%) · OCI {flow}. Il solo movimento non qualifica la proposta."
        if badge:
            return explain_deficit(dict(origin_badge=badge, origin_tooltip=f'{text} {comparison}'), market_id(spec), record)

    if kind in JOINT_KINDS:
        badge = 'Incrocio 1°T+2°T'
        text = f'Probabilità congiunta dalle distribuzioni del 1°T (λ {ht}) e del 2°T (λ {st}).'
    elif period == 'ht':
        badge = 'Bilanciamento 1°T' if kind == 'btts' else f'Ritmo 1°T (λ {ht})'
        text = (f"Gol attesi nei primi 45′ · λ Casa {origin_number(home_ht)} · Ospite {origin_number(away_ht)}."
                if kind == 'btts' else f"Volume gol atteso nei primi 45′ · λ totale {ht}.")
    elif period == 'sh':
        badge = 'Spinta Ripresa 2°T' if kind == 'btts' else f'Dinamica 2°T (λ {st})'
        text = (f'Gol attesi nella ripresa · λ Casa {origin_number(home_st)} · Ospite {origin_number(away_st)}.'
                if kind == 'btts' else f'Volume gol atteso nella ripresa · λ totale {st}.')
    elif kind == 'btts':
        badge = "Indice GG 90'"
        text = f"Marcatura congiunta sui 90′ · λ Casa {origin_number(home_ft)} · Ospite {origin_number(away_ft)}."
    elif kind in ('result', 'dnb', 'handicap'):
        home, away = origin_number(home_ft), origin_number(away_ft)
        badge = f"Forza 90' ({home}-{away})"
        text = f"Gol attesi sui 90′ · λ Casa {home} · Ospite {away}."
    else:
        badge = f"Volume 90' (λ {ft})"
        text = f"Proiezione Poisson sui 90′ · λ totale {ft}."
    return explain_deficit(dict(origin_badge=badge, origin_tooltip=f'{text} {comparison}'), market_id(spec), record)


def signal(key, row, reason, *, relaxed=False):
    return dict(market_id=key, source_market_id=row['source_market_id'], label=row['label'],
                odd=row['odd'], probability=row['calibrated'], ev=row['ev'], ev_low=row['ev_low'],
                model_fair_odd=row.get('model_fair_odd'), quote_state=row.get('quote_state'),
                tier=row['tier'], badge=BADGES.get(row['tier']), horizon=row['horizon'],
                horizon_label=TAGS[row['horizon']], niche_id=row['niche_id'],
                niche_label=NICHE_LABELS.get(row['niche_id']), reasons=[reason],
                motivation=selection_motivation(row, reason, relaxed),
                **{field: row[field] for field in ('origin_badge', 'origin_tooltip', 'selection_context',
                    'lambda_profile', 'decision_quality_policy', *OPPOSITE_FIELDS, *PRICE_UPGRADE_FIELDS) if field in row})


def mark_opposite_conflicts(records, sources, as_of, kickoff):
    """Compare executable opposite prices in this same immutable snapshot."""
    for key, row in records.items():
        spec = json.loads(key)
        kind, side = spec.get('kind'), spec.get('side')
        reverse = ({'over': 'under', 'under': 'over'} if kind == 'total' else
                   {'yes': 'no', 'no': 'yes'} if kind == 'btts' else {})
        opposite = market_id({**spec, 'side': reverse[side]}) if side in reverse else None
        other = records.get(opposite) or {}
        quote = row.get('quote') or {}
        matches = []
        # Sources include all recorded representations of the same contract.
        for source in sources.values():
            if (not opposite or market_id(canonical(source.get('spec') or {})) != opposite
                    or not price_usable(source, as_of, kickoff) or not price_identity(source)
                    or not finite(other.get('calibrated'))):continue
            same = str(source['bookmaker_id']) == str(quote.get('bookmaker_id')) if quote.get('bookmaker_id') is not None else None
            ev = other['calibrated']*source['odd']-1
            matches.append((ev >= 0, same is True, ev, str(source['id']), source, same))
        best = max(matches, key=lambda v:v[:4]) if matches else None
        conflict = bool(best and best[0] and finite(row.get('ev')) and row['ev'] < 0 and
                        finite(row.get('odd')) and row.get('quote'))
        evidence = dict(opposite_market=opposite, opposite_odd=best[4]['odd'] if best else None,
            opposite_ev=best[2] if best else None, opposite_margin_pct=best[4].get('selected_margin_pct') if best else None,
            opposite_same_operator=best[5] if best else None, cross_same_operator=best[5] if best else None,
            opposite_conflict_flag=conflict,
            opposite_quote_observed_at=utc(best[4].get('quote_observed_at')) if best else None)
        row.update(evidence)
        source = sources.get(row['source_market_id'])
        if source is not None:
            source.update(evidence)
            if conflict:
                for name in ('opportunity', 'platinum'):
                    decision = source.get(name)
                    if decision:
                        decision.update(eligible=False, strict_eligible=False, rank=None,
                            reasons=list(dict.fromkeys([*(decision.get('reasons') or []), 'opposite_value_conflict'])))
        if conflict:row['edge_exclusions'].append('opposite_value_conflict')


def routing_markets(role_markets, records):
    """Transfer admission only to the real, verified better-price contract."""
    result = {role:set(markets) for role,markets in role_markets.items()}
    for markets in result.values():
        for key in list(markets):
            better = records.get(key, {}).get('price_replaced_by')
            if better in records:
                markets.remove(key);markets.add(better)
    return result


def selection_quality_rank(role, key, row):
    ev = row.get('ev'); margin = (row.get('features') or {}).get('margin_pct')
    profile = row.get('lambda_profile') or {}
    event = row.get('price_upgrade_control_market', key)
    preferred = (role != 'most_probable' or event in BASE_MARKETS and finite(row.get('odd'))
                 and 1.55 <= row['odd'] <= 2.15 and row['calibrated'] >= .45)
    return (not row.get('opposite_conflict_flag'), finite(ev) and ev >= 0,
            preferred, not profile.get('weak_side', False))


def fill_study_slots(triad, records, allow_model_estimates, allow_unpriced, locked, role_markets,
                     ranker=None, candidate_filter=None):
    model_only = not any(finite(row['odd']) for row in records.values())
    if model_only and not allow_unpriced:
        return
    fixed = set(locked.values()) | {pick['market_id'] for pick in triad.values() if pick}
    options, standard = {}, {}
    for role, pick in triad.items():
        if pick:
            continue
        floor, minimum, maximum = STUDY_LIMITS[role]
        locked_market = locked.get(role)
        candidates, preferred = [None], set()
        available = {locked_market} & records.keys() if locked_market else records.keys() - fixed
        for key in available:
            row = records[key]
            if candidate_filter and not candidate_filter(role,key,row,fixed):
                continue
            if row['spec_kind'] not in ('result', 'total', 'btts', *JOINT_KINDS) and not row.get('price_upgrade_source'):
                continue
            is_estimate = not finite(row['odd'])
            if is_estimate and not (model_only or allow_model_estimates):
                continue
            value = row['model_fair_odd'] if is_estimate else row['odd']
            if not operational_role_allowed(role,json.loads(key),value,row.get('ev'),(row.get('features') or {}).get('margin_pct')):
                continue
            in_role = key in role_markets[role]
            if not in_role and not locked_market:
                continue  # Outside-role observations belong in the full matrix.
            if not (finite(value) and value > 1 and 0 < row['calibrated'] <= 1):
                continue
            within_limits = (row.get('study_supported') and row['calibrated'] >= floor
                             and minimum <= value <= maximum and (in_role or not model_only))
            if (locked_market or within_limits or in_role) and compatible(key, *fixed):
                candidates.append(key)
                if within_limits and in_role:
                    preferred.add(key)
        options[role], standard[role] = candidates, preferred
    if not options:
        return

    def score(picks):
        # Keep the standard choices wherever a complete, compatible triad
        # permits it. Rank relaxed candidates by their calibrated probability.
        ranks = []
        for role, key in zip(options, picks):
            if key is None:
                ranks.append((0, False, 0., 0., ''))
                continue
            row = records[key]
            preferred = key in standard[role] and not row.get('opposite_conflict_flag')
            ranking = (*selection_quality_rank(role,key,row),*(ranker(role,key,row) if ranker else (row['calibrated'],row['ev'] or 0.,key)))
            ranks.append((2 if preferred else 1, preferred and finite(row['odd']),*ranking))
        # A third relaxed filler must not displace a standard role candidate.
        return (sum(rank[0] == 2 for rank in ranks), sum(rank[0] > 0 for rank in ranks),
                sum(rank[1] for rank in ranks), ranks)

    def valid(picks):
        extra = [key for key in picks if key is not None]
        return not extra or len(set(extra)) == len(extra) and compatible(extra[0], *extra[1:], *fixed)

    best = max(filter(valid, product(*options.values())), key=score)
    for role, key in zip(options, best):
        if key is not None:
            row = records[key]
            estimate = row['odd'] is None
            study_row = {**row, 'tier': 'BLU', 'quote_state': 'model_estimate' if estimate else row['quote_state']}
            triad[role] = {**signal(key, study_row, 'stima_modello' if estimate else 'solo_studio',
                                    relaxed=key not in standard[role]), 'study_only': True}


def role_rank(role, key, row):
    prefix = selection_quality_rank(role,key,row)
    adjusted = row['ev']-margin_friction((row.get('features') or {}).get('margin_pct'))
    if role == 'most_probable':
        return (*prefix, row['calibrated'], row['ev_low'], adjusted, key)
    return (*prefix, bool(row['niche_id']), adjusted+priority_bonus(key, role, row), row['calibrated'], key)


def qualified_refresh_candidate(row):
    """Use documented binary prices and existing conservative value requirements."""
    odd, p, ev, low = (row.get(k) for k in ('odd', 'calibrated', 'ev', 'ev_low'))
    quote = row.get('quote') or {}
    return (row.get('spec_kind') in ('result', 'total', 'btts', *JOINT_KINDS)
            and row.get('tier') in BADGES and row.get('supported') is True
            and finite(row.get('fair')) and 0 < row['fair'] < 1
            and quote.get('quote_status') == 'recent' and quote.get('bookmaker_id') is not None
            and all(finite(v) for v in (odd, p, ev, low)) and odd > 1 and 0 < p <= 1
            and ev > 0 and 0 <= low <= ev + 5e-7 + 1e-12
            and isclose(ev, p*odd-1, rel_tol=0, abs_tol=odd*5e-9+1e-12))


def coherent_slot_prices(records, slots):
    prices = {role: records.get(key, {}).get('odd') for role, key in slots.items()}
    primary, recommended, shot = (prices.get(role) for role in TRIAD_MARKETS)
    return (not (finite(primary) and finite(recommended) and recommended <= primary)
            and not (finite(recommended) and finite(shot) and shot < recommended))


def fill_qualified_alternatives(triad, records, observed, locked, limits, ev_recoveries, roles,
                                role_markets=TRIAD_MARKETS, ranker=None, candidate_filter=None,
                                away_symmetric_shadow=False):
    """Fill an empty Consigliata with positive value before considering recovery."""
    role = 'recommended_alternative'
    if role not in roles or triad[role]:
        return
    candidates = {key for key, row in records.items() if qualified_refresh_candidate(row)}
    if not candidates:
        return
    held = {**locked, **{slot: pick['market_id'] for slot, pick in triad.items() if pick}}
    proposed, _ = route(records, observed, locked=held, limits=limits,
                        role_markets={**role_markets, role: candidates}, ev_recoveries=ev_recoveries,
                        ranker=ranker,candidate_filter=candidate_filter,
                        away_symmetric_shadow=away_symmetric_shadow)
    pick = proposed[role]
    if (not pick or not coherent_slot_prices(records, {**held, role: pick['market_id']})
            or any(triad[other] and (not proposed[other]
                   or proposed[other]['market_id'] != triad[other]['market_id'])
                   for other in triad if other != role)):
        return
    triad[role] = signal(pick['market_id'], records[pick['market_id']], 'alternativa_qualificata')
    triad[role]['selection_policy'] = {'id': POSITIVE_ALTERNATIVES_POLICY, 'action': 'qualified_alternative'}


def ev_recovery_candidates(records, sources, as_of, kickoff, statistics_at, commitments=None, *, policy=None):
    """Compare independent statistics and a prudent scenario with same-book fair.

    This is statistical qualification, never positive value at the offered price.
    Missing evidence fails closed. Old commitments cannot be promoted or replaced.
    """
    policy = EV_RECOVERY_AGGIO_V1 if policy is None else policy
    minimum = policy.get('min_ev')
    at, stats_at = stamp(as_of), stamp(statistics_at)
    if (policy.get('enabled') is not True or not finite(minimum) or not -.10 <= minimum < 0
            or not at or not stats_at or stats_at > at):
        return {}
    candidates = {}
    for role in policy.get('roles', ()):
        if role not in TRIAD_MARKETS:
            continue
        prior = (commitments or {}).get(role)
        if prior and (prior.get('ev_recovery') or {}).get('policy_id') != 'EV_RECOVERY_AGGIO_V1':
            continue
        for key in TRIAD_MARKETS[role] & records.keys():
            if prior and prior['market_id'] != key:
                continue
            row = records[key]
            source = sources.get(row['source_market_id']) or {}
            odd, ev, low = (row.get(k) for k in ('odd', 'ev', 'ev_low'))
            if (row.get('supported') is not True or source.get('v7_supported') is not True
                    or source.get('quality') != 'supported' or source.get('prematch_stable')
                    or source.get('probability_model') != VERSION or source.get('quote_source') == 'synthetic_combo'
                    or not all(finite(v) for v in (odd, ev, low)) or not minimum <= ev < 0
                    or odd != source.get('odd') or not price_usable(source, as_of, kickoff)):
                continue
            fair_p, margin = outcome_fair(source), source.get('selected_margin_pct')
            statistical, calibrated = row.get('statistics'), row.get('calibrated')
            prudent = (1 + low) / odd
            if (fair_p is None or fair_p != row.get('fair')
                    or not all(finite(p) and 0 < p <= 1 for p in (statistical, calibrated, prudent))
                    or min(statistical, calibrated, prudent) < fair_p
                    # Probability is stored to 8 decimals; prudent EV to 4 percent decimals.
                    or not isclose(ev, calibrated*odd-1, rel_tol=0, abs_tol=odd*5e-9+1e-12)
                    or low > ev + 5e-7 + 1e-12
                    or not isclose(fair_p, 1 / odd / (1 + margin / 100), rel_tol=1e-9, abs_tol=1e-12)):
                continue
            # The margin only identifies the fair benchmark. No price-derived bonus.
            price_support = {}
            if role in policy.get('price_support_roles', ()):
                poisson, break_even = row.get('raw'), 1 / odd
                # These estimates share goal history. Require agreement, never
                # count them as two independent votes or add margin to EV.
                # Stored probabilities have eight decimals: half a final digit.
                if (not finite(poisson) or not 0 < poisson <= 1
                        or min(poisson, statistical) + 5e-9 < break_even):
                    continue
                price_support = dict(poisson_probability=poisson,
                                     break_even_probability=break_even)
            candidates.setdefault(role, {})[key] = dict(
                policy_id='EV_RECOVERY_AGGIO_V1', role=role, qualification='statistical_recovery',
                original_ev=ev, original_ev_low=low, probability=calibrated,
                statistical_probability=statistical, prudent_probability=prudent,
                fair_probability=fair_p, margin_pct=margin,
                bookmaker_id=source['bookmaker_id'], statistics_at=utc(stats_at), **price_support)
    return candidates


def away_shot_admitted(row, *, symmetric_shadow=False):
    """Keep live admission; the private trial removes direction only at 4.50-7.50."""
    odd = row.get('odd')
    if not finite(odd) or odd < 4.50:
        return False
    if symmetric_shadow and odd <= 7.50:
        return True
    delta = (row.get('features') or {}).get('net_delta_pct')
    return finite(delta) and delta > 0


def route(records, observed, *, study=False, allow_model_estimates=False, allow_unpriced=False,
          locked=None, role_markets=TRIAD_MARKETS, limits=TRIAD_LIMITS, ev_recoveries=None,
          qualified_alternatives=(), ranker=None, candidate_filter=None, away_symmetric_shadow=False):
    role_markets = routing_markets(role_markets, records)
    custom_filter = candidate_filter
    def candidate_filter(role,key,row,held):
        return (operational_role_allowed(role,json.loads(key),row.get('odd'),row.get('ev'),
                    (row.get('features') or {}).get('margin_pct'))
                and (not custom_filter or custom_filter(role,key,row,held)))
    locked = locked or {}
    reserved = set(locked.values())
    triad = dict.fromkeys(TRIAD_MARKETS)
    chosen = []
    for role, markets in role_markets.items():
        floor, minimum, maximum = limits[role]
        candidates = []
        available = {locked[role]} if role in locked else markets - reserved
        for key in available & markets & records.keys():
            row = records[key]
            if candidate_filter and not candidate_filter(role,key,row,reserved|set(chosen)):
                continue
            odd, ev, low = row['odd'], row['ev'], row['ev_low']
            if not (row['tier'] and finite(odd) and minimum <= odd <= maximum
                    and row['calibrated'] >= floor and finite(ev) and ev > 0):
                continue
            if (role != 'high_odds_shot' or row['tier'] == 'BLU') and (not finite(low) or low < 0):
                continue
            if role == 'recommended_alternative' and chosen and odd <= records[chosen[0]]['odd']:
                continue
            if role == 'high_odds_shot' and key == AWAY and not away_shot_admitted(
                    row, symmetric_shadow=away_symmetric_shadow):
                continue
            if key not in chosen and compatible(key, *reserved, *chosen):
                candidates.append(key)
        if not candidates:
            continue
        key = max(candidates, key=lambda key: (ranker or role_rank)(role,key,records[key]))
        triad[role] = signal(key, records[key], role)
        chosen.append(key)

    # Also consider already-qualified positive alternatives before negative EV
    # recovery can occupy the empty slot. Existing positive native picks stay.
    if qualified_alternatives:
        fill_qualified_alternatives(triad, records, observed, locked, limits,
                                    ev_recoveries, qualified_alternatives, role_markets, ranker, candidate_filter,
                                    away_symmetric_shadow=away_symmetric_shadow)
        chosen = [pick['market_id'] for pick in triad.values() if pick]

    # Finish every positive-EV choice first. Recovery cannot displace it.
    for role, proofs in (ev_recoveries or {}).items():
        if role not in role_markets or triad[role]:
            continue
        floor, minimum, maximum = limits[role]
        available = {locked[role]} if role in locked else role_markets[role] - reserved
        candidates = []
        for key in available & proofs.keys() & records.keys():
            row, proof = records[key], proofs[key]
            if candidate_filter and not candidate_filter(role,key,row,reserved|set(chosen)):
                continue
            odd = row['odd']
            if (not finite(odd) or not minimum <= odd <= maximum or row['calibrated'] < floor
                    or proof['original_ev'] != row['ev'] or proof['probability'] != row['calibrated']
                    or key in chosen or not compatible(key, *reserved, *chosen)):
                continue
            primary, alternative = triad['most_probable'], triad['recommended_alternative']
            if (role == 'most_probable' and alternative and odd >= alternative['odd']
                    or role == 'recommended_alternative' and primary and odd <= primary['odd']):
                continue
            if role == 'high_odds_shot' and key == AWAY and not away_shot_admitted(
                    row, symmetric_shadow=away_symmetric_shadow):
                continue
            candidates.append(key)
        if candidates:
            key = max(candidates, key=lambda k: (ranker or role_rank)(role,k,records[k]))
            triad[role] = {**signal(key, {**records[key], 'tier': 'BLU', 'ev_recovery': proofs[key]},
                                   'ev_recovery_aggio_v1'),
                           'ev_recovery': dict(proofs[key])}
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
        fill_study_slots(triad, records, allow_model_estimates, allow_unpriced, locked, role_markets,
                         ranker,candidate_filter)
    return triad, signals


def commit_priced_slots(triad, captured_at, previous):
    commitments = dict(previous)
    for role, pick in triad.items():
        if not pick or not finite(pick.get('odd')) or pick['odd'] <= 1 or role in commitments:
            continue
        commitments[role] = {field: pick[field] for field in ('market_id', 'label', 'odd', 'horizon')}
        commitments[role].update(captured_at=captured_at, study_only=pick.get('study_only') is True)
        if pick.get('ev_recovery'):
            commitments[role]['ev_recovery'] = dict(pick['ev_recovery'])
        if pick.get('selection_policy'):
            commitments[role]['selection_policy'] = dict(pick['selection_policy'])
    return commitments


def improves_slot(role, candidate, incumbent, gain):
    low, ev = candidate.get('ev_low'), candidate.get('ev')
    return (candidate.get('supported') is True and finite(candidate.get('fair'))
            and (candidate.get('quote') or {}).get('quote_status') == 'recent'
            and finite(low) and low >= 0 and low + 1e-9 >= incumbent['ev_low'] + gain
            and finite(ev) and ev >= incumbent['ev']
            and (role != 'most_probable' or candidate['calibrated'] >= incumbent['calibrated']))


def revise_priced_slots(records, observed, previous, at, kickoff, *, national):
    """Allow one supported upgrade until one hour before kickoff."""
    if (not previous or kickoff - at < timedelta(hours=1)
            or any(pick.get('replaces') for pick in previous.values())):
        return previous

    gain = .04 if national else .02
    locked = {role: pick['market_id'] for role, pick in previous.items()}
    reserved = set(locked.values())
    current, _ = route(records, observed, locked=locked)

    for role, market_set in TRIAD_MARKETS.items():
        original = previous.get(role)
        if not original:
            continue
        incumbent = records.get(original['market_id']) or {}
        if not (finite(incumbent.get('ev_low')) and finite(incumbent.get('ev'))):
            continue
        candidates = [key for key in (market_set & records.keys()) - reserved
                      if improves_slot(role, records[key], incumbent, gain)]
        for key in sorted(candidates, key=lambda key: role_rank(role, key, records[key]), reverse=True):
            proposed, _ = route(records, observed, locked={**locked, role: key})
            if not proposed[role] or any(current[other] and not proposed[other]
                                         for other in locked if other != role):
                continue
            replacement = commit_priced_slots({role: proposed[role]}, utc(at), {})[role]
            replacement['replaces'] = original
            return {**previous, role: replacement}
    return previous


def refresh_unqualified_slots(records, observed, previous, at, kickoff, *, national,
                              ev_recoveries, alternative_roles):
    """Repair one currently unqualified slot; retain its complete original record."""
    if not previous or kickoff-at < timedelta(hours=1):
        return previous
    locked = {role: pick['market_id'] for role, pick in previous.items()}
    current, _ = route(records, observed, locked=locked, ev_recoveries=ev_recoveries,
                       qualified_alternatives=alternative_roles)
    reserved = set(locked.values())
    gain = .04 if national else .02  # Existing revision hysteresis, not a learned threshold.
    for role, market_set in TRIAD_MARKETS.items():
        if role not in previous or current[role]:
            continue
        incumbent = records.get(previous[role]['market_id']) or {}
        # Missing prices/evidence are not proof that a frozen choice deteriorated.
        if not all(finite(incumbent.get(k)) for k in ('odd', 'calibrated', 'ev', 'ev_low')):
            continue
        pool = records.keys() if role in alternative_roles else market_set
        candidates = [key for key in pool if key in records and key not in reserved
                      and qualified_refresh_candidate(records[key])
                      and records[key]['ev_low']+1e-9 >= incumbent['ev_low']+gain]
        for key in sorted(candidates, key=lambda k: role_rank(role, k, records[k]), reverse=True):
            proposed, _ = route(records, observed, locked={**locked, role: key},
                                 ev_recoveries=ev_recoveries, qualified_alternatives=alternative_roles)
            pick = proposed[role]
            if (not pick or pick.get('ev_recovery')
                    or not coherent_slot_prices(records, {**locked, role: key})
                    or any(current[other] and (not proposed[other]
                           or proposed[other]['market_id'] != current[other]['market_id'])
                           for other in locked if other != role)):
                continue
            replacement = commit_priced_slots({role: pick}, utc(at), {})[role]
            replacement['replaces'] = previous[role]
            replacement['selection_policy'] = {'id': 'TRIAD_QUALIFIED_REFRESH_V1',
                                               'action': 'replace_unqualified'}
            return {**previous, role: replacement}
    return previous


def prior_triad_commitments(assessment, meta, as_of):
    """Use the last verified publication when upgrading an already visible fixture."""
    from v7_archive import valid_snapshot

    matrix = assessment.get('matrix') or {}
    saved = matrix.get('v7') or {}
    if saved.get('status') == 'published' and valid_snapshot(saved, meta, as_of):
        if 'triad_commitments' in saved:
            if saved.get('measurement'):
                from official_selections import archived_choice
                return {role: archived_choice(slot, saved) for role, slot in saved['triad_commitments'].items()}
            return saved['triad_commitments']
        return commit_priced_slots(saved.get('triad') or {}, saved['captured_at'], {})
    waiting = matrix.get('v7_waiting_commitments') or {}
    if (waiting.get('fixture_id') == str(meta.get('fixture_id')) and stamp(meta.get('kickoff'))
            and stamp(waiting.get('kickoff')) == stamp(meta['kickoff'])):
        return waiting.get('slots') or {}
    return {}


def capture(assessment, meta, as_of, weight=BASE_WEIGHT, calibration=None, *, commitments=None,
            triad_refresh=None, recent_selection=None):
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
    publish_prices(assessment, meta, as_of, learned_model, projected, weight, means,
                   fair_totals=totals, fair_result=fair_result)
    records, pricing_context = {}, {}
    sources = {r['id']: r for r in original_rows}
    profiles = assessment.get('profiles') or {}
    from v7_movement_shadow import peak_support
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
        raw_estimate, statistical_estimate = raw_model.estimate(spec), stat_model.estimate(spec)
        pricing_context[key] = row, estimate, raw_estimate, statistical_estimate, usable
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
                            raw=round(min(1., max(0., raw_estimate['p'])), 8),
                            statistics=round(min(1., max(0., statistical_estimate['p'])), 8),
                            corrected=round(bounded_probability(fixed_model, spec), 8),
                            calibrated=round(min(1., max(0., estimate['p'])), 8), fair=fair_p,
                            v51=original_p/100 if finite(original_p) else None, odd=odd, ev=ev, supported=supported,
                            study_supported=bool(supported or limited_national_history), quote_state=quote_state,
                            ev_low=ev_low/100 if usable and finite(ev_low) else None,
                            model_fair_odd=estimate['fair_odd'], quote=quote, features=evidence,
                            pricing_evidence=pricing_evidence(row, estimate, raw_estimate, statistical_estimate, usable,
                                                              include_relations=False),
                            edge_exclusions=exclusions, policy_exclusions=(source.get('opportunity') or {}).get('reasons') or [],
                            platinum_exclusions=(source.get('platinum') or {}).get('reasons') or [])
        records[key]['lambda_profile'] = market_lambda_profile(spec, stats,
            without_peak=source.get('without_peak') if type(source.get('without_peak')) is bool
                else peak_support(profiles, spec.get('period')))
        records[key]['decision_quality_policy'] = DECISION_QUALITY_POLICY
    mark_opposite_conflicts(records, sources, as_of, meta['kickoff'])
    niches, observed_patterns = niche_evidence(records, assessment, calibrated, stats, totals, weight,
                                               [r for r in fair_rows if r['quote_status'] == 'recent'], means)
    deficit_context = single_deficit(assessment, meta, as_of)
    for key, row in records.items():
        row['niche_id'] = next((name for name, keys in niches.items() if key in keys), None)
        source = sources[row['source_market_id']]
        margin = (row.get('features') or {}).get('margin_pct')
        promotable = (not row.get('opposite_conflict_flag') and finite(row.get('ev')) and row['ev'] > 0
                      and finite(margin) and 0 <= margin <= 8 and price_identity(source))
        if promotable and (source.get('platinum') or {}).get('eligible') is True:
            row['tier'] = 'PLATINUM'
        elif promotable and (source.get('opportunity') or {}).get('eligible') is True:
            row['tier'] = 'GOLD'
        elif row['niche_id'] or (row['supported'] and finite(row['ev']) and row['ev'] > 0
                                and finite(row['ev_low']) and row['ev_low'] >= 0):
            row['tier'] = 'BLU'
        else:
            row['tier'] = None
        if key == GOAL['over15_ht'] and deficit_context:
            floor, minimum, maximum = TRIAD_LIMITS['recommended_alternative']
            if (row['calibrated'] >= floor and finite(row['odd']) and minimum <= row['odd'] <= maximum):
                row['selection_context'] = deficit_context
                if not priority_bonus(key, 'recommended_alternative', row):
                    row.pop('selection_context')
        row.update(market_origin(source['spec'], calibrated, row, conversion=observed_patterns['conversion']))
        legs = COMBO_LEGS.get(key)
        if legs and row['odd'] is None and all(finite(records.get(leg, {}).get('odd')) for leg in legs):
            row['reference_legs'] = [dict(market_id=leg, label=records[leg]['label'], odd=records[leg]['odd'],
                                          quote=records[leg]['quote']) for leg in legs]

    price_upgrades = upgrade_equivalent_prices(records, sources, as_of, meta['kickoff'])

    rome = ZoneInfo('Europe/Rome')
    days_ahead = (kickoff.astimezone(rome).date()-at.astimezone(rome).date()).days
    policy = TRIAD_QUALIFIED_REFRESH_V1 if triad_refresh is None else triad_refresh
    alternative_roles = policy.get('alternative_roles', ()) if policy.get('enabled') is True else ()
    national = bool(national_competition(meta.get('league')))
    # Restore the pre-lock election: rank every usable market in the latest
    # prematch snapshot. Keep earlier choices only as immutable history.
    # An explicit disabled override remains available for baseline comparison.
    latest_selection = commitments is not None and (recent_selection or {}).get('enabled') is not False
    previous_commitments = commitments
    if latest_selection:
        commitments = {}
    recoveries = ev_recovery_candidates(records, sources, as_of, meta['kickoff'], statistics_at, commitments)
    revised = (refresh_unqualified_slots(records, observed_patterns, commitments, at, kickoff,
                national=national, ev_recoveries=recoveries, alternative_roles=alternative_roles)
               if not latest_selection and policy.get('enabled') is True and policy.get('repair_commitments') is True else commitments)
    if not latest_selection and revised is commitments:
        revised = revise_priced_slots(records, observed_patterns, commitments, at, kickoff, national=national)
    if revised is not commitments:
        commitments = revised
        recoveries = ev_recovery_candidates(records, sources, as_of, meta['kickoff'], statistics_at, commitments)
    locked = {role: slot['market_id'] for role, slot in (commitments or {}).items()}
    triad, signals = route(records, observed_patterns, study=True,
                           allow_model_estimates=0 <= days_ahead <= 5, allow_unpriced=True, locked=locked,
                           ev_recoveries=recoveries, qualified_alternatives=alternative_roles)
    for pick in triad.values():
        if pick and pick.get('ev_recovery'):
            records[pick['market_id']].update(tier='BLU', ev_recovery=dict(pick['ev_recovery']))
    # Describe current qualification separately from the role. The observations
    # remain inspectable; no movement alone can publish a confirmed signal.
    selected = {pick['market_id']:pick for pick in triad.values() if pick}
    for key, record in records.items():
        pick = selected.get(key)
        source = sources[record['source_market_id']]
        if pick:
            record['pricing_evidence'] = pricing_evidence(*pricing_context[key], as_of=as_of)
        record['confirmation'] = confirmation(json.loads(key),record,source,as_of,meta['kickoff'],
                                               pick=pick,patterns=observed_patterns)
        record.update(market_origin(source['spec'],calibrated,record,conversion=observed_patterns['conversion']))
        if pick:
            pick.update({field:record[field] for field in ('origin_badge','origin_tooltip','confirmation')})
            pick['observation_only'] = record['confirmation']['observation_only']
    for role,pick in triad.items():
        if pick and pick['observation_only']:
            pick.update(study_only=True,tier='BLU',badge=BADGES['BLU'])
            pick['motivation'] = selection_motivation(records[pick['market_id']],'solo_osservazione',True)
            signals[SIGNAL_ROLES[role]] = None
    for name in ('radar_conversione','radar_sorprese'):
        pick = signals.get(name)
        if not pick:
            continue
        record = records[pick['market_id']]
        pick.update({field:record[field] for field in ('origin_badge','origin_tooltip','confirmation')})
        pick['observation_only'] = record['confirmation']['observation_only']
        if pick['observation_only']:
            pick.update(study_only=True,tier='BLU',badge=BADGES['BLU'])
            pick['motivation'] = selection_motivation(record,'solo_osservazione',True)
    for tier_name in ('gold','platinum'):
        signals[tier_name] = next((p for p in triad.values() if p and not p.get('observation_only')
                                   and p['tier'] == tier_name.upper()),None)
    first = next((pick for pick in triad.values() if pick), None)
    tier = max((pick['tier'] for pick in triad.values() if pick), key=TIER_RANK.__getitem__,
               default='BLU' if niches or any(row['tier'] for row in records.values()) else None)
    payload = dict(version=VERSION, status='published', fixture_id=str(meta['fixture_id']),
                   decision_quality_policy=DECISION_QUALITY_POLICY, price_upgrades=price_upgrades,
                   qualification_policy=POSITIVE_ALTERNATIVES_POLICY,
                   study_selection_policy=STUDY_SELECTION_POLICY,
                   captured_at=utc(at), kickoff=utc(kickoff), statistics_at=utc(statistics_at),
                   statistics=stats, raw_rates=raw, market_projection=projected, rates=calibrated,
                   goal_structure=goal_structure(calibrated),
                   sensitivity=dict(matrix['v7_sensitivity']),
                   weight=weight if projected else 0., calibration=calibration or 'baseline',
                   fair_inputs=dict(totals={p: {str(k): v for k, v in lines.items()} for p, lines in totals.items()},
                                    result=fair_result),
                   data_status='team_history' if any(r['supported'] for r in records.values()) else 'limited',
                   baseline_model=assessment.get('model_id'), means=means, markets=records,
                   signals=signals, triad=triad, tier=tier, badge=BADGES.get(tier),
                   horizon=first['horizon'] if first else None,
                   niche_id=(first and first['niche_id']) or next(iter(niches), None),
                   niches={name: sorted(keys) for name, keys in niches.items()})
    if commitments is not None:
        from decision_shadow import capture_v7
        payload['triad_commitments'] = (save_latest_selection(triad, payload['captured_at'], previous_commitments)
            if latest_selection else commit_priced_slots(triad, payload['captured_at'], commitments))
        if latest_selection:
            payload['triad_timing'] = dict(id=LATEST_PREMATCH_POLICY, status='current',
                                          evaluated_at=payload['captured_at'])
        payload['decision_shadow'] = capture_v7(records, sources, observed_patterns, triad,
                                                locked=bool(locked), allow_model_estimates=0 <= days_ahead <= 5,
                                                as_of=as_of,kickoff=meta['kickoff'],profiles=profiles,
                                                ev_recoveries=recoveries,qualified_alternatives=alternative_roles,
                                                statistical_rates=stats,statistics_at=utc(statistics_at))
    from official_selections import trace_snapshot
    trace_snapshot(payload, sources, profiles)
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
    if not confirmed_triad_coherent(triad):
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
