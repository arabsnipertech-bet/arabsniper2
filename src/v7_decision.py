"""Shadow proposals and routing. Observed movement never changes a probability."""
from collections import defaultdict
from hashlib import sha256
import json

from lateral_matrix import label, market_id, specification
from matrix_quotes import safe_price_row
from multimarket_model import stamp
from v7_probability import (BASE_WEIGHT, VERSION, blend_rates, fair_inputs, finite,
                            market_rates, outcome_fair, price_usable, raw_rates,
                            score_model, statistical_rates, utc)

GOAL = {
    name: market_id(specification('total', period, 'over', line))
    for name, period, line in (('over15_ht', 'ht', 1.5), ('over25_ht', 'ht', 2.5),
                              ('over35_ht', 'ht', 3.5), ('over25_ft', 'ft', 2.5),
                              ('over35_ft', 'ft', 3.5))
}
GG = market_id(specification('btts', 'ht', 'yes'))
AWAY = market_id(specification('result', 'ft', '2'))
LINEAR = frozenset(GOAL.values()) - {GOAL['over35_ht']}
COMBOS = (specification('pt_ft_total', 'ft', 'over', 1.5, 2.5),
          specification('pt_btts_ft_total', 'ft', 'yes', 2.5))
LONG_PRICE = 4.50  # Observation boundary, versioned as part of the shadow routing.
MIN_LINEAR_P = .32  # Eligibility for an exploratory card, independent of Gold/Platinum.


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
        # Preserve legacy identities and ledger traces. Use one price and one
        # movement per event, both taken from the same executable offer.
        pick = max(usable, key=lambda r: (r['odd'], stamp(r['quote_observed_at']), r['id'] == key, r['id']),
                   default=next((r for r in group if r['id'] == key), group[0]))
        chosen[key] = pick, bool(usable)
        inputs.extend(dict(r, spec=canonical(r['spec'])) for r in usable
                      if r['quote_status'] == 'recent' and outcome_fair(r) is not None)
    return chosen, inputs


def features(row, usable, as_of):
    if not usable:
        return {}
    move = row.get('selected_movement') or {}
    from_at, to_at, now = (stamp(t) for t in (move.get('observed_from'), move.get('observed_to'), as_of))
    repeated = bool(from_at and to_at and from_at < to_at <= now)
    oci = row.get('oci')
    return dict(delta_pct=move.get('delta_pct') if repeated else None,
                oci=oci if finite(oci) else None,
                oci_band=('below_-50' if oci < -50 else '-50_to_0' if oci < 0 else
                          '0_to_50' if oci < 50 else '50_plus') if finite(oci) else None,
                persistence=move.get('persistence') if repeated else None,
                reversal=move.get('reversal') if repeated else None,
                margin_pct=row.get('selected_margin_pct'),
                paired_books=row.get('paired_books'),
                agreeing_books=row.get('agreeing_books'))


def bounded_probability(model, spec):
    return min(1., max(0., model.estimate(spec)['p']))


def _directions(record):
    delta = record.get('features', {}).get('delta_pct')
    return (delta > 0, delta < 0) if finite(delta) else (False, False)


def route(rows, fair_result):
    """One best proposal per role, with all research hypotheses logged."""
    def market(key):
        return rows.get(key, {})

    def rank(key):
        row = rows[key]
        return row['calibrated'], row['ev'] if row['ev'] is not None else -1., key

    def edge(key):
        return not market(key).get('edge_exclusions', ['missing'])

    gg, over, over_ht, upset, four = GG, GOAL['over25_ft'], GOAL['over15_ht'], AWAY, GOAL['over35_ht']
    gg_up, over_down = _directions(market(gg))[0], _directions(market(over))[1]
    gg_oci, gg_price = market(gg).get('features', {}).get('oci'), market(gg).get('odd')
    away_price = market(upset).get('odd')
    gg_surprise = gg_up and finite(gg_price) and gg_price >= 5.50 and finite(gg_oci) and gg_oci < -50
    away_surprise = _directions(market(upset))[0] and finite(away_price) and away_price >= LONG_PRICE
    conversion = gg_up and over_down
    excluded_result = any(r['spec_kind'] == 'result' and r['supported'] and r['odd'] is not None
                          and r.get('policy_exclusions') and r.get('edge_exclusions') for r in rows.values())
    favorite = ('1', 'x', '2')[max(range(3), key=fair_result.__getitem__)] if fair_result else None
    favorite_down = any(r['spec_kind'] == 'result' and r['side'] == favorite and _directions(r)[1]
                        for r in rows.values())
    linear = [k for k in LINEAR & rows.keys() if edge(k) and rows[k]['calibrated'] >= MIN_LINEAR_P
              and rows[k]['odd'] < LONG_PRICE]
    best = over if over in linear and (conversion or edge(gg)) else max(linear, key=rank, default=None)
    alt_order = [gg, over_ht] if best == over else [over_ht, over]
    alternatives = [k for k in alt_order if best and k != best and edge(k) and
                    (k != gg or rows[k]['odd'] > rows[best]['odd'])]
    if not alternatives and best:
        alternatives = sorted((k for k in linear if k != best), key=rank, reverse=True)
    alternative = next(iter(alternatives), None)
    surprises = [k for k, observed in ((gg, gg_surprise), (upset, away_surprise)) if observed]
    longshots = [k for k in (four, *surprises) if k not in (best, alternative) and edge(k)
                 and rows[k]['odd'] >= LONG_PRICE and rows[k]['calibrated'] < MIN_LINEAR_P]
    # Pure probability and offered price must independently support each
    # longshot, including Over 3,5 PT. Conversion never generates that pick.
    colpaccio = max(longshots, key=lambda k: (rows[k]['ev'], rows[k]['calibrated'], k), default=None)
    study = max((k for k in LINEAR & rows.keys() if rows[k]['supported']),
                key=lambda k: (rows[k]['statistics'], k), default=None)

    def signal(key, *reasons):
        return dict(market_id=key, reasons=list(reasons)) if key else None

    return dict(
        lineare=signal(best, 'conversione_gg_pt' if best == over and conversion else
                       'alternativa_goal_1x2' if excluded_result else 'probabilita_e_prezzo'),
        alternativa=signal(alternative, 'prezzo_e_probabilita_propri'),
        colpaccio=signal(colpaccio, 'quota_alta_e_riscontro_statistico'),
        studio=signal(study, 'stima_prematch'),
        radar_conversione=signal(over if conversion else None, 'gg_pt_sale_over25_ft_scende'),
        radar_sorprese=signal(max(surprises, key=lambda k: (edge(k), rows[k]['calibrated'] or 0., k),
                                  default=None), 'quota_alta_in_salita'),
        observations={
            'gg_pt_up_over25_ft_down': conversion,
            'favorite_down_goals_up': favorite_down and any(_directions(market(k))[0] for k in LINEAR | {gg}),
            'away_high_rising': away_surprise, 'gg_ht_high_rising_negative_oci': gg_surprise,
            'result_excluded': excluded_result,
            'over25_ft_intermediate_persistence': (
                0 < market(over).get('features', {}).get('persistence') < 1
                if finite(market(over).get('features', {}).get('persistence')) else None),
        })


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
    baseline = {r['id']: r for r in original_rows}
    records = {}
    candidates = [(canonical(r['spec']), r, usable) for r, usable in chosen.values()]
    candidates += [(spec, {}, False) for spec in COMBOS]
    for spec, row, usable in candidates:
        key = market_id(spec)
        fair_p = outcome_fair(row) if usable else None
        if fair_result and spec['kind'] == 'result' and spec['period'] == 'ft':
            fair_p = fair_result[('1', 'x', '2').index(spec['side'])]
        original = baseline.get(key, row)
        original_p = original.get('statistical_pct')
        odd = row.get('odd') if usable else None
        estimate = learned_model.estimate(spec)
        probability = min(1., max(0., estimate['p']))
        ev = estimate['win_weight'] * (odd - 1) - estimate['loss_weight'] if odd else None
        pure = bounded_probability(raw_model, spec)
        stat_estimate = stat_model.estimate(spec)
        independent = min(1., max(0., stat_estimate['p']))
        supported = row.get('quality') == 'supported'
        exclusions = []
        if not usable:
            exclusions.append('quota_non_eseguibile')
        if fair_p is None:
            exclusions.append('fair_non_disponibile')
        if not supported:
            exclusions.append('statistica_insufficiente')
        if ev is not None and ev <= 0:
            exclusions.append('ev_non_positivo')
        if odd and stat_estimate['win_weight'] * (odd - 1) <= stat_estimate['loss_weight']:
            exclusions.append('prezzo_non_sostenuto_dalle_statistiche')
        quote = ({**{k: row.get(k) for k in ('first_odd', 'bookmaker', 'quote_status')},
                  **{k: utc(row.get(k)) for k in ('first_observed_at', 'quote_observed_at',
                                                  'provider_updated_at')}} if usable else None)
        records[key] = dict(source_market_id=row.get('id'), label=label(spec),
                            spec_kind=spec['kind'], side=spec['side'],
                            raw=round(pure, 8), statistics=round(independent, 8),
                            corrected=round(bounded_probability(fixed_model, spec), 8),
                            calibrated=round(probability, 8), fair=fair_p,
                            v51=original_p / 100 if finite(original_p) else None,
                            odd=odd, ev=ev, supported=supported,
                            quote=quote,
                            features=features(row, usable, as_of),
                            edge_exclusions=exclusions,
                            policy_exclusions=(row.get('opportunity') or {}).get('reasons') or [],
                            platinum_exclusions=(row.get('platinum') or {}).get('reasons') or [])
    signals = route(records, fair_result)
    signals['gold'] = {'market_id': (matrix.get('opportunity_policy') or {}).get('best_market_id')}
    signals['platinum'] = {'market_id': (matrix.get('platinum_policy') or {}).get('best_market_id')}
    signals['radar_blu'] = {'market_id': (matrix.get('quote_monitoring') or {}).get('watch_id')}
    payload = dict(version=VERSION, status='shadow', fixture_id=str(meta['fixture_id']),
                   captured_at=utc(at), kickoff=utc(kickoff),
                   statistics_at=utc(statistics_at), statistics=stats, raw_rates=raw,
                   market_projection=projected, rates=calibrated, weight=weight if projected else 0.,
                   calibration=calibration or 'baseline',
                   fair_inputs=dict(totals={p: {str(k): v for k, v in lines.items()} for p, lines in totals.items()},
                                    result=fair_result),
                   data_status='team_history' if any(r['supported'] for r in records.values()) else 'limited',
                   baseline_model=assessment.get('model_id'), means=means, markets=records, signals=signals)
    return {**payload, 'sha256': fingerprint(payload)}
