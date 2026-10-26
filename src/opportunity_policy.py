"""One versioned proposal policy; never a fixture or probability-matrix filter.

Thresholds are explicit product constraints, not calibrated win guarantees.
The browser consumes these decisions/ranks rather than reimplementing scoring.
"""
from collections import Counter
from math import isfinite

POLICY_ID = 'playable-value-v1'
ODDS_MIN, ODDS_MAX = 1.40, 3.50
LABELS = {'convergence': 'Convergenza di Trend',
          'contrarian': 'Opportunità Contrarian',
          'unconfirmed': 'Valore statistico · flusso non confermato',
          'excluded': 'Fuori proposta'}
REASONS = {
    'market': 'Mercato escluso dal box: resta nella matrice',
    'probability': 'Probabilità inferiore alla soglia del mercato',
    'price': 'Quota fuori dall’intervallo 1,40–3,50',
    'stale': 'Quota recente non disponibile',
    'history': 'Storico statistico insufficiente',
    'adapter': 'Associazione della quota da aggiornare',
    'margin': 'Aggio anomalo: prezzo da verificare',
    'edge': 'Vantaggio insufficiente rispetto all’aggio',
    'sensitivity': 'Vantaggio insufficiente nello scenario prudente',
}


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def market_tier(row):
    spec = row.get('spec') or {}
    kind, period, line = spec.get('kind'), spec.get('period'), spec.get('line')
    if period not in ('ft', 'ht', 'sh'): return None
    primary = kind in ('result', 'dc', 'btts') or kind == 'total' and line in (1.5, 2.5)
    if primary and period == 'ft': return 0
    if primary or kind in ('dnb', 'handicap', 'asian_total', 'team_total', 'total'): return 1
    if kind in ('result_total', 'result_btts', 'dc_total', 'dc_btts', 'btts_total', 'range'): return 2
    # Exact scores and HT/FT combinations remain visible, never promoted here.
    return None


def evaluate(row):
    tier = market_tier(row)
    minimum_p = 35 if tier == 0 else 40
    p, odd = row.get('statistical_pct'), row.get('odd')
    # Only the complete partition of the operator offering this exact price.
    # A median of other operators must not validate an anomalous best price.
    margin = row.get('selected_margin_pct')
    delta = row.get('selected_margin_delta_pp')
    margin_known = numeric(margin)
    anomalous = margin_known and not 0 <= margin <= 30
    low_limit = 5 if tier == 0 else 7
    band = ('anomalous' if anomalous else 'unknown' if not margin_known else
            'low' if margin < low_limit else 'high' if margin > 9 else 'medium')
    min_ev, min_low = {'low': (2., 0.), 'medium': (4., 1.),
                       'high': (max(8., margin - 1), 2.) if margin_known else (8., 2.),
                       'unknown': (10., 3.), 'anomalous': (10., 3.)}[band]
    if numeric(delta) and delta >= 1:
        min_ev += 2
        min_low += 1
    reasons = []
    if tier is None: reasons.append('market')
    if not numeric(p) or not minimum_p <= p <= 100: reasons.append('probability')
    if not numeric(odd) or not ODDS_MIN <= odd <= ODDS_MAX: reasons.append('price')
    if row.get('quote_status') != 'recent': reasons.append('stale')
    if row.get('quality') != 'supported': reasons.append('history')
    if (row.get('spec') or {}).get('kind') == 'handicap' and row.get('quote_adapter_version') != 2:
        reasons.append('adapter')
    if anomalous: reasons.append('margin')
    ev, ev_low = row.get('ev_pct'), row.get('ev_low_pct')
    if not numeric(ev) or ev < min_ev: reasons.append('edge')
    if not numeric(ev_low) or ev_low <= 0 or ev_low < min_low: reasons.append('sensitivity')
    eligible = not reasons
    oci = row.get('oci')
    pairs, agreeing = row.get('paired_books', 0), row.get('agreeing_books', 0)
    selected = row.get('selected_movement') or {}
    coherent = (numeric(oci) and abs(oci) >= 40 and
                row.get('movement_confirmed') is True and row.get('fair_direction_coherent') is True and
                numeric(pairs) and numeric(agreeing) and pairs >= 2 and agreeing >= 2 and agreeing > pairs/2 and
                numeric(selected.get('delta_pct')) and selected['delta_pct'] * oci < 0 and
                numeric(selected.get('fair_delta_pp')) and selected['fair_delta_pp'] * oci > 0 and
                numeric(selected.get('persistence')) and selected['persistence'] >= .5 and
                selected.get('reversal') is False)
    signal = 'excluded' if not eligible else 'unconfirmed'
    if eligible and margin_known and coherent:
        signal = 'convergence' if oci > 0 else 'contrarian'
    return {'policy_id': POLICY_ID, 'eligible': eligible, 'rank': None, 'priority': tier,
            'min_probability_pct': minimum_p, 'min_ev_pct': min_ev,
            'min_ev_low_pct': min_low, 'margin_band': band,
            'signal': signal, 'label': LABELS[signal], 'reasons': reasons}


def ranking_key(row):
    d = row['opportunity']
    return (d['priority'], -(row['ev_low_pct'] - d['min_ev_low_pct']),
            -row['ev_pct'], -row['statistical_pct'], str(row['id']))


def annotate(matrix, as_of=None):
    rows = matrix.get('rows', [])
    for row in rows: row['opportunity'] = evaluate(row)
    eligible = sorted((r for r in rows if r['opportunity']['eligible']), key=ranking_key)
    for rank, row in enumerate(eligible, 1): row['opportunity']['rank'] = rank
    matrix['opportunity_policy'] = {'id': POLICY_ID, 'evaluated_at': as_of,
        'odds_min': ODDS_MIN, 'odds_max': ODDS_MAX, 'primary_min_probability_pct': 35,
        'secondary_min_probability_pct': 40, 'eligible_markets': len(eligible),
        'excluded_reasons': dict(Counter(reason for r in rows for reason in r['opportunity']['reasons'])),
        'best_market_id': eligible[0]['id'] if eligible else None}
    return eligible[0] if eligible else None


def best_opportunity(rows):
    # Recompute on copies: public projections cannot trust stale saved ranks.
    matrix = {'rows': [dict(r) for r in rows]}
    return annotate(matrix)
