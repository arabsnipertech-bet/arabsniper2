"""Evaluate frozen pre-match probabilities; missing scores never count as losses."""
from collections import defaultdict
from math import sqrt

MARKETS = {'over15_ht': ('over', 'home_ht', 'away_ht', 2),
           'gg_ht': ('yes', 'home_ht', 'away_ht', None),
           'over25_ft': ('over', 'home_ft', 'away_ft', 3)}


def event_outcome(market, result):
    _, home, away, threshold = MARKETS[market]
    if result.get(home) is None or result.get(away) is None:
        return None
    h, a = result[home], result[away]
    return h > 0 and a > 0 if threshold is None else h + a >= threshold


def settle_predictions(store, output):
    """Attach results without modifying any frozen forecast fields."""
    for day in output.get('days', []):
        for match in day.get('matches', []):
            fixture = match.get('fixture') or {}
            record = store.get('fixtures', {}).get(str(fixture.get('fixture_id')))
            result = fixture.get('result') or (match.get('prediction_audit') or {}).get('result')
            if record is None or not result or result.get('status') not in {'FT', 'AET', 'PEN'}:
                continue
            record['settlement'] = {'result': dict(result), 'outcomes': {
                market: event_outcome(market, result) for market in MARKETS}}
    return store


def build_report(store):
    groups = defaultdict(list)
    for record in store.get('fixtures', {}).values():
        outcomes = (record.get('settlement') or {}).get('outcomes', {})
        for market, (side, *_rest) in MARKETS.items():
            outcome = outcomes.get(market)
            probability = (record.get('probabilities_calibrated', {}).get(market) or {}).get(side)
            if outcome is None or probability is None or not 0 <= probability <= 1:
                continue
            # Display structure is preserved verbatim; derive relation from ranked selections only.
            choice = next((c for c in record.get('ranked_choices', [])
                           if c.get('market') == market and c.get('selection') == side), {})
            relation = choice.get('relation', 'unclassified')
            band = min(int(probability * 10), 9) * 10
            row = (float(probability), bool(outcome), choice.get('odd'))
            for key in ((market, 'all', 'all'), (market, relation, 'all'),
                        (market, relation, f'{band}-{band+10}%')):
                groups[key].append(row)
    report = []
    for (market, relation, band), rows in sorted(groups.items()):
        n = len(rows); wins = sum(y for _, y, _ in rows); rate = wins / n
        z = 1.96; denominator = 1 + z*z/n
        center = (rate + z*z/(2*n))/denominator
        margin = z*sqrt(rate*(1-rate)/n + z*z/(4*n*n))/denominator
        priced = [(y, odd) for _, y, odd in rows if isinstance(odd, (int, float)) and odd > 1]
        report.append({'market': market, 'relation': relation, 'probability_band': band,
            'n': n, 'wins': wins, 'hit_rate_pct': round(rate*100, 3),
            'mean_probability_pct': round(sum(p for p, _, _ in rows)/n*100, 3),
            'brier_score': round(sum((p-y)**2 for p,y,_ in rows)/n, 6),
            'wilson_95_pct': [round((center-margin)*100, 3), round((center+margin)*100, 3)],
            'priced_n': len(priced), 'roi_pct': round(sum((odd if y else 0)-1 for y,odd in priced)/len(priced)*100, 3) if priced else None})
    return {'schema_version': 1, 'note': 'First observed pre-match forecasts only. ROI uses recorded quotes, not executed bets. No automatic threshold fitting.', 'groups': report}
