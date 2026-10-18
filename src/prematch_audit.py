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
            choice = next((c for c in record.get('market_candidates', record.get('ranked_choices', []))
                           if c.get('market') == market and c.get('selection') == side), {})
            relation = choice.get('relation', 'unclassified')
            band = min(int(probability * 10), 9) * 10
            row = (float(probability), bool(outcome), choice.get('odd'))
            for key in ((market, 'all', 'all'), (market, relation, 'all'),
                        (market, relation, f'{band}-{band+10}%'),
                        (market, 'recovered' if choice.get('recovered') else 'ordinary' if choice.get('gate',{}).get('eligible', True) else 'rejected', 'all')):
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
    return {'schema_version': 1, 'note': 'First observed pre-match forecasts only. ROI uses recorded quotes, not executed bets. No automatic threshold fitting.', 'groups': report, 'quote_groups': quote_report(store)}


def quote_report(store):
    """Descriptive results for every observed 1X2 and positive goal-market quote."""
    groups = defaultdict(list)
    for record in store.get('fixtures', {}).values():
        result = (record.get('settlement') or {}).get('result', {})
        if not result:
            continue
        for market, observation in record.get('quote_observation', {}).items():
            sides = ('1', 'x', '2') if market == '1x2_ft' else (MARKETS[market][0],) if market in MARKETS else ()
            for side in sides:
                odd = observation.get('current', {}).get('odds', {}).get(side)
                if not isinstance(odd, (int,float)) or odd <= 1:
                    continue
                if market == '1x2_ft':
                    h, a = result.get('home_ft'), result.get('away_ft')
                    outcome = None if h is None or a is None else side == ('1' if h>a else '2' if a>h else 'x')
                else:
                    outcome = event_outcome(market, result)
                if outcome is None:
                    continue
                features = observation.get('movement_features', {}).get(side, {})
                drop = features.get('drop_pct')
                movement = 'missing' if drop is None else 'drop' if drop>0 else 'rise' if drop<0 else 'stable'
                odds_band = '<1.5' if odd<1.5 else '1.5-2' if odd<2 else '2-3' if odd<3 else '3-5' if odd<5 else '>=5'
                for signal in ('all', movement, 'reversal' if features.get('direction_reversals',0)>0 else 'no_reversal',
                               'favorite_inversion' if features.get('favorite_inversion') else 'no_favorite_inversion'):
                    groups[(market,side,odds_band,signal)].append((bool(outcome),odd))
    rows = []
    for (market,side,band,signal), values in sorted(groups.items()):
        n=len(values); wins=sum(y for y,_ in values)
        rows.append({'market':market,'selection':side,'odds_band':band,'signal':signal,'n':n,'wins':wins,
                     'hit_rate_pct':round(wins/n*100,3),'roi_pct':round(sum((odd if y else 0)-1 for y,odd in values)/n*100,3)})
    return rows
