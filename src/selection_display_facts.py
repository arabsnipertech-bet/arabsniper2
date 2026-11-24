"""Copy saved numerical references for display; no admission or model changes."""
from v7_probability import finite


def selection_facts(spec, record, snapshot):
    stats = snapshot.get('statistics') or {}
    features = record.get('features') or {}
    confirmation = record.get('confirmation') or {}
    period = spec.get('period')
    if spec.get('kind') == 'pt_btts_ft_total':
        period = 'ht'
    facts = {'kind': spec.get('kind'), 'period': period}
    suffix = {'ht': 'ht', 'sh': 'st', 'ft': 'ft'}.get(period)
    if spec.get('kind') in ('halves_total', 'both_halves', 'pt_ft_total'):
        suffix = None
    if suffix:
        home, away = (stats.get(team+'_'+suffix) for team in ('home', 'away'))
        if all(finite(v) and v >= 0 for v in (home, away)):
            facts.update(lambda_home=home, lambda_away=away)
    else:
        pair_labels = (('lambda_pt', 'ht'), ('lambda_ft', 'ft')) if spec.get('kind') == 'pt_ft_total' else (('lambda_pt', 'ht'), ('lambda_st', 'st'))
        for label, tail in pair_labels:
            pair = [stats.get(team+'_'+tail) for team in ('home', 'away')]
            if all(finite(v) and v >= 0 for v in pair):
                facts[label] = sum(pair)
    margin = features.get('margin_pct')
    if finite(margin) and margin >= 0:
        facts['margin_pct'] = margin
    delta = confirmation.get('opening_delta_pct')
    if not finite(delta):
        delta = features.get('opening_delta_pct')
    if finite(delta):
        facts['opening_delta_pct'] = delta
    return facts
