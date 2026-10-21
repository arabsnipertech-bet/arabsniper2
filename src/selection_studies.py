"""Fixed observational cohorts. Never modifies forecasts or tunes selection."""
from collections import defaultdict
from math import isfinite
from multimarket_audit import metrics

STUDY_ID = 'goal-convergence-study-v1'
VALIDATION_FROM = '2026-10-22'
MARKETS = {'over15_ht', 'over25_ft', 'over35_ft'}
RULES = {
    'both_over_50': 'Statistica >50% e book >50%.',
    'agreement_10pp': 'Entrambe >50%; distanza <=10 punti percentuali.',
    'agreement_below_break_even': 'Accordo <=10 punti; riferimento sotto pareggio.',
    'agreement_above_break_even': 'Accordo <=10 punti; riferimento sopra pareggio.',
}


def valid(x, lo=0, hi=100):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and isfinite(x) and lo <= x <= hi


def classify(row):
    p, b = row.get('statistical_probability'), row.get('book_probability_pct')
    if not valid(p, 0, 1) or not valid(b):
        return None
    both = p > .5 and b > 50
    agreement = both and abs(p*100-b) <= 10 + 1e-9
    odd = row.get('odd')
    ref = (row.get('price_assessment') or {}).get('reference_probability_pct')
    priced = valid(odd, 1, float('inf')) and odd > 1 and valid(ref)
    return {'both_over_50': both, 'agreement_10pp': agreement,
            'agreement_below_break_even': agreement and ref < 100/odd if priced else None,
            'agreement_above_break_even': agreement and ref > 100/odd if priced else None}


def build_studies(report):
    groups = defaultdict(list)
    for row in report['markets']:
        if row['market'] not in MARKETS or row['selection'] != 'over':
            continue
        phase = ('validation' if report['audit_date'] >= VALIDATION_FROM and row['period']=='prospective'
                 else 'exploratory')
        f = row['fixture']
        labels = classify(row)
        for rule in RULES:
            eligible = labels.get(rule) if labels is not None else None
            membership = 'missing_data' if eligible is None else 'selected' if eligible else 'not_selected'
            for scope in ('all_markets', 'primary_only') if row['primary'] else ('all_markets',):
                # Whole sample and league slices are separate, never additive.
                for country, league in [('*', '*'), (f.get('country', 'unknown'), str(f.get('league_id', 'unknown')))]:
                    key = (row['snapshot'], row['model_id'], phase, row['market'], scope, country, league, rule, membership)
                    groups[key].append(row)
    summaries = []
    for key, rows in sorted(groups.items()):
        # Missing model/book probabilities cannot enter a matched benchmark.
        paired = [r for r in rows if r['outcome'] is not None and classify(r) is not None]
        def observations(book=False):
            return [(r['book_probability_pct']/100 if book else r['statistical_probability'],
                     r['outcome'], r['odd'] if valid(r['odd'],1,float('inf')) and r['odd']>1 else None) for r in paired]
        stat, book = metrics(observations()), metrics(observations(True))
        summaries.append(dict(zip(('snapshot','model_id','phase','market','scope','country','league_id','rule','membership'),key),
                              fixtures=len(rows), settled=sum(r['outcome'] is not None for r in rows),
                              pending=sum(r['outcome'] is None for r in rows),
                              statistics=stat, book=book,
                              fixture_ids=[r['fixture_id'] for r in rows]))
    return {'study_id': STUDY_ID, 'validation_from': VALIDATION_FROM, 'rules': RULES,
            'note': 'Exploratory rules fixed after observing 21 October. 10pp is an experimental definition, not a learned optimum. First/latest and overlapping cohorts must not be added. No stake adjustment. Paper ROI only; original recorded odds, not personal ticket odds.',
            'groups': summaries}


def study_markdown(study):
    lines = ['## Studi sulla selezione', '', study['note'], '',
             '| Snapshot | Modello | Fase | Mercato | Regola | Gruppo | Partite | Pendenti | Vinte/valutate | ROI teorico % |',
             '|---|---|---|---|---|---|---:|---:|---|---:|']
    for g in study['groups']:
        if g['country'] != '*' or g['scope'] != 'primary_only' or g['model_id'] != 'context-goals-v3.4':
            continue
        m=g['statistics']
        values=[g[k] for k in ('snapshot','model_id','phase','market','rule','membership')]
        values += [g['fixtures'],g['pending'],f"{m.get('wins',0)}/{m['n']}",m.get('paper_roi_pct','—')]
        lines.append('| '+' | '.join(str(v) for v in values)+' |')
    lines += ['', 'Il JSON include anche alternative non principali, scartati, dati mancanti e suddivisioni per campionato. Nessuna promozione automatica delle regole.', '']
    return '\n'.join(lines)
