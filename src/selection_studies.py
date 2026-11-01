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


def study_contract(row):
    from opportunity_policy import goal_market
    spec = row.get('spec') or {}
    if spec.get('kind') == 'result' and spec.get('period') == 'ft':
        return '1x2_' + spec['side']
    if spec.get('kind') == 'halves_total':
        return 'combo_ht_sh'
    key = goal_market(row)
    return key if key in MARKETS | {'gg_ht'} else None


def generate_ideas(assessment, as_of, kickoff):
    from lateral_matrix import ScoreModel, label, market_id, study_result_totals
    from opportunity_policy import study_blockers
    from quote_cache import within_window
    from quote_sources import fresh
    matrix = assessment.get('matrix') or {}
    if any((matrix.get(policy) or {}).get('best_market_id')
           for policy in ('opportunity_policy', 'platinum_policy')):
        return []
    positive = {}
    ideas = {}
    def add(kind, item):
        key = (kind, item['market'])
        old = ideas.get(key)
        if old is None or (item['odd'] or 0) > (old['odd'] or 0):
            ideas[key] = dict(item, type=kind)
    for row in matrix.get('rows', []):
        market = study_contract(row)
        policy = row.get('opportunity') or {}
        blockers = study_blockers(row)
        if not market or not blockers:
            continue
        base = row.get('calibration_base') or row
        pct = base.get('statistical_pct')
        if not valid(pct) or pct <= 0:
            continue
        live = row.get('quote_status') in ('recent', 'cached_stable') and within_window(row, as_of, kickoff)
        odd = row.get('odd') if live and row.get('quote_executable') is not False else None
        odd = odd if valid(odd, 1, float('inf')) and odd > 1 else None
        current = row.get('oci') if live else None
        previous = [o for o in (row.get('study_flow'), row.get('cached_quote')) if o
                    and valid(o.get('oci'), -100, 100) and within_window(o, as_of, kickoff)]
        source = max(previous, key=lambda o: o.get('quote_observed_at') or '', default={})
        historical = not valid(current, -100, 100)
        oci = source.get('oci') if historical else current
        delta = row.get('delta_pct') if live else None
        if delta is None and live:
            delta = (row.get('selected_movement') or {}).get('delta_pct')
        fair_delta = row.get('fair_delta_pp') if live else None
        down = delta if valid(delta, -100, float('inf')) else float('inf')
        flow = oci if oci is not None else 0
        value = odd is not None and odd*pct > 100
        item = dict(market=market, spec=row['spec'], label=row['label'], odd=odd,
                    poisson_pct=pct, fair_odd=100/pct, oci=oci, historical_oci=historical and oci is not None,
                    flow_at=(source if historical else row).get('quote_observed_at'),
                    delta_pct=delta, fair_delta_pp=fair_delta, agreeing_books=row.get('agreeing_books'),
                    bookmaker_id=row.get('bookmaker_id') if odd else None, updated=row.get('provider_updated_at') if odd else None,
                    blocking_reasons=blockers, odds_min=policy.get('odds_min'), odds_max=policy.get('odds_max'))
        if down < 0 or value or flow >= 30:
            positive.setdefault(market, item)
        if market.startswith('1x2_'):
            if value and (down <= -2 or (fair_delta or 0) >= 1.5 or flow > 0):
                add('studio_1x2_value_flow', item)
        else:
            if flow >= 35 or down <= -2.5 and (row.get('agreeing_books') or 0) >= 4:
                add('studio_goal_flow_near_miss', item)
            if market == 'over15_ht' and flow >= 35 and pct < 32:
                add('studio_ht_flow_forcing', item)
    if 'over25_ft' in positive and assessment.get('rates'):
        model = None
        for side in ('1', '2'):
            if '1x2_'+side not in positive:
                continue
            model = model or ScoreModel(assessment['rates'])
            for spec, p in study_result_totals(model, side):
                if p <= 0:
                    continue
                offer = (matrix.get('study_combo_quotes') or {}).get(market_id(spec), {})
                if not fresh(offer.get('updated'), as_of, kickoff):
                    offer = {}
                add('studio_combo_1x2_over25', dict(market=spec['side']+'_over25_ft', spec=spec, label=label(spec),
                    odd=offer.get('odd'), bookmaker_id=offer.get('bookmaker_id'), updated=offer.get('updated'),
                    poisson_pct=p*100, fair_odd=1/p, oci=None, blocking_reasons=['market']))
    return list(ideas.values())


def freeze_study(match, at):
    """Compact publication-time snapshot. Closing quotes survive changes of category."""
    from multimarket_model import stamp
    from lateral_matrix import market_id
    from quote_sources import fresh
    meta = match.get('fixture') or {}
    matrix = (match.get('multimarket') or {}).get('matrix') or {}
    monitor = matrix.get('quote_monitoring') or {}
    now, start, decision = map(stamp, (at, meta.get('kickoff'), monitor.get('as_of')))
    if ('studio_ideas' not in monitor or not all((now, start, decision)) or not decision <= now < start
            or meta.get('status_short') not in ('NS', 'TBD') or meta.get('result') or not meta.get('fixture_id')):
        return None
    closes = []
    for row in matrix.get('rows', []):
        if not study_contract(row) or row.get('quote_executable') is False:
            continue
        for book, point in (row.get('calibration_closes') or {}).items():
            closes.append([market_id(row['spec']), str(book), *point])
        if row.get('quote_status') == 'recent' and row.get('bookmaker_id') is not None:
            closes.append([market_id(row['spec']), str(row['bookmaker_id']), row.get('provider_updated_at'),
                           row.get('quote_observed_at'), row.get('odd')])
    for key, offer in (matrix.get('study_combo_quotes') or {}).items():
        if fresh(offer['updated'], at, meta['kickoff']):
            closes.extend([key, *point] for point in offer.get('closes', []))
    closes = [point for point in closes if stamp(point[2]) and
              0 < (start - stamp(point[2])).total_seconds() <= 10800]
    if not monitor['studio_ideas'] and not closes:
        return None
    return dict(fixture={k: meta.get(k) for k in ('fixture_id', 'kickoff', 'home', 'away')},
                captured_at=at, studio_ideas=monitor['studio_ideas'], closes=closes)


class StudyEvidence:
    def __init__(self):
        self.first, self.last = {}, {}

    def observe(self, record):
        from lateral_matrix import settle_spec
        from multimarket_model import stamp
        from zoneinfo import ZoneInfo
        meta = record['fixture']
        at, start = stamp(record['captured_at']), stamp(meta['kickoff'])
        if not at or not start or at >= start or record.get('retrospective'):
            return
        fid = str(meta['fixture_id'])
        for spec_id, book, updated, seen, odd in record.get('closes', []):
            u, s = stamp(updated), stamp(seen)
            if not (u and s and u <= s <= at and (start-u).total_seconds() <= 10800
                    and valid(odd, 1, float('inf')) and odd > 1):
                continue
            key = (fid, spec_id, book)
            self.last[key] = max(self.last.get(key, (u, s, odd)), (u, s, odd))
        result = (record.get('settlement') or {}).get('result') or {}
        for idea in record['studio_ideas']:
            key = (fid, idea['type'], idea['market'])
            payout = settle_spec(idea['spec'], result)
            row = dict(idea, fid=fid, date=start.astimezone(ZoneInfo('Europe/Rome')).date().isoformat(),
                       at=at.isoformat(), p=idea['poisson_pct']/100, y=None if payout is None else int(payout[0]))
            if key not in self.first or at < stamp(self.first[key]['at']):
                self.first[key] = row

    def samples(self):
        from lateral_matrix import market_id
        from multimarket_model import stamp
        for key in sorted(self.first):
            row = self.first[key]
            close = self.last.get((row['fid'], market_id(row['spec']), str(row.get('bookmaker_id'))))
            updated = stamp(row.get('updated'))
            clv = None
            if close and updated and close[0] > updated and close[1] > stamp(row['at']) and row['odd']:
                clv = row['odd']/close[2] - 1
            yield dict(row, clv=clv, outcome='pending' if row['y'] is None else 'won' if row['y'] else 'lost')


def study_summary(rows, min_n=50):
    from statistics import mean, stdev
    groups, blockers = defaultdict(list), defaultdict(list)
    for row in rows:
        groups[row['type']].append(row)
        reasons = row['blocking_reasons']
        if len(reasons) == 1:
            blockers[reasons[0]].append(row)

    def metrics(items):
        settled = [r for r in items if r['y'] is not None]
        priced = [r for r in settled if r.get('odd')]
        paired = [r for r in priced if r.get('clv') is not None]
        def average(values):
            return mean(values) if values else None
        def profits(rows):
            return [r['odd']*r['y']-1 for r in rows]
        # Promotion uses one predeclared representative per independent fixture.
        first = {}
        for r in sorted(priced, key=lambda r: (r['at'], r['market'], r['type'])):
            first.setdefault(r['fid'], r)
        matched = [r for r in first.values() if r.get('clv') is not None]
        returns = profits(matched)
        n = len(matched)
        se = stdev(returns)/n**.5 if n > 1 else None
        return dict(selections=len(items), settled=len(settled), pending=len(items)-len(settled),
                    wins=sum(r['y'] for r in settled), losses=sum(1-r['y'] for r in settled),
                    hit_rate=average([r['y'] for r in settled]), brier_score=average([(r['p']-r['y'])**2 for r in settled]),
                    priced_n=len(priced), roi=average(profits(priced)), clv_n=len(paired), clv=average([r['clv'] for r in paired]),
                    paired_fixtures=n, standard_error=se, paired_roi=average(returns),
                    promotion_candidate=n >= min_n and se is not None and mean(returns)>se and mean(r['clv'] for r in matched)>0)
    by_type = {}
    for kind, items in groups.items():
        markets = defaultdict(list)
        for row in items:
            markets[row['market']].append(row)
        by_type[kind] = dict(metrics(items), by_market={k: metrics(v) for k, v in markets.items()})
    return {'by_type': by_type, 'by_blocking_reason': {k: metrics(v) for k, v in blockers.items()}}


def study_audit(store):
    evidence = StudyEvidence()
    for record in store.get('study_observations', {}).values():
        evidence.observe(record)
    rows = list(evidence.samples())
    return {**study_summary(rows), 'selections': rows}
