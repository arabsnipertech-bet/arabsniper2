"""Prospective paired benchmark: frozen inputs only, never re-fit predictions."""
from collections import defaultdict
from math import log
from adaptive_calibration import VERSION, VALIDATION_CYCLE, number, bound
from multimarket_model import stamp
from lateral_matrix import settle_spec, market_id, MODEL_ID
from selection_studies import StudyEvidence

BANDS = ((1.40, 1.70, '1.40-1.69'), (1.70, 2.10, '1.70-2.09'),
         (2.10, 3.51, '2.10-3.50'), (3.51, 4.500000001, '3.51-4.50'))


def metrics(rows, probability=None):
    n = len(rows)
    priced = [r for r in rows if number(r.get('odd')) and r['odd'] > 1]
    closes = [r['clv'] for r in rows if number(r.get('clv'))]
    mean = lambda xs: sum(xs)/len(xs) if xs else None
    out = dict(contracts_count=n, distinct_fixtures_count=len({r['fid'] for r in rows}),
               net_units=sum(r['odd']*r['y']-1 for r in priced), n=n, fixtures=len({r['fid'] for r in rows}), hit_rate=mean([r['y'] for r in rows]),
               mean_odd=mean([r['odd'] for r in priced]), priced_n=len(priced),
               roi_pct=100*mean([r['odd']*r['y']-1 for r in priced]) if priced else None,
               clv_n=len(closes), clv_pct=100*mean(closes) if closes else None)
    if probability:
        out['brier_score'] = mean([(r[probability]-r['y'])**2 for r in rows])
        out['log_loss'] = mean([-log(bound(r[probability] if r['y'] else 1-r[probability], 1e-12, 1)) for r in rows])
    return out


def build_benchmark(store):
    first, operations, closes = {}, {}, {}
    from decision_shadow import DecisionAudit
    decisions = DecisionAudit()
    from official_selections import OfficialSelections
    official = OfficialSelections()
    studies = StudyEvidence()
    excluded = 0
    for collection in ('multimarket_evaluations', 'analysis_updates'):
        for _, record in store.get(collection, {}).items():
            a, meta = record.get('multimarket') or {}, record.get('fixture') or {}
            at, start = stamp(record.get('captured_at')), stamp(meta.get('kickoff'))
            if (record.get('retrospective') or not at or not start or at >= start
                    or a.get('model_id') != MODEL_ID or meta.get('fixture_id') is None
                    or start.date().isoformat() < a.get('validation_from', '2026-10-26')):
                continue
            decisions.observe(record)
            official.observe(record)
            fid = str(meta['fixture_id'])
            result = (record.get('settlement') or {}).get('result') or {}
            matrix = a.get('matrix') or {}
            for row in matrix.get('rows', []):
                spec = row.get('spec') or {}
                if not spec or spec['kind'] in ('dnb','handicap') or (spec['kind']=='asian_total' and spec.get('line',0)%1 != .5): continue
                payout = settle_spec(spec, result)
                if payout not in ((1., 0.), (0., 1.)): continue
                contract = market_id(dict(spec, kind='total') if spec['kind'] == 'asian_total' and spec.get('line', 0) % 1 == .5 else spec)
                key = (fid, contract)
                book = str(row.get('bookmaker_id'))
                for provider, point in (row.get('calibration_closes') or {}).items():
                    if len(point) != 3: continue
                    u, seen = stamp(point[0]), stamp(point[1])
                    if u and seen and u <= seen <= at < start and 0 < (start-u).total_seconds() <= 10800 and number(point[2]) and point[2] > 1:
                        ck = (*key, str(provider))
                        closes[ck] = max(closes.get(ck, (u, seen, point[2])), (u, seen, point[2]))
                odd = row.get('odd')
                if not number(odd) or odd <= 1 or row.get('quote_executable') is False or row.get('quote_status') not in ('recent', 'cached_stable'):
                    continue
                updated = stamp(row.get('provider_updated_at'))
                seen = stamp(row.get('quote_observed_at'))
                if updated and seen and updated <= seen <= at < start and (start-updated).total_seconds() <= 10800 and row.get('quote_status') == 'recent':
                    ck = (*key, book)
                    closes[ck] = max(closes.get(ck, (updated,seen,odd)), (updated,seen,odd))
                raw = (row.get('pure_poisson') or row.get('calibration_base') or row).get('win_weight')
                fair = row.get('market_fair_pct')
                p = row.get('statistical_pct')
                entry = dict(fid=fid, market=contract, odd=odd, y=int(payout[0]), base=raw, anchored=(row.get('calibration_base') or row).get('win_weight'),
                             fair=fair/100 if number(fair) else None, calibrated=p/100 if number(p) else None,
                             at=at, updated=stamp(row.get('provider_updated_at')), book=book, clv=None)
                rank = (at, spec['kind'] == 'asian_total', -odd, book)
                if key not in first or rank < first[key][0]: first[key] = (rank, entry)
                for category, policy in (('gold_certificate', 'opportunity'), ('platinum_certificate', 'platinum')):
                    decision = row.get(policy) or {}
                    if decision.get('eligible') is True and decision.get('rank') == 1:
                        opkey = (category, fid)
                        if opkey not in operations or rank < operations[opkey][0]:
                            operations[opkey] = (rank, entry)
    def with_clv(row):
        row = dict(row)
        close = closes.get((row['fid'], row['market'], row['book']))
        if close and row['updated'] and close[0] > row['updated'] and close[1] > row['at']:
            row['clv'] = row['odd']/close[2]-1
        return row
    matched = []
    for _, row in first.values():
        if all(number(row.get(k)) and 0 <= row[k] <= 1 for k in ('base','fair','anchored','calibrated')):
            matched.append(with_clv(row))
        else: excluded += 1
    def levels(rows):
        base, calibrated = metrics(rows, 'base'), metrics(rows, 'calibrated')
        calibrated['delta_brier_vs_base'] = (calibrated['brier_score']-base['brier_score']) if rows else None
        market = metrics(rows, 'fair')
        market.pop('roi_pct', None)
        return dict(market_fair=market, poisson_base=base, favorite_anchor_only=metrics(rows,'anchored'), calibrated_v51=calibrated)
    # Operations are separate prospective choices on closed fixtures. Their
    # counts must never be added together or used as extra training fixtures.
    ops = defaultdict(list)
    for (category, _), (_, row) in operations.items(): ops[category].append(with_clv(row))
    for record in store.get('study_observations', {}).values(): studies.observe(record)
    dedup = {}
    suppressed_pending = set()
    for row in studies.samples():
        if row['y'] is None:
            if row.get('suppressed'): suppressed_pending.add((row['fid'],market_id(row['spec'])))
            continue
        key = (row['fid'], market_id(row['spec']), bool(row.get('suppressed')))
        old = dedup.get(key)
        if old is None or row['at'] < old['at']: dedup[key] = row
        elif row['at'] == old['at'] and row.get('best_study'): old['best_study'] = True
    for row in dedup.values():
        if row.get('suppressed'): ops['suppressed_traps'].append(row)
        else:
            ops['radar_all_visible'].append(row)
            if row.get('best_study'): ops['radar_star'].append(row)
    categories = ('gold_certificate','platinum_certificate','radar_star','radar_all_visible','suppressed_traps')
    summaries = {k: metrics(ops[k]) for k in categories}
    summaries['gold_certificate']['band_1.70_1.85'] = metrics([r for r in ops['gold_certificate'] if 1.70 <= r['odd'] <= 1.85])
    for cat in categories:
        summaries[cat]['by_odds_band'] = {label: metrics([r for r in ops[cat] if number(r.get('odd')) and lo <= r['odd'] < hi]) for lo,hi,label in BANDS}
        summaries[cat]['by_market'] = {m: metrics([r for r in ops[cat] if r['market'] == m]) for m in sorted({r['market'] for r in ops[cat]})}
    traps = ops['suppressed_traps']
    priced = [r for r in traps if number(r.get('odd')) and r['odd'] > 1]
    summaries['suppressed_traps'].update(total_suppressed=len(traps)+len(suppressed_pending),
        pending=len(suppressed_pending), saved_losses=sum(1-r['y'] for r in traps),
        saved_loss_rate=sum(1-r['y'] for r in traps)/len(traps) if traps else None,
        avoided_net_units=-sum(r['odd']*r['y']-1 for r in priced))
    decision_report = decisions.report()
    return dict(observed_populations=official.report(), decision_benchmark=decision_report, decision_path_comparison=decision_report['decision_path_comparison'], version=VERSION, validation_cycle=VALIDATION_CYCLE, snapshot='first_executable_prematch',
                matched_contracts=len(matched), matched_fixtures=len({r['fid'] for r in matched}),
                excluded_incomplete_probabilities=excluded, levels=levels(matched),
                by_odds_band={label: levels([r for r in matched if lo <= r['odd'] < hi]) for lo,hi,label in BANDS},
                operational_selections=summaries,
                notes=['Four probability levels use identical contracts, quotes, snapshots and outcomes.',
                       'Operations are separate first published selections; overlapping cohorts are not additive.',
                       'Unit-stake returns are hypothetical. Missing CLV remains null; saved losses excludes forgone wins, avoided_net_units includes them.'])


def markdown(benchmark):
    lines = ['\n## four_level_benchmark', '', '| Livello | Contratti | Gare | Brier | Log-loss | ROI % | CLV % |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for name, group in benchmark['levels'].items():
        values = [name, *(group.get(k) for k in ('n','fixtures','brier_score','log_loss','roi_pct','clv_pct'))]
        lines.append('| '+' | '.join('—' if v is None else str(round(v,6)) if isinstance(v,float) else str(v) for v in values)+' |')
    lines += ['', '| Selezioni operative | Contratti | Gare distinte | Hit rate | Quota media | ROI % | CLV % |', '|---|---:|---:|---:|---:|---:|---:|']
    for name,g in benchmark['operational_selections'].items():
        lines.append('| '+' | '.join(str(v) if v is not None else '—' for v in [name, *(g.get(k) for k in ('n','fixtures','hit_rate','mean_odd','roi_pct','clv_pct'))])+' |')
    lines += ['', '| Fascia | Livello | Contratti | Brier | Log-loss | ROI % | CLV % |', '|---|---|---:|---:|---:|---:|---:|']
    for band, groups in benchmark['by_odds_band'].items():
        for name,g in groups.items():
            lines.append('| '+' | '.join(str(v) if v is not None else '—' for v in [band,name,*(g.get(k) for k in ('n','brier_score','log_loss','roi_pct','clv_pct'))])+' |')
    lines += ['', '| Categoria | Mercato / fascia | Contratti | Gare distinte | Hit rate | ROI % | CLV % |', '|---|---|---:|---:|---:|---:|---:|']
    for category, summary in benchmark['operational_selections'].items():
        details = {**summary['by_market'], **summary['by_odds_band']}
        if 'band_1.70_1.85' in summary: details['1.70-1.85'] = summary['band_1.70_1.85']
        for name,g in details.items():
            lines.append('| '+' | '.join(str(v) if v is not None else '—' for v in [category,name,*(g.get(k) for k in ('n','fixtures','hit_rate','roi_pct','clv_pct'))])+' |')
    traps=benchmark['operational_selections']['suppressed_traps']
    lines += ['', f"Trappole: {traps['saved_losses']} perdite evitate, tasso {traps['saved_loss_rate']}; saldo teorico evitato {traps['avoided_net_units']} unità (incluse vincite escluse).", 'Campioni operativi sovrapposti non sommabili.', '']
    decision = benchmark.get('decision_benchmark') or {}
    lines += ['', '## Decisioni effettive sui medesimi snapshot', '',
              '| Snapshot | Percorso | Attivate | Cambiate vs Poisson | Chiuse | Pendenti | Unità | ROI % | IC 95% ROI per fixture |',
              '|---|---|---:|---:|---:|---:|---:|---:|---|']
    for snapshot, paths in (decision.get('decision_path_comparison') or {}).items():
        for name, value in paths.items():
            values=[snapshot,name,*(value.get(k) for k in ('first_choices_activated','changed_vs_pure_poisson','n','pending_choices','net_units','roi_pct')),
                    (value.get('uncertainty') or {}).get('roi_pct_interval')]
            lines.append('| '+' | '.join('—' if x is None else str(x) for x in values)+' |')
    lines += ['', '## Varianti isolate (solo shadow)', '', '| Snapshot | Variante | Gare confrontate | Scelte cambiate | Attivate | ROI % | Differenza ROI pp |', '|---|---|---:|---:|---:|---:|---:|']
    for snapshot, variants in (decision.get('experiments') or {}).items():
        for name,value in variants.items():
            lines.append('| '+' | '.join(str(v) if v is not None else '—' for v in
                (snapshot,name,value['compared_fixtures_count'],value['changed_vs_baseline'],value['first_choices_activated'],value['roi_pct'],value['roi_difference_pp']))+' |')
    lines += ['', '## Coorti Platinum (contratti, non pubblicazioni)', '', '| Snapshot | Coorte | Contratti | Gare distinte | ROI % |', '|---|---|---:|---:|---:|']
    for snapshot, groups in (decision.get('platinum_value_cohorts') or {}).items():
        for name,value in groups.items():
            lines.append('| '+' | '.join(str(v) if v is not None else '—' for v in (snapshot,name,value['n'],value['fixtures'],value['roi_pct']))+' |')
    return '\n'.join(lines)
