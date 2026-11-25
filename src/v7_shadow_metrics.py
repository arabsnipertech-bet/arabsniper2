"""Private, fixture-clustered monitoring of frozen prospective hypotheses."""
from collections import defaultdict
from math import log
from random import Random

from multimarket_model import stamp
from v7_probability import finite
from v7_movement_shadow import CONFIG, CONTROL, PROTOCOL, VARIANTS, GG_FT_SAME_RULES


def totals(rows):
    sums=[0.,0.,0.,0.,0.,0.,0.]
    for row in rows:
        sums[6]+=1
        win,loss,p=row.get('win'),row.get('loss'),row.get('probability')
        binary=win in (0,1) and loss in (0,1) and win+loss==1
        if binary and finite(p) and 0<=p<=1:
            sums[0]+=(p-win)**2
            sums[1]-=log(max(1e-12,p if win else 1-p))
            sums[2]+=win;sums[3]+=1
        roi=row.get('roi')
        if finite(roi) and finite(row.get('odd')) and row['odd']>1:
            sums[4]+=roi;sums[5]+=1
    return sums


def ratios(sums):
    brier,ll,hits,binary,roi,priced,_=sums
    return dict(brier=brier/binary if binary else None,log_loss=ll/binary if binary else None,
                hit_rate=hits/binary if binary else None,roi=roi/priced if priced else None)


def summary(rows,available,draws=400):
    by=defaultdict(list)
    for row in rows:by[row['fixture_id']].append(row)
    aggregate=totals(rows)
    out=dict(**ratios(aggregate),selected=len(rows),distinct_fixtures=len(by),available_fixtures=available,
             probability_scored=int(aggregate[3]),priced_settled=int(aggregate[5]),
             pending_or_nonbinary=len(rows)-int(aggregate[3]),
             coverage=len(by)/available if available else None,net_units=aggregate[4],
             bootstrap=dict(method='fixture_cluster_percentile',confidence=.95,draws=draws,
                            distinct_fixtures=len(by),intervals={k:None for k in ratios(aggregate)}))
    if len(by)<2:return out
    clusters=[totals(by[k]) for k in sorted(by)]
    rng=Random(20261122);samples=defaultdict(list)
    for _ in range(draws):
        chosen=[clusters[rng.randrange(len(clusters))] for _ in clusters]
        values=ratios([sum(c[i] for c in chosen) for i in range(7)])
        for name,value in values.items():
            if value is not None:samples[name].append(value)
    for name,values in samples.items():
        values.sort();last=len(values)-1
        out['bootstrap']['intervals'][name]=[values[int(.025*last)],values[int(.975*last)]]
    return out


def report(archives,cutoff,draws=None):
    """No replay or refitting: only protocols actually frozen before kickoff."""
    archives = list(archives)
    draws=CONFIG['bootstrap_draws'] if draws is None else draws
    snapshots={};unavailable=set();kickoffs=defaultdict(set)
    for archive in archives:
        for fid,fixture in (archive.get('fixtures') or {}).items():
            observed=stamp(fixture.get('observed_at'))
            if observed and observed>cutoff:continue
            shadow=fixture.get('decision_shadow') or {}
            if shadow.get('status')=='kickoff_conflict':unavailable.add(str(fid));continue
            for timing in ('latest','before_hour'):
                frozen=shadow.get(timing) or {};ledger=frozen.get('movement_study') or {}
                if ledger.get('protocol')!=PROTOCOL or ledger.get('available') is not True:
                    if timing=='latest':unavailable.add(str(fid))
                    continue
                at,start=stamp(ledger.get('captured_at')),stamp(ledger.get('kickoff'))
                if not at or not start or not at<start<=cutoff:continue
                config=ledger.get('config_sha256')
                if not config:continue
                key=(str(fid),start.isoformat(),timing,config)
                kickoffs[str(fid)].add(start.isoformat())
                old=snapshots.get(key)
                if old is None or at>stamp(old[0]['captured_at']):snapshots[key]=(ledger,frozen)
    conflicts={fid for fid,values in kickoffs.items() if len(values)>1}
    snapshots={k:v for k,v in snapshots.items() if k[0] not in conflicts}
    unavailable.update(conflicts)
    unavailable.difference_update(k[0] for k in snapshots)
    grouped=defaultdict(list)
    for key,value in snapshots.items():grouped[(key[2],key[3])].append((key[0],*value))
    reports={}
    for (timing,config),items in sorted(grouped.items()):
        selections=defaultdict(list);cohorts=defaultdict(list);margin_counts=defaultdict(int)
        variant_available=defaultdict(int)
        matched=defaultdict(lambda:dict(identical_target=0,different_target=0,absent=0,brier_delta_sum=0.,log_loss_delta_sum=0.))
        for fid,ledger,frozen in items:
            variants=frozen.get('variants') or {};baseline=variants.get(CONTROL) or {}
            for name in ('published',CONTROL,*VARIANTS):
                if not isinstance(variants.get(name),dict):
                    continue  # A missing historical experiment is not an empty selection.
                variant_available[name]+=1
                triad=variants.get(name) or {}
                for role,pick in triad.items():
                    if not pick:continue
                    row={**pick,'fixture_id':fid,'role':role}
                    selections[name].append(row)
                    if name==CONTROL:continue
                    prior=baseline.get(role)
                    if not prior:matched[name]['absent']+=1;continue
                    if prior.get('market_id')!=pick.get('market_id'):
                        matched[name]['different_target']+=1;continue
                    win=pick.get('win');loss=pick.get('loss');p0=prior.get('probability');p1=pick.get('probability')
                    if win not in (0,1) or loss not in (0,1) or win+loss!=1 or not all(finite(p) and 0<=p<=1 for p in (p0,p1)):
                        matched[name]['absent']+=1;continue
                    matched[name]['identical_target']+=1
                    matched[name]['brier_delta_sum']+=(p1-win)**2-(p0-win)**2
                    matched[name]['log_loss_delta_sum']+=-log(max(1e-12,p1 if win else 1-p1))+log(max(1e-12,p0 if win else 1-p0))
            for row in ledger.get('cohorts',[]):
                item={**row,'fixture_id':fid}
                for tag in ('oci_moderate','rising_statistical_value','rising_without_value','cross_1x2_available',
                            'favorite_135_170','favorite_inversion','gg_pt_up_over25_ft_down'):
                    if row.get(tag) is True:cohorts[tag+'|'+row['market_id']].append(item)
                if row.get('oci_moderate') is True:
                    cohorts['oci_moderate_'+row['opening_direction']+'|'+row['market_id']].append(item)
                if row.get('cross_direction'):
                    # First direction is the initial favorite, second the goal market.
                    for tag in ('all','favorite_135_170','favorite_inversion'):
                        if tag=='all' or row.get(tag) is True:
                            name='cross_'+tag+'|'+row['cross_direction']+'|'+row['market_id']
                            cohorts[name].append(item)
                if finite(row.get('excess_margin_ev_cost')):
                    margin_counts['comparable_rows']+=1
                    if row['excess_margin_ev_cost']>0:margin_counts['above_reference_rows']+=1
                if row.get('negative_ev_becomes_nonnegative_at_reference_margin') is True:
                    margin_counts['negative_at_quote_nonnegative_at_reference_margin']+=1
        available=len(items)
        def variant_summary(name,role=None):
            count=variant_available[name]
            selected=selections.get(name,[])
            if role is not None:selected=[r for r in selected if r['role']==role]
            return dict(**summary(selected,count,draws),available=count>0,
                        unavailable_fixtures=available-count,
                        missing_reason=None if count else 'variant_not_frozen')
        reports[timing+'|'+config]=dict(timing=timing,config_sha256=config,available_fixtures=available,
            comparison_baseline=CONTROL,
            variants={name:variant_summary(name) for name in ('published',CONTROL,*VARIANTS)},
            role_variants={'recommended_alternative':{
                name:variant_summary(name,'recommended_alternative') for name in (CONTROL,GG_FT_SAME_RULES)}},
            cohorts={name:summary(rows,available,draws) for name,rows in sorted(cohorts.items())},
            margin_counterfactual_counts=dict(margin_counts),
            identical_event_comparisons={name:{**s,
                'brier_delta':s['brier_delta_sum']/s['identical_target'] if s['identical_target'] else None,
                'log_loss_delta':s['log_loss_delta_sum']/s['identical_target'] if s['identical_target'] else None}
                for name,s in matched.items()})
    return dict(protocol=PROTOCOL,shadow_only=True,automatic_promotion=False,unavailable_fixtures=len(unavailable),
                kickoff_conflict_fixtures=sorted(conflicts),
                frozen_fixture_snapshots=len(snapshots),reports=reports,
                comparison_basis='Calibration on each selected event; probability-skill deltas only for identical market targets. ROI is monitoring only.',
                missing_protocol='not_reconstructed; historical snapshots unchanged',
                research_trace=research_report(archives,cutoff,draws))


def paired_metrics(rows, draws=400, candidate='probability'):
    """Same contracts, paired against their own fair, clustered two ways."""
    paired = [r for r in rows if r.get('outcome') in (0, 1)
              and finite(r.get(candidate)) and 0 <= r[candidate] <= 1
              and finite(r.get('fair')) and 0 < r['fair'] < 1]
    def losses(p, y):
        return (p-y)**2, -log(max(1e-12, p if y else 1-p))
    values = []
    for row in paired:
        a, b = losses(row[candidate], row['outcome']), losses(row['fair'], row['outcome'])
        values.append((row, (a[0]-b[0], a[1]-b[1], row[candidate]-row['outcome'],
                             a[0], a[1], b[0], b[1], 1)))
    names = ('brier_delta', 'log_loss_delta', 'calibration_error', 'brier', 'log_loss', 'fair_brier', 'fair_log_loss')
    n = len(values)
    point = {name: sum(v[i] for _, v in values)/n if n else None for i, name in enumerate(names)}
    intervals = {}
    for cluster in ('fixture_id', 'date'):
        grouped = defaultdict(list)
        for row, value in values: grouped[row[cluster]].append(value)
        totals = [tuple(sum(v[i] for v in group) for i in range(8)) for _, group in sorted(grouped.items())]
        estimates = {name: [] for name in names}
        if len(totals) >= 2:
            rng = Random(20261125)
            for _ in range(draws):
                sample = [totals[rng.randrange(len(totals))] for _ in totals]
                denominator = sum(v[7] for v in sample)
                for i, name in enumerate(names): estimates[name].append(sum(v[i] for v in sample)/denominator)
        result = {}
        for name, sample in estimates.items():
            sample.sort(); last = len(sample)-1
            result[name] = [sample[int(.025*last)], sample[int(.975*last)]] if sample else None
        intervals[cluster] = dict(clusters=len(totals), intervals=result,
                                  status='available' if len(totals) >= 2 else 'insufficient_independent_clusters')
    bins = []
    for low in range(10):
        cell = [r for r in paired if min(9, int(r[candidate]*10)) == low]
        if cell: bins.append(dict(lower=low/10, upper=(low+1)/10, n=len(cell),
            probability=sum(r[candidate] for r in cell)/len(cell), hit_rate=sum(r['outcome'] for r in cell)/len(cell)))
    return dict(n=n, candidate=candidate, paired_against='same_operator_fair', **point,
                calibration_bins=bins, bootstrap=dict(confidence=.95, draws=draws, clusters=intervals))


def research_report(archives, cutoff, draws=400):
    """Read only new sealed measurement ledgers; never reconstruct old cohorts."""
    from v7_probability import MEASUREMENT_PROTOCOL
    latest = {}; starts = defaultdict(set); unavailable = set()
    for archive in archives:
        for fid, fixture in (archive.get('fixtures') or {}).items():
            fid = str(fid)
            # The existing learning pipeline forwards only settled shadow
            # envelopes. Read their newly frozen trace, never regenerate it.
            if not fixture.get('measurement'):
                frozen = (fixture.get('decision_shadow') or {}).get('latest') or {}
                ledger = frozen.get('movement_study') or {}
                if ledger.get('measurement_protocol') == MEASUREMENT_PROTOCOL:
                    cohorts = ledger.get('cohorts') or []
                    fixture = dict(fixture, kickoff=ledger.get('kickoff'),
                        captured_at=ledger.get('captured_at'),
                        measurement=dict(protocol=MEASUREMENT_PROTOCOL,
                            config_sha256=ledger.get('measurement_config_sha256'),
                            markets={r['market_id']: r['audit'] for r in cohorts if r.get('audit')},
                            equivalent_prices=ledger.get('equivalent_prices') or []),
                        outcomes={r['market_id']: {k: r.get(k) for k in ('win', 'loss', 'roi')} for r in cohorts})
            start, captured, observed = (stamp(fixture.get(k)) for k in ('kickoff', 'captured_at', 'observed_at'))
            if not start or not captured or not captured < start <= cutoff or observed and observed > cutoff: continue
            starts[fid].add(start.isoformat())
            trace = fixture.get('measurement') or {}
            if trace.get('protocol') != MEASUREMENT_PROTOCOL:
                unavailable.add(fid); continue
            config = trace.get('config_sha256')
            if not config: unavailable.add(fid); continue
            key = (fid, start.isoformat(), config)
            old = latest.get(key)
            if old is None or captured > stamp(old['captured_at']): latest[key] = fixture
    conflicts = {fid for fid, times in starts.items() if len(times) > 1}
    latest = {k: v for k, v in latest.items() if k[0] not in conflicts}
    groups = defaultdict(list)
    for (fid, _, config), fixture in latest.items(): groups[config].append((fid, fixture))
    reports = {}
    for config, fixtures in sorted(groups.items()):
        cells = defaultdict(list); prices = []; composition = defaultdict(int); days = defaultdict(int)
        weight_rows = defaultdict(list)
        counts = defaultdict(int)
        for fid, fixture in fixtures:
            from v7_archive import ROME
            day = stamp(fixture['kickoff']).astimezone(ROME).date().isoformat()
            days[day] += 1; trace = fixture['measurement']
            weights = (((fixture.get('decision_shadow') or {}).get('latest') or {}).get('research_weight_grid') or {})
            for market, item in weights.items():
                weight_rows[market].append(dict(fixture_id=fid, date=day, fair=item.get('fair'),
                    outcome=item.get('outcome'), **{'weight_'+k:v for k,v in (item.get('variants') or {}).items()}))
            for price in trace.get('equivalent_prices', []):
                prices.append(dict(price, fixture_id=fid, date=day))
            for market, info in trace.get('markets', {}).items():
                outcome = (fixture.get('outcomes') or {}).get(market) or {}
                binary = outcome.get('win') in (0, 1) and outcome.get('loss') in (0, 1) and outcome['win']+outcome['loss'] == 1
                item = dict(info, fixture_id=fid, date=day, market=market,
                            outcome=int(outcome['win']) if binary else None, roi=outcome.get('roi'))
                roles = info.get('official_roles') or []
                for role in roles: composition[role+'|'+market] += 1
                for name, cohort in (info.get('observers') or {}).items():
                    counts[name+'|total'] += 1
                    if cohort.get('available') is not True:
                        counts[name+'|missing'] += 1; continue
                    state = 'matched' if cohort.get('matched') is True else 'complement'
                    counts[name+'|available'] += 1
                    if info.get('quote_usable') is True: counts[name+'|executable'] += 1
                    # Publish every predeclared cell, including losing and unselected candidates.
                    base = name+'|'+state
                    for cell in (base, base+'|'+('official' if roles else 'unselected'), base+'|day='+day):
                        cells[cell].append(item)
        details = {}
        for name, rows in sorted(cells.items()):
            paired = paired_metrics(rows, draws)
            settled = [r for r in rows if finite(r.get('roi'))]
            unselected = '|unselected' in name
            details[name] = dict(rows=len(rows), distinct_fixtures=len({r['fixture_id'] for r in rows}),
                pending=sum(r['outcome'] is None for r in rows),
                metrics=paired,
                raw_vs_fair=paired_metrics(rows, draws, 'raw_probability'),
                statistics_vs_fair=paired_metrics(rows, draws, 'statistical_probability'),
                roi_control=sum(r['roi'] for r in settled)/len(settled) if settled else None,
                unselected_winners=sum(r['outcome'] == 1 for r in rows) if unselected else None,
                role_changes=0, production_choices_changed=False)
        reports[config] = dict(fixtures=len(fixtures), days=dict(days), coverage=dict(counts),
            cells=details, composition=dict(composition), equivalent_price_comparisons=prices,
            audit_weight_grid={market: {weight: paired_metrics(rows, draws, weight)
                for weight in sorted({k for r in rows for k in r if k.startswith('weight_')})}
                for market, rows in weight_rows.items()},
            price_quality=dict(comparisons=len(prices), improved=sum(p['odd_gain'] > 0 for p in prices),
                mean_ev_gain=sum(p['ev_gain'] for p in prices)/len(prices) if prices else None),
            prospective_status='accumulating; no automatic promotion')
    return dict(protocol=MEASUREMENT_PROTOCOL, shadow_only=True, automatic_promotion=False,
                unavailable_fixtures=len(unavailable-set(k[0] for k in latest)),
                kickoff_conflict_fixtures=sorted(conflicts), reports=reports,
                inference='Associations only; final V7 contains the market. ROI is control only.',
                historical_backfill=False)
