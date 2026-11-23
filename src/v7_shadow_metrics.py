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
                missing_protocol='not_reconstructed; historical snapshots unchanged')
