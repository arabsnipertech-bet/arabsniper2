"""Matched-cohort evaluation of immutable, versioned multimarket forecasts."""
from collections import defaultdict
from math import log, sqrt
from multimarket_model import COMBO, MODEL_ID, VALIDATION_FROM, stamp


def outcome(market, side, result):
    if result.get('status') not in {'FT','AET','PEN'}:
        return None
    ht = [result.get(k) for k in ('home_ht','away_ht')]
    ft = [result.get(k) for k in ('home_ft','away_ft')]
    if market == '1x2_ft':
        if None in ft: return None
        return side == ('1' if ft[0]>ft[1] else '2' if ft[1]>ft[0] else 'x')
    needed = ht if market in {'over05_ht','over15_ht','gg_ht'} else ft
    if None in needed or market in {COMBO,'over05_sh'} and None in ht:
        return None
    if market in {COMBO,'over05_sh'} and any(a<b for a,b in zip(ft,ht)):
        return None
    h,a = needed
    events = {'over05_ht':h+a>=1,'over15_ht':h+a>=2,'gg_ht':h>0 and a>0,
              'over25_ft':h+a>=3,'over35_ft':h+a>=4,'gg_ft':h>0 and a>0,
              'home15_ft':h>=2,'away15_ft':a>=2}
    if market in {COMBO,'over05_sh'}:
        event = sum(ft)-sum(ht)>=1 and (sum(ht)>=2 if market==COMBO else True)
    else:
        event = events.get(market)
    return event if side in {'over','yes'} or event is None else not event


def settle(store, result_by_id):
    for record in store.get('multimarket_evaluations', {}).values():
        result = result_by_id.get(str(record.get('fixture',{}).get('fixture_id')))
        if not result or result.get('status') not in {'FT','AET','PEN'}:
            continue
        record['settlement'] = {'result':dict(result), 'outcomes': {
            f"{m['market']}::{m['selection']}":outcome(m['market'],m['selection'],result)
            for m in record.get('multimarket',{}).get('markets',[])}}


def metrics(rows):
    n=len(rows)
    if not n: return {'n':0}
    wins=sum(y for p,y,odd in rows); rate=wins/n; z=1.96; d=1+z*z/n
    center=(rate+z*z/(2*n))/d
    margin=z*sqrt(rate*(1-rate)/n+z*z/(4*n*n))/d
    priced=[(y,odd) for p,y,odd in rows if isinstance(odd,(int,float)) and odd>1]
    return {'n':n,'wins':wins,'hit_rate_pct':round(rate*100,3),
            'mean_probability_pct':round(sum(p for p,y,o in rows)/n*100,3),
            'brier':round(sum((p-y)**2 for p,y,o in rows)/n,6),
            'log_loss':round(-sum(log(max(1e-12,min(1-1e-12,p if y else 1-p))) for p,y,o in rows)/n,6),
            'wilson_95_pct':[round((center-margin)*100,3),round((center+margin)*100,3)],
            'priced_n':len(priced),
            'paper_roi_pct':round(sum((odd if y else 0)-1 for y,odd in priced)/len(priced)*100,3) if priced else None}


def build_report(store):
    groups=defaultdict(list); calibration=defaultdict(list); features=defaultdict(list)
    frozen=defaultdict(set); settled=defaultdict(set); pending=defaultdict(set)
    for record in store.get('multimarket_evaluations',{}).values():
        assessment=record.get('multimarket',{})
        version=assessment.get('model_id','unknown')
        kickoff=stamp(record.get('fixture',{}).get('kickoff'))
        captured=stamp(record.get('captured_at'))
        if not kickoff or not captured or captured>=kickoff:
            continue
        # Retrospective source inputs are never counted as prospective forecasts.
        period='retrospective' if record.get('retrospective') else 'prospective' if kickoff.date().isoformat()>=VALIDATION_FROM else 'development'
        cohort=(version,period); fid=str(record.get('fixture',{}).get('fixture_id'))
        frozen[cohort].add(fid)
        outcomes=record.get('settlement',{}).get('outcomes',{})
        if not outcomes or any(v is None for v in outcomes.values()): pending[cohort].add(fid)
        if any(v is not None for v in outcomes.values()): settled[cohort].add(fid)
        for market in assessment.get('markets',[]):
            key=f"{market['market']}::{market['selection']}"
            y=outcomes.get(key)
            if y is None: continue
            probabilities=market['probabilities']
            odd=market.get('odd')
            p=probabilities['combined']
            selected=bool(market.get('research_candidate'))
            for group in ('all','candidate' if selected else 'rejected'):
                groups[(version,period,key,group,'combined')].append((p,y,odd))
            calibration[(version,period,key,min(int(p*10),9)*10)].append((p,y,odd))
            b=market.get('book_probability_pct')
            # Compare every variant on exactly the same market/side/fixture cohort.
            if b is not None:
                for variant,value in [*probabilities.items(),('book',b/100)]:
                    groups[(version,period,key,'matched_benchmark',variant)].append((value,y,odd))
            if odd and 2<=odd<=5:
                groups[(version,period,key,'odds_2_to_5','combined')].append((p,y,odd))
            movement=market.get('movement',{}); drop=movement.get('median_drop_pct')
            if drop is not None:
                features[(version,period,key,'drop_ge_10' if drop>=10 else 'other_paired_movements')].append((p,y,odd))
            mf=assessment.get('features',{})
            droughts=[mf.get(s,{}).get('drought',{}).get('minutes_lower_bound') for s in ('home','away')]
            if all(isinstance(v,(int,float)) for v in droughts):
                features[(version,period,key,'drought_ge_90' if max(droughts)>=90 else 'drought_below_90')].append((p,y,odd))
            # Test the user's 1.75/1.80 -> 1.50/1.60 hypothesis with SAME operators.
            one_x_two=assessment.get('quotes',{}).get('1x2_ft',[])
            eligible_steam=[q for q in one_x_two
                if q.get('movement',{}).get('observations',0)>=2
                and 1.70<=q.get('movement',{}).get('first_odds',{}).get('1',0)<=1.85]
            steam=any(1.45<=q.get('odds',{}).get('1',0)<=1.65 for q in eligible_steam)
            if eligible_steam:
                features[(version,period,key,'home_favourite_steam' if steam else 'no_home_favourite_steam')].append((p,y,odd))
            rates=assessment.get('rates',{}).get('statistics',[])
            if len(rates)==4:
                for name,condition in [('lambda_ht_ge_1_5',sum(rates[:2])>=1.5),('lambda_ft_ge_3',sum(rates)>=3)]:
                    features[(version,period,key,name if condition else 'below_'+name)].append((p,y,odd))
            for name,field in [('historical_ht_mean','metrics'),('recent_ht_mean','recent5_ht')]:
                metric='total_ht' if field=='metrics' else 'avg_total_ht'
                values=[mf.get(s,{}).get(field,{}).get(metric) for s in ('home','away')]
                if all(isinstance(v,(int,float)) for v in values):
                    suffix='_ge_1_5' if sum(values)/2>=1.5 else '_below_1_5'
                    features[(version,period,key,name+suffix)].append((p,y,odd))
            oci=market.get('legacy_oci')
            if isinstance(oci,(int,float)):
                features[(version,period,key,'oci_ge_45' if oci>=45 else 'oci_below_45')].append((p,y,odd))
            margin_delta=movement.get('median_overround_delta_pp')
            if margin_delta is not None:
                features[(version,period,key,'aggio_rising' if margin_delta>=.5 else 'aggio_falling' if margin_delta<=-.5 else 'aggio_stable')].append((p,y,odd))
    return {'model_id':MODEL_ID,'validation_from':VALIDATION_FROM,'status':'experimental',
        'note':'No threshold tuning. Retrospective development is separated from frozen prospective forecasts. ROI is theoretical, not executed.',
        'cohorts':[{'model_id':v,'period':p,'frozen_fixtures':len(ids),'settled_fixtures':len(settled[(v,p)]),'pending_fixtures':len(pending[(v,p)])} for (v,p),ids in sorted(frozen.items())],
        'groups':[dict(zip(('model_id','period','market_selection','group','variant'),key),**metrics(rows)) for key,rows in sorted(groups.items())],
        'calibration':[dict(zip(('model_id','period','market_selection','band_from_pct'),key),**metrics(rows)) for key,rows in sorted(calibration.items())],
        'feature_comparisons':[dict(zip(('model_id','period','market_selection','feature'),key),**metrics(rows)) for key,rows in sorted(features.items())]}
