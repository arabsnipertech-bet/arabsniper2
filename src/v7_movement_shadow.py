"""Frozen, prospective-only November 22 hypotheses. No provider or publisher."""
from copy import deepcopy
from hashlib import sha256
import json

from multimarket_model import stamp
from v7_probability import finite, price_usable, utc

PROTOCOL = 'V7_MOVEMENT_SHADOW_20261122_V1'
CONFIG = dict(oci_min=0,oci_max=50,ft_saturation_drop=-5,
              margin_reference_ft=6.5,margin_reference_half=8.,
              favorite_band=[1.35,1.70],conversion_ft_band=[1.80,2.50],
              under_floor=1.75,primary_preferred=[1.75,2.15],recommended_band=[1.85,2.60],
              recommended_margin=6.5,gg_floors=[.21,.25],gg_min_odd=4.20,
              bootstrap_draws=400,automatic_promotion=False)
VARIANTS = ('nov22_oci_window','nov22_cross_markets','nov22_margin_friction',
            'nov22_triad_rank_21','nov22_triad_rank_25')
CONTROL = 'nov22_control'


def fingerprint(value):
    return sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def direction(value):
    return 'missing' if not finite(value) else 'down' if value < -1e-9 else 'up' if value > 1e-9 else 'stable'


def opening(record, source, as_of, kickoff):
    quote=record.get('quote') or {}
    first,odd=quote.get('first_odd'),record.get('odd')
    a,b,now=(stamp(x) for x in (quote.get('first_observed_at'),quote.get('quote_observed_at'),as_of))
    if (not price_usable(source,as_of,kickoff) or not all((a,b,now)) or not a<b<=now
            or not finite(first) or first<=1 or not finite(odd) or odd<=1
            or str(quote.get('bookmaker_id'))!=str(source.get('bookmaker_id'))
            or source.get('odd')!=odd):return None
    return 100*(odd/first-1)


def peak_support(profiles, period):
    if period not in ('ht','ft'):return None
    teams=[(profiles.get(s) or {}).get('for_'+period) or {} for s in ('home','away')]
    if not all(finite(t.get(k)) for t in teams for k in ('mean','without_peak','sample')):return None
    if not all(t['sample']>=4 and 0<=t['without_peak']<=t['mean'] for t in teams):return False
    mean=sum(t['mean'] for t in teams)
    return bool(mean>0 and sum(t['without_peak'] for t in teams)>=.8*mean)


def anchor(records,sources,as_of,kickoff):
    candidates=[]
    for key,row in records.items():
        spec=json.loads(key);source=sources.get(row['source_market_id']) or {}
        if spec.get('kind')!='result' or spec.get('period')!='ft' or opening(row,source,as_of,kickoff) is None:continue
        start,end=source.get('opening_market_odds') or {},source.get('selected_market_odds') or {}
        if (set(start)!= {'1','x','2'} or set(end)!= {'1','x','2'}
                or not all(finite(v) and v>1 for v in [*start.values(),*end.values()])):continue
        margin=source.get('selected_margin_pct')
        if not finite(margin) or not 0<=margin<=30:continue
        side=spec.get('side');quote=row.get('quote') or {}
        total=sum(1/v for v in end.values())
        if (start.get(side)!=quote.get('first_odd') or end.get(side)!=row.get('odd')
                or abs(100*(total-1)-margin)>1e-6
                or not finite(row.get('fair')) or abs(1/end[side]/total-row['fair'])>1e-8):continue
        # Complete vectors from ONE operator, never three unrelated best prices.
        candidates.append((margin,str(source['bookmaker_id']),key,start,end))
    if not candidates:return dict(available=False,reason='same_book_1x2_opening_unavailable')
    _,book,key,start,end=min(candidates,key=lambda v:v[:3])
    fav0=min(('1','2'),key=lambda side:(start[side],side));fav1=min(('1','2'),key=lambda side:(end[side],side))
    return dict(available=True,bookmaker_id=book,source_market_id=records[key]['source_market_id'],
                opening=deepcopy(start),current=deepcopy(end),deltas={s:100*(end[s]/start[s]-1) for s in start},
                first_observed_at=records[key]['quote']['first_observed_at'],
                quote_observed_at=records[key]['quote']['quote_observed_at'],
                initial_favorite=fav0,current_favorite=fav1,favorite_inversion=fav0!=fav1,
                favorite_135_170=1.35<=start[fav0]<=1.70)


def capture(records,sources,observed,published,*,as_of=None,kickoff=None,profiles=None,
            ev_recoveries=None,qualified_alternatives=()):
    """Return private variants plus a compact same-snapshot cohort ledger."""
    from v7_decision import (GG,GG_FT,GOAL,RESULTS,TRIAD_LIMITS,TRIAD_MARKETS,
                             compatible,route,role_rank)
    from lateral_matrix import market_id,specification
    at,start=stamp(as_of),stamp(kickoff)
    if not at or not start or not at<start:
        return dict(protocol=PROTOCOL,shadow_only=True,available=False,reason='missing_live_prematch_context'),{}
    profiles=profiles or {}
    # Never mutate records used by the production selector, not even temporary EV.
    pool=deepcopy(records)
    names=['most_probable','recommended_alternative','high_odds_shot']
    first,recommended,shot=names
    rows=[];deltas={};friction={};eligible={}
    for key,row in pool.items():
        spec=json.loads(key);source=sources.get(row['source_market_id']) or {}
        usable=price_usable(source,as_of,kickoff)
        delta=opening(row,source,as_of,kickoff);deltas[key]=delta
        move=row.get('features') or {};oci=source.get('oci')
        if source.get('historical_oci'):oci=None
        state='missing' if not finite(oci) else 'neutral' if oci==0 else 'current'
        margin=move.get('margin_pct');ev=row.get('ev');p=row.get('calibrated')
        valid=bool(usable and row.get('supported') is True and finite(ev) and finite(p))
        eligible[key]=valid
        if not usable:
            continue  # Unquoted rows already exist in the frozen V7 matrix.
        peak=peak_support(profiles,spec.get('period'))
        leg=move.get('delta_pct')
        moderate=bool(valid and key in (GOAL['over15_ht'],GOAL['over25_ft'])
                      and state=='current' and 0<oci<50 and ev>=0
                      and (spec.get('period')!='ft' or finite(leg) and leg>-5))
        basis=(row.get('pricing_evidence') or {}).get('basis')
        fair=row.get('fair');odd=row.get('odd')
        reference=CONFIG['margin_reference_ft'] if spec.get('period')=='ft' else CONFIG['margin_reference_half']
        cost,ev_reference=None,None
        if (valid and basis=='binary' and finite(fair) and 0<fair<1 and finite(margin) and 0<=margin<=30
                and finite(odd) and odd>1 and abs(1/(odd*(1+margin/100))-fair)<1e-8):
            capped=min(margin,reference)
            reference_odd=odd if margin<=reference else 1/(fair*(1+capped/100))
            cost=max(0.,p*(reference_odd-odd))
            ev_reference=p*reference_odd-1
            friction[key]=ev-cost
        rows.append(dict(market_id=key,probability=p,odd=row.get('odd'),ev=ev,ev_low=row.get('ev_low'),
            statistics=row.get('statistics'),fair=fair,margin_pct=margin,opening_delta_pct=delta,
            last_leg_delta_pct=leg,opening_direction=direction(delta),last_leg_direction=direction(leg),
            bookmaker_id=source.get('bookmaker_id'),first_observed_at=utc(source.get('first_observed_at')),
            quote_observed_at=utc(source.get('quote_observed_at')),provider_updated_at=utc(source.get('provider_updated_at')),
            oci=oci if finite(oci) else None,oci_state=state,
            quote_usable=bool(usable),supported=row.get('supported') is True,without_peak_supported=peak,
            oci_moderate=moderate,ft_saturation=bool(spec.get('period')=='ft' and
                ((finite(oci) and oci>=50) or (finite(leg) and leg<=-5))),
            rising_statistical_value=bool(valid and finite(delta) and delta>0 and peak is True and ev>=0),
            rising_without_value=bool(valid and finite(delta) and delta>0 and ev<0),
            margin_reference_pct=reference,excess_margin_ev_cost=cost,
            ev_at_reference_margin=ev_reference,shadow_ranking_score=friction.get(key),
            negative_ev_becomes_nonnegative_at_reference_margin=(ev<0<=ev_reference)
                if finite(ev) and finite(ev_reference) else None))
    same_book=anchor(pool,sources,as_of,kickoff)
    goalkeys={GOAL['over15_ht'],GG,GOAL['over25_ft'],GOAL['over35_ft']}
    for row in rows:
        key=row['market_id'];ft=deltas.get(GOAL['over25_ft']);gg=deltas.get(GG)
        row['cross_1x2_available']=same_book['available'] and key in goalkeys and row['opening_delta_pct'] is not None
        row['favorite_135_170']=same_book.get('favorite_135_170') if row['cross_1x2_available'] else None
        row['favorite_inversion']=same_book.get('favorite_inversion') if row['cross_1x2_available'] else None
        row['favorite_opening_delta_pct']=(same_book['deltas'][same_book['initial_favorite']]
                                            if row['cross_1x2_available'] else None)
        row['cross_direction']=(direction(row['favorite_opening_delta_pct'])+'|'+row['opening_direction']
                                  if row['cross_1x2_available'] else None)
        row['cross_same_operator']=(str(row['bookmaker_id'])==same_book['bookmaker_id']
                                      if row['cross_1x2_available'] else None)
        q=pool.get(GOAL['over25_ft'],{}).get('odd')
        row['gg_pt_up_over25_ft_down']=bool(key in (GG,GOAL['over25_ft']) and finite(ft) and ft<0
            and finite(gg) and gg>0 and finite(q) and 1.80<=q<=2.50)
    bykey={r['market_id']:r for r in rows}
    quoted={k:r for k,r in pool.items() if eligible[k]}
    def simulate(allowed,**kwargs):
        # Radar emissions are not part of this private subset selector.
        flags={**observed,'conversion':False,'rebound_gg':False}
        recoveries={role:{k:proof for k,proof in candidates.items() if k in allowed}
                    for role,candidates in (ev_recoveries or {}).items()}
        return route(allowed,flags,study=True,allow_model_estimates=False,allow_unpriced=False,
                     ev_recoveries=recoveries,qualified_alternatives=qualified_alternatives,**kwargs)[0]
    moderate={k:r for k,r in quoted.items() if bykey[k]['oci_moderate']}
    cross={k:r for k,r in quoted.items() if bykey[k]['cross_1x2_available'] or bykey[k]['gg_pt_up_over25_ft_down']}
    def base_rank(role,key,row):
        if role==first and not finite(row.get('ev_low')):
            row={**row,'ev_low':-1e9}
        return role_rank(role,key,row)
    def friction_rank(role,key,row):
        ranking=list(base_rank(role,key,row));index=2 if role==first else 1
        ranking[index]+=friction.get(key,row['ev'])-row['ev']
        return tuple(ranking)
    variants={CONTROL:simulate(quoted)}
    variants.update(nov22_oci_window=simulate(moderate),nov22_cross_markets=simulate(cross),
                    nov22_margin_friction=simulate(quoted,ranker=friction_rank))
    under={market_id(specification('total',p,'under',1.5)) for p in ('ht','sh')}
    recmarkets={GG_FT,GOAL['over15_ht'],RESULTS['1'],GOAL['over15_sh']}
    roles={**TRIAD_MARKETS,first:TRIAD_MARKETS[first]|under,recommended:TRIAD_MARKETS[recommended]|recmarkets}
    limits={**TRIAD_LIMITS,recommended:(TRIAD_LIMITS[recommended][0],1.85,3.50)}
    def ranking(role,key,row):
        odd=row.get('odd')
        baseline=base_rank(role,key,row)
        if role==first:return (1.75<=odd<=2.15,*baseline)
        preferred=(role==recommended and key in recmarkets and 1.85<=odd<=2.60
                   and finite((row.get('features') or {}).get('margin_pct')) and 0<=row['features']['margin_pct']<=6.5)
        return (bool(preferred),*baseline)
    for floor in (.21,.25):
        def candidate_filter(role,key,row,others):
            odd=row.get('odd')
            if role==first and key in under and odd<1.75:return False
            def admissible(k,r):
                if k in others:return False
                q=r.get('odd');ev=r.get('ev')
                if not (finite(q) and q>=TRIAD_LIMITS[shot][1] and r['calibrated']>=TRIAD_LIMITS[shot][0]):return False
                if k==GG and (q<4.20 or r['calibrated']<floor or not finite(ev) or ev<=0):return False
                return compatible(k,*others)
            if role!=shot:return True
            if key not in roles[shot] or not admissible(key,row):return False
            positive=any(admissible(k,r) and finite(r.get('ev')) and r['ev']>=0
                         for k,r in quoted.items() if k in roles[shot])
            return not positive or row['ev']>=0
        variants['nov22_triad_rank_'+str(int(floor*100))]=simulate(quoted,role_markets=roles,limits=limits,
                                                               ranker=ranking,candidate_filter=candidate_filter)
    ledger=dict(protocol=PROTOCOL,config=deepcopy(CONFIG),config_sha256=fingerprint(CONFIG),shadow_only=True,
        available=True,captured_at=as_of,kickoff=kickoff,cohorts=rows,anchor_1x2=same_book,
        coverage=dict(matrix_rows=len(pool),usable_quoted_rows=len(rows),statistically_supported_quoted_rows=sum(eligible.values())),
        comparison_control=CONTROL,control_scope='same executable statistically supported pool; current routing, recovery certificates and Plus PT retained',
        probability_weights_changed=False,automatic_promotion=False,
        margin_protocol='EV unchanged; shadow ranking score subtracts an explicit extra prudence cost. Reference-margin return is a counterfactual, not causal attribution.',
        prospective_protocol='Evaluate only frozen ledgers; never reconstruct absent variants on historical snapshots.')
    ledger['evidence_sha256']=fingerprint(dict(captured_at=as_of,kickoff=kickoff,cohorts=rows,anchor_1x2=same_book))
    return ledger,variants
