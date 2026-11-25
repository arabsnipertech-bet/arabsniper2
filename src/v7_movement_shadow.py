"""Frozen, prospective-only November 22 hypotheses. No provider or publisher."""
from copy import deepcopy
from collections import defaultdict
from hashlib import sha256
import json

from multimarket_model import stamp
from v7_probability import finite, price_usable, utc

PROTOCOL = 'V7_MOVEMENT_SHADOW_20261122_V1'
CONFIG = dict(oci_min=0,oci_max=50,ft_saturation_drop=-5,
              routing_revision='TRIAD_POSITIVE_ALTERNATIVES_V2',
              away_symmetric_band=[4.50,7.50],
              gg_ft_review='SAME_RULES_NATIVE_CONTEXT_V1',
              margin_reference_ft=6.5,margin_reference_half=8.,
              favorite_band=[1.35,1.70],conversion_ft_band=[1.80,2.50],
              under_floor=1.75,primary_preferred=[1.75,2.15],recommended_band=[1.85,2.60],
              recommended_margin=6.5,gg_floors=[.21,.25],gg_min_odd=4.20,
              bootstrap_draws=400,automatic_promotion=False)
VARIANTS = ('nov22_oci_window','nov22_cross_markets','nov22_margin_friction',
            'nov22_triad_rank_21','nov22_triad_rank_25','nov23_gg_ft_same_rules','nov24_away_symmetric')
CONTROL = 'nov22_control'
GG_FT_SAME_RULES = 'nov23_gg_ft_same_rules'

FLOW_FIELDS = ('paired_books', 'agreeing_books', 'fair_direction_coherent',
               'movement_confirmed', 'persistence', 'reversal')


def evidence_state(value):
    if value is None: return 'missing'
    if value is False: return 'failed'
    if value is True: return 'passed'
    if finite(value) and value == 0: return 'neutral'
    return 'recorded'


def instrument(snapshot, sources, profiles):
    """Two cohort observers, plus lossless trace of existing calculations.

    No routing call, probability calculation, provider request or old replay.
    """
    from matrix_quotes import equivalent_prices
    from v7_decision import GG_FT, RESULTS
    from v7_probability import MEASUREMENT_CONFIG
    measurement = snapshot['measurement']
    profiles = profiles or {}
    rates = snapshot.get('rates') or {}
    stats_at, captured = stamp(snapshot.get('statistics_at')), stamp(snapshot.get('captured_at'))
    statistics_valid = bool(stats_at and captured and stats_at <= captured)
    threshold = MEASUREMENT_CONFIG['away_raw_gap']
    measurement['observers'] = dict(
        gg_ft_weak_side=dict(lambda_threshold=1., lambda_origin='v7.rates',
                             native_lambda_secondary_only=True, rerouting=False),
        away_raw_market_gap=dict(minimum_probability_gap=threshold,
                                 raw_origin='market.raw', final_origin='market.calibrated', rerouting=False))
    measurement['statistics_samples'] = deepcopy(profiles or {})
    measurement['markets'] = {}
    selected = defaultdict(list)
    for role, pick in (snapshot.get('triad') or {}).items():
        if pick: selected[pick['market_id']].append(role)
    for key, row in snapshot['markets'].items():
        source = sources.get(row.get('source_market_id')) or {}
        spec = json.loads(key)
        move = source.get('selected_movement') or {}
        flow = {k: deepcopy(source.get(k)) for k in FLOW_FIELDS}
        # The engine records the reversal of the selected operator's window.
        if 'reversal' not in source: flow['reversal'] = deepcopy(move.get('reversal'))
        peak = peak_support(profiles or {}, spec.get('period'))
        native = snapshot.get('statistics') or {}
        raw, final = row.get('raw'), row.get('calibrated')
        lambdas = [rates.get(k) for k in ('home_ft', 'away_ft')]
        weak_available = statistics_valid and all(finite(v) and v >= 0 for v in lambdas)
        gap_available = statistics_valid and finite(raw) and finite(final)
        cohorts = {}
        if key == GG_FT:
            cohorts['gg_ft_weak_side'] = dict(available=weak_available,
                matched=any(v < 1. for v in lambdas) if weak_available else None,
                lambda_ft=deepcopy(lambdas), lambda_origin='v7.rates',
                native_lambda_ft=[native.get(k) for k in ('home_ft', 'away_ft')],
                reason=None if weak_available else 'missing_or_future_lambda_evidence')
        if key == RESULTS['2']:
            gap = raw-final if gap_available else None
            cohorts['away_raw_market_gap'] = dict(available=gap_available,
                matched=gap >= threshold-1e-12 if gap_available else None, raw_minus_v7=gap,
                reason=None if gap_available else 'missing_or_future_probability_evidence')
        oci = source.get('oci') if not source.get('historical_oci') else None
        info = dict(raw_probability=raw, statistical_probability=row.get('statistics'),
            probability=final, fair=row.get('fair'), odd=row.get('odd'),
            quote_usable=bool(finite(row.get('odd')) and row['odd'] > 1 and
                              price_usable(source, snapshot['captured_at'], snapshot['kickoff'])),
            probability_origins=dict(raw='raw_poisson', statistics='pre_market_statistics', probability='final_v7'),
            without_peak=peak, without_peak_state=evidence_state(peak),
            sample_sizes={side: {name: value.get('sample') for name, value in profiles.get(side, {}).items()
                                if isinstance(value, dict)} for side in ('home', 'away')},
            observed_goal_samples={side: {period: values.get('sample') for period, values in
                    (snapshot.get('means', {}).get(side) or {}).items() if isinstance(values, dict)}
                    for side in ('home', 'away')},
            flow_fields=flow, flow_states={k: ('no_reversal' if v is False else 'reversal_detected')
                if k == 'reversal' and type(v) is bool else evidence_state(v) for k, v in flow.items()},
            flow_origins={k: 'selected_movement.reversal' if k == 'reversal' and k not in source else k
                          for k in FLOW_FIELDS},
            missing_flow_fields=[k for k, v in flow.items() if v is None],
            partitions=deepcopy(source.get('audit_partitions')),
            opening_delta_pct=opening(row, source, snapshot['captured_at'], snapshot['kickoff']),
            last_leg_delta_pct=(row.get('features') or {}).get('delta_pct'),
            oci=oci, oci_state='missing' if not finite(oci) else 'neutral' if oci == 0 else 'current',
            official_roles=selected.get(key, []), supported=row.get('supported'), tier=row.get('tier'),
            edge_exclusions=deepcopy(row.get('edge_exclusions')),
            policy_exclusions=deepcopy(row.get('policy_exclusions')), observers=cohorts)
        measurement['markets'][key] = info
    measurement['equivalent_prices'] = equivalent_prices(snapshot['markets'], sources,
                                                          snapshot['captured_at'], snapshot['kickoff'])
    ledger = (snapshot.get('decision_shadow') or {}).get('movement_study')
    if ledger and ledger.get('available') is True:
        ledger['measurement_protocol'] = measurement['protocol']
        ledger['measurement_config_sha256'] = measurement['config_sha256']
        ledger['probability_origins'] = deepcopy(measurement['probability_origins'])
        ledger['observers'] = deepcopy(measurement['observers'])
        ledger['equivalent_prices'] = deepcopy(measurement['equivalent_prices'])
        for row in ledger.get('cohorts', []):
            row['audit'] = deepcopy(measurement['markets'].get(row['market_id']))
        # Existing evidence_sha256 describes the original protocol, untouched.
        ledger['measurement_sha256'] = fingerprint(measurement)


def fingerprint(value):
    return sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def direction(value):
    return 'missing' if not finite(value) else 'down' if value < -1e-9 else 'up' if value > 1e-9 else 'stable'


def statistical_context(rates, observed_at, as_of):
    """Record the existing statistical lambdas, never infer them from outcomes."""
    at, now = stamp(observed_at), stamp(as_of)
    required = ('home_ht','away_ht','home_ft','away_ft')
    if not isinstance(rates,dict) or not all(finite(rates.get(k)) and rates[k]>=0 for k in required):
        return dict(available=False,reason='missing_native_statistical_rates')
    if not at or not now or at>now:
        return dict(available=False,reason='missing_or_future_statistics_timestamp')
    values = {k:rates[k] for k in (*required,'home_st','away_st') if finite(rates.get(k)) and rates[k]>=0}
    return dict(available=True,source='v7.statistics',observed_at=utc(at),rates=values,
                lambda_ht_total=values['home_ht']+values['away_ht'],
                lambda_ft_total=values['home_ft']+values['away_ft'],
                independence='Lambdas underlie statistical probabilities; not an additional independent vote')


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
            ev_recoveries=None,qualified_alternatives=(),statistical_rates=None,statistics_at=None):
    """Return private variants plus a compact same-snapshot cohort ledger."""
    from v7_decision import (GG,GG_FT,GOAL,RESULTS,TRIAD_LIMITS,TRIAD_MARKETS,
                             compatible,route,role_rank)
    from lateral_matrix import market_id,specification
    at,start=stamp(as_of),stamp(kickoff)
    if not at or not start or not at<start:
        return dict(protocol=PROTOCOL,shadow_only=True,available=False,reason='missing_live_prematch_context'),{}
    profiles=profiles or {}
    statistics = statistical_context(statistical_rates,statistics_at,as_of)
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
        if key in (GOAL['over15_ht'],GG_FT):
            available = same_book['available']
            initial = same_book.get('initial_favorite')
            current = same_book.get('current_favorite')
            row['over_gg_1x2_context'] = dict(available=available,
                same_operator=str(row['bookmaker_id'])==same_book.get('bookmaker_id') if available else None,
                initial_favorite=initial,current_favorite=current,
                current_favorite_odd=(same_book.get('current') or {}).get(current) if available else None,
                initial_favorite_delta_pct=(same_book.get('deltas') or {}).get(initial) if available else None,
                favorite_inversion=same_book.get('favorite_inversion') if available else None,
                interpretation='context association; same_operator distinguishes single-book confirmation')
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
    variants[GG_FT_SAME_RULES]=simulate(quoted,role_markets={
        **TRIAD_MARKETS,recommended:TRIAD_MARKETS[recommended]|{GG_FT}})
    variants['nov24_away_symmetric']=simulate(quoted,away_symmetric_shadow=True)
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
        statistical_context=statistics,
        gg_ft_same_rules=dict(variant=GG_FT_SAME_RULES,control=CONTROL,
            only_change='GG FT admitted to the recommended role; same candidate pool, limits and qualifications',
            production_selector_changed=False,lower_quote_floor=False),
        away_symmetric=dict(variant='nov24_away_symmetric',control=CONTROL,band=[4.50,7.50],
            only_change='2 FT high-odds admission accepts any direction inside the declared band',
            production_selector_changed=False),
        coverage=dict(matrix_rows=len(pool),usable_quoted_rows=len(rows),statistically_supported_quoted_rows=sum(eligible.values())),
        comparison_control=CONTROL,control_scope='same executable statistically supported pool; current routing, recovery certificates and Plus PT retained',
        probability_weights_changed=False,automatic_promotion=False,
        margin_protocol='EV unchanged; shadow ranking score subtracts an explicit extra prudence cost. Reference-margin return is a counterfactual, not causal attribution.',
        prospective_protocol='Evaluate only frozen ledgers; never reconstruct absent variants on historical snapshots.')
    ledger['evidence_sha256']=fingerprint(dict(captured_at=as_of,kickoff=kickoff,cohorts=rows,
                                              anchor_1x2=same_book,statistical_context=statistics))
    return ledger,variants
