"""Read-only historical combinations of native lambdas, P, EV and book margin."""
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from math import isfinite, log, sqrt
import os
from pathlib import Path
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from audit_storage import read_store
from gold_storage import client, previous_manifest, restore_subset
from lateral_matrix import market_id, specification, label, settle_spec
from multimarket_model import stamp
from v7_archive import ROME, checked_score
from v7_shadow_metrics import paired_metrics

START,END=date(2026,11,19),date(2026,11,25)
PROTOCOL='V7_FACTOR_COMBINATIONS_20261126_V1'
ROLE_NAMES=('most_probable','recommended_alternative','high_odds_shot')
DEFINITIONS=[
    ('total','ht','over',1.5,1.35,.40),('total','ht','under',1.5,1.35,.60),
    ('total','ht','over',2.5,1.50,.18),('btts','ht','yes',None,.50,.26),
    ('total','ft','over',2.5,2.50,.45),('total','ft','under',2.5,2.50,.55),
    ('total','ft','over',3.5,3.00,.32),('btts','ft','yes',None,1.00,.45),
    ('total','sh','over',1.5,1.35,.45),('total','sh','under',1.5,1.35,.55),
    ('total','sh','over',2.5,1.50,.16),('btts','sh','yes',None,.60,.32),
    ('result','ft','1',None,.25,.40),('result','ft','2',None,.25,.40),
    ('halves_total','ft','over',.5,1.20,.45),
]
SPECS={}
for kind,period,side,line,lam,p in DEFINITIONS:
    spec=specification(kind,period,side,line,**({'extra':.5} if kind=='halves_total' else {}))
    SPECS[market_id(spec)]=dict(spec=spec,name=label(spec),lambda_threshold=lam,p_threshold=p)
PROFILES=('base_lambda_registrata','lambda','probabilita','ev_nonnegativo','aggio_entro_6_5',
          'lambda_e_probabilita','probabilita_ev_aggio','lambda_probabilita_ev',
          'quattro_fattori','quattro_fattori_prudente','prudente_senza_picco',
          'lambda_ev','lambda_ev_aggio8','lambda_probabilita_ruolo_ev_aggio8',
          'quattro_fattori_quota150','prudente_quota150')


def number(x):return type(x) in (float,int) and isfinite(x)
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


class ReadOnly:
    def __init__(self,storage):self.storage,self.gets,self.bytes=storage,0,0
    def get_object(self,**args):
        key=args['Key']
        if args['Bucket']!='arabsniper-gold' or not (key=='state/current.json' or key.startswith('state/objects/')):
            raise ValueError('Read scope')
        self.gets+=1
        if self.gets>160:raise ValueError('Read budget')
        value=self.storage.get_object(**args);self.bytes+=value.get('ContentLength',0)
        if self.bytes>160*1024*1024:raise ValueError('Byte budget')
        return value


def rates_for(fixture,ledger,captured):
    """Use only recorded pre-market rates, never final V7 or inverted P."""
    origins=(fixture.get('measurement') or {}).get('probability_origins') or {}
    pre=origins.get('pre_market_statistics') or {}
    sources=[]
    if isinstance(pre.get('rates'),dict) and stamp(pre.get('observed_at')):
        sources.append((pre['rates'],pre['observed_at'],'v7.statistics'))
    native=ledger.get('statistical_context') or {}
    if native.get('available') is True and native.get('source')=='v7.statistics':
        sources.append((native.get('rates') or {},native.get('observed_at'),'v7.statistics'))
    valid=[]
    for rates,seen,origin in sources:
        observed=stamp(seen)
        if observed and observed<=captured and all(number(rates.get(k)) and rates[k]>=0
                for k in ('home_ht','away_ht','home_ft','away_ft')):
            valid.append((rates,seen,origin))
    if not valid:return {},None,'not_recorded'
    rates,seen,origin=valid[0]
    if any(any(number(other.get(k)) and abs(other[k]-v)>1e-8 for k,v in rates.items() if number(v))
           for other,_,_ in valid[1:]):return {},None,'conflicting_recorded_origins'
    return rates,seen,origin


def intensity(rates,spec):
    def pair(period):
        names={'ht':('home_ht','away_ht'),'sh':('home_st','away_st'),'ft':('home_ft','away_ft')}[period]
        a,b=(rates.get(k) for k in names)
        return (a,b) if number(a) and number(b) else (None,None)
    kind,period,side=spec['kind'],spec['period'],spec['side']
    h,a=pair(period)
    if kind=='halves_total':
        ht,at=pair('ht');hs,ass=pair('sh')
        return min(ht+at,hs+ass) if all(number(v) for v in (ht,at,hs,ass)) else None
    if not number(h) or not number(a):return None
    if kind=='btts':return min(h,a)
    if kind=='result':return h-a if side=='1' else a-h
    return h+a


def selected_at_snapshot(fixture,latest,market,row,captured):
    published=(latest.get('variants') or {}).get('published') or {}
    same=[]
    for role in ROLE_NAMES:
        pick=published.get(role) or {}
        if (pick.get('market_id')==market and number(pick.get('probability')) and
                abs(pick['probability']-row['probability'])<=1e-8 and
                number(pick.get('odd')) and abs(pick['odd']-row['odd'])<=1e-8):same.append(role)
    strict=[]
    for role,pick in (fixture.get('triad') or {}).items():
        if not pick or pick.get('market_id')!=market:continue
        if (stamp(pick.get('captured_at'))==captured and
                pick.get('forecast_sha256')==fixture.get('forecast_sha256') and
                number(pick.get('probability')) and abs(pick['probability']-row['probability'])<=1e-8 and
                number(pick.get('odd')) and abs(pick['odd']-row['odd'])<=1e-8):strict.append(role)
    return same,strict


def collect(archives,cutoff):
    rows=[];coverage={};rejected=Counter();starts=defaultdict(set)
    for day,archive in archives.items():
        for fid,f in archive.get('fixtures',{}).items():
            if stamp(f.get('kickoff')):starts[fid].add(stamp(f['kickoff']).isoformat())
    conflicts={fid for fid,v in starts.items() if len(v)>1}
    for day,archive in archives.items():
        count=Counter(fixtures=len(archive.get('fixtures',{})))
        for fid,f in archive.get('fixtures',{}).items():
            if fid in conflicts:count['kickoff_conflict']+=1;continue
            at,start,seen=(stamp(f.get(k)) for k in ('captured_at','kickoff','observed_at'))
            if not at or not start or not seen or not at<start<seen<=cutoff or str(start.astimezone(ROME).date())!=day:
                count['invalid_temporal_identity']+=1;continue
            latest=(f.get('decision_shadow') or {}).get('latest') or {}
            ledger=latest.get('movement_study') or {}
            if (ledger.get('available') is not True or stamp(ledger.get('captured_at'))!=at
                    or stamp(ledger.get('kickoff'))!=start or latest.get('forecast_sha256')!=f.get('forecast_sha256')):
                count['no_comparable_frozen_ledger']+=1;continue
            rates,rate_at,origin=rates_for(f,ledger,at)
            count['native_lambda_recorded' if rates else 'native_lambda_not_recorded']+=1
            for row in ledger.get('cohorts',[]):
                key=row.get('market_id')
                if key not in SPECS:continue
                spec=SPECS[key]['spec'];p,q=row.get('probability'),row.get('odd')
                if row.get('supported') is not True or row.get('quote_usable') is not True:
                    rejected['unsupported_or_unexecutable']+=1;continue
                if not number(p) or not 0<=p<=1 or not number(q) or q<=1:
                    rejected['invalid_p_or_odd']+=1;continue
                if any(stamp(row.get(k)) and stamp(row[k])>at for k in ('quote_observed_at','provider_updated_at')):
                    rejected['future_quote']+=1;continue
                calculated_ev=p*q-1
                if not number(row.get('ev')) or abs(calculated_ev-row['ev'])>max(1,q)*1e-7:
                    rejected['inconsistent_ev']+=1;continue
                ev=row['ev']
                margin,fair=row.get('margin_pct'),row.get('fair')
                identity=number(margin) and margin>=0 and number(fair) and 0<fair<1 and abs(fair-1/q/(1+margin/100))<1e-7
                weights=settle_spec(spec,checked_score(f.get('result') or {})) if f.get('status')=='settled' else None
                y=int(weights[0]) if weights is not None and sum(weights)==1 and weights[0] in (0,1) else None
                roles,strict=selected_at_snapshot(f,latest,key,row,at)
                rows.append(dict(fixture_id=fid,date=day,config=ledger.get('config_sha256'),market=key,
                    market_name=SPECS[key]['name'],probability=p,odd=q,ev=ev,ev_low=row.get('ev_low'),
                    margin=margin,fair=fair if identity else None,price_identity=bool(identity),
                    intensity=intensity(rates,spec),lambda_origin=origin,statistics_at=rate_at,
                    statistical_probability=row.get('statistics'),without_peak=row.get('without_peak_supported'),
                    bookmaker=row.get('bookmaker_id'),quote_observed_at=row.get('quote_observed_at'),
                    provider_updated_at=row.get('provider_updated_at'),
                    selected=bool(roles),selected_roles=roles,strict_fixed=bool(strict),
                    forecast_sha256=f.get('forecast_sha256'),captured_at=f['captured_at'],outcome=y,
                    roi=q*y-1 if y is not None else None))
                count['candidate_rows']+=1
        coverage[day]=dict(count)
    return rows,dict(coverage=coverage,rejected=dict(rejected),kickoff_conflicting_ids=len(conflicts))


def complementary_pairs(rows):
    groups=defaultdict(dict)
    for r in rows:
        spec=SPECS[r['market']]['spec']
        if spec['kind']=='total' and (spec['period'],spec.get('line')) in (('ht',1.5),('sh',1.5),('ft',2.5)):
            groups[r['config'],r['fixture_id'],r['date'],spec['period'],spec['line']][spec['side']]=r
    cells=defaultdict(list);events=defaultdict(list);excluded=Counter()
    for (config,fid,day,period,line),sides in groups.items():
        if set(sides)!={'over','under'}:excluded['one_price_missing']+=1;continue
        over,under=sides['over'],sides['under']
        if abs(over['probability']+under['probability']-1)>1e-6:
            excluded['noncomplementary_probability']+=1;continue
        if over['outcome'] is not None and under['outcome'] is not None and over['outcome']+under['outcome']!=1:
            raise ValueError('Inconsistent complementary outcomes')
        category=('both_nonnegative' if over['ev']>=0 and under['ev']>=0 else
                  'over_only_nonnegative' if over['ev']>=0 else
                  'under_only_nonnegative' if under['ev']>=0 else 'both_negative')
        events[config,period,line,category].append((over,under))
        if over['bookmaker'] is None or str(over['bookmaker'])!=str(under['bookmaker']):
            excluded['different_or_missing_operator']+=1;continue
        times=[stamp(r['quote_observed_at']) for r in (over,under)]
        if not all(times) or abs((times[0]-times[1]).total_seconds())>60:
            excluded['unaligned_quotes']+=1;continue
        if not all(r['price_identity'] for r in (over,under)) or abs(over['fair']+under['fair']-1)>1e-6:
            excluded['not_same_complete_partition']+=1;continue
        if abs(over['probability']+under['probability']-1)>1e-6:
            excluded['noncomplementary_probability']+=1;continue
        if category=='both_nonnegative' and over['ev']>1e-6 and under['ev']>1e-6:
            raise ValueError('Inconsistent simultaneous positive EV on complete binary partition')
        cells[config,period,line,category].append((over,under))
    reports=[]
    for (config,period,line,category),pairs in sorted(cells.items()):
        selected_over=[o for o,u in pairs if o['selected'] and not u['selected']]
        selected_under=[u for o,u in pairs if u['selected'] and not o['selected']]
        alternatives_under=[u for o,u in pairs if o['selected'] and not u['selected']]
        reports.append(dict(config=config,period=period,line=line,value_category=category,
            pairs=len(pairs),over=metrics([o for o,u in pairs]),under=metrics([u for o,u in pairs]),
            selected_over=metrics(selected_over),selected_under=metrics(selected_under),
            alternative_under_when_over_selected=metrics(alternatives_under),selection_rule_changed=False))
    summaries=[]
    for (config,period,line,category),pairs in sorted(events.items()):
        summaries.append(dict(config=config,period=period,line=line,value_category=category,pairs=len(pairs),
            over=metrics([o for o,u in pairs]),under=metrics([u for o,u in pairs]),
            selected_over=metrics([o for o,u in pairs if o['selected'] and not u['selected']]),
            alternative_under_when_over_selected=metrics([u for o,u in pairs if o['selected'] and not u['selected']]),
            operator_aligned_pairs=sum(o['bookmaker'] is not None and str(o['bookmaker'])==str(u['bookmaker']) for o,u in pairs),
            interpretation='Complementary events at one frozen snapshot. Different operators may supply the executable quotes; this does not isolate the effect of margin.'))
    return dict(cells=reports,event_complements=summaries,excluded=dict(excluded),tolerance_seconds=60)


def passes(row,profile):
    info=SPECS[row['market']];value=row['intensity'];threshold=info['lambda_threshold']
    if not number(value):return None
    spec=info['spec'];lam=value<=threshold if spec['side']=='under' else value>=threshold
    p=row['probability']>=info['p_threshold'];ev=row['ev']>=0
    m=row['price_identity'] and number(row['margin']) and 0<=row['margin']<=6.5
    prudent=(row['price_identity'] and number(row['margin']) and 0<=row['margin']<=8 and
             row['ev']>0 and number(row['ev_low']) and row['ev_low']>=.02)
    m8=row['price_identity'] and number(row['margin']) and 0<=row['margin']<=8
    role_p=(.32 if spec['kind']=='btts' and spec['period'] in ('ft','sh') or
            spec['kind']=='total' and spec['period']=='ht' and spec.get('line')==1.5 and spec['side']=='over' else
            .16 if (spec['kind']=='btts' and spec['period']=='ht') or
                       (spec['kind']=='total' and spec.get('line')==2.5 and spec['period'] in ('ht','sh')) else
            info['p_threshold'])
    if profile=='prudente_senza_picco' and spec['period']=='sh':return None
    return {'base_lambda_registrata':True,'lambda':lam,'probabilita':p,'ev_nonnegativo':ev,
        'aggio_entro_6_5':m,'lambda_e_probabilita':lam and p,'probabilita_ev_aggio':p and ev and m,
        'lambda_probabilita_ev':lam and p and ev,'quattro_fattori':lam and p and ev and m,
        'quattro_fattori_prudente':lam and p and prudent,
        'prudente_senza_picco':lam and p and prudent and row['without_peak'] is True,
        'lambda_ev':lam and ev,'lambda_ev_aggio8':lam and ev and m8,
        'lambda_probabilita_ruolo_ev_aggio8':lam and row['probability']>=role_p and ev and m8,
        'quattro_fattori_quota150':lam and p and ev and m and row['odd']>=1.5,
        'prudente_quota150':lam and p and prudent and row['odd']>=1.5}[profile]


def wilson(w,n):
    if not n:return None
    z=1.959963984540054;p=w/n;den=1+z*z/n
    center=(p+z*z/(2*n))/den;half=z*sqrt(p*(1-p)/n+z*z/(4*n*n))/den
    return [max(0,center-half),min(1,center+half)]


def metrics(rows,bootstrap=False):
    settled=[r for r in rows if r['outcome'] in (0,1)];n=len(settled);wins=sum(r['outcome'] for r in settled)
    def mean(field):
        values=[r[field] for r in settled if number(r.get(field))]
        return sum(values)/len(values) if values else None
    result=dict(rows=len(rows),n=n,fixtures=len({r['fixture_id'] for r in settled}),
        days=len({r['date'] for r in settled}),wins=wins,hit=wins/n if n else None,hit_ci95=wilson(wins,n),
        pending=len(rows)-n,mean_p=mean('probability'),mean_odd=mean('odd'),mean_ev=mean('ev'),
        mean_margin=mean('margin'),mean_lambda=mean('intensity'),
        selected_n=sum(r['selected'] for r in settled),selected_wins=sum(r['outcome'] for r in settled if r['selected']),
        strict_fixed_n=sum(r['strict_fixed'] for r in settled),strict_fixed_wins=sum(r['outcome'] for r in settled if r['strict_fixed']),
        roi_control=mean('roi'))
    result['excess_hit_vs_p']=result['hit']-result['mean_p'] if n else None
    selected=[r for r in settled if r['selected']]
    for name in ('probability','odd','ev','intensity'):
        values=[r[name] for r in selected if number(r.get(name))]
        result['selected_mean_'+name]=sum(values)/len(values) if values else None
    if bootstrap:
        result['paired_vs_fair']=paired_metrics(settled,draws=4000)
        result['selected_paired_vs_fair']=paired_metrics(selected,draws=4000)
    return result


def band(value,edges):
    if not number(value):return 'missing'
    for low,high in zip(edges,edges[1:]):
        if low<=value<high:return f'{low:g}..{high:g}'
    return 'outside'


def analyze(rows,cutoff):
    groups=defaultdict(list)
    for row in rows:groups[row['config'],row['market']].append(row)
    cells=[];marginals=[];baselines=[];walkforward=[]
    for (config,market),all_rows in sorted(groups.items()):
        info=SPECS[market];native=[r for r in all_rows if number(r['intensity'])]
        base=dict(config=config,market=info['name'])
        baselines.append(dict(base,**metrics(all_rows),native_lambda_rows=len(native),
                             native_lambda_settled=sum(r['outcome'] is not None for r in native)))
        dates=sorted({r['date'] for r in native})
        scopes={'all':native,'train_to_24':[r for r in native if r['date']<'2026-11-25'],
                'control_25':[r for r in native if r['date']=='2026-11-25']}
        for scope,data in scopes.items():
            for profile in PROFILES:
                filtered=[r for r in data if passes(r,profile) is True]
                boot=(scope in ('all','control_25') and profile in ('base_lambda_registrata','probabilita_ev_aggio','quattro_fattori','quattro_fattori_prudente'))
                cells.append(dict(base,scope=scope,profile=profile,
                    applicable=not(profile=='prudente_senza_picco' and info['spec']['period']=='sh'),**metrics(filtered,boot)))
        if len(dates)>=2:
            for later in dates[1:]:
                prior=[r for r in native if r['date']<later];future=[r for r in native if r['date']==later]
                for profile in ('base_lambda_registrata','probabilita_ev_aggio','quattro_fattori','quattro_fattori_prudente'):
                    walkforward.append(dict(base,profile=profile,training_dates=sorted({r['date'] for r in prior}),
                        test_date=later,training=metrics([r for r in prior if passes(r,profile)]),
                        test=metrics([r for r in future if passes(r,profile)])))
        edges={'intensity':[-1e9,info['lambda_threshold']-.25,info['lambda_threshold'],info['lambda_threshold']+.25,1e9],
               'probability':[0,max(0,info['p_threshold']-.05),info['p_threshold'],min(1,info['p_threshold']+.1),1.00000001],
               'ev':[-1e9,-.15,0,.1,1e9],'margin':[-1e9,0,4,6.5,8,1e9],
               'odd':[1,1.5,1.7,2.1,2.6,3.5,4.5,5.5,7.5,1e9]}
        for feature,bounds in edges.items():
            buckets=defaultdict(list)
            for row in all_rows:buckets[band(row.get(feature),bounds)].append(row)
            for bucket,data in buckets.items():marginals.append(dict(base,feature=feature,band=bucket,**metrics(data)))
    analogs=[]
    for config in sorted({r['config'] for r in rows}):
        definitions={
          'over_pt_simile_Heart_Brann':lambda r:SPECS[r['market']]['spec']==specification('total','ht','over',1.5) and
              number(r['intensity']) and 1.0<=r['intensity']<=1.4 and .35<=r['probability']<=.45 and -.15<=r['ev']<0 and
              r['price_identity'] and 0<=r['margin']<=6.5,
          'gg_pt_simile_Brighton':lambda r:SPECS[r['market']]['spec']==specification('btts','ht','yes') and
              number(r['intensity']) and .5<=r['intensity']<=.7 and .18<=r['probability']<=.28 and r['ev']>0 and
              r['price_identity'] and 0<=r['margin']<=8,
          'under_st_simile_Brighton':lambda r:SPECS[r['market']]['spec']==specification('total','sh','under',1.5) and
              number(r['intensity']) and 1.1<=r['intensity']<=1.4 and .43<=r['probability']<=.53 and 0<=r['ev']<=.15 and
              r['price_identity'] and 0<=r['margin']<=8}
        for name,test in definitions.items():analogs.append(dict(config=config,example=name,**metrics([r for r in rows if r['config']==config and test(r)])))
    return dict(protocol=PROTOCOL,cutoff=cutoff.isoformat(),definitions={key:value for key,value in SPECS.items()},
        profiles=list(PROFILES),baselines=baselines,cells=cells,marginals=marginals,walkforward=walkforward,analogs=analogs,
        complementary= complementary_pairs(rows),
        methodology=dict(historical_chronological_check=True,prospective_validation=False,
            configurations_never_pooled_for_inference=True,lambda_source='recorded v7.statistics before market',
            lambda_not_inverted_from_P=True,original_choices_not_backfilled=True,ROI_selects_rules=False,
            future_fixtures_excluded=True,production_changes=False,automatic_promotion=False,
            note='Fixed hypotheses, not a search for the best hit rate. Wilson intervals assume independent fixtures; paired metrics also cluster by date.'))


def main():
    cutoff=datetime.now(timezone.utc)
    if END>=cutoff.astimezone(ROME).date():raise ValueError('Closed dates only')
    if os.environ.get('API_SPORTS_KEY','').strip():raise ValueError('Provider key forbidden')
    storage=ReadOnly(client(fast_restore=True));bucket=os.environ['R2_BUCKET']
    index=previous_manifest(storage,bucket)
    dates=[str(START+timedelta(days=i)) for i in range((END-START).days+1)]
    paths=[f'auditarchive/prematch_{day}_v7.json' for day in dates]
    if not all(p in index for p in paths):raise ValueError('Missing closed archive')
    with tempfile.TemporaryDirectory(prefix='v7-factor-study-') as folder:
        root=Path(folder);restore_subset(storage,bucket,root,index,paths,fast_restore=True)
        archives={day:read_store(root/path,{}) for day,path in zip(dates,paths)}
        rows,coverage=collect(archives,cutoff)
        report=analyze(rows,cutoff);report['coverage']=coverage
        report['source']=dict(files={p:index[p] for p in paths},frame_sha256=digest(rows),
            remote_writes=0,provider_calls=0,model_fits=0,social_sends=0,rows=len(rows),
            gets=storage.gets,bytes=storage.bytes)
        output=ROOT/'test-artifacts/factor-combinations-review.json';output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False))
        print(json.dumps(dict(status='complete',rows=len(rows),native_lambda_rows=sum(number(r['intensity']) for r in rows),
            cells=len(report['cells']),coverage=coverage,remote_writes=0,provider_calls=0,model_fits=0)),flush=True)


if __name__=='__main__':main()
