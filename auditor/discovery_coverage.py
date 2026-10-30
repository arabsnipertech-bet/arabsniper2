"""Read-only paired audit of today's/tomorrow's official whitelist.

Private fixture payloads remain on the ephemeral runner; only aggregates leave it.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'auditor'))
from gold_storage import client as storage_client, sha
from gold_competitions import fixture_allowed
from matrix_quotes import collect
from market_discovery import identify, VERSION
from quote_sources import fresh, number, synthetic_margin, TARGETS
from near_kickoff_quotes import NearKickoffRefresher
from odds_collection import OddsEnricher
from quote_coverage import seed_saved_prices
BASELINE='4bcff1b1c2cba61f55f0f732e9c08678978aae76'
MARKETS=('ht:1.5','ht:2.5','ft:2.5','ft:3.5','gg_ht','combo')
STAGES=('before','discovery_same_feed','after_acquisition','after_refresh')


def measurements(groups,now,kickoff):
    real=set();synthetic=set();pairs=set();books=set();seen=set();stale=set();aliases=set()
    for group in groups.values():
        for token,spec in group.get('specs',{}).items():
            kind=spec.get('kind');side=spec.get('side')
            combo=kind=='halves_total' and spec.get('line')==.5 and spec.get('extra')==.5 and side=='over'
            total=kind in ('total','asian_total') and (spec.get('period'),spec.get('line')) in TARGETS and side=='over'
            gg=kind=='btts' and spec.get('period')=='ht' and side=='yes'
            if not (combo or total or gg): continue
            key='combo' if combo else 'gg_ht' if gg else f'{spec["period"]}:{spec["line"]}'
            seen.add(key)
            if not fresh(group.get('provider_updated_at'),now,kickoff): stale.add(key);continue
            if group.get('quote_source')=='synthetic_combo':
                if synthetic_margin(group) is None: continue
                synthetic.add(key)
            else:
                odds=group.get('odds') or {}
                if not group.get('complete') or not odds or any(number(v) is None or v<=1 for v in odds.values()): continue
                margin=100*(sum(1/v for v in odds.values())-1)
                if not 0<=margin<=9+1e-9: continue
                real.add(key)
                if group.get('quote_source')=='asian_halfline': aliases.add(key)
            books.add(group['bookmaker_id']);pairs.add((key,group['bookmaker_id']))
    valid=real|synthetic
    return {'real':len(real),'synthetic_only':len(synthetic-real),'usable':len(valid),
        'missing':6-len(seen),'stale_without_valid_alternative':len(stale-valid),
        'invalid_or_stale':6-len(valid),'operator_markets':len(pairs),'asian_fallback_rows':len(aliases),
        'markets_with_two_books':sum(sum(m==key for m,b in pairs)>=2 for key in MARKETS),
        'by_market':{key:int(key in valid) for key in MARKETS},'books':books}


def main():
    started=datetime.now(timezone.utc);rome=ZoneInfo('Europe/Rome')
    dates=[(started.astimezone(rome).date()+timedelta(days=i)).isoformat() for i in range(2)]
    spec=importlib.util.spec_from_file_location('discovery_audit_client',ROOT/'src/3appdays.py')
    engine=importlib.util.module_from_spec(spec);sys.modules[spec.name]=engine;spec.loader.exec_module(engine)
    api=engine.APIClient(os.environ['API_SPORTS_KEY']);cap=1600
    api._http_remaining=cap  # Global ceiling also covers retries/catalogue/status calls.
    info,_=api.get_bounded('status',{},3);quota=(info or {}).get('response',{}).get('requests',{})
    used,limit=quota.get('current'),quota.get('limit_day')
    if type(used) is not int or type(limit) is not int or limit-used<=max(7500,limit*.1)+cap:
        raise RuntimeError('Insufficient verified quota for bounded audit')
    saved={};sources={};s3=storage_client()
    for i in range(1,6):
        name=f'arab_quote_day{i}.json';raw=s3.get_object(Bucket=os.environ['R2_BUCKET'],Key=name)['Body'].read()
        day=json.loads(raw);sources[name]={'date':day.get('date'),'sha256':sha(raw)}
        for m in day.get('matches',[]): saved[str(m['fixture']['fixture_id'])]=m
    catalog,_=api.get_bounded('odds/bets',{},3)
    books,_=api.get_bounded('odds/bookmakers',{},3)
    state={};enricher=OddsEnricher(api,state,max_calls=240,max_seconds=180,market_catalog=(catalog or {}).get('response',[]))
    enricher.catalog=(books or {}).get('response',[])
    refresh=NearKickoffRefresher(api,seed_saved_prices(list(saved.values())),40)
    schedule={};fixtures=[]
    for date in dates:
        response,_=api.get_bounded('fixtures',{'date':date,'timezone':'Europe/Rome'},3)
        if response is None or response.get('errors'): raise RuntimeError('Official schedule unavailable: '+date)
        allowed=[r for r in response.get('response',[]) if fixture_allowed(r)]
        upcoming=[r for r in allowed if engine.fixture_meta(r).get('status_short') in ('NS','TBD') and
                  datetime.fromisoformat(r['fixture']['date'].replace('Z','+00:00'))>started]
        schedule[date]={'whitelist_all_statuses':len(allowed),'upcoming':len(upcoming),
            'published_upcoming':sum(str(r['fixture']['id']) in saved for r in upcoming),
            'not_yet_published_upcoming':sum(str(r['fixture']['id']) not in saved for r in upcoming)}
        fixtures.extend((date,engine.fixture_meta(r)) for r in upcoming)
    fixtures.sort(key=lambda item:(item[1]['kickoff'],str(item[1]['fixture_id'])))
    worker="""import json,sys
sys.path.insert(0,sys.argv[1])
from matrix_quotes import collect
for line in sys.stdin:
 d=json.loads(line);g,_=collect(d['payload'],d['fid'],d['now']);print(json.dumps(g),flush=True)
"""
    baseline_root=Path(os.environ['BASELINE_ROOT']).resolve()
    worker=subprocess.Popen([sys.executable,'-u','-c',worker,str(baseline_root/'src')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,cwd=baseline_root)
    rows=[];errors=Counter();hashes=[];recognized=Counter();rejected=Counter()
    try:
        for date,meta in fixtures:
            if api.request_count>=cap-280: break
            fid=meta['fixture_id'];payload,_=api.get_bounded('odds',{'fixture':int(fid)},cap-280-api.request_count)
            now=datetime.now(timezone.utc).isoformat()
            if not payload or not payload.get('collection',{}).get('complete'):
                errors[date]+=1;continue
            hashes.append(sha(json.dumps(payload,sort_keys=True).encode()))
            worker.stdin.write(json.dumps({'payload':payload,'fid':fid,'now':now})+'\n');worker.stdin.flush()
            old=json.loads(worker.stdout.readline());new,diagnostics=collect(payload,fid,now)
            for rec in payload.get('response',[]):
                for book in rec.get('bookmakers',[]):
                    for bet in book.get('bets',[]):
                        key=str(bet.get('id'))+':'+str(bet.get('name'))
                        (recognized if identify(bet.get('name')) else rejected)[key]+=1
            results={'before':measurements(old,now,meta['kickoff']),'discovery_same_feed':measurements(new,now,meta['kickoff'])}
            rows.append({'date':date,'meta':meta,'payload':payload,'now':now,'results':results})
            if len(rows)%40==0: print(f'Paired snapshots: {len(rows)}/{len(fixtures)}; HTTP {api.request_count}/{cap}',flush=True)
        # Allocate the bounded extra probes fairly across both dates, poorest first.
        pending={d:sorted([r for r in rows if r['date']==d],key=lambda r:(r['results']['discovery_same_feed']['usable'],r['meta']['kickoff'])) for d in dates}
        for index in range(max((len(v) for v in pending.values()),default=0)):
            for date in dates:
                if index>=len(pending[date]): continue
                row=pending[date][index];meta=row['meta'];now=row['now'];fid=meta['fixture_id']
                enricher.register_fixture(meta)
                candidate=enricher.enrich(row['payload'],fid) if api.request_count<cap-40 else row['payload']
                current=datetime.now(timezone.utc).isoformat()
                latest=refresh.recover(candidate,meta,current) if api.request_count<cap else candidate
                # Identical clock for every stage of this fixture. Otherwise future
                # provider timestamps becoming current can masquerade as recovery.
                current=datetime.now(timezone.utc).isoformat()
                worker.stdin.write(json.dumps({'payload':row['payload'],'fid':fid,'now':current})+'\n');worker.stdin.flush()
                old=json.loads(worker.stdout.readline())
                original,_=collect(row['payload'],fid,current)
                acquired,_=collect(candidate,fid,current)
                recovered,_=collect(latest,fid,current)
                row['results']={stage:measurements(groups,current,meta['kickoff']) for stage,groups in
                    (('before',old),('discovery_same_feed',original),('after_acquisition',acquired),('after_refresh',recovered))}
    finally:
        worker.stdin.close();worker.wait(timeout=30)
    per_day={}
    for date in dates:
        selected=[r for r in rows if r['date']==date];stages={}
        for stage in STAGES:
            totals=Counter();by_market=Counter();operators=set()
            for r in selected:
                item=dict(r['results'][stage]);operators.update(item.pop('books'));by_market.update(item.pop('by_market'));totals.update(item)
            denom=6*len(selected)
            stages[stage]={**dict(totals),'by_market':dict(by_market),'unique_books':len(operators),
                'usable_pct':round(100*totals['usable']/denom,4) if denom else None,
                'real_pct':round(100*totals['real']/denom,4) if denom else None}
        per_day[date]={**schedule[date],'audited_fixtures':len(selected),'api_errors':errors[date],
            'not_audited':schedule[date]['upcoming']-len(selected),'market_denominator':6*len(selected),'stages':stages}
    report={'version':VERSION,'evaluation_clock':'Identical per-fixture timestamp for all four stages, after both recovery attempts','baseline_revision':BASELINE,'source_revision':os.environ.get('GITHUB_SHA'),
        'read_only':True,'started_at':started.isoformat(),'completed_at':datetime.now(timezone.utc).isoformat(),
        'scope':'Official whitelisted NS/TBD upcoming fixtures today and tomorrow in Europe/Rome; started matches excluded from prematch coverage',
        'definition':'Six exact requested goal settlements. Fresh whole same-book pairs, overround 0–9%; synthetic Combo validates both legs separately. No cached prices; Asian equivalents deduplicated. Gold/Platinum/Radar eligibility is not quote coverage.',
        'days':per_day,'api_calls':api.request_count,'api_call_cap':cap,'enrichment':enricher.stats,
        'near_kickoff_refresh':refresh.report,'catalog_bookmakers':len(enricher.catalog),
        'recognized_market_labels':dict(recognized),'rejected_market_labels':dict(rejected),
        'sources':sources,'paired_snapshot_sha256':sha(json.dumps(hashes).encode())}
    dest=ROOT/'coverage-audit';dest.mkdir(exist_ok=True)
    (dest/'discovery.json').write_text(json.dumps(report,indent=2)+'\n')
    print('DISCOVERY_REPORT '+json.dumps(report,sort_keys=True),flush=True)
    if not rows: raise RuntimeError('No complete paired fixtures measured')

if __name__=='__main__': main()
