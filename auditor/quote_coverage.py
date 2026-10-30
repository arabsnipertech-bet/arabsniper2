"""Read-only paired coverage audit. No private picks/raw odds in public output."""
from collections import Counter
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from gold_storage import client as storage_client, sha
from matrix_quotes import collect, partition
from quote_sources import fresh, number, TARGETS, synthetic_margin
from near_kickoff_quotes import NearKickoffRefresher

BASELINE = 'dcad78027cba3b79661e4f09e571ecb18bf8bc53'


def measurements(groups, now, kickoff):
    """Four exact Over settlements + Combo; equivalent Asian rows count once.

    Valid means fresh, complete pair, margin in [0,9%]. Synthetic Combo uses
    the two source margins. Gold's stricter 8%/other rules are not relaxed.
    """
    real, native, synthetic, pairs, books, seen, stale = set(), set(), set(), set(), set(), set(), set()
    aliases = set()
    for group in groups.values():
        for token, spec in group.get('specs', {}).items():
            combo = spec.get('kind') == 'halves_total' and spec.get('line') == .5 and spec.get('extra') == .5
            total = spec.get('kind') in ('total', 'asian_total') and (spec.get('period'),spec.get('line')) in TARGETS
            if spec.get('side') != 'over' or not (combo or total): continue
            key = 'combo' if combo else f'{spec["period"]}:{spec["line"]}'
            seen.add(key)
            if not fresh(group.get('provider_updated_at'),now,kickoff):
                stale.add(key);continue
            derived = group.get('quote_source') == 'synthetic_combo'
            if derived:
                if synthetic_margin(group) is None: continue
                synthetic.add(key)
            else:
                odds = group.get('odds') or {}
                if not group.get('complete') or any(number(v) is None or v <= 1 for v in odds.values()): continue
                margin = 100*(sum(1/v for v in odds.values())-1)
                if not 0 <= margin <= 9+1e-9: continue
                real.add(key)
                if spec['kind'] != 'asian_total': native.add(key)
                if group.get('quote_source') == 'asian_halfline': aliases.add(key)
            pairs.add((key,group['bookmaker_id']))
            books.add(group['bookmaker_id'])
    usable = real|synthetic
    return {'real':len(real), 'standard_rows':len(native), 'synthetic':len(synthetic-real),
            'usable':len(usable), 'valid_operator_markets':len(pairs),
            'asian_fallback_rows':len(aliases), 'direct_combo':int('combo' in real), 'stale':len(stale-usable),
            'missing':5-len(seen), 'invalid_or_stale':5-len(usable), 'books':sorted(books)}


def seed_saved_prices(matches):
    state={'matrix_quotes':{}}
    for match in matches:
        meta=match['fixture'];fixture={'kickoff':meta['kickoff'],'groups':{}}
        for row in match.get('multimarket',{}).get('matrix',{}).get('rows',[]):
            if number(row.get('first_odd')) is None or row['first_odd'] <= 1 or not row.get('spec') or not row.get('bet_id'): continue
            group, token, _ = partition(row['spec'])
            key=f'{row["bet_id"]}|{group}|{row.get("bookmaker_id")}';point=fixture['groups'].setdefault(key,[{
                'odds':{},'complete':False, 'provider_updated_at':row.get('provider_updated_at'),
                'observed_at':row.get('first_observed_at')}])[0]
            point['odds'][token]=row['first_odd']
        state['matrix_quotes'][str(meta['fixture_id'])]=fixture
    return state


def main():
    started=datetime.now(timezone.utc)
    date=os.environ.get('COVERAGE_DATE') or started.astimezone(ZoneInfo('Europe/Rome')).date().isoformat()
    s3=storage_client();bucket=os.environ['R2_BUCKET'];matches={};sources={}
    for index in range(1,6):
        name=f'arab_quote_day{index}.json';raw=s3.get_object(Bucket=bucket,Key=name)['Body'].read()
        day=json.loads(raw);sources[name]={'sha256':sha(raw),'date':day.get('date')}
        for match in day.get('matches',[]):
            meta=match['fixture'];kickoff=datetime.fromisoformat(meta['kickoff'].replace('Z','+00:00'))
            if kickoff>started and kickoff.astimezone(ZoneInfo('Europe/Rome')).date().isoformat()==date:
                matches[str(meta['fixture_id'])]=match
    ordered=sorted(matches.values(),key=lambda m:(m['fixture']['kickoff'],str(m['fixture']['fixture_id'])))
    spec=importlib.util.spec_from_file_location('coverage_api_client',ROOT/'src/3appdays.py')
    engine=importlib.util.module_from_spec(spec);sys.modules[spec.name]=engine;spec.loader.exec_module(engine)
    api=engine.APIClient(os.environ['API_SPORTS_KEY'])
    cap=400
    info,_=api.get_bounded('status',{},3)
    quota=(info or {}).get('response',{}).get('requests',{})
    used,limit=quota.get('current'),quota.get('limit_day')
    if type(used) is not int or type(limit) is not int or limit-used <= max(7500,limit*.1)+cap:
        raise RuntimeError('Insufficient verified API quota for coverage audit')
    catalog,_=api.get_bounded('odds/bets',{},3)
    halves_catalog=[b for b in (catalog or {}).get('response',[]) if 'halves' in str(b.get('name','')).lower()]
    state=seed_saved_prices(ordered);refresh=NearKickoffRefresher(api,state,40)
    baseline_root=Path(os.environ['BASELINE_ROOT']).resolve()
    worker="""import json,sys
sys.path.insert(0,sys.argv[1])
from matrix_quotes import collect
for line in sys.stdin:
 d=json.loads(line);groups,_=collect(d['payload'],d['fid'],d['now'])
 print(json.dumps(groups),flush=True)
"""
    baseline=subprocess.Popen([sys.executable,'-u','-c',worker,str(baseline_root/'src')],
                              stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,cwd=baseline_root)
    sums={name:Counter() for name in ('before','after_same_feed','after_refresh')}
    books={name:set() for name in sums};completed=errors=0
    direct_labels=Counter();direct_fixtures=set()
    baseline_groups_hashes=[]
    try:
        for match in ordered:
            if api.request_count >= cap-40: break
            meta=match['fixture'];fid=meta['fixture_id']
            payload,_=api.get_bounded('odds',{'fixture':int(fid)},cap-40-api.request_count)
            now=datetime.now(timezone.utc).isoformat()
            if not payload or not payload.get('collection',{}).get('complete'):
                errors+=1;continue
            for record in payload.get('response',[]):
                for book in record.get('bookmakers',[]):
                    for bet in book.get('bets',[]):
                        if bet.get('id') == 184:
                            direct_fixtures.add(str(fid))
                            direct_labels.update(str(v.get('value')) for v in bet.get('values',[]))
            baseline.stdin.write(json.dumps({'payload':payload,'fid':fid,'now':now})+'\n');baseline.stdin.flush()
            old=json.loads(baseline.stdout.readline())
            baseline_groups_hashes.append(sha(json.dumps(payload,sort_keys=True).encode()))
            new,_=collect(payload,fid,now)
            recovered=refresh.recover(payload,meta,now)
            latest,_=collect(recovered,fid,now)
            for name,groups in (('before',old),('after_same_feed',new),('after_refresh',latest)):
                data=measurements(groups,now,meta['kickoff']);books[name].update(data.pop('books'));sums[name].update(data)
            completed+=1
            if completed%20==0: print(f'Audited {completed} fixtures; HTTP calls {api.request_count}/{cap}',flush=True)
    finally:
        baseline.stdin.close();baseline.wait(timeout=30)
    report={'version':1,'date':date,'started_at':started.isoformat(),'completed_at':datetime.now(timezone.utc).isoformat(),
        'source_revision':os.environ.get('GITHUB_SHA'),'baseline_revision':BASELINE,'read_only':True,
        'scope':'Upcoming published Gold fixtures today, ordered by kickoff; five exact requested markets each',
        'eligible_fixtures':len(ordered),'audited_fixtures':completed,'api_errors':errors,
        'not_audited':len(ordered)-completed,'market_denominator':5*completed,
        'api_calls':api.request_count,'api_call_cap':cap,'near_kickoff_refresh':refresh.report,
        'halves_market_catalog':halves_catalog,
        'catalog_184_observed':{'fixtures':len(direct_fixtures),'outcome_labels':dict(direct_labels)},
        'sources':sources,'paired_snapshot_sha256':sha(json.dumps(baseline_groups_hashes).encode()),
        'definition':'Fresh complete same-book pairs with 0–9% overround; synthetic Combo validates each leg. No cached prices. Four equivalent Asian settlements deduplicated.',
        'stages':{name:{**dict(total),'unique_books':len(books[name]),
            'usable_pct':round(100*total['usable']/(5*completed),4) if completed else None,
            'real_pct':round(100*total['real']/(5*completed),4) if completed else None} for name,total in sums.items()}}
    destination=ROOT/'coverage-audit';destination.mkdir(exist_ok=True)
    (destination/'coverage.json').write_text(json.dumps(report,indent=2)+'\n')
    print('COVERAGE_REPORT '+json.dumps(report,sort_keys=True),flush=True)
    if not completed: raise RuntimeError('No complete fixtures measured')


if __name__=='__main__': main()
