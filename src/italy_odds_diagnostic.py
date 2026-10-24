"""Read-only diagnosis of missing Italian prematch odds; no feed writes."""
import os,json,time
from datetime import datetime,timezone
import requests
from multimarket_model import collect_quotes
IDS=[1608717,1608718,1609100,1608720,1609099]
session=requests.Session()
session.headers['x-apisports-key']=os.environ['API_SPORTS_KEY']
calls=0
def get(path,params):
    global calls
    if calls>=500: raise RuntimeError('Diagnostic call cap reached')
    time.sleep(.2)
    calls+=1
    r=session.get('https://v3.football.api-sports.io/'+path,params=params,timeout=25)
    if r.status_code!=200: raise RuntimeError('Provider HTTP '+str(r.status_code))
    p=r.json()
    if p.get('errors'): raise RuntimeError('Provider returned errors; diagnostic stopped')
    rows=list(p.get('response') or [])
    total=1 if 'page' in params else p.get('paging',{}).get('total',1)
    if total>60: raise RuntimeError('Unexpected pagination')
    for page in range(2,total+1):
        rows.extend(get(path,{**params,'page':page})['response'])
    p['response']=rows
    return p
def inventory(p,fid):
    rows=[r for r in p['response'] if str(r.get('fixture',{}).get('id'))==str(fid)]
    now=datetime.now(timezone.utc).isoformat()
    usable=collect_quotes({'response':rows},now).get(str(fid),{})
    return {'raw_rows':len(rows),'raw_books':len({b['id'] for r in rows for b in r.get('bookmakers',[])}),
            'usable':{k:len(v) for k,v in usable.items()},
            'updates':sorted({str(r.get('update')) for r in rows}),
            'markets':sorted({(str(bet.get('id')),str(bet.get('name'))) for r in rows for b in r.get('bookmakers',[]) for bet in b.get('bets',[])})}
def main():
    q=get('status',{})['response']['requests']
    if q['limit_day']-q['current']<8000: raise RuntimeError('Insufficient quota reserve')
    books=get('odds/bookmakers',{})['response']
    print('DIAGNOSTIC_CATALOG '+json.dumps(books),flush=True)
    for fid in IDS:
        f=get('fixtures',{'id':fid})['response']
        if len(f)!=1: raise RuntimeError('Fixture identity unresolved')
        f=f[0];league=f['league'];start=f['fixture']['date']
        routes=[('fixture',{'fixture':fid}),('league_date',{'league':league['id'],'season':league['season'],'date':start[:10]})]
        routes += [('bet_'+str(b),{'fixture':fid,'bet':b}) for b in (1,5,6,34)]
        routes += [('book_'+str(b['id']),{'fixture':fid,'bookmaker':b['id']}) for b in books]
        results=[]
        for name,params in routes:
            results.append({'route':name,**inventory(get('odds',params),fid)})
        report={'fixture_id':fid,'match':f['teams']['home']['name']+' - '+f['teams']['away']['name'],'kickoff':start,'status':f['fixture']['status']['short'],'league':league['name'],'routes':results,'calls':calls}
        print('ITALY_ODDS_DIAGNOSTIC '+json.dumps(report),flush=True)
    print('DIAGNOSTIC_COMPLETE '+str(calls),flush=True)
if __name__=='__main__':main()
