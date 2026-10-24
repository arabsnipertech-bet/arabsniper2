"""Final display-only quote recovery, independent of statistical rebuilds."""
import argparse
import gzip
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from audit_storage import read_store, write_store
from external_odds import Recovery, MARKETS

def repair(root, recovery=None):
    root=Path(root); recovery=recovery or Recovery(); changed={}; counts={}
    now=datetime.now(timezone.utc).isoformat(); targets={'present':0,'priced':0,'complete_1x2':0}
    for i in range(1,6):
        path=root/'data'/f'arab_quote_day{i}.json'
        if not path.exists(): continue
        day=read_store(path); before=json.dumps(day,sort_keys=True)
        for match in day.get('matches',[]):
            recovery.apply(match,now)
            if match.get('fixture',{}).get('home') in ('Treviso','Sambenedettese'):
                targets['present']+=1
                targets['priced']+=int(any(r.get('market')=='over25_ft' and r.get('selection')=='over' and isinstance(r.get('odd'),(int,float)) and r['odd']>1 for r in match.get('multimarket',{}).get('markets',[])))
                targets['complete_1x2']+=int(all(any(r.get('market')=='1x2_ft' and r.get('selection')==side and isinstance(r.get('odd'),(int,float)) and r['odd']>1 for r in match.get('multimarket',{}).get('markets',[])) for side in ('1','x','2')))
        if json.dumps(day,sort_keys=True)!=before:
            day['external_quotes_checked_at']=now
            day['external_quote_markets']=list(MARKETS)
            write_store(path,day);changed[day['date']]=day
        counts[str(i)]={'matches':len(day.get('matches',[])), 'over25_present':sum(any(r.get('market')=='over25_ft' and r.get('selection')=='over' and isinstance(r.get('odd'),(int,float)) and r['odd']>1 for r in m.get('multimarket',{}).get('markets',[])) for m in day.get('matches',[]))}
    dashboard=root/'data/arab_quote_dashboard.json'
    if changed and dashboard.exists():
        data=read_store(dashboard)
        data.setdefault('method',{}).setdefault('external_odds',{})['markets']=list(MARKETS)
        data['days']=[changed.get(d.get('date'),d) for d in data.get('days',[])]
        write_store(dashboard,data)
    print('FINAL_QUOTE_RECOVERY='+json.dumps({'changed_days':len(changed),'requests':recovery.report['requests'],'errors':recovery.report['errors'],'markets_recovered':recovery.report['markets_recovered'],'coverage':counts,'reported_cases':targets}),flush=True)
    return changed

def remote():
    from gold_storage import client
    s3=client();bucket=os.environ['R2_BUCKET']
    original=s3.get_object(Bucket=bucket,Key='state/current.json')
    index=json.loads(original['Body'].read()); etag=original['ETag']
    if index.get('version')!=1:raise ValueError('Invalid checkpoint')
    with TemporaryDirectory() as folder:
        root=Path(folder)
        def restore(path):
            item=index['files'][path]; sha=item['sha256']
            if len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):raise ValueError('Invalid digest')
            raw=s3.get_object(Bucket=bucket,Key='state/objects/'+sha)['Body'].read()
            if len(raw)!=item['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('Checksum mismatch')
            p=root/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
            if p.suffix=='.json':
                d=json.loads(raw)
                if d.get('_storage')=='arabsniper-audit-parts-v1':
                    for part in d['parts']:restore(str(p.relative_to(root).parent/'_audit_parts'/(part['sha256']+'.json.gz')))
        for path in [*(f'data/arab_quote_day{i}.json' for i in range(1,6)),'data/arab_quote_dashboard.json']:restore(path)
        changed=repair(root)
        if not changed:return
        # Same workflow concurrency group as Fast/Night; detect any other writer too.
        if s3.head_object(Bucket=bucket,Key='state/current.json')['ETag']!=etag:raise ValueError('Checkpoint changed; retry safely')
        for p in (root/'data').rglob('*'):
            if not p.is_file():continue
            raw=p.read_bytes();sha=hashlib.sha256(raw).hexdigest();path=p.relative_to(root).as_posix()
            if index['files'].get(path,{}).get('sha256')==sha:continue
            s3.put_object(Bucket=bucket,Key='state/objects/'+sha,Body=raw,Metadata={'sha256':sha})
            index['files'][path]={'sha256':sha,'bytes':len(raw)}
        # Publish only affected Gold days; all forecasts and other checkpoint objects stay intact.
        for day in changed.values():
            raw=json.dumps(day,ensure_ascii=False,separators=(',',':')).encode()
            s3.put_object(Bucket=bucket,Key=f"arab_quote_day{day['day']}.json",Body=raw,ContentType='application/json',CacheControl='private, no-store')
        raw=json.dumps(index,sort_keys=True,separators=(',',':')).encode()
        s3.put_object(Bucket=bucket,Key='state/manifests/'+hashlib.sha256(raw).hexdigest()+'.json',Body=raw)
        s3.put_object(Bucket=bucket,Key='state/current.json',Body=raw,ContentType='application/json',IfMatch=etag)
        # Confirm server-side values, logging counts only.
        for day in changed.values():
            saved=json.loads(s3.get_object(Bucket=bucket,Key=f"arab_quote_day{day['day']}.json")['Body'].read())
            if saved!=day:raise ValueError('Published feed verification failed')
        print('FINAL_QUOTE_PUBLICATION_VERIFIED',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--remote',action='store_true');args=parser.parse_args()
    if args.remote:remote()
    else:repair(Path(__file__).resolve().parents[1])
