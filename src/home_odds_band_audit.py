"""Read-only hypothesis check: home win odds 1.58–1.75, frozen before kickoff."""
import gzip
import hashlib
import json
import math
import os
from collections import defaultdict
from datetime import datetime
from pathlib import PurePosixPath

TARGETS = ('over05_ht', 'over15_ht', 'gg_ht', 'over25_ft')

def stamp(value):
    try:
        d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return d if d.tzinfo else None
    except ValueError:
        return None

def valid(v):
    return type(v) in (int, float) and math.isfinite(v) and v > 1

def outcome(market, result):
    if result.get('status') not in ('FT','AET','PEN'): return None
    fields = ('home_ht','away_ht') if market.endswith('ht') else ('home_ft','away_ft')
    scores = [result.get(k) for k in fields]
    if any(type(x) not in (int,float) or x<0 for x in scores): return None
    if market == 'gg_ht': return all(x>0 for x in scores)
    return sum(scores) >= {'over05_ht':1,'over15_ht':2,'over25_ft':3}[market]

def metrics(rows):
    settled = [(y,o) for y,o in rows if type(y) is bool]
    n=len(settled);w=sum(y for y,o in settled)
    out={'total':len(rows),'settled':n,'wins':w,'pending':len(rows)-n,'hit_rate_pct':round(100*w/n,2) if n else None}
    if n:
        p=w/n;z=1.96;d=1+z*z/n;c=(p+z*z/(2*n))/d;e=z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/d
        out['wilson95_pct']=[round((c-e)*100,2),round((c+e)*100,2)]
    priced=[(y,o) for y,o in settled if valid(o)]
    out['priced']=len(priced)
    out['paper_roi_pct']=round(100*sum((o if y else 0)-1 for y,o in priced)/len(priced),2) if priced else None
    return out

def analyze(store):
    snapshots={'first':{},'latest':{}};skipped=defaultdict(int)
    for collection in ('multimarket_evaluations','analysis_updates'):
        for record in store.get(collection,{}).values():
            f=record.get('fixture',{});a=record.get('multimarket',{})
            t,k=stamp(record.get('captured_at')),stamp(f.get('kickoff'))
            if record.get('retrospective') or a.get('model_id')!='context-goals-v3.4' or not t or not k or t>=k:
                skipped['invalid_or_other_model']+=1;continue
            if k.date().isoformat()<'2026-10-20':continue
            fid=str(f.get('fixture_id'))
            for policy in ('first','latest'):
                old=snapshots[policy].get(fid)
                if old is None or (t<stamp(old['captured_at']) if policy=='first' else t>stamp(old['captured_at'])):
                    snapshots[policy][fid]=record
    groups=defaultdict(list);dates=set()
    for policy,records in snapshots.items():
        for record in records.values():
            a=record['multimarket'];f=record['fixture'];day=stamp(f['kickoff']).date().isoformat()
            rows={(m.get('market'),m.get('selection')):m for m in a.get('markets',[])}
            home=rows.get(('1x2_ft','1'),{});odd=home.get('odd')
            if not valid(odd) or (home.get('quote') or {}).get('indicative'):
                skipped[policy+'_missing_home_quote']+=1;continue
            dates.add(day);band='1.58-1.75' if 1.58<=odd<=1.75 else 'outside'
            for market in TARGETS:
                side='yes' if market=='gg_ht' else 'over';r=rows.get((market,side),{})
                pair=(outcome(market,record.get('settlement',{}).get('result',{})),r.get('odd'))
                for scope in ('all_dates',day):
                    groups[(policy,scope,band,market)].append(pair)
                    groups[(policy,scope,'all_priced_home',market)].append(pair)
    return {'hypothesis':'Home 1X2 price inclusive 1.58–1.75; descriptive, not validated or causal',
            'dates':sorted(dates),'excluded':dict(skipped),'model':'context-goals-v3.4',
            'note':'First/latest are separate overlapping cohorts. Missing results are pending. ROI uses target-market prices, never home-win prices. No model modifications.',
            'groups':[dict(zip(('snapshot','date','band','market'),key),**metrics(rows)) for key,rows in sorted(groups.items())]}

def load_private_store():
    from gold_storage import client
    s3=client();bucket=os.environ['R2_BUCKET']
    index=json.loads(s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read())
    if index.get('version')!=1:raise ValueError('Invalid manifest')
    def raw(path):
        item=index['files'][path];sha=item['sha256']
        if len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):raise ValueError('Invalid digest')
        data=s3.get_object(Bucket=bucket,Key='state/objects/'+sha)['Body'].read()
        if len(data)!=item['bytes'] or hashlib.sha256(data).hexdigest()!=sha:raise ValueError('Checksum mismatch')
        return data
    path='data/arab_quote_prematch_audit.json';value=json.loads(raw(path))
    if value.get('_storage')=='arabsniper-audit-parts-v1':
        chunks=[]
        for part in value['parts']:
            data=gzip.decompress(raw(str(PurePosixPath(path).parent/'_audit_parts'/(part['sha256']+'.json.gz'))))
            if len(data)!=part['bytes'] or hashlib.sha256(data).hexdigest()!=part['sha256']:raise ValueError('Part checksum')
            chunks.append(data)
        data=b''.join(chunks)
        if len(data)!=value['bytes'] or hashlib.sha256(data).hexdigest()!=value['sha256']:raise ValueError('Manifest checksum')
        value=json.loads(data)
    return value

if __name__=='__main__':
    print('HOME_ODDS_BAND_REPORT='+json.dumps(analyze(load_private_store()),separators=(',',':')))
