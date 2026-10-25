"""Read-only, encrypted research export for the authorized 23–24 October audit.
Never publishes raw forecasts, credentials or results to logs or public git.
"""
import base64, gzip, hashlib, json, os, sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from gold_storage import client, sha, allowed
from audit_storage import read_store, FORMAT
from daily_audit import fingerprint
from current_audit import fetch_results
from multimarket_model import stamp
DATES=('2026-10-23','2026-10-24')
TZ=ZoneInfo('Europe/Rome')

def eligible(record):
    f=record.get('fixture') or {}; k=stamp(f.get('kickoff')); c=stamp(record.get('captured_at'))
    return bool(k and c and c<k and k.astimezone(TZ).date().isoformat() in DATES and not record.get('retrospective') and f.get('fixture_id') is not None and (record.get('multimarket') or {}).get('model_id'))

def retain(best, record):
    if not eligible(record):return
    f=record['fixture']; c=stamp(record['captured_at']);k=stamp(f['kickoff']);version=record['multimarket']['model_id']
    for policy in ('first','latest','two_hours'):
        if policy=='two_hours' and c>k-timedelta(hours=2):continue
        key=(version,str(f['fixture_id']),policy)
        old=best.get(key)
        if old is None or (c<stamp(old['captured_at']) if policy=='first' else c>stamp(old['captured_at'])):best[key]=record

def compact(record, policy, results):
    f=record['fixture'];a=record['multimarket'];markets=[]
    for m in a.get('markets',[]):
        r={k:v for k,v in m.items() if k!='movement'}
        movement=m.get('movement') or {}
        r['movement']={k:v for k,v in movement.items() if k!='operators'}
        r['movement']['operators']=[{k:o.get(k) for k in ('bookmaker','first_observed_fair_pct','fair_initial_pct','fair_current_pct','margin_initial_pct','margin_current_pct','delta_pct','net_delta_pct','leg_fair_delta_pp','observed_from','observed_to')} for o in movement.get('operators',[])]
        markets.append(r)
    return {'fixture':f,'captured_at':record['captured_at'],'policy':policy,'model_id':a['model_id'],
            'forecast_sha256':fingerprint(record),'result':results.get(str(f['fixture_id'])),
            'assessment':{k:v for k,v in a.items() if k not in ('markets','best_candidate')},
            'primary':{k:(a.get('best_candidate') or {}).get(k) for k in ('market','selection','odd','probability_pct')},'markets':markets}

def main():
    s3=client();bucket=os.environ['R2_BUCKET']
    raw=s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read();manifest=json.loads(raw)
    if manifest.get('version')!=1:raise ValueError('Invalid manifest')
    files=manifest['files']; downloaded=set()
    def retrieve(path):
        if not allowed(path) or path not in files:raise ValueError('Required private source absent')
        entry=files[path];digest=entry['sha256']
        if len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):raise ValueError('Invalid object digest')
        target=ROOT/path
        if path not in downloaded:
            value=s3.get_object(Bucket=bucket,Key='state/objects/'+digest)['Body'].read()
            if len(value)!=entry['bytes'] or sha(value)!=digest:raise ValueError('Private source checksum mismatch')
            target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(value);downloaded.add(path)
        value=json.loads(target.read_bytes()) if target.suffix=='.json' else None
        if isinstance(value,dict) and value.get('_storage')==FORMAT:
            for part in value['parts']:
                retrieve(str(Path(path).parent/'_audit_parts'/(part['sha256']+'.json.gz')))
        return target
    best={};sources={};counts={}
    paths=[f'auditordata/prematch_{d}.json' for d in DATES]+['data/arab_quote_prematch_audit.json']
    for path in paths:
        if path not in files:continue
        payload=read_store(retrieve(path));sources[path]=files[path]['sha256'];count=0
        for collection in ('multimarket_evaluations','analysis_updates'):
            for record in payload.get(collection,{}).values():
                if eligible(record):retain(best,record);count+=1
        counts[path]=count
        del payload
    if not best:raise ValueError('No authentic prematch records for requested dates')
    results={};api_counts={}
    for day in DATES:
        payload=fetch_results(day,os.environ['API_SPORTS_KEY'])
        matches=payload['days'][0]['matches'];api_counts[day]=len(matches)
        for match in matches:results[match['fixture']['fixture_id']]=match['fixture']['result']
    rows=[compact(r,p,results) for (_,_,p),r in sorted(best.items())]
    report={'generated_at':datetime.now(TZ).isoformat(),'dates':DATES,'source_revision':os.environ.get('GITHUB_SHA'),
            'checkpoint_sha256':sha(raw),'sources':sources,'input_records':counts,'api_results_counts':api_counts,'records':rows}
    # Hybrid authenticated encryption: only the analyst's local private key can read it.
    from cryptography.hazmat.primitives import hashes,serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    pub=serialization.load_pem_public_key((ROOT/'auditor/weekend-recipient-public.pem').read_bytes())
    key=AESGCM.generate_key(bit_length=256);nonce=os.urandom(12);aad=b'arabsniper-weekend-2026-10-23-24-v1'
    plain=gzip.compress(json.dumps(report,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode())
    enc=AESGCM(key).encrypt(nonce,plain,aad)
    wrapped=pub.encrypt(key,padding.OAEP(mgf=padding.MGF1(hashes.SHA256()),algorithm=hashes.SHA256(),label=None))
    envelope={k:base64.b64encode(v).decode() for k,v in {'key':wrapped,'nonce':nonce,'aad':aad,'ciphertext':enc}.items()}
    out=ROOT/'encrypted-review';out.mkdir(exist_ok=True)
    (out/'weekend.enc.json').write_text(json.dumps(envelope))
    print('Encrypted research export completed; no raw forecasts published.')
if __name__=='__main__':main()
