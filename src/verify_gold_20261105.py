"""One-date read-only verification. Does not publish or alter the audit checkpoint."""
import json,os,hashlib
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from gold_storage import client,previous_manifest,sha
from audit_storage import atomic_bytes,FORMAT
from audit_ledger import entries
from multimarket_model import stamp
from lateral_matrix import settle_spec
from daily_audit import fingerprint

DAY='2026-11-05';ROME=ZoneInfo('Europe/Rome')
assert datetime.now(ROME).date().isoformat()=='2026-11-06','One-off closed'
s3=client();bucket=os.environ['R2_BUCKET'];manifest=previous_manifest(s3,bucket)
name=f'auditordata/prematch_{DAY}.json'
def download(name):
    item=manifest[name]
    raw=s3.get_object(Bucket=bucket,Key='state/objects/'+item['sha256'])['Body'].read()
    assert len(raw)==item['bytes'] and sha(raw)==item['sha256']
    atomic_bytes(Path(name),raw)
    return raw
raw=download(name)
if len(raw)<1024*1024:
    v=json.loads(raw)
    if v.get('_storage')==FORMAT:
        for part in v['parts']:download('auditordata/_audit_parts/'+part['sha256']+'.json.gz')
first={};count=0
for collection,key,record in entries(name):
    if collection not in ('multimarket_evaluations','analysis_updates') or not record:continue
    f=record.get('fixture') or {};at=stamp(record.get('captured_at'));start=stamp(f.get('kickoff'));fid=str(f.get('fixture_id') or '')
    if not fid or not at or not start or at>=start or record.get('retrospective') or start.astimezone(ROME).date().isoformat()!=DAY:continue
    for row in (record.get('multimarket') or {}).get('matrix',{}).get('rows',[]):
        odd=row.get('odd');spec=row.get('spec')
        if row.get('quote_executable') is not True or (row.get('opportunity') or {}).get('eligible') is not True or not spec or type(odd) not in (float,int) or not 1<odd<1000:continue
        k=(fid,json.dumps(spec,sort_keys=True));count+=1
        if k in first and at>=stamp(first[k]['captured_at']):continue
        first[k]=dict(fixture_id=fid,spec=spec,tier='Gold',label=row.get('label') or row.get('market'),odd=odd,captured_at=at.isoformat(),kickoff=start.isoformat(),forecast_sha256=fingerprint(record))
r=requests.get('https://v3.football.api-sports.io/fixtures',params={'date':DAY,'timezone':'Europe/Rome'},headers={'x-apisports-key':os.environ['API_SPORTS_KEY']},timeout=60)
r.raise_for_status();payload=r.json();assert not payload.get('errors') and isinstance(payload.get('response'),list)
results={str(x['fixture']['id']):x for x in payload['response']}
best={};verified=[]
for row in sorted(first.values(),key=lambda x:(-x['odd'],x['fixture_id'],x['label'] or '')):
    m=results.get(row['fixture_id'])
    if not m:continue
    status=m['fixture']['status']['short'];ht=m['score']['halftime'];ft=m['score']['fulltime']
    result=dict(status=status,home_ht=ht['home'],away_ht=ht['away'],home_ft=ft['home'],away_ft=ft['away'])
    if status not in ('FT','AET','PEN'):continue
    outcome=settle_spec(row['spec'],result)
    if not outcome or outcome[0]!=1 or outcome[1]!=0:continue
    row=dict(row,match=m['teams']['home']['name']+' - '+m['teams']['away']['name'],country=m['league']['country'],league=m['league']['name'],result=result)
    verified.append(row);best.setdefault(row['fixture_id'],row)
report=dict(date=DAY,ready=len(best)>=5,examples=list(best.values())[:10] if len(best)>=5 else [],gold_winning_fixtures=len(best),gold_unique_markets=len(first),gold_observations=count,verified_at=datetime.now(ROME).isoformat(),source_archive_sha256=manifest[name]['sha256'],source='API-Football final results + frozen pre-match Gold archive')
Path('verified-gold.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps(report,ensure_ascii=False))
