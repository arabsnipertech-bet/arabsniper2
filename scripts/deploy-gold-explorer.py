"""Authorized one-time Explorer release. Archive-only; preserve the live Worker.
Run only from main after CI. No football providers, schedules or messages.
"""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from gold_storage import client, previous_manifest, restore_subset, GOLD
from gold_explorer import publish, read
from audit_storage import read_store

def module(name,file):
    spec=importlib.util.spec_from_file_location(name,ROOT/'scripts'/file)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result
ops=module('ops','deploy-telegram-campaign.py')
patcher=module('patcher','explorer-worker-patch.py')
EXPECTED='bea94f61b9f3aef85295221f2940dc8ccee23a2f9e7cc32c1874735666715836'
class ExplorerStore:
    def __init__(self,s3):self.s3=s3;self.writes=0
    def get_object(self,**kwargs):
        assert kwargs['Key'].startswith('explorer/')
        return self.s3.get_object(**kwargs)
    def put_object(self,**kwargs):
        assert kwargs['Key'].startswith('explorer/')
        self.writes+=1;return self.s3.put_object(**kwargs)

def upload(code):
    boundary='----ArabSniperExplorer20261117'
    body=(f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\nContent-Type: application/json\r\n\r\n'+json.dumps({'main_module':'worker.js'})+f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="worker.js"; filename="worker.js"\r\nContent-Type: application/javascript+module\r\n\r\n'+code+f'\r\n--{boundary}--\r\n').encode()
    raw,_=ops.request(ops.BASE+ops.WORKER+'/content',os.environ['CF_TOKEN'],body,'PUT',{'Content-Type':'multipart/form-data; boundary='+boundary})
    assert json.loads(raw).get('success'),'Worker content update failed'

def main():
    assert os.environ.get('GITHUB_REF')=='refs/heads/main','Release must run from main'
    s3=client();bucket=os.environ['R2_BUCKET'];live=ops.live()
    assert ops.digest(live)==EXPECTED,'Live Worker changed since verified preflight'
    patched=patcher.patch_worker(live);suites=patcher.verify(patched)
    settings=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    bindings={b['name']:b for b in settings['bindings']}
    assert 'GOLD_EXPLORER_ENABLED' not in bindings,'Explorer flag already exists; inspect previous release'
    assert bindings['ACCESS_DB']['id']==ops.DB and bindings['GOLD_DATA']['bucket_name']==bucket
    backup='ops-backups/gold-explorer/'+os.environ['GITHUB_RUN_ID']+'.json'
    s3.put_object(Bucket=bucket,Key=backup,Body=json.dumps({'worker':live,'settings':settings}).encode(),ContentType='application/json')
    for statement in (ROOT/'workers/stripe-access/explorer-schema.sql').read_text().split(';'):
        if statement.strip():ops.query(statement)
    index=previous_manifest(s3,bucket);assert index
    feeds=['data/'+name for name in GOLD[:5]]
    results=sorted(p for p in index if p.startswith('auditarchive/prematch_') and p.endswith('_v7.json'))
    legacy=sorted(p for p in index if p.startswith('auditordata/prematch_') and p.endswith('.json') and len(Path(p).stem)==19)
    target=ExplorerStore(s3);started=time.monotonic()
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        restore_subset(s3,bucket,root,index,feeds+results)
        days=[read_store(root/p) for p in feeds]
        publish(target,bucket,root,days,'refresh',legacy_limit=None,source_manifest=index)
        for i,path in enumerate(legacy):
            restore_subset(s3,bucket,root,index,[path])
            publish(target,bucket,root,[],'results',legacy_limit=None,source_manifest=index)
            (root/path).unlink()
            print('Existing historical archive imported',i+1,'/',len(legacy),flush=True)
    current=read(target,bucket,'explorer/current.json',{})
    assert current.get('count',0)>0,'No real Explorer data available'
    assert ops.live()==live,'Concurrent Worker change detected; no deployment performed'
    flag_added=False;uploaded=False
    try:
        upload(patched);uploaded=True
        assert ops.live()==patched,'Worker verification failed'
        after=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
        assert sorted(settings['bindings'],key=lambda b:b['name'])==sorted(after['bindings'],key=lambda b:b['name']),'Existing bindings changed'
        # Adding one secret binding leaves all existing configuration untouched.
        ops.cf(ops.WORKER+'/secrets',os.environ['CF_TOKEN'],{'name':'GOLD_EXPLORER_ENABLED','text':'true','type':'secret_text'},'PUT');flag_added=True
        import requests
        response=requests.get('https://arabsniperbet.com/api/auth/explorer/current',timeout=30,allow_redirects=False)
        assert response.status_code==401,'Live private route must reject anonymous access'
        assert response.headers.get('Content-Type','').startswith('application/json')
        after=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
        kept=[b for b in after['bindings'] if b['name']!='GOLD_EXPLORER_ENABLED']
        assert sorted(settings['bindings'],key=lambda b:b['name'])==sorted(kept,key=lambda b:b['name']),'Existing bindings changed during flag activation'
        assert ops.live()==patched
    except Exception:
        if flag_added:ops.cf(ops.WORKER+'/secrets/GOLD_EXPLORER_ENABLED',os.environ['CF_TOKEN'],method='DELETE')
        if uploaded and ops.live()==patched:upload(live)
        raise
    report={'worker_deployed':True,'worker_sha':ops.digest(patched),'previous_sha':EXPECTED,'private_backup':backup,
      'original_modules_preserved':True,'isolated_suites':suites,'anonymous_http':401,
      'current_rows':current['count'],'result_archives':len(results),'legacy_archives':len(legacy),
      'explorer_writes':target.writes,'football_requests':0,'seconds':round(time.monotonic()-started,2)}
    out=ROOT/'test-artifacts/explorer-release.json';out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(report,indent=2))
    print(json.dumps(report,indent=2))
    with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as output:output.write('## Explorer backend release\n```json\n'+json.dumps(report,indent=2)+'\n```\n')
if __name__=='__main__':
    try:main()
    except Exception as e:raise SystemExit('Explorer release stopped: '+(str(e) if isinstance(e,AssertionError) else type(e).__name__)) from None
