"""Finalize the already uploaded, exact verified Explorer Worker after propagation.
No upload, football collection, subscription changes or historical re-import.
"""
import importlib.util,json,os,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('release',ROOT/'scripts/deploy-gold-explorer.py')
release=importlib.util.module_from_spec(spec);spec.loader.exec_module(release)
ops=release.ops
EXPECTED_PATCH='525408978cea191e620a89d6fe3d5645779686c2f694f4943934b49aaef1c698'
BACKUP='ops-backups/gold-explorer/44060683182.json'
def bindings(settings):return sorted([b for b in settings['bindings'] if b['name']!='GOLD_EXPLORER_ENABLED'],key=lambda b:b['name'])
def main():
    assert os.environ.get('GITHUB_REF')=='refs/heads/main'
    s3=release.client();bucket=os.environ['R2_BUCKET']
    backup=json.loads(s3.get_object(Bucket=bucket,Key=BACKUP)['Body'].read())
    assert ops.digest(backup['worker'])==release.EXPECTED,'Unexpected backup source'
    expected=release.patcher.patch_worker(backup['worker'])
    assert ops.digest(expected)==EXPECTED_PATCH,'Local Explorer module changed'
    live=ops.live();assert live==expected,'Production Worker differs from exact tested patch'
    suites=release.patcher.verify(live)
    settings=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    assert not any(b['name']=='GOLD_EXPLORER_ENABLED' for b in settings['bindings']),'Flag already exists; inspect activation before retry'
    assert bindings(settings)==bindings(backup['settings']),'Existing bindings differ from backup'
    current=release.read(s3,bucket,'explorer/current.json',{})
    catalog=release.read(s3,bucket,'explorer/catalog.json',{})
    imported=sum('legacy_sha' in d for d in catalog.get('days',{}).values())
    assert current.get('count',0)>0 and imported>=30,'Imported archive incomplete'
    added=False
    try:
        assert ops.live()==expected,'Concurrent Worker update'
        ops.cf(ops.WORKER+'/secrets',os.environ['CF_TOKEN'],{'name':'GOLD_EXPLORER_ENABLED','text':'true','type':'secret_text'},'PUT');added=True
        import requests
        status=None
        for attempt in range(20):
            response=requests.get('https://arabsniperbet.com/api/auth/explorer/current',timeout=30,allow_redirects=False)
            status=response.status_code
            if status==401 and response.headers.get('Content-Type','').startswith('application/json'):break
            assert status in (401,404,503),'Unexpected anonymous response '+str(status)
            time.sleep(2)
        else:raise AssertionError('Explorer activation did not propagate; HTTP '+str(status))
        after=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
        assert bindings(after)==bindings(settings),'Existing bindings changed'
        assert ops.live()==expected,'Concurrent Worker update after activation'
    except Exception:
        if added:ops.cf(ops.WORKER+'/secrets/GOLD_EXPLORER_ENABLED',os.environ['CF_TOKEN'],method='DELETE')
        raise
    report={'worker_sha':EXPECTED_PATCH,'original_modules_preserved':True,'worker_uploads':0,'football_requests':0,'anonymous_http':401,'legacy_archives':imported,'legacy_rows':sum(d.get('legacy_count',0) for d in catalog['days'].values()),'current_rows':current['count'],'results':sum(d.get('results',0) for d in catalog['days'].values()),'isolated_suites':suites,'private_backup':BACKUP}
    out=ROOT/'test-artifacts/explorer-release.json';out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
if __name__=='__main__':
    try:main()
    except Exception as e:raise SystemExit('Explorer activation stopped: '+(str(e) if isinstance(e,AssertionError) or str(e).startswith(('Remote request failed','Cloudflare operation rejected')) else type(e).__name__)) from None
