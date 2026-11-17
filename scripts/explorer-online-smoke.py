"""Real API/browser verification using a temporary session for the project owner.
No OTP emails, payments, subscription changes or third-party account sessions.
"""
import hashlib, importlib.util, json, os, secrets, subprocess, tempfile, time
from pathlib import Path
import requests
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('ops',ROOT/'scripts/deploy-telegram-campaign.py')
ops=importlib.util.module_from_spec(spec);spec.loader.exec_module(ops)
ORIGIN='https://arabsniperbet.com';BASE=ORIGIN+'/api/auth/explorer/'
def main():
    token=secrets.token_hex(32);digest=hashlib.sha256(('__Host-gold_session_24h:'+token).encode()).hexdigest()
    owner='bruno.pirolo@gmail.com';now=int(time.time())
    session=requests.Session();session.cookies.set('__Host-gold_session_24h',token,domain='arabsniperbet.com',path='/')
    created_search=None;created_favorite=None
    def api(path,body=None):
        r=session.get(BASE+path,timeout=30) if body is None else session.post(BASE+path,json=body,headers={'Origin':ORIGIN},timeout=30)
        assert r.status_code==200,'Authenticated Explorer API failed: '+path.split('/')[0]
        return r.json()
    ops.query('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)',[digest,owner,now+600])
    try:
        current=api('current');assert current['count']>0
        page=api('objects/'+current['pages'][0]);assert page['rows']
        existing=api('searches')['items']
        created_search=api('searches',{'name':'QA Explorer '+str(now),'filters':{'market':'GG FT'}})['id']
        assert any(r['id']==created_search for r in api('searches')['items'])
        api('searches',{'id':created_search,'name':'QA Explorer updated','filters':{'team':'Roma'}})
        api('delete',{'type':'search','id':created_search});created_search=None
        assert len(api('searches')['items'])==len(existing)
        # Create a favorite only when it is not already an owner favorite.
        all_ids=set();offset=0
        while True:
            saved=api('favorites?offset='+str(offset));all_ids.update(r['id'] for r in saved['items'])
            if saved.get('next') is None:break
            offset=saved['next']
        row=next((r for r in page['rows'] if r['id'] not in all_ids),None)
        assert row,'No unused favorite available for isolated CRUD smoke'
        api('favorites',{'id':row['id'],'object':current['pages'][0]});created_favorite=row['id']
        assert any(r['id']==row['id'] for r in api('favorites')['items'])
        api('delete',{'type':'favorite','id':row['id']});created_favorite=None
        catalog=api('catalog');assert catalog['days']
        history=api('days/'+sorted(catalog['days'])[-1]);assert 'results' in history
        # Allow the independently triggered Pages build to finish.
        for attempt in range(24):
            assets=session.get(ORIGIN+'/gold-explorer.mjs',timeout=30)
            if assets.status_code==200 and '/api/auth/explorer/' in assets.text:break
            time.sleep(5)
        else:raise AssertionError('Pages has not published Explorer assets')
        with tempfile.TemporaryDirectory() as folder:
            file=Path(folder)/'session.json';file.write_text(json.dumps({'token':token}));file.chmod(0o600)
            result=subprocess.run(['node','tests/browser_explorer_live.cjs',str(file)],cwd=ROOT,capture_output=True,text=True,timeout=240)
            assert result.returncode==0,'Real browser validation failed; no private output emitted'
        report={'owner_session':'temporary; deleted after smoke','real_api_crud':True,'real_history':True,'subscription_mutations':0,'otp_emails':0,'third_party_sessions':0}
        (ROOT/'test-artifacts/explorer-live-api.json').write_text(json.dumps(report,indent=2));print(json.dumps(report))
    finally:
        if created_search:ops.query('DELETE FROM explorer_searches WHERE email=? AND id=?',[owner,created_search])
        if created_favorite:ops.query('DELETE FROM explorer_favorites WHERE email=? AND id=?',[owner,created_favorite])
        ops.query('DELETE FROM gold_sessions WHERE digest=?',[digest])
if __name__=='__main__':
    try:main()
    except Exception as e:raise SystemExit('Online smoke stopped: '+(str(e) if isinstance(e,AssertionError) else type(e).__name__)) from None
