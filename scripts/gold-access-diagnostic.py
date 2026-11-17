"""Diagnose existing Gold session endpoints without OTP emails or entitlement changes."""
import hashlib,importlib.util,json,os,secrets,time
from pathlib import Path
import requests
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('ops',ROOT/'scripts/deploy-telegram-campaign.py');ops=importlib.util.module_from_spec(spec);spec.loader.exec_module(ops)
BASE='https://arabsniperbet.com'
def main():
    report={}
    for path in ['/api/auth/session','/api/auth/explorer/current']:
        r=requests.get(BASE+path,timeout=30);report['anonymous '+path]={'status':r.status_code,'json':r.headers.get('Content-Type','').startswith('application/json')}
    token=secrets.token_hex(32);digest=hashlib.sha256(('__Host-gold_session_24h:'+token).encode()).hexdigest();now=int(time.time());owner='bruno.pirolo@gmail.com'
    ops.query('INSERT INTO gold_sessions(digest,email,expires_at) VALUES (?,?,?)',[digest,owner,now+300])
    try:
        session=requests.Session();session.cookies.set('__Host-gold_session_24h',token,domain='arabsniperbet.com',path='/')
        for path in ['/api/auth/session','/api/auth/explorer/current','/api/gold-data/arab_quote_day1.json']:
            r=session.get(BASE+path,timeout=30,stream=True)
            entry={'status':r.status_code}
            if path.endswith('/session'):
                try:
                    obj=r.json();entry.update(error=obj.get('error'),active=obj.get('active'),authenticated=obj.get('authenticated'))
                except ValueError:entry['json']=False
            r.close();report['owner '+path]=entry
        for table in ['gold_sessions','gold_codes','gold_limits','gold_access_cache','gold_renewals']:
            report[table+'_columns']=[r['name'] for r in ops.query('PRAGMA table_info('+table+')')]
        report['owner_pending_renewals']=len(ops.query("SELECT id FROM gold_renewals WHERE email=? AND status='open' AND expires_at>?",[owner,now]))
        report['worker_sha']=ops.digest(ops.live())
        print(json.dumps(report,indent=2))
    finally:ops.query('DELETE FROM gold_sessions WHERE digest=?',[digest])
if __name__=='__main__':main()
