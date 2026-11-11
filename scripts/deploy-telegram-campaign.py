"""One-shot, content-only campaign deployment. Never logs customer data or secrets."""
import email.parser, email.policy, hashlib, json, os, subprocess, sys, tempfile
from pathlib import Path
from urllib.request import Request, build_opener, HTTPRedirectHandler, urlopen
from urllib.error import HTTPError

ACCOUNT='dfdb02b4139b31c0ab4456a50eb371a2'
DB='2f9dbeae-4b26-494d-9b88-a3a89a1c4f25'
CAMPAIGN='gold-engine-2026-11'
BASE='https://api.cloudflare.com/client/v4/accounts/'+ACCOUNT
WORKER='/workers/scripts/stripe-checker'
ORIGINAL='d6e904d6db2dab78d8339bf6b1da70d5ed32cdbe8c0e8f6d4de584bc54b6688f'
BASELINE='cb8ee1887ce17665b83024e57a3e404e44f1ff58'
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args): return None
client=build_opener(NoRedirect())
def request(url,token=None,data=None,method='GET',headers=None):
    h=headers or {}
    if token:h['Authorization']='Bearer '+token
    try:
        with client.open(Request(url,data=data,method=method,headers=h),timeout=60) as r:
            return r.read(),r.headers
    except HTTPError as e:
        raise RuntimeError('Remote request failed HTTP '+str(e.code)) from None
    except Exception:
        raise RuntimeError('Remote request failed; credentials withheld') from None

def cf(path,token,body=None,method='GET'):
    raw,_=request(BASE+path,token,json.dumps(body).encode() if body is not None else None,method,{'Content-Type':'application/json'})
    obj=json.loads(raw)
    if not obj.get('success'):raise RuntimeError('Cloudflare operation rejected')
    return obj['result']
def query(sql,params=None):
    result=cf('/d1/database/'+DB+'/query',os.environ['D1_TOKEN'],{'sql':sql,'params':params or []},'POST')
    if not all(r.get('success') for r in result):raise RuntimeError('D1 operation failed')
    return result[0].get('results',[])
def live():
    raw,h=request(BASE+WORKER+'/content/v2',os.environ['CF_TOKEN'])
    ct=h.get('Content-Type','')
    if 'multipart/' in ct:
        msg=email.parser.BytesParser(policy=email.policy.default).parsebytes(('Content-Type: '+ct+'\r\nMIME-Version: 1.0\r\n\r\n').encode()+raw)
        parts=[(p.get_param('name',header='content-disposition'),p.get_payload(decode=True)) for p in msg.iter_parts()]
    else:parts=[('worker.js',raw)]
    assert len(parts)==1 and parts[0][0]=='worker.js','Unexpected module layout'
    return parts[0][1].decode()
def digest(s):return hashlib.sha256(s.encode()).hexdigest()
def baseline():
    url='https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/'+BASELINE+'/workers/stripe-access/telegram-trial.mjs'
    return request(url)[0].decode().replace('export ','')
def replacement():return Path('workers/stripe-access/telegram-trial.mjs').read_text().replace('export ','')
def fingerprint():
    rows=query('SELECT * FROM telegram_trial_claims ORDER BY email')
    return digest(json.dumps(rows,sort_keys=True)),len(rows),rows

def smoke(code):
    # Exercise the actual downloaded module with synthetic env and blocked network.
    with tempfile.TemporaryDirectory() as d:
        p=Path(d)/'worker.mjs';p.write_text(code)
        subprocess.run(['node','--check',str(p)],check=True,capture_output=True)
        harness=Path(d)/'smoke.mjs'
        harness.write_text("""
import assert from 'node:assert/strict';
import worker from './worker.mjs';
const sent=[];
globalThis.fetch=async(url,opts)=>{assert.equal(url,'https://api.telegram.org/botfake/sendMessage');sent.push(JSON.parse(opts.body));return Response.json({ok:true});};
const env={TELEGRAM_TRIAL_ENABLED:'true',ACCESS_DB:{},TELEGRAM_BOT_TOKEN:'fake',TELEGRAM_WEBHOOK_SECRET:'fake',TELEGRAM_TRIAL_HMAC_SECRET:'fake',STRIPE_SECRET_KEY:'fake',RESEND_API_KEY:'fake',EMAIL_FROM:'fake@example.org'};
const req=new Request('https://stripe-checker.arabsnipertech.workers.dev/api/telegram-webhook',{method:'POST',headers:{'X-Telegram-Bot-Api-Secret-Token':'fake'},body:JSON.stringify({message:{chat:{id:42,type:'private'},from:{id:42},text:'/prova'}})});
assert.equal((await worker.fetch(req,env)).status,200);
assert.equal(sent.length,1);assert.match(sent[0].text,/anche se hai già provato Gold/);
console.log('Actual deployed module: new campaign /prova smoke passed (synthetic, no network)');
""")
        subprocess.run(['node',str(harness)],check=True,capture_output=True)
    print('Deployed module synthetic /prova test: OK')
def telegram(method,**values):
    raw,_=request('https://api.telegram.org/bot'+os.environ['BOT_TOKEN']+'/'+method,data=json.dumps(values).encode(),method='POST',headers={'Content-Type':'application/json'})
    data=json.loads(raw)
    if not data.get('ok'):raise RuntimeError('Telegram operation rejected')
    return data['result']
def health():
    bot=telegram('getMe');assert bot['username'].lower()=='arabsniperbot'
    hook=telegram('getWebhookInfo')
    assert hook['url'] in ['https://stripe-checker.arabsnipertech.workers.dev/api/telegram-webhook','https://arabsniperbet.com/api/telegram-webhook']
    assert not hook.get('allowed_updates') or 'message' in hook['allowed_updates']
    # An unauthenticated request must not activate a trial.
    try:request(hook['url'],data=b'{}',method='POST',headers={'Content-Type':'application/json'})
    except RuntimeError as e:assert str(e)=='Remote request failed HTTP 403'
    else:raise RuntimeError('Webhook authentication guard failed')
    print('Bot identity, webhook configuration and authentication: OK; pending updates:',hook.get('pending_update_count',0))
    return bot

def deploy():
    old=baseline();new=replacement();code=live()
    already=code.count(new)==1
    original=code.replace(new,old) if already else code
    assert digest(original)==ORIGINAL,'Production changed; refusing to overwrite another release'
    assert original.count(old)==1
    patched=original.replace(old,new)
    smoke(patched)
    settings=cf(WORKER+'/settings',os.environ['CF_TOKEN'])
    bindings={b['name']:b for b in settings['bindings']}
    assert bindings['ACCESS_DB']['id']==DB
    assert bindings['TELEGRAM_TRIAL_ENABLED'].get('text')=='true'
    assert bindings.get('TELEGRAM_TRIAL_CAMPAIGN',{}).get('text',CAMPAIGN)==CAMPAIGN
    before,count,rows=fingerprint()
    # Private rollback snapshot; never upload source or customer records as public Actions artifacts.
    import boto3
    s3=boto3.client('s3',endpoint_url='https://'+ACCOUNT+'.r2.cloudflarestorage.com',region_name='auto',aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'])
    backup=json.dumps({'worker':original,'settings':settings,'telegram_trial_claims':rows},ensure_ascii=False).encode()
    key='ops-backups/telegram-campaign/'+CAMPAIGN+'/'+os.environ['GITHUB_RUN_ID']+'.json'
    s3.put_object(Bucket='arabsniper-gold',Key=key,Body=backup,ContentType='application/json')
    print('Private rollback snapshot saved; original trial rows:',count)
    query(Path('workers/stripe-access/telegram-campaign-schema.sql').read_text())
    assert fingerprint()[0]==before,'Legacy trial history changed during schema creation'
    if not already:
        # Confirm no concurrent deployment since read/preflight.
        assert digest(live())==ORIGINAL,'Concurrent deployment detected'
        boundary='----ArabSniperCampaignContent202611'
        body=(f'--{boundary}\r\nContent-Disposition: form-data; name="metadata"\r\nContent-Type: application/json\r\n\r\n'+json.dumps({'main_module':'worker.js'})+f'\r\n--{boundary}\r\nContent-Disposition: form-data; name="worker.js"; filename="worker.js"\r\nContent-Type: application/javascript+module\r\n\r\n'+patched+f'\r\n--{boundary}--\r\n').encode()
        raw,_=request(BASE+WORKER+'/content',os.environ['CF_TOKEN'],body,'PUT',{'Content-Type':'multipart/form-data; boundary='+boundary})
        assert json.loads(raw).get('success'),'Worker content upload rejected'
    after=live();assert after==patched,'Deployed content differs'
    newsettings=cf(WORKER+'/settings',os.environ['CF_TOKEN'])
    assert sorted(settings['bindings'],key=lambda b:b['name'])==sorted(newsettings['bindings'],key=lambda b:b['name']),'Bindings changed unexpectedly'
    for k in ['compatibility_date','compatibility_flags','observability','limits','placement','logpush','tail_consumers']:
        assert settings.get(k)==newsettings.get(k),'Worker configuration changed: '+k
    smoke(after);health()
    print('Campaign DEPLOYED; SHA256:',digest(after))
    print('Original trial rows remain:',fingerprint()[1])
    with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as out:
        out.write('Campaign '+CAMPAIGN+' deployed and verified. Legacy history preserved. Configuration unchanged.\n')

def announce():
    new=replacement();code=live();assert code.count(new)==1
    assert digest(code.replace(new,baseline()))==ORIGINAL
    health()
    channel=telegram('getChat',chat_id='@arabsniper_signals')
    assert channel['type']=='channel' and channel['username'].lower()=='arabsniper_signals'
    bot=telegram('getMe');member=telegram('getChatMember',chat_id=channel['id'],user_id=bot['id'])
    assert member['status']=='administrator' and member.get('can_post_messages')
    query('CREATE TABLE IF NOT EXISTS telegram_campaign_announcements (campaign TEXT PRIMARY KEY, status TEXT NOT NULL, message_id INTEGER, created_at INTEGER NOT NULL)')
    existing=query('SELECT status,message_id FROM telegram_campaign_announcements WHERE campaign=?',[CAMPAIGN])
    if existing:
        assert existing[0]['status']=='sent','Previous send outcome uncertain; verify channel before retrying'
        print('Announcement already sent: https://t.me/arabsniper_signals/'+str(existing[0]['message_id']));return
    reserved=query("INSERT OR IGNORE INTO telegram_campaign_announcements(campaign,status,created_at) VALUES (?,'sending',unixepoch()) RETURNING campaign",[CAMPAIGN])
    assert reserved,'Another publication owns this campaign'
    text=("🎁 NUOVO MOTORE ARABSNIPERBET · ALTRI 7 GIORNI GOLD GRATIS\n\n"
          "La nuova prova è aperta a tutti: nuovi iscritti e persone che hanno già utilizzato una prova Gold.\n\n"
          "⚽ Esplora le analisi del nuovo motore e confronta le proposte nell’area Gold.\n"
          "✅ 7 giorni gratuiti, una sola volta per email e account Telegram in questa campagna\n"
          "✅ Se hai già un accesso attivo, i 7 giorni si aggiungono alla sua scadenza\n"
          "✅ Nessuna carta richiesta e nessun rinnovo automatico della prova\n\n"
          "📲 Apri https://t.me/ArabSniperBot e scrivi /prova in PRIVATO.\n"
          "Invia la tua email e conferma il codice ricevuto, poi accedi con la stessa email su:\n"
          "https://arabsniperbet.com/gold-dashboard-live.html\n\n"
          "18+ · Analisi statistiche, nessuna garanzia di vincita.")
    sent=telegram('sendMessage',chat_id=channel['id'],text=text,disable_web_page_preview=True)
    assert sent['chat']['id']==channel['id']
    query("UPDATE telegram_campaign_announcements SET status='sent',message_id=? WHERE campaign=?",[sent['message_id'],CAMPAIGN])
    link='https://t.me/arabsniper_signals/'+str(sent['message_id'])
    print('Announcement sent: '+link)
    with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as out:out.write('\nAnnouncement: '+link+'\n')

if __name__=='__main__':
    try:
        {'deploy':deploy,'announce':announce}[sys.argv[1]]()
    except Exception as exc:
        # Avoid exception repr/tracebacks that may contain credential-bearing URLs.
        print('STOP:',str(exc) if isinstance(exc,(AssertionError,RuntimeError)) else type(exc).__name__)
        sys.exit(1)
