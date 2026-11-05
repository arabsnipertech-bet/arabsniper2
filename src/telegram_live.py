"""One receipt-backed, full-time Telegram live per Rome day. No model mutations."""
import io
import json
import math
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

ROME = ZoneInfo('Europe/Rome')
REPO = 'arabsnipertech-bet/arabsniper2'
CHANNEL = '@arabsniper_signals'
WORKFLOW = 'telegram-live.yml'
TARGETS = {'over25_ft': 3, 'over35_ft': 4}
LIVE = {'1H', 'HT', '2H', 'LIVE'}
CLOSED = {'FT', 'AET', 'PEN'}
STOPPED = {'CANC', 'ABD', 'AWD', 'WO', 'PST'}

def now_local():
    return datetime.now(ROME)

def moment(value):
    try:
        d = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return d if d.tzinfo else None
    except (AttributeError, ValueError, TypeError):
        return None

def eligible(p, now, upcoming=True):
    start = moment(p.get('kickoff'))
    probability = p.get('model_probability', p.get('statistical_pct'))
    odd = p.get('odd')
    return bool(start and p.get('market') in TARGETS and
        type(odd) in (int,float) and math.isfinite(odd) and 1.7 <= odd <= 3 and
        type(probability) in (int,float) and math.isfinite(probability) and 0 < probability <= 100 and
        re.fullmatch(r'\d+', str(p.get('id', p.get('fixture_id', '')))) and
        (now <= start <= now + timedelta(minutes=20) if upcoming else
         now-timedelta(hours=4) <= start <= now+timedelta(minutes=20)))

def candidates(receipts, now, upcoming=True):
    result = {}
    for r in receipts:
        if type(r.get('message_id')) is not int or r['message_id'] <= 0:
            continue
        if r.get('day') not in {now.date().isoformat(), (now.date()-timedelta(days=1)).isoformat()}:
            continue
        for p in r.get('picks', []):
            if not eligible(p, now, upcoming):
                continue
            fid = str(p.get('id', p.get('fixture_id')))
            item = dict(p, id=fid, source_message_id=r['message_id'], day=r['day'])
            # First published market is frozen; later updates cannot rewrite it.
            if fid not in result or r['message_id'] < result[fid]['source_message_id']:
                result[fid] = item
    return sorted(result.values(), key=lambda p:(
        -p.get('model_probability', p.get('statistical_pct')),
        abs(p['odd']-2), {'Gold':0,'Blu':1,'Platinum':2}.get(p.get('tier'),3), p['id']))

def gh(path, body=None):
    req = Request('https://api.github.com/repos/'+REPO+'/actions/'+path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],
                 'Accept':'application/vnd.github+json','Content-Type':'application/json'})
    with urlopen(req, timeout=25) as res:
        data = res.read()
        return json.loads(data) if data else {}

class SafeRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and urlparse(req.full_url).netloc != urlparse(newurl).netloc:
            new.remove_header('Authorization')
        return new


def receipts(now):
    days = {now.date().isoformat(),(now.date()-timedelta(days=1)).isoformat()}
    names = {f'tg-{day}-{slot}-sent' for day in days for slot in ('0030','0430','1230','1730','1830','2000','2200')}
    output=[]
    for name in sorted(names):
        data=gh('artifacts?'+urlencode({'name':name,'per_page':100}))
        for row in data.get('artifacts',[]):
            if row.get('expired') or row.get('name')!=name: continue
            req=Request(row['archive_download_url'],headers={'Authorization':'Bearer '+os.environ['GH_TOKEN']})
            with build_opener(SafeRedirect).open(req,timeout=25) as res:
                raw=res.read(4_000_001)
            if len(raw)>4_000_000: raise RuntimeError('Receipt archive exceeds bound')
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                if z.getinfo('post.json').file_size>200_000: raise RuntimeError('Receipt exceeds bound')
                output.append(json.loads(z.read('post.json')))
    return output

def dispatch():
    if os.environ.get('FAST_BACKUP_DRY_RUN')=='true': return
    runs=gh('workflows/'+WORKFLOW+'/runs?per_page=20').get('workflow_runs',[])
    if any(r.get('status') in ('queued','in_progress','waiting','pending') for r in runs):
        print('Live monitor already active'); return
    if candidates(receipts(now_local()),now_local(),upcoming=False):
        gh('workflows/'+WORKFLOW+'/dispatches',{'ref':'main'})
        print('Live monitor dispatched')
    else:
        print('No published full-time live candidate near kickoff')

def api(path):
    req=Request('https://v3.football.api-sports.io/'+path,
                headers={'x-apisports-key':os.environ['API_SPORTS_KEY']})
    with urlopen(req,timeout=20) as res: data=json.load(res)
    if data.get('errors') or not isinstance(data.get('response'),list):
        raise RuntimeError('Sports provider response unavailable')
    return data['response']

def observation(row):
    status=row['fixture']['status']['short']
    goals=(row.get('score',{}).get('fulltime') if status in CLOSED|{'ET','BT','P'} else row.get('goals')) or {}
    h,a=goals.get('home'),goals.get('away')
    if not all(type(v) is int and v>=0 for v in (h,a)):
        if status in STOPPED: h,a='—','—'
        else: return None
    minute=row['fixture']['status'].get('elapsed')
    if minute is not None and (type(minute) is not int or minute<0): return None
    events=[e for e in row.get('events',[]) if e.get('type')=='Goal' and e.get('detail') not in ('Missed Penalty','Cancelled')]
    event=events[-1] if events and type(h) is int and type(a) is int and len(events)==h+a else {}
    missed=[]
    for e in row.get('events',[]):
        if (e.get('type')=='Goal' and e.get('detail')=='Missed Penalty'
                and type((e.get('time') or {}).get('elapsed')) is int
                and 0 <= e['time']['elapsed'] <= 90):
            timing=e.get('time') or {};team=e.get('team') or {}
            key=json.dumps([timing.get('elapsed'),timing.get('extra'),team.get('id'),(e.get('player') or {}).get('id')])
            missed.append(dict(key=key,team=team.get('name'),minute=timing.get('elapsed')))
    event_minute=(event.get('time') or {}).get('elapsed')
    return dict(status=status,home=h,away=a,minute=minute,missed=missed,penalty=event.get('detail')=='Penalty',
                scorer_team=(event.get('team') or {}).get('name'),
                goal_minute=event_minute if type(event_minute) is int else None)

def advance(state, obs, now):
    """Two independent observations >=45s apart confirm scores and settlements."""
    messages=[]
    if obs['status'] in STOPPED:
        if not state.get('done'):
            messages.append(('notice','Gara sospesa definitivamente, rinviata o annullata: esito da verificare.'))
            state['done']=True
        return messages
    if obs['status'] in {'SUSP','INT'}:
        if not state.get('paused'):
            messages.append(('notice','PARTITA SOSPESA O INTERROTTA · nessun esito assegnato.'))
            state['paused']=True
        return messages
    if obs['status'] not in LIVE|CLOSED|{'ET','BT','P'}: return messages
    if state.pop('paused',False):
        messages.append(('notice','PARTITA RIPRESA · monitoraggio nuovamente in corso.'))
    if not state.get('started'):
        messages.append(('live','LIVE IN CORSO · seguiamo la singola gratuita.'))
        state['started']=True
        state.setdefault('score',[0,0])
    seen=state.setdefault('missed_seen',{})
    sent=state.setdefault('missed_sent',[])
    present={e['key'] for e in obs.get('missed',[])}
    for key in list(seen):
        if key not in present and key not in sent: del seen[key]
    for e in obs.get('missed',[]):
        seen.setdefault(e['key'],now)
        if now-seen[e['key']]>=45 and e['key'] not in sent and not state.get('won'):
            detail='RIGORE SBAGLIATO · nessun gol assegnato.'
            if e.get('team'): detail+=' '+e['team']
            if type(e.get('minute')) is int: detail+=f" · evento al {e['minute']}′."
            messages.append(('missed',detail));sent.append(e['key'])
    signature=[obs['home'],obs['away'],obs['status'] in CLOSED|{'ET','BT','P'}]
    if state.get('candidate')!=signature:
        state['candidate']=signature; state['candidate_at']=now
        return messages
    if now-state.get('candidate_at',now)<45: return messages
    score=signature[:2]; previous=state.get('score',[0,0]); target=TARGETS[state['pick']['market']]
    if score!=previous:
        decreased=any(a<b for a,b in zip(score,previous))
        if decreased:
            messages.append(('notice','RETTIFICA DEL PUNTEGGIO · possibile gol annullato/VAR.'))
        elif not state.get('won'):
            penalty=obs.get('penalty') is True and sum(score)-sum(previous)==1
            detail='RIGORE SEGNATO! · punteggio confermato dal feed.' if penalty else 'GOOOOLLL! · punteggio confermato dal feed.'
            # Event details only when the provider event list agrees with the total.
            if obs.get('scorer_team') and obs.get('goal_minute') is not None:
                detail+=' '+obs['scorer_team']+f" · evento al {obs['goal_minute']}′."
            messages.append(('penalty' if penalty else 'goal',detail))
        state['score']=score
    hit=sum(score)>=target
    if state.get('won') and not hit:
        state['won']=False
        messages.append(('notice','RETTIFICA · soglia non più raggiunta; il live prosegue.'))
    if hit and not state.get('won'):
        messages.append(('win','ArabCassaaaaaa! · soglia del pronostico raggiunta.'))
        state['won']=True
    if signature[2]:
        if not hit and not state.get('settled'):
            messages.append(('lose','XLoooseeeee · pronostico non centrato nei 90 minuti più recupero.'))
        state['settled']=True
        # Keep checking after the final score for delayed provider/VAR corrections.
        state.setdefault('final_at',now)
        if now-state['final_at']>=600: state['done']=True
    return messages

class Store:
    def __init__(self,day):
        from gold_storage import client
        self.s3=client();self.bucket=os.environ['R2_BUCKET'];self.key='telegram-live/'+day+'.json';self.etag=None
    def read(self):
        from botocore.exceptions import ClientError
        try: r=self.s3.get_object(Bucket=self.bucket,Key=self.key)
        except ClientError as e:
            if e.response['Error']['Code'] in ('NoSuchKey','404'): return None
            raise
        self.etag=r['ETag'];return json.load(r['Body'])
    def write(self,state):
        # Conditional writes fence overlapping/resumed runners before any send.
        conditions={'IfMatch':self.etag} if self.etag else {'IfNoneMatch':'*'}
        r=self.s3.put_object(Bucket=self.bucket,Key=self.key,Body=json.dumps(state).encode(),ContentType='application/json',**conditions)
        self.etag=r['ETag']

def telegram(method,data,files=None):
    import requests
    try:
        r=requests.post('https://api.telegram.org/bot'+os.environ['BOT_TOKEN']+'/'+method,
                        data=data,files=files,timeout=25)
        result=r.json()
    except (requests.RequestException,ValueError):
        raise RuntimeError('Telegram delivery uncertain; inspect before retry') from None
    if not result.get('ok'): raise RuntimeError('Telegram rejected request')
    return result['result']

def deliver(state,store,events,obs):
    for kind,detail in events:
        p=state['pick'];minute=obs.get('minute')
        phase='FT' if obs['status'] in CLOSED else (str(minute)+'′' if minute is not None else obs['status'])
        caption=(f"{detail}\n\n{p['match']}\n{obs['home']}–{obs['away']} · {phase}\n"
                 f"{p['label']} · quota pubblicata {p['odd']:.2f}\n"
                 f"Pronostico originale: https://t.me/arabsniper_signals/{p['source_message_id']}\n"
                 "Dati API-Football · 18+ · Le analisi non garantiscono risultati.")
        event=dict(kind=kind,caption=caption,created_at=now_local().isoformat())
        state['outbox']=event;store.write(state)
        with open('assets/telegram-live/'+kind+'.gif','rb') as f:
            result=telegram('sendAnimation',{'chat_id':CHANNEL,'caption':caption,
                'reply_parameters':json.dumps({'message_id':p['source_message_id'],'allow_sending_without_reply':True})},
                {'animation':('live.gif',f,'image/gif')})
        if result.get('chat',{}).get('username','').lower()!=CHANNEL[1:] or not result.get('message_id'):
            raise RuntimeError('Unexpected live receipt')
        state.setdefault('receipts',[]).append(dict(event,message_id=result['message_id']))
        state.pop('outbox')
        if state.get('pending_events') and list(events[0]) == state['pending_events'][0]:
            state['pending_events'].pop(0)
        store.write(state)
        print('Live notification confirmed',kind,result['message_id'],flush=True)

def select(now):
    # Resume yesterday if a late fixture crosses midnight.
    for day in ((now.date()-timedelta(days=1)).isoformat(),now.date().isoformat()):
        store=Store(day);state=store.read()
        if state and not state.get('done'): return store,state
        if day==now.date().isoformat() and state: return store,state
    store=Store(now.date().isoformat());store.read()
    for p in candidates(receipts(now),now):
        rows=api('fixtures?id='+p['id'])
        if len(rows)!=1 or rows[0]['fixture']['status']['short']!='NS': continue
        league=rows[0].get('league',{})
        coverage=api('leagues?'+urlencode({'id':league['id'],'season':league['season']}))
        seasons=[s for l in coverage for s in l.get('seasons',[]) if s.get('year')==league['season']]
        if not seasons or not seasons[0].get('coverage',{}).get('fixtures',{}).get('events'): continue
        state={'pick':p,'chosen_at':now.isoformat(),'receipts':[],'done':False}
        store.write(state);print('Daily live selected',p['id'],flush=True);return store,state
    return store,None

def monitor():
    store,state=select(now_local())
    if not state or state.get('done'): print('No new live to monitor');return
    if state.get('outbox'): raise RuntimeError('Unconfirmed Telegram delivery requires receipt reconciliation')
    while state.get('pending_events'):
        deliver(state,store,[state['pending_events'][0]],state['last_observation'])
    deadline=time.monotonic()+185*60
    errors=0
    while time.monotonic()<deadline:
        p=state['pick'];now=now_local()
        if now < moment(p['kickoff']): time.sleep(30);continue
        if now-moment(p['kickoff'])>timedelta(hours=6):
            raise RuntimeError('Live exceeded six hours; manual review required')
        try:
            rows=api('fixtures?id='+p['id'])
            if len(rows)!=1 or str(rows[0]['fixture']['id'])!=p['id']: raise RuntimeError('Missing fixture')
            obs=observation(rows[0])
            if obs is None: raise RuntimeError('Missing score')
        except Exception:
            errors+=1
            # Never infer a goal/result during a provider outage.
            if errors>=3 and not state.get('outage'):
                previous=state.get('last_observation')
                if previous:
                    deliver(state,store,[('notice','AGGIORNAMENTO LIVE IN RITARDO · ultimo punteggio disponibile, non attuale.')],previous)
                state['outage']=True;store.write(state)
            time.sleep(60);continue
        if state.get('outage'):
            state.pop('candidate',None)
        errors=0;state['outage']=False
        state['last_observation']=obs
        events=advance(state,obs,time.time())
        # Save transitions atomically with pending events, then deliver one at a time.
        state['pending_events']=[list(e) for e in events]
        store.write(state)
        while state['pending_events']:
            e=state['pending_events'][0]
            deliver(state,store,[e],obs)
        if state.get('done'): print('Live finished',flush=True);return
        time.sleep(60)
    print('Monitor checkpoint saved; external backup can resume',flush=True)

def check():
    # Read-only readiness: credentials, Telegram permissions, private state access.
    bot=telegram('getMe',{})
    member=telegram('getChatMember',{'chat_id':CHANNEL,'user_id':bot['id']})
    assert member.get('can_post_messages') and bot.get('username','').lower()=='arabsniperbot'
    Store(now_local().date().isoformat()).read()
    values=receipts(now_local())
    ids=[str(p.get('id',p.get('fixture_id',''))) for r in values for p in r.get('picks',[]) if re.fullmatch(r'\d+',str(p.get('id',p.get('fixture_id',''))))]
    if ids:
        assert len(api('fixtures?id='+ids[-1]))==1
    print('Live readiness OK; published candidates:',len(candidates(values,now_local(),False)))

if __name__=='__main__':
    if os.environ.get('GITHUB_REPOSITORY')!=REPO or os.environ.get('GITHUB_REF')!='refs/heads/main':
        raise SystemExit('Production main only')
    {'dispatch':dispatch,'monitor':monitor,'check':check}[sys.argv[1]]()
