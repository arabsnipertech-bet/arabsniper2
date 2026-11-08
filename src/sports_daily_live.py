"""One announced sports-only live per Rome day, with receipt-backed recovery."""
import json,os,sys,time,secrets
from datetime import timedelta
from urllib.parse import urlencode
from telegram_live import Store,api,telegram,gh,receipts,moment,now_local,CHANNEL
WORKFLOW='telegram-sports-live.yml'
CLOSED={'FT','AET','PEN','CANC','ABD','AWD','WO','PST'}
class SportsStore(Store):
    def __init__(self,day):
        super().__init__(day);self.key='sports-daily-live/'+day+'.json'
def reserve_publication(post,now):
    if not post.get('analysis_only'):return
    day=now.date().isoformat();store=SportsStore(day);state=store.read()
    if state:
        if state.get('source_slot')==post['slot'] and state.get('pending') and moment(state['pick']['kickoff'])>now and any(p['id']==state['pick']['id'] for p in post['picks']):post['sports_live']=state['announcement']
        if not state.get('pending') or moment(state['pick']['kickoff'])>now:return
        # An unannounced, expired reservation may be replaced; a published one cannot.
        if any(r.get('sports_live',{}).get('token')==state['announcement']['token'] for r in receipts(now)):return
    legacy=Store(day).read()
    if legacy and not legacy.get('pending_publication'):return
    for p in post['picks']:
        start=moment(p['kickoff']);local=start.astimezone(now.tzinfo)
        if not start>now or local.hour*60+local.minute<750:continue
        rows=api('fixtures?id='+p['id'])
        if len(rows)!=1 or str(rows[0]['fixture']['id'])!=p['id'] or rows[0]['fixture']['status']['short']!='NS':continue
        league=rows[0]['league'];coverage=api('leagues?'+urlencode({'id':league['id'],'season':league['season']}))
        seasons=[s for l in coverage for s in l.get('seasons',[]) if s.get('year')==league['season']]
        if not any(s.get('coverage',{}).get('fixtures',{}).get('events') for s in seasons):continue
        announcement={k:p[k] for k in ('id','match','kickoff')};announcement['token']=secrets.token_hex(16)
        state=dict(pick=p,announcement=announcement,source_slot=post['slot'],day=day,pending=True,done=False,receipts=[])
        store.write(state);post['sports_live']=announcement;return

def dispatch():
    if os.getenv('FAST_BACKUP_DRY_RUN')=='true':return
    runs=gh('workflows/'+WORKFLOW+'/runs?per_page=20').get('workflow_runs',[])
    if any(r['status'] in ('queued','in_progress','pending','waiting') for r in runs):return
    now=now_local()
    for r in receipts(now):
        p=r.get('sports_live') or {};start=moment(p.get('kickoff'))
        if r.get('message_id') and start and now-timedelta(hours=5)<=start<=now+timedelta(minutes=20):
            gh('workflows/'+WORKFLOW+'/dispatches',{'ref':'main'});return

def observation(row):
    f=row['fixture'];g=row.get('goals') or {};status=f['status']['short']
    if status=='NS':return None
    if status not in {'CANC','ABD','AWD','WO','PST'} and not all(type(g.get(k)) is int and g[k]>=0 for k in ('home','away')):return None
    return dict(status=status,home=g.get('home'),away=g.get('away'),minute=f['status'].get('elapsed'))
def signature(o):return [o['status'],o['home'],o['away']]
def advance(state,obs,now):
    sig=signature(obs)
    if state.get('candidate')!=sig:state.update(candidate=sig,candidate_at=now);return None
    if now-state['candidate_at']<45:return None
    old=state.get('last')
    if old and signature(old)==sig:return None
    title='🟠 LIVE SPORTIVO IN CORSO'
    if obs['status'] in CLOSED:title='🏁 RISULTATO FINALE' if obs['status'] in ('FT','AET','PEN') else 'ℹ️ AGGIORNAMENTO GARA'
    elif obs['status']=='HT':title='⏸ INTERVALLO'
    elif old and old['status']=='HT' and obs['status']=='2H':title='▶️ SECONDO TEMPO'
    elif old and (old['home'],old['away'])!=(obs['home'],obs['away']):
        decrease=all(type(v) is int for v in (old['home'],old['away'],obs['home'],obs['away'])) and (obs['home']<old['home'] or obs['away']<old['away'])
        title='↩️ RETTIFICA PUNTEGGIO' if decrease else '⚽ PUNTEGGIO AGGIORNATO'
    state['last']=obs
    if obs['status'] in CLOSED:state['done']=True
    return title

def monitor():
    now=now_local();store=SportsStore(now.date().isoformat());state=store.read()
    if not state or state.get('done'):print('No sports live due');return
    if state.get('pending'):
        matches=[r for r in receipts(now) if r.get('sports_live',{}).get('token')==state['announcement']['token'] and r.get('message_id')]
        if not matches:print('No confirmed announcement; no monitoring');return
        state['source_message_id']=min(r['message_id'] for r in matches);state['pending']=False;store.write(state)
    if state.get('outbox'):raise RuntimeError('Delivery uncertain; reconcile before retry')
    p=state['pick'];start=moment(p['kickoff'])
    if start>now+timedelta(minutes=20) or now-start>timedelta(hours=5):return
    deadline=time.monotonic()+185*60
    while time.monotonic()<deadline:
        if now_local()<start:time.sleep(30);continue
        try:
            rows=api('fixtures?id='+p['id'])
            if len(rows)!=1 or str(rows[0]['fixture']['id'])!=p['id']:raise ValueError('Fixture mismatch')
            obs=observation(rows[0])
        except Exception as e:
            print('Provider temporarily unavailable',type(e).__name__,flush=True);time.sleep(60);continue
        if obs:
            title=advance(state,obs,time.time());store.write(state)
            if title:
                score='–'.join(str(obs[k]) if obs[k] is not None else '—' for k in ('home','away'))
                phase=obs['status'] if obs['status'] in CLOSED|{'HT'} else (str(obs['minute'])+'′' if obs['minute'] is not None else obs['status'])
                text=f"{title}\n\n{p['country']} · {p['league']}\n{p['match']}\n{score} · {phase}\n\nFonte API-Football · Aggiornamento sportivo"
                state['outbox']={'text':text};store.write(state)
                msg=telegram('sendMessage',{'chat_id':CHANNEL,'text':text,'reply_parameters':json.dumps({'message_id':state['source_message_id'],'allow_sending_without_reply':True})})
                assert msg.get('message_id') and msg.get('chat',{}).get('username','').lower()==CHANNEL[1:]
                state['receipts'].append(msg['message_id']);state.pop('outbox');store.write(state)
                print('Confirmed sports update',msg['message_id'],flush=True)
            if state.get('done'):return
        time.sleep(60)
if __name__=='__main__':{'dispatch':dispatch,'monitor':monitor}[sys.argv[1]]()
