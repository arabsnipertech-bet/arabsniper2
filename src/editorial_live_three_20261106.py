"""One-day sports score updates only; no markets, odds or bet settlement."""
import json,os,sys,time,hashlib,subprocess
from datetime import datetime
from zoneinfo import ZoneInfo
from gold_storage import client
from telegram_live import api,telegram
ROME=ZoneInfo('Europe/Rome')
DAY='2026-11-06'
IDS=('1551135','1565302','1559827')
NAMES={'1551135':'Septemvri Sofia – Lokomotiv Sofia','1565302':'UTA Arad – Petrolul Ploiești','1559827':'Vysočina Jihlava – Karviná'}
CLOSED={'FT','AET','PEN','CANC','ABD','AWD','WO'}
KEY='editorial/live-three-20261106/state.json'
def active():
    n=datetime.now(ROME)
    return n.date().isoformat()==DAY and 15<=n.hour<21
def snapshot(row):
    f=row['fixture'];g=row.get('goals') or {};status=f['status']['short']
    h,a=g.get('home'),g.get('away')
    if status!='NS' and status not in {'PST','CANC','ABD','AWD','WO'} and not all(type(v) is int and v>=0 for v in (h,a)):raise ValueError('Missing verified score')
    return dict(status=status,home=h,away=a,minute=f['status'].get('elapsed'),league=row['league']['name'],country=row['league']['country'])
def signature(x):return (x['status'],x['home'],x['away'])
def event(old,new):
    if new['status'] in CLOSED:return '🏁 RISULTATO FINALE' if new['status'] in {'FT','AET','PEN'} else 'ℹ️ AGGIORNAMENTO GARA'
    if new['status']=='HT':return '⏸ INTERVALLO'
    if old['status']=='NS' and new['status'] in {'1H','2H'}:return '🟠 PARTITA IN CORSO'
    if old['status']=='HT' and new['status']=='2H':return '▶️ SECONDO TEMPO'
    if (old['home'],old['away'])!=(new['home'],new['away']):
        decrease=all(type(v) is int for v in (old['home'],old['away'],new['home'],new['away'])) and (new['home']<old['home'] or new['away']<old['away'])
        return '↩️ RETTIFICA DEL PUNTEGGIO' if decrease else '⚽ PUNTEGGIO AGGIORNATO'
    return 'ℹ️ AGGIORNAMENTO GARA'
def line(fid,x):
    score='in attesa del calcio d’inizio' if x['status']=='NS' else str(x['home'] if x['home'] is not None else '—')+'–'+str(x['away'] if x['away'] is not None else '—')
    phase={'NS':'','HT':'intervallo','FT':'finale','AET':'dopo supplementari','PEN':'dopo rigori','PST':'rinviata','CANC':'annullata','ABD':'interrotta'}.get(x['status'],str(x['minute'])+'′' if x['minute'] is not None else x['status'])
    return NAMES[fid]+'\n'+x['country']+' · '+x['league']+'\n'+score+(' · '+phase if phase else '')
def dispatch():
    if not active() or os.getenv('FAST_BACKUP_DRY_RUN')=='true':return
    repo='arabsnipertech-bet/arabsniper2';wf='editorial-live-three-20261106.yml'
    raw=subprocess.check_output(['gh','api',f'repos/{repo}/actions/workflows/{wf}/runs?per_page=20'],text=True)
    if any(r['status'] in ('queued','in_progress','waiting','requested','pending') for r in json.loads(raw)['workflow_runs']):return
    subprocess.run(['gh','workflow','run',wf,'--repo',repo,'--ref','main'],check=True)
def monitor():
    if not active():return
    s3=client();bucket=os.environ['R2_BUCKET']
    try:state=json.loads(s3.get_object(Bucket=bucket,Key=KEY)['Body'].read())
    except Exception as e:
        if getattr(e,'response',{}).get('Error',{}).get('Code') not in ('NoSuchKey','404'):raise
        state={'fixtures':{},'sequence':0}
    def save():s3.put_object(Bucket=bucket,Key=KEY,Body=json.dumps(state).encode(),ContentType='application/json')
    def send_once(token,text):
        k='editorial/live-three-20261106/events/'+hashlib.sha256(token.encode()).hexdigest()+'.json'
        try:s3.put_object(Bucket=bucket,Key=k,Body=b'{"status":"started"}',ContentType='application/json',IfNoneMatch='*')
        except Exception as e:
            if getattr(e,'response',{}).get('Error',{}).get('Code') in ('PreconditionFailed','412'):return
            raise
        m=telegram('sendMessage',{'chat_id':'@arabsniper_signals','text':text,'reply_parameters':json.dumps({'message_id':80,'allow_sending_without_reply':True})})
        assert m['chat']['username'].lower()=='arabsniper_signals'
        s3.put_object(Bucket=bucket,Key=k,Body=json.dumps({'status':'sent','message_id':m['message_id']}).encode(),ContentType='application/json')
        print('Confirmed sports update',m['message_id'],flush=True)
    def read(fid):
        rows=api('fixtures?id='+fid)
        if len(rows)!=1 or str(rows[0]['fixture']['id'])!=fid:raise ValueError('Ambiguous fixture')
        return snapshot(rows[0])
    if not state['fixtures']:
        initial={fid:read(fid) for fid in IDS}
        state['fixtures']={fid:{'last':x,'closed':x['status'] in CLOSED} for fid,x in initial.items()}
        save()
    if not state.get('announced'):
        send_once('intro','📡 IL TACCUINO · MONITORAGGIO SPORTIVO\n\n'+ '\n\n'.join(line(fid,state['fixtures'][fid]['last']) for fid in IDS)+'\n\nSeguiamo inizio gara, variazioni del punteggio, intervallo e finale. Fonte API-Football; possibili ritardi o rettifiche del fornitore.')
        state['announced']=True;save()
    started=time.monotonic()
    while active() and time.monotonic()-started<280*60:
        if all(x.get('closed') for x in state['fixtures'].values()):
            print('All three fixtures completed',flush=True);return
        for fid in IDS:
            item=state['fixtures'][fid]
            if item.get('closed'):continue
            try:new=read(fid)
            except Exception as e:
                print('Provider unavailable',fid,type(e).__name__,flush=True);continue
            old=item['last']
            if signature(new)==signature(old):
                item.pop('candidate',None);continue
            candidate=item.get('candidate')
            if not candidate or signature(candidate['value'])!=signature(new):
                item['candidate']={'value':new,'since':time.time()};save();continue
            if time.time()-candidate['since']<45:continue
            state['sequence']+=1
            token=f"{fid}:{state['sequence']}"
            text=event(old,new)+'\n\n'+line(fid,new)+'\n\nFonte API-Football · Aggiornamento sportivo'
            # Save the accepted observation before dispatch: ambiguous delivery is never blindly retried.
            item.update(last=new,closed=new['status'] in CLOSED);item.pop('candidate',None);save()
            try:send_once(token,text)
            except Exception as e:print('Delivery unconfirmed; inspect before retry',fid,type(e).__name__,flush=True)
        save();time.sleep(60)
if __name__=='__main__':
    dispatch() if len(sys.argv)>1 and sys.argv[1]=='dispatch' else monitor()
