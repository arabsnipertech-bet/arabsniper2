"""Publish the reviewed one-off summary with a durable no-retry claim."""
import json,os,copy
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from telegram_live import telegram
from gold_storage import client
from lateral_matrix import settle_spec
from social_card import render

now=datetime.now(ZoneInfo('Europe/Rome'))
assert now.date().isoformat()=='2026-11-06','One-off closed'
r=json.loads(Path('verified/verified-gold.json').read_text())
assert r['date']=='2026-11-05' and r['ready']
assert r['source_archive_sha256']=='1fd69a80778a1e1cee5c9bf5a9e5b5a81f85b0b650b9432a784cbdae5187c42a'
rows=r['verified_winners'];chosen=[];seen=set()
for p in [*[x for x in rows if x['tier']=='Gold'],*rows]:
    if p['fixture_id'] in seen:continue
    seen.add(p['fixture_id']);chosen.append(p)
    if len(chosen)==10:break
assert [p['fixture_id'] for p in chosen]==['1602542','1636408','1636250','1636473','1549535','1632055','1648266','1636245','1636219','1636399']
response=requests.get('https://v3.football.api-sports.io/fixtures',params={'date':r['date'],'timezone':'Europe/Rome'},headers={'x-apisports-key':os.environ['API_SPORTS_KEY']},timeout=60)
response.raise_for_status();data=response.json();assert not data.get('errors')
fixtures={str(m['fixture']['id']):m for m in data['response']}
for p in chosen:
    m=fixtures[p['fixture_id']];ht=m['score']['halftime'];ft=m['score']['fulltime']
    result=dict(status=m['fixture']['status']['short'],home_ht=ht['home'],away_ht=ht['away'],home_ft=ft['home'],away_ft=ft['away'])
    assert result==p['result'],'Official score changed; stop publication'
    assert settle_spec(p['spec'],result)==(1,0)
    assert datetime.fromisoformat(p['captured_at'])<datetime.fromisoformat(p['kickoff'])
text=('✅ 10 ESITI CORRETTI DALLE ANALISI DEL 5 NOVEMBRE\n\n'
      'Una selezione verificata dall’area Gold: 1 proposta Gold e 9 analisi Studio, con le categorie indicate in grafica.\n\n'
      'Mercati e quote erano registrati prima del calcio d’inizio; risultati finali controllati su API-Football. Le quote mostrate sono quelle archiviate, non necessariamente quelle del successivo post Telegram.\n\n'
      'È una selezione di successi, non il bilancio completo della giornata. Le analisi Studio non sono selezioni qualificate Gold.\n\n'
      '🎁 Esplora l’area Gold gratis per 7 giorni: scrivi /prova in privato a @ArabSniperBot\n'
      'https://arabsniperbet.com/gold\n18+ · I risultati passati non garantiscono quelli futuri.')
assert len(text.encode('utf-16-le'))//2<=1024
visual=copy.deepcopy(r);visual['examples']=copy.deepcopy(chosen)
for p in visual['examples']:
    s=p['result'];p['match']+=f"  {s['home_ft']}–{s['away_ft']}"
    if p['spec']['period']=='ht':p['label']+=f" (PT {s['home_ht']}–{s['away_ht']})"
    if p['country']=='World':p['country']='UEFA'
post=dict(day=now.date().isoformat(),slot='EXTRA',picks=[],successes=visual,text=text)
render(post,'verified-card.jpg')
s3=client();bucket=os.environ['R2_BUCKET'];key='telegram/oneoff/verified-results-20261105.json'
receipt=dict(report_date=r['date'],examples=chosen,text=text,status='delivery_started',created_at=now.isoformat())
s3.put_object(Bucket=bucket,Key=key,Body=json.dumps(receipt,ensure_ascii=False).encode(),ContentType='application/json',IfNoneMatch='*')
with open('verified-card.jpg','rb') as f:
    message=telegram('sendPhoto',{'chat_id':'@arabsniper_signals','caption':text},{'photo':('verified-card.jpg',f,'image/jpeg')})
assert message.get('message_id') and message['chat']['username'].lower()=='arabsniper_signals'
receipt.update(status='sent',message_id=message['message_id'])
Path('receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
s3.put_object(Bucket=bucket,Key=key,Body=json.dumps(receipt,ensure_ascii=False).encode(),ContentType='application/json')
print('Confirmed Telegram message:',message['message_id'])
