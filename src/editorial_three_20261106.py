import json,os,copy
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from PIL import Image,ImageDraw
from social_card import font,paragraph
from gold_storage import client
from telegram_live import telegram
from facebook_page import graph,PAGE
now=datetime.now(ZoneInfo('Europe/Rome'))
assert now.date().isoformat()=='2026-11-06' and now.hour<16,'Pre-match publication window closed'
rows=json.loads(Path('editorial/editorial-data.json').read_text())
assert [x['id'] for x in rows]==['1551135','1565302','1559827']
for x in rows:
    assert x['gold_present'] and x['rates'] and x['status']['short']=='NS'
    r=requests.get('https://v3.football.api-sports.io/fixtures',params={'id':x['id']},headers={'x-apisports-key':os.environ['API_SPORTS_KEY']},timeout=30)
    r.raise_for_status();v=r.json();assert not v.get('errors') and len(v['response'])==1
    assert v['response'][0]['fixture']['status']['short']=='NS'
text="📚 IL TACCUINO DEGLI AMMINISTRATORI\nTre partite per leggere il modello · 6 novembre\n\n🇧🇬 Septemvri Sofia–Lokomotiv Sofia · 16:00\nBulgaria, First League\nGol medi stimati: casa 1,30 · ospite 1,62.\n\n🇷🇴 UTA Arad–Petrolul Ploiești · 16:30\nRomania, Liga I\nGol medi stimati: casa 1,62 · ospite 1,48.\n\n🇨🇿 Vysočina Jihlava–Karviná · 18:00\nRepubblica Ceca, FNL\nGol medi stimati: casa 1,68 · ospite 1,80.\n\n🔎 Come leggerli: sono i lambda sui 90 minuti del modello disponibile nell’area Gold, non gol già osservati né risultati previsti esatti. Poisson distribuisce queste medie su molti punteggi possibili; la qualità dipende dai dati e dalle ipotesi.\n\nDati del modello rilevati oggi alle 14:55–14:56; orari italiani. Rubrica di approfondimento calcistico: nessuna indicazione di giocata."
assert len(text.encode('utf-16-le'))//2<=1024
im=Image.new('RGB',(1080,1350),'#091925');d=ImageDraw.Draw(im)
d.text((55,45),'ArabSniperBet',font=font(40,'bold'),fill='#F5F3EB')
d.text((55,135),'IL TACCUINO',font=font(70,'serif'),fill='#EAC070')
d.text((55,220),'DEGLI AMMINISTRATORI',font=font(38,'bold'),fill='#F5F3EB')
d.text((55,295),'6 NOVEMBRE · OSSERVAZIONE STATISTICA',font=font(24,'mono'),fill='#77DCD2')
names=['Septemvri Sofia – Lokomotiv Sofia','UTA Arad – Petrolul Ploiești','Vysočina Jihlava – Karviná']
leagues=['Bulgaria · First League · 16:00','Romania · Liga I · 16:30','Repubblica Ceca · FNL · 18:00']
for i,x in enumerate(rows):
    y=380+i*235
    d.line((55,y,1025,y),fill='#425868',width=2)
    d.text((55,y+25),leagues[i],font=font(24,'mono'),fill='#77DCD2')
    paragraph(d,55,y+70,names[i],36,960,55,'bold')
    rates=x['rates'];s=('Gol medi stimati sui 90 minuti: '+format(rates['home_ft'],'.2f')+' casa / '+format(rates['away_ft'],'.2f')+' ospite').replace('.',',')
    paragraph(d,55,y+140,s,27,960,70)
paragraph(d,55,1120,'Lambda è una media del modello, non un risultato esatto. Dati Gold: oggi 14:55–14:56. Orari italiani.',28,960,110)
d.text((55,1275),'ANALISI CALCISTICA · NESSUNA GIOCATA CONSIGLIATA',font=font(22,'mono'),fill='#77DCD2')
im.save('editorial.jpg',quality=92)
s3=client();bucket=os.environ['R2_BUCKET'];receipt={'text':text,'verified_at':now.isoformat()}
for channel in ('telegram','facebook'):
    key='editorial/three-fixtures-20261106/'+channel+'.json'
    s3.put_object(Bucket=bucket,Key=key,Body=json.dumps({'status':'started'}).encode(),ContentType='application/json',IfNoneMatch='*')
    if channel=='telegram':
        with open('editorial.jpg','rb') as f:
            message=telegram('sendPhoto',{'chat_id':'@arabsniper_signals','caption':text},{'photo':('editorial.jpg',f,'image/jpeg')})
        assert message['chat']['username'].lower()=='arabsniper_signals'
        ident=message['message_id']
    else:
        assert graph('me?fields=id')['id']==PAGE
        with open('editorial.jpg','rb') as f:
            result=requests.post('https://graph.facebook.com/v25.0/'+PAGE+'/photos',headers={'Authorization':'Bearer '+os.environ['FACEBOOK_PAGE_ACCESS_TOKEN']},data={'message':text},files={'source':('editorial.jpg',f,'image/jpeg')},timeout=30)
        result.raise_for_status();ident=result.json()['post_id']
        assert graph(ident+'?fields=id')['id']==ident
    receipt[channel]=ident
    Path('editorial-receipt.json').write_text(json.dumps(receipt,ensure_ascii=False))
    s3.put_object(Bucket=bucket,Key=key,Body=json.dumps({'status':'sent','id':ident}).encode(),ContentType='application/json')
    print('Confirmed',channel,ident)
