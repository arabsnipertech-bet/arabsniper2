"""Sports editorial selection independent of betting markets, prices and tiers."""
import math
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
ROME=ZoneInfo('Europe/Rome')
MAJOR={('England','Premier League'),('Spain','La Liga'),('Italy','Serie A'),('Germany','Bundesliga'),('France','Ligue 1')}
NEXT={('Netherlands','Eredivisie'),('Portugal','Primeira Liga'),('Belgium','Jupiler Pro League'),('Turkey','Süper Lig'),('Brazil','Serie A'),('Argentina','Liga Profesional Argentina'),('USA','Major League Soccer'),('Japan','J1 League')}
def moment(v):
    try:
        d=datetime.fromisoformat(v.replace('Z','+00:00'));return d if d.tzinfo else None
    except (ValueError,TypeError,AttributeError):return None
def number(v):return type(v) in (int,float) and math.isfinite(v) and v>=0
def priority(f):
    pair=(f.get('country'),f.get('league'))
    return 0 if pair in MAJOR else 1 if pair in NEXT else 2

def model_probabilities(assessment):
    result={}
    for row in (assessment.get('matrix') or {}).get('rows',[]):
        spec=row.get('spec') or {};value=row.get('statistical_pct')
        if not number(value) or value>100 or spec.get('period',row.get('period'))!='ft':continue
        kind,side,line=spec.get('kind'),spec.get('side'),spec.get('line')
        key=side if kind=='result' and side in ('1','x','2') else 'over25' if kind in ('total','asian_total') and side=='over' and line==2.5 else 'gg' if kind=='btts' and side=='yes' else None
        if key and key not in result:result[key]=value
    return result
def probability_lines(p):
    v=p.get('probabilities') or {}
    def pct(key):return f"{v[key]:.1f}%".replace('.',',') if number(v.get(key)) and v[key]<=100 else 'n.d.'
    return [f"Prob. modello FT: 1 {pct('1')} · X {pct('x')} · 2 {pct('2')}",
            f"Prob. modello FT: Over 2,5 {pct('over25')} · GG {pct('gg')}"]

def select(board,now,start,end,excluded=()):
    updated=moment(board.get('updated_at'))
    if board.get('date')!=now.astimezone(ROME).date().isoformat() or not updated or not timedelta(0)<=now-updated<=timedelta(minutes=90):return []
    rows={}
    for m in board.get('matches',[]):
        f=m.get('fixture') or {};a=m.get('multimarket') or {};r=a.get('rates') or {};profiles=a.get('profiles') or {}
        fid=str(f.get('fixture_id') or '');kick=moment(f.get('kickoff'));at=moment(a.get('captured_at'))
        if not fid.isdigit() or fid in excluded or not kick or not now<kick or not start<=kick<=end or f.get('status_short')!='NS' or f.get('result'):continue
        if not at or not timedelta(0)<=now-at<=timedelta(minutes=90) or not f.get('match') or not f.get('league') or not f.get('country'):continue
        if not all(number(r.get(s+'_ft')) for s in ('home','away')):continue
        sample={s:(profiles.get(s,{}).get('for_ft',{}).get('sample') or 0) for s in ('home','away')}
        if min(sample.values())<4:continue
        detail={}
        for side in ('home','away'):
            p=profiles.get(side,{})
            ht=r.get(side+'_ht')
            detail[side]=dict(ft=r[side+'_ft'],pt=ht if number(ht) and ht<=r[side+'_ft'] and (p.get('for_ht',{}).get('sample') or 0)>=4 else None,
                scored=p.get('for_ft',{}).get('mean'),conceded=p.get('against_ft',{}).get('mean'),sample=sample[side])
        row=dict(id=fid,fixture_id=fid,match=f['match'],country=f['country'],league=f['league'],kickoff=f['kickoff'],captured_at=a['captured_at'],analysis=detail,probabilities=model_probabilities(a),league_priority=priority(f))
        if fid not in rows or at>moment(rows[fid]['captured_at']):rows[fid]=row
    return sorted(rows.values(),key=lambda p:(p['league_priority'],-min(x['sample'] for x in p['analysis'].values()),-moment(p['captured_at']).timestamp(),p['kickoff'],p['id']))[:4]
def fmt(v):return f'{v:.2f}'.replace('.',',') if number(v) else 'n.d.'
def caption(post):
    picks=post['picks'];lines=[f"📊 ArabSniper · Taccuino delle {post['slot']}"]
    if not picks:return lines[0]+'\nNessuna nuova scheda con dati sufficienti e aggiornati nella fascia prevista.'
    lines+=['Dalla pagina Gold: partite, statistiche e probabilità del modello nella grafica. Gol attesi: medie del modello, non risultati esatti. Medie segnati/subiti: dati storici del campione.']
    if len(picks)<4:lines.append(f'Disponibili {len(picks)} schede su 4: nessuna integrazione con dati insufficienti.')
    live=post.get('sports_live')
    if live:lines.append('📡 LIVE SPORTIVO DEL GIORNO: '+live['match']+' · '+moment(live['kickoff']).astimezone(ROME).strftime('%H:%M')+'. Seguiremo gol, intervallo e finale.')
    lines+=['Confrontate i dati e i loro limiti: campioni diversi e ipotesi del modello possono cambiare la lettura.','Nessuna indicazione di giocata. Orari italiani.']
    return '\n\n'.join(lines)
def render(post,path):
    from PIL import Image,ImageDraw
    from social_card import font,paragraph
    im=Image.new('RGB',(1080,1600),'#091925');d=ImageDraw.Draw(im)
    d.text((50,35),'ArabSniperBet',font=font(39,'bold'),fill='#F5F3EB')
    d.text((50,105),'IL TACCUINO DELLE PARTITE',font=font(39,'serif'),fill='#EAC070')
    d.text((50,165),post['day']+' · '+post['slot']+' · ORARI ITALIANI',font=font(23,'mono'),fill='#77DCD2')
    for i,p in enumerate(post['picks']):
        y=235+i*300;a=p['analysis'];h=a['home'];v=a['away']
        d.line((50,y,1030,y),fill='#425868',width=2)
        paragraph(d,50,y+18,p['country']+' · '+p['league']+' · '+moment(p['kickoff']).astimezone(ROME).strftime('%H:%M'),23,970,34)
        paragraph(d,50,y+65,p['match'],36,970,58,'bold')
        pt=f"{fmt(h['pt'])} / {fmt(v['pt'])}";st=f"{fmt(h['ft']-h['pt'] if h['pt'] is not None else None)} / {fmt(v['ft']-v['pt'] if v['pt'] is not None else None)}"
        text=f"Gol attesi casa / ospite: {fmt(h['ft'])} / {fmt(v['ft'])}\nPT: {pt} · ST: {st}\nMedie segnati/subiti: casa {fmt(h['scored'])}/{fmt(h['conceded'])} · ospite {fmt(v['scored'])}/{fmt(v['conceded'])}\nCampioni FT: {h['sample']} / {v['sample']} gare · dati {moment(p['captured_at']).astimezone(ROME):%H:%M}"
        for line_no,line in enumerate(text.split('\n')+probability_lines(p)):
            paragraph(d,50,y+125+line_no*29,line,22,970,28)
    if not post['picks']:paragraph(d,50,300,caption(post),42,960,700)
    paragraph(d,50,1460,'Gol attesi: stime, non certezze. PT/ST: primo/secondo tempo. Medie storiche FT su campioni disponibili; n.d. = dato insufficiente.',23,970,100)
    im.save(path,format='JPEG',quality=92)
