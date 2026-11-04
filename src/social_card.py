"""Shared 4:5 matchday artwork. Facts come from the publication receipt."""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
INK = '#091824'
WHITE = '#F6F4E9'
MUTED = '#ADC1C6'
ROME = ZoneInfo('Europe/Rome')


def font(size, style='regular'):
    family = {'regular':'Sans', 'bold':'Sans-Bold', 'mono':'SansMono', 'serif':'Serif'}[style]
    return ImageFont.truetype(f'/usr/share/fonts/truetype/dejavu/DejaVu{family}.ttf', size)


def paragraph(draw, x, y, value, size, width, height, style='regular', color=WHITE):
    for point in range(size, 11, -1):
        face = font(point, style)
        lines = ['']
        for word in value.split():
            trial = (lines[-1]+' '+word).strip()
            if lines[-1] and face.getlength(trial)>width:
                lines.append(word)
            else:
                lines[-1] = trial
        step = int(point*1.24)
        if step*len(lines)<=height and max(face.getlength(line) for line in lines)<=width:
            break
    for line in lines:
        draw.text((x,y),line,font=face,anchor='lt',fill=color)
        y += step
    return y


def render(post, path):
    picks = post.get('picks', [])
    slot = post.get('slot','')
    promo = not picks and slot in ('12:45','17:15','22:00')
    edu = slot=='16:45'
    accent = '#EAC070' if promo or picks else '#77DCD2' if edu else '#A8BFD8'
    image = Image.open(ROOT/'assets/social/stadium.jpg').convert('RGB').resize((1080,1350),Image.Resampling.LANCZOS)
    draw = ImageDraw.Draw(image)
    draw.rectangle((0,0,1080,94),fill=INK)
    draw.text((54,26),'ArabSniperBet',font=font(39,'bold'),fill=WHITE,anchor='lt')
    draw.text((1025,37),'ANALISI CALCISTICHE',font=font(20,'mono'),fill=MUTED,anchor='rt')
    draw.rectangle((55,520,113,526),fill=accent)
    label = 'AREA GOLD' if promo else 'QUADERNO DI ANALISI' if edu else 'SELEZIONI PREPARTITA' if picks else 'AGGIORNAMENTO'
    draw.text((55,554),label,font=font(25,'mono'),fill=accent,anchor='lt')
    day = post.get('day','')
    stamp = datetime.fromisoformat(day).strftime('%d.%m.%Y') if day else ''
    draw.text((1024,556),f'{stamp}   /   {slot}',font=font(21,'mono'),fill=MUTED,anchor='rt')
    if picks:
        draw.text((55,617),'La singola' if len(picks)==1 else 'Le singole',font=font(77,'serif'),fill=WHITE,anchor='lt')
        step = min(272,510//len(picks))
        for n,p in enumerate(picks):
            y=734+n*step
            shade={'platinum':'#D2DCEC','gold':'#EAC070','blu':'#85BAFF'}.get(p['tier'].lower(),accent)
            draw.line((55,y,1024,y),fill='#3B505C',width=2)
            start=datetime.fromisoformat(p['kickoff']).astimezone(ROME)
            draw.text((55,y+18),f"{n+1:02d}   /   {p['tier'].upper()}   /   {start:%H:%M}",font=font(23,'mono'),fill=shade,anchor='lt')
            paragraph(draw,55,y+58,p['match'],54 if len(picks)==1 else 38,757,step-107,'bold')
            paragraph(draw,55,y+step-46,p['label'],32,760,42,color=MUTED)
            draw.text((1025,y+60),f"{p['odd']:.2f}".replace('.',','),font=font(54,'serif'),fill=shade,anchor='rt')
            draw.text((1025,y+121),'QUOTA',font=font(17,'mono'),fill=MUTED,anchor='rt')
    elif edu:
        parts=post['text'].split('\n\n')
        title=parts[1].lstrip('🔎 ').capitalize().replace('oci','OCI')
        y=paragraph(draw,55,625,title,76,955,215,'serif')
        paragraph(draw,58,y+48,parts[2],41,940,380)
    elif promo:
        draw.text((50,616),'7 giorni',font=font(134,'serif'),fill=WHITE,anchor='lt')
        draw.text((58,797),'ACCESSO GOLD GRATUITO',font=font(43,'bold'),fill=accent,anchor='lt')
        paragraph(draw,58,900,'Esplora le partite, le quote e le analisi del modello.',46,890,160)
        draw.rounded_rectangle((55,1104,1005,1195),radius=12,fill=accent)
        draw.text((85,1132),'Scrivi /prova a @ArabSniperBot',font=font(36,'bold'),fill=INK,anchor='lt')
    else:
        paragraph(draw,55,650,'Controllo di fascia',76,940,220,'serif')
        paragraph(draw,58,870,post['text'],39,940,295)
    draw.line((55,1252,1024,1252),fill='#50616A',width=2)
    draw.text((55,1280),'arabsniperbet.com',font=font(26,'bold'),fill=WHITE,anchor='lt')
    draw.text((1024,1285),'18+   /   ANALISI SPORTIVA',font=font(19,'mono'),fill=MUTED,anchor='rt')
    image.save(path,format='JPEG',quality=90,optimize=True)
