"""Render original vector-style loop animations; no prediction data in assets."""
from pathlib import Path
from math import sin, cos, pi
from PIL import Image, ImageDraw, ImageFont

STATES = {'live':('LIVE IN CORSO','#FFAA32','SEGUIAMO LA SINGOLA'),
          'goal':('GOOOOLLL!','#F5CC70','IL PUNTEGGIO SI AGGIORNA'),
          'win':('ArabCassaaaaaa!','#5AE59B','SOGLIA RAGGIUNTA'),
          'lose':('XLoooseeeee','#FF6572','ESITO NON CENTRATO'),
          'penalty':('RIGORE SEGNATO!','#5AE59B','DAL DISCHETTO ALLA RETE'),
          'missed':('RIGORE SBAGLIATO','#FF6572','NESSUN GOL ASSEGNATO'),
          'notice':('VERIFICA LIVE','#FFAA32','AGGIORNAMENTO DEL FEED')}

def font(size,bold=True):
    return ImageFont.truetype('/usr/share/fonts/truetype/dejavu/DejaVuSans'+('-Bold' if bold else '')+'.ttf',size)

def centered(draw,text,y,size,color,width=580):
    while draw.textlength(text,font=font(size))>width:size-=1
    draw.text((320,y),text,font=font(size),fill=color,anchor='mm')

def frame(kind,n):
    title,color,subtitle=STATES[kind];t=n/32;im=Image.new('RGB',(640,640));d=ImageDraw.Draw(im)
    for y in range(640):
        d.line((0,y,640,y),fill=(7+int(5*y/640),20+int(14*y/640),33+int(14*y/640)))
    for x in range(-640,1000,100):d.line((320,200,x,640),fill='#1D3846',width=2)
    for y in range(380,640,60):d.line((0,y,640,y),fill='#1D3846',width=2)
    d.rounded_rectangle((24,24,616,616),radius=28,outline='#344D5A',width=2)
    centered(d,'ArabSniperBet',68,31,'#F3F2E9')
    centered(d,'LIVE MATCHDAY',110,15,'#94AFBD')
    pulse=1+0.05*sin(2*pi*t)
    r=int(76*pulse);cx,cy=320,258
    for j in range(3):
        rr=r+14*j+int(10*t)
        d.ellipse((cx-rr,cy-rr,cx+rr,cy+rr),outline=color if j==0 else '#294450',width=2)
    if kind in ('penalty','missed'):
        d.rectangle((237,210,403,303),outline='#F3F2E9',width=5)
        for x in range(249,403,19):d.line((x,214,x,299),fill='#48616D',width=1)
        for y in range(226,303,18):d.line((240,y,400,y),fill='#48616D',width=1)
        if kind=='penalty':
            by=int(320-62*min(t*2,1));bx=320+int(40*min(t*2,1))
            d.ellipse((bx-12,by-12,bx+12,by+12),fill='#F3F2E9',outline='#122430',width=3)
        else:
            d.line((284,223,356,291),fill=color,width=12)
            d.line((356,223,284,291),fill=color,width=12)
    elif kind in ('live','notice'):
        d.polygon([(320,207),(365,292),(275,292)],fill=color)
        centered(d,'!',266,52,'#10212D')
    elif kind=='goal':
        bx=320+int(38*sin(2*pi*t));by=250-int(16*sin(pi*t))
        d.ellipse((bx-39,by-39,bx+39,by+39),fill='#F5F3E9')
        vertices=[(bx+15*cos(2*pi*i/5-pi/2),by+15*sin(2*pi*i/5-pi/2)) for i in range(5)]
        d.polygon(vertices,fill='#142C3B')
    elif kind=='win':d.line([(281,256),(310,286),(361,226)],fill=color,width=15)
    else:
        d.line((290,228,350,288),fill=color,width=13);d.line((350,228,290,288),fill=color,width=13)
    if kind in ('goal','win'):
        for i in range(28):
            x=45+(i*83)%550;y=150+int((i*37+t*220)%350)
            d.rectangle((x,y,x+4,y+10),fill=color if i%2 else '#F3F2E9')
    centered(d,title,408,46+int(2*sin(2*pi*t)),color)
    centered(d,subtitle,461,19,'#F3F2E9')
    d.rounded_rectangle((95,510,545,552),radius=12,fill='#173440')
    centered(d,'SINGOLA GRATUITA · CANALE TELEGRAM',531,15,'#ADC3CC')
    centered(d,'arabsniperbet.com  ·  18+',586,17,'#94AFBD')
    return im

def render_all(folder='assets/telegram-live'):
    out=Path(folder);out.mkdir(parents=True,exist_ok=True)
    for kind in STATES:
        frames=[frame(kind,n) for n in range(32)]
        frames[0].save(out/(kind+'.gif'),save_all=True,append_images=frames[1:],duration=100,loop=0,optimize=True)
if __name__=='__main__':render_all()
