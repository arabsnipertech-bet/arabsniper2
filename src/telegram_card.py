"""Draw production Telegram image cards using only verified post.json fields."""
import json
import sys
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont

ROME=ZoneInfo("Europe/Rome")
BG=(6,16,26); GOLD=(238,191,72); WHITE=(247,248,251); GREEN=(40,177,123); DIM=(171,186,197)
FONT="/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
BOLD="/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

def font(size,bold=False):
    return ImageFont.truetype(BOLD if bold else FONT,size)

def truncate(draw,text,f,max_width):
    while text and draw.textbbox((0,0),text,font=f)[2]>max_width:
        text=text[:-1]
    return text if len(text)==0 else text.rstrip(" .") + ("…" if draw.textbbox((0,0),text,font=f)[2]>max_width else "")

def draw_card(post, path):
    picks=post.get("picks") or []
    if not picks:
        raise ValueError("No confirmed selection to render")
    if len(picks)>3:
        raise ValueError("Card supports at most three picks")
    im=Image.new("RGB",(1080,1350),BG); d=ImageDraw.Draw(im)
    d.rectangle((0,0,1080,20),fill=GOLD)
    d.text((72,70),"ArabSniperBet",fill=GOLD,font=font(78,True))
    d.text((75,170),"FOOTBALL SIGNALS  •  TELEGRAM",fill=DIM,font=font(30,True))
    d.rounded_rectangle((70,240,1010,355),radius=22,fill=(18,40,49),outline=GOLD,width=4)
    d.text((115,272),"TRIPLA DEL MODELLO" if len(picks)==3 else "PRONOSTICO SINGOLO",
           fill=WHITE,font=font(46,True))
    y=415
    for i,p in enumerate(picks):
        h=205 if len(picks)==3 else 350
        d.rounded_rectangle((72,y,1008,y+h),radius=26,fill=(15,34,43),outline=GREEN,width=3)
        dt=datetime.fromisoformat(p["kickoff"]).astimezone(ROME)
        d.text((105,y+26),f"{dt:%d/%m  %H:%M}   •   EVENTO {i+1}",fill=GOLD,font=font(32,True))
        match=p.get("match","")
        if len(picks)==1 and " - " in match:
            parts=match.split(" - ",1)
            d.text((105,y+103),truncate(d,parts[0],font(52,True),870),fill=WHITE,font=font(52,True))
            d.text((105,y+170),"VS  "+truncate(d,parts[1],font(52,True),770),fill=WHITE,font=font(52,True))
            d.text((105,y+270),truncate(d,p["label"],font(40,True),670),fill=GREEN,font=font(40,True))
            d.text((825,y+270),f"{p['odd']:.2f}",fill=GOLD,font=font(43,True))
        else:
            d.text((105,y+85),truncate(d,match,font(39,True),860),fill=WHITE,font=font(39,True))
            d.text((105,y+147),truncate(d,p["label"],font(32,True),665),fill=GREEN,font=font(32,True))
            d.text((830,y+147),f"{p['odd']:.2f}",fill=GOLD,font=font(38,True))
        y+=h+20
    if len(picks)==3:
        total=1
        for p in picks: total*=p["odd"]
        d.text((105,1133),f"QUOTA COMBINATA INDICATIVA  {total:.2f}",fill=GOLD,font=font(33,True))
    d.rectangle((0,1243,1080,1350),fill=(12,45,43))
    d.text((66,1260),"arabsniperbet.com/gold",font=font(31,True),fill=GOLD)
    d.text((66,1302),"t.me/arabsniper_signals  •  Analisi, non garanzie",font=font(23),fill=WHITE)
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    im.save(path,format="PNG",optimize=True)
    return path

if __name__=="__main__":
    draw_card(json.loads(Path(sys.argv[1]).read_text()),sys.argv[2])
