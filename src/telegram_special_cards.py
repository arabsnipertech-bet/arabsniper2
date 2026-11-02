"""Render special community-selected multipla cards; no bookmaker or stakes."""
import json,sys
from pathlib import Path
from PIL import Image,ImageDraw,ImageFont

FONT="/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
BOLD="/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
def font(n,bold=False):return ImageFont.truetype(BOLD if bold else FONT,n)
def fitted(draw,text,size,max_width):
    while size>24 and draw.textbbox((0,0),text,font=font(size,True))[2]>max_width:size-=2
    return font(size,True)
def render(ticket,path):
    n=len(ticket["picks"])
    if n not in (2,4):raise ValueError("Special card supports exactly 2 or 4 picks")
    bg=(7,17,31); gold=(241,193,73); light=(245,249,255); green=(56,201,155)
    im=Image.new("RGB",(1080,1350),bg);d=ImageDraw.Draw(im)
    # Stadium spotlight motif without any third-party branding.
    for i in range(22):
        c=(11+i//4,27+i//3,44+i//2)
        d.arc((-500+i*12,50-i*15,1550-i*12,1250+i*15),190,360,fill=c,width=3)
    d.rectangle((0,0,1080,18),fill=gold)
    d.text((65,52),"ArabSniperBet",font=font(79,True),fill=gold)
    d.text((70,157),"FOOTBALL SIGNALS   /   TELEGRAM",font=font(29,True),fill=light)
    d.rounded_rectangle((60,227,1020,341),radius=25,fill=(20,50,62),outline=gold,width=4)
    d.text((93,261),"MULTIPLA SPECIALE  /  "+str(n)+" EVENTI",font=font(43,True),fill=light)
    d.text((70,374),"03 NOV 2026   •   ORE 06:00 (ITALIA)",font=font(31,True),fill=green)
    title=ticket["market"]
    d.text((70,428),title,font=font(42,True),fill=gold)
    top=505
    h=155 if n==4 else 270
    gap=18 if n==4 else 30
    for i,p in enumerate(ticket["picks"]):
        y=top+i*(h+gap)
        d.rounded_rectangle((62,y,1018,y+h),radius=22,fill=(16,40,52),outline=(56,111,113),width=3)
        d.text((87,y+16),f"EVENTO {i+1}",font=font(24,True),fill=green)
        if n==2:
            d.text((87,y+60),p["home"],font=fitted(d,p["home"],46,660),fill=light)
            d.text((87,y+132),"VS "+p["away"],font=fitted(d,"VS "+p["away"],46,650),fill=light)
            d.text((87,y+215),p["label"],font=font(26,True),fill=green)
            d.text((818,y+104),f'{p["odd"]:.2f}',font=font(56,True),fill=gold)
        else:
            d.text((87,y+52),p["home"],font=fitted(d,p["home"],36,685),fill=light)
            d.text((87,y+103),"VS "+p["away"],font=fitted(d,"VS "+p["away"],32,675),fill=light)
            d.text((830,y+74),f'{p["odd"]:.2f}',font=font(47,True),fill=gold)
    odd=1
    for p in ticket["picks"]:odd*=p["odd"]
    y=1135
    d.rounded_rectangle((70,y,1010,y+93),radius=21,fill=(28,55,54),outline=gold,width=3)
    d.text((99,y+25),"QUOTA TOTALE INDICATIVA",font=font(28,True),fill=light)
    d.text((805,y+13),f"{odd:.2f}",font=font(54,True),fill=gold)
    d.text((71,1250),"t.me/arabsniper_signals  •  arabsniperbet.com",font=font(27,True),fill=light)
    d.text((71,1293),"Proposte condivise • Non certificazione Gold/Platinum • 18+",font=font(20),fill=green)
    im.save(path,"PNG",optimize=True)
if __name__=="__main__":render(json.loads(Path(sys.argv[1]).read_text()),sys.argv[2])
