"""Post three independent, explainable public-feed spunti at 13 Rome. No faux Gold claims."""
import json
import math
import os
import subprocess
from datetime import datetime, timedelta, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram_gold_promo import telegram, posted_before

ROME = ZoneInfo("Europe/Rome")
CHANNEL = "@arabsniper_signals"
UTC_CRONS = {"0 11 * * *", "13 11 * * *", "0 12 * * *", "13 12 * * *"}
MARKETS = {"over15_ht", "over25_ht", "gg_ht", "over25_ft", "over35_ft"}

def instant(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else None
    except (ValueError, TypeError):
        return None

def eligible(now, event, cron, oneoff=False, recovery_slot=None):
    now = now.astimezone(ROME)
    due = datetime.combine(now.date(), time(13), tzinfo=ROME)
    if oneoff:
        return now.date().isoformat()=="2026-11-03" and timedelta(0)<=now-due<timedelta(minutes=90)
    allowed=(event=="schedule" and cron in UTC_CRONS) or (
        event=="workflow_dispatch" and recovery_slot=="13:00")
    return allowed and timedelta(0)<=now-due<timedelta(minutes=25)

def spunti(feed, now):
    """Only today's fresh public social projection; 3 distinct unstarted selections."""
    now = now.astimezone(ROME)
    if feed.get("date") != now.date().isoformat():
        return []
    observed=instant(feed.get("updated_at"))
    if not observed or not timedelta(0)<=now-observed<=timedelta(minutes=90):
        return []
    by_id = {str(x.get("fixture_id")):x for x in feed.get("selections",[]) if isinstance(x,dict)}
    found=[]
    seen=set()
    for public in feed.get("social_selections",[]):
        fid=str(public.get("fixture_id") or "")
        full=by_id.get(fid) or {}
        start=instant(public.get("kickoff"))
        captured=instant(public.get("captured_at"))
        quote_time=instant(public.get("provider_updated_at"))
        odd=public.get("odd")
        label=public.get("label")
        if (not fid.isdigit() or fid in seen or not start or not captured or not quote_time
            or not captured<=now<start or now-captured>timedelta(minutes=90)
            or now-quote_time>timedelta(hours=8) or quote_time>now
            or start.astimezone(ROME).date()!=now.date()
            or type(odd) not in (int,float) or not math.isfinite(odd) or odd<=1
            or not isinstance(label,str) or not label.strip() or not public.get("match")):
            continue
        # Prefer the exact public social-selection snapshot, including the fourth
        # Facebook candidate. The compact top-three homepage projection is fallback.
        def field(key):
            return public[key] if key in public else full.get(key)

        confirmed=field("market_confirmation")
        probability=field("statistical_pct")
        margin=field("margin_pct")
        first_odd=field("first_odd")
        delta=field("delta_pct")
        oci=field("oci")
        bookmaker=field("bookmaker")
        movement_confirmed=field("movement_confirmed")
        books=field("bookmakers_count")
        reasons=[]
        if type(probability) in (float,int) and math.isfinite(probability) and 0<=probability<=100:
            # A model-derived rate is not empirical accuracy or guaranteed success.
            reasons.append(f"Statistica del modello per questo mercato: {probability:.1f}%")
            implied=100/odd
            reasons.append(f"Probabilità implicita della quota: {implied:.1f}% (senza correzione dell'aggio)")
            reasons.append(f"Differenza modello/implicita: {probability-implied:+.1f} punti; valutazione da verificare")
        else:
            reasons.append("Stima statistica numerica non disponibile in questo snapshot")
        if (type(first_odd) in (float,int) and math.isfinite(first_odd) and first_odd>1
            and type(delta) in (float,int) and math.isfinite(delta)
            and abs(delta - 100*(odd/first_odd-1)) < 0.3):
            direction="in discesa" if delta< -0.05 else "in salita" if delta>0.05 else "stabile"
            reasons.append(f"Movimento verificato: prima quota {first_odd:.2f} → ora {odd:.2f} ({delta:+.1f}%, {direction})")
            if movement_confirmed is True:
                reasons.append("Movimento confermato dalle verifiche del motore")
            else:
                reasons.append("Variazione osservata, ma conferma del flusso NON disponibile")
        else:
            reasons.append("Movimento apertura/attuale non verificabile: non attribuiamo cali o rialzi")
        if type(oci) in (float,int) and math.isfinite(oci):
            reasons.append(f"OCI osservato: {oci:+.1f} (indice di mercato, non probabilità)")
        else:
            reasons.append("OCI non disponibile: nessuna conferma di direzione basata su OCI")
        if type(margin) in (float,int) and math.isfinite(margin) and 0<=margin<=100:
            reasons.append(f"Aggio/margine osservato: {margin:.1f}%")
        if isinstance(bookmaker,str) and bookmaker.strip():
            reasons.append(f"Quota osservata presso: {bookmaker.strip()}")
        if type(books) is int and books>0:
            reasons.append(f"Operatori nel campione: {books}")
        if confirmed == "unconfirmed":
            reasons.append("Segnale del flusso NON confermato; spunto di osservazione, non Gold certificato")
        elif confirmed == "confirmed":
            reasons.append("Conferma di mercato segnalata dal modello")
        else:
            reasons.append("Conferma ulteriore di mercato non disponibile")
        found.append(dict(fixture_id=fid,match=public["match"],kickoff=start,
                          odd=odd,label=label.strip(),quote_time=quote_time,
                          reasons=reasons))
        seen.add(fid)
        if len(found)==3:
            break
    return found

def message_for(items,now):
    if len(items)!=3:
        return None
    now=now.astimezone(ROME)
    lines=[f"🔎 ARABSNIPER · 3 SPUNTI DEL GIORNO · {now:%d/%m %H:%M}",
           "Tre analisi indipendenti dal feed pubblico, NON una multipla né certificazioni Platinum. Statistiche e quote sono osservazioni, non risultati futuri.",
           "Per ogni partita: statistica del modello, confronto con la quota e movimento di mercato solo quando verificabile.",""]
    for i,item in enumerate(items,1):
        kickoff=item["kickoff"].astimezone(ROME)
        quote_at=item["quote_time"].astimezone(ROME)
        lines.extend([f"{i}️⃣ {item['match']} · ore {kickoff:%H:%M}",
                      f"{item['label']} · quota {item['odd']:.2f} (rilevata {quote_at:%H:%M})"])
        lines += ["• "+why for why in item["reasons"]]
        lines.append("")
    lines.extend(["👑 Scopri le analisi Gold: https://arabsniperbet.com/gold",
                  "18+ · Gioca responsabilmente."])
    return "\n".join(lines)

def main():
    now=datetime.now(ROME)
    oneoff=os.environ.get("ALLOW_ONEOFF")=="true" and os.environ.get("GITHUB_EVENT_NAME")=="push"
    if not eligible(now,os.environ.get("GITHUB_EVENT_NAME"),
                    os.environ.get("GITHUB_EVENT_SCHEDULE"),oneoff=oneoff,
                    recovery_slot=os.environ.get("RECOVERY_SLOT")):
        print("No authorized timely 13:00 spunti slot")
        return
    name=f"tg-spunti-{now:%Y-%m-%d}-1300-sent"
    if posted_before(name):
        print("Spunti were already posted; no duplicate")
        return
    feed=json.loads(Path("data/arab_quote_homepage.json").read_text(encoding="utf-8"))
    selected=spunti(feed,now)
    if len(selected)!=3:
        print(f"Only {len(selected)} verified pre-match public selections; skip")
        return
    content=message_for(selected,now)
    token=os.environ.get("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        raise RuntimeError("GitHub TELEGRAM_BOT_TOKEN missing")
    bot=telegram("getMe",token)
    if bot.get("username","").lower()!="arabsniperbot":
        raise RuntimeError("Unexpected Telegram bot")
    channel=telegram("getChat",token,chat_id=CHANNEL)
    admin=telegram("getChatMember",token,chat_id=channel["id"],user_id=bot["id"])
    if (channel.get("type")!="channel" or channel.get("username","").lower()!=CHANNEL[1:]
        or admin.get("status")!="administrator" or not admin.get("can_post_messages")):
        raise RuntimeError("Wrong Telegram channel permissions")
    sent=telegram("sendMessage",token,chat_id=channel["id"],text=content,disable_web_page_preview="true")
    if sent.get("chat",{}).get("id")!=channel["id"]:
        raise RuntimeError("Unexpected Telegram posting destination")
    Path("spunti-receipt.json").write_text(json.dumps({
        "date":now.date().isoformat(),"message_id":sent["message_id"],
        "fixtures":[p["fixture_id"] for p in selected]}),encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"],"a",encoding="utf-8") as fh:
        print(f"artifact={name}",file=fh)
    print(f"Sent public spunti: https://t.me/arabsniper_signals/{sent['message_id']}")

if __name__=="__main__":
    main()
