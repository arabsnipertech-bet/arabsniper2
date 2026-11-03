"""Publish evidence-led Telegram content from frozen picks and official audits only."""
import json
import math
import os
import subprocess
from datetime import datetime, timedelta, time
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram_gold_promo import telegram, posted_before

ROME=ZoneInfo("Europe/Rome")
CHANNEL="@arabsniper_signals"
SLOTS={"45 10 * * *":"audit","45 16 * * *":"market"}
LABELS={"over15_ht":"Over 1,5 PT","over25_ht":"Over 2,5 PT","gg_ht":"Goal PT","over25_ft":"Over 2,5 FT","over35_ft":"Over 3,5 FT"}


def choose_slot(now,event,cron):
    if event!="schedule" or cron not in SLOTS:
        return None
    hour,minute=map(int,cron.split()[:2])
    due=datetime.combine(now.date(),time(hour,minute),tzinfo=ROME)
    if not timedelta(0)<=now-due<=timedelta(minutes=35):
        return None
    return SLOTS[cron]


def audit_message(report):
    """Show the entire verified posted population, not just the winners."""
    data=report.get("selections_total") or {}
    total,won,lost,pending=(data.get(k) for k in ("total","won","lost","pending"))
    if not all(type(x) is int and x>=0 for x in (total,won,lost,pending)) or total!=won+lost+pending or total==0:
        return None
    settled=won+lost
    day=report.get("date")
    if not isinstance(day,str) or len(day)!=10:
        return None
    lines=[f"📊 ARABSNIPER · RISCONTRI TRASPARENTI {day}",
           "Solo pronostici effettivamente pubblicati e confrontati con risultati ufficiali.",
           f"Selezioni: {total} | ✅ Vinte: {won} | ❌ Perse: {lost} | ⏳ Da verificare: {pending}"]
    if settled:
        lines.append(f"Riscontro sui {settled} esiti chiusi: {won/settled*100:.1f}%")
    pnl=data.get("theoretical_pnl")
    if settled and isinstance(pnl,(int,float)) and math.isfinite(pnl):
        lines.append(f"Saldo teorico (1 unità/selezione): {pnl:+.2f} unità, senza commissioni")
    ticket=report.get("triple") or {}
    if ticket.get("status") in ("won","lost","pending"):
        verdict={"won":"✅ centrata","lost":"❌ non centrata","pending":"⏳ in attesa"}[ticket["status"]]
        lines.append("Tripla ore 13: "+verdict)
    lines+=["Non nascondiamo le selezioni non riuscite. Le quote e i risultati passati non garantiscono risultati futuri.",
            "🎁 Gold 7 giorni senza carta, addebiti o rinnovo: https://t.me/ArabSniperBot",
            "18+ · Gioca responsabilmente."]
    return "\n".join(lines)


def market_message(selections):
    valid=[]
    for p in selections:
        delta=p.get("delta_pct")
        oci=p.get("oci")
        odd=p.get("odd")
        if (not all(type(x) in (int,float) and math.isfinite(x) for x in (delta,oci,odd))
            or odd<=1 or not p.get("match") or p.get("market") not in LABELS
            or not p.get("bookmaker") or abs(delta)>95 or abs(oci)>1000):
            continue
        valid.append(p)
    if not valid:
        return None
    # Reuse existing Gold/Platinum qualification: never upgrade a non-eligible match.
    p=max(valid,key=lambda x:(x["source_tier"]=="platinum",abs(x["delta_pct"]),x["oci"]))
    movement=("in calo" if p["delta_pct"]<0 else "in aumento" if p["delta_pct"]>0 else "stabile")
    reasons=[f"Movimento quota {movement}: {p['delta_pct']:+.1f}%",
             f"Indice OCI del modello: {p['oci']:+.1f}",
             f"Operatore rilevato: {p['bookmaker']}"]
    if p.get("market_confirmed"):
        reasons.append("Conferma di mercato segnalata dal modello")
    if p.get("stable"):
        reasons.append("Segnale di stabilità presente")
    kickoff=datetime.fromisoformat(p["kickoff"]).astimezone(ROME).strftime("%H:%M")
    lines=[f"🔎 ARABSNIPER · PERCHÉ QUESTA ANALISI ({p['source_tier'].upper()})",
           str(p["match"]),f"{LABELS[p['market']]} · Quota osservata {p['odd']:.2f} · Inizio {kickoff}",
           "Motivazioni basate sul dato registrato:"]
    lines.extend("• "+x for x in reasons)
    lines+=["I movimenti delle quote descrivono il mercato: non assicurano l'esito.",
            "🎁 Prova Gold 7 giorni, senza carta o rinnovo: https://t.me/ArabSniperBot",
            "18+ · Gioca responsabilmente."]
    return "\n".join(lines)


def load_audit(day):
    repo=os.environ["GITHUB_REPOSITORY"]
    res=json.loads(subprocess.check_output(
        ["gh","api",f"repos/{repo}/actions/artifacts?per_page=100"],text=True))
    prefix=f"telegram-audit-{day}-"
    candidates=[a for a in res.get("artifacts",[]) if a["name"].startswith(prefix) and not a["expired"]]
    if not candidates:
        print("No audited report: do not publish unverifiable results")
        return None
    item=max(candidates,key=lambda a:a.get("created_at",""))
    path=Path("evidence-audit")
    path.mkdir(exist_ok=True)
    subprocess.check_call(["gh","run","download",str(item["workflow_run"]["id"]),
                           "-n",item["name"],"-D",str(path)])
    return json.loads((path/"report.json").read_text(encoding="utf-8"))


def main():
    now=datetime.now(ROME)
    slot=choose_slot(now,os.environ.get("GITHUB_EVENT_NAME"),os.environ.get("GITHUB_EVENT_SCHEDULE"))
    if not slot:
        print("Not an eligible scheduled execution; no publication")
        return
    name=f"telegram-evidence-{now:%Y-%m-%d}-{slot}-sent"
    if posted_before(name):
        print("Already sent this slot; no publication")
        return
    if slot=="audit":
        report=load_audit((now.date()-timedelta(days=1)).isoformat())
        message=audit_message(report) if report else None
    else:
        from telegram_private_feed import fetch_current_private
        message=market_message(fetch_current_private(now))
    if not message:
        print("Verified inputs insufficient; skip publication")
        return
    token=os.environ.get("TELEGRAM_BOT_TOKEN","").strip()
    if not token:
        raise RuntimeError("Missing Telegram bot token in GitHub secrets")
    bot=telegram("getMe",token)
    if bot.get("username","").lower()!="arabsniperbot":
        raise RuntimeError("Unexpected bot")
    channel=telegram("getChat",token,chat_id=CHANNEL)
    if channel.get("type")!="channel" or channel.get("username","").lower()!=CHANNEL[1:]:
        raise RuntimeError("Unexpected channel")
    admin=telegram("getChatMember",token,chat_id=channel["id"],user_id=bot["id"])
    if admin.get("status")!="administrator" or not admin.get("can_post_messages"):
        raise RuntimeError("Missing posting permissions")
    sent=telegram("sendMessage",token,chat_id=channel["id"],text=message,
                  disable_web_page_preview="true")
    if sent.get("chat",{}).get("id")!=channel["id"]:
        raise RuntimeError("Wrong destination")
    Path("evidence-receipt.json").write_text(json.dumps({"slot":slot,"date":now.date().isoformat(),
                    "message_id":sent["message_id"]}),encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"],"a",encoding="utf-8") as f:
        print(f"artifact={name}",file=f)
    print(f"Evidence published: https://t.me/arabsniper_signals/{sent['message_id']}")


if __name__=="__main__":
    main()
