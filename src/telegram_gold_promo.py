"""Isolated Telegram promotion sender. Does not alter the predictions pipeline."""
import json
import os
import subprocess
from datetime import datetime, timedelta, time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
CHANNEL = "@arabsniper_signals"
SLOTS = {"45 12 * * *": "12:45", "15 17 * * *": "17:15", "15 21 * * *": "21:15"}
MESSAGES = (
    "🎁 ARABSNIPERBET GOLD · 7 GIORNI GRATIS!\n\n"
    "Prova l'accesso completo all'Area Gold per 7 giorni: nessuna carta di credito, "
    "nessun addebito e nessun rinnovo automatico.\n\n"
    "👉 https://t.me/ArabSniperBot\n"
    "Apri il bot in privato, scrivi /prova e verifica la tua email con il codice ricevuto.\n\n"
    "Una sola prova gratuita per email/account Telegram. 18+ · Gioca responsabilmente.",
    "⚽ Vuoi scoprire le analisi Gold di ArabSniperBet?\n\n"
    "🎁 Ora puoi accedere per 7 giorni gratuitamente, senza carta, senza addebiti "
    "e senza rinnovo automatico. Nessun limite aggiuntivo di utilizzo Gold durante la prova.\n\n"
    "✅ Avvia @ArabSniperBot: https://t.me/ArabSniperBot\n"
    "Invia /prova, conferma la tua email e accedi all'Area Gold.\n\n"
    "L'offerta è utilizzabile una sola volta per email/account Telegram. 18+ · Gioca responsabilmente.",
    "🎁 GOLD GRATIS PER 7 GIORNI · ARABSNIPERBET\n\n"
    "Analisi e selezioni Gold con accesso completo per 7 giorni. "
    "Non devi inserire una carta di credito, non paghi nulla e la prova termina "
    "senza rinnovo automatico.\n\n"
    "👉 https://t.me/ArabSniperBot — scrivi /prova in privato.\n"
    "Conferma l'email con il codice ricevuto, poi accedi a "
    "https://arabsniperbet.com/gold-dashboard-live.html\n\n"
    "Una prova per email/account Telegram. 18+ · Le analisi non garantiscono risultati."
)


def selected(now, event, schedule):
    """Never send on push/manual events; skip delayed schedules."""
    if event != "schedule" or schedule not in SLOTS:
        return None
    slot = SLOTS[schedule]
    h, m = map(int, slot.split(":"))
    expected = datetime.combine(now.date(), time(h, m), tzinfo=ROME)
    if not timedelta(0) <= now - expected <= timedelta(minutes=45):
        return None
    return slot


def name_for(day, slot):
    return f"telegram-gold-promo-{day}-{slot.replace(':', '')}-sent"


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True, stderr=subprocess.DEVNULL)


def posted_before(name):
    repo = os.environ["GITHUB_REPOSITORY"]
    response = json.loads(gh("api", f"repos/{repo}/actions/artifacts?name={name}&per_page=100"))
    return any(a["name"] == name and not a["expired"] for a in response.get("artifacts", []))


def telegram(method, token, **fields):
    req = Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=urlencode(fields).encode("utf-8"),
        method="POST",
    )
    try:
        with urlopen(req, timeout=20) as response:
            payload = json.load(response)
    except Exception as exc:
        raise RuntimeError(f"Telegram {method} failed; token withheld") from None
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} rejected; token withheld")
    return payload["result"]


def main():
    now = datetime.now(ROME)
    slot = selected(now, os.environ.get("GITHUB_EVENT_NAME"), os.environ.get("GITHUB_EVENT_SCHEDULE"))
    if slot is None:
        print("Not a timely scheduled promotion: no publication")
        return
    day = now.date().isoformat()
    artifact = name_for(day, slot)
    if posted_before(artifact):
        print("This promotion has already been recorded; skipping")
        return
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN GitHub Actions secret")
    bot = telegram("getMe", token)
    if bot.get("username", "").lower() != "arabsniperbot":
        raise RuntimeError("Wrong Telegram bot identity")
    channel = telegram("getChat", token, chat_id=CHANNEL)
    member = telegram("getChatMember", token, chat_id=channel["id"], user_id=bot["id"])
    if (channel.get("type") != "channel" or
        channel.get("username", "").lower() != CHANNEL[1:] or
        member.get("status") != "administrator" or
        not member.get("can_post_messages")):
        raise RuntimeError("Bot is not authorized to post in expected channel")
    index = list(SLOTS.values()).index(slot)
    sent = telegram("sendMessage", token, chat_id=channel["id"], text=MESSAGES[index],
                    disable_web_page_preview="true")
    if sent.get("chat", {}).get("id") != channel["id"]:
        raise RuntimeError("Unexpected chat in Telegram response")
    Path("promo-receipt.json").write_text(json.dumps({
        "date": day, "slot": slot, "message_id": sent["message_id"],
        "channel": CHANNEL
    }), encoding="utf-8")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        print(f"artifact={artifact}", file=output)
    print(f"Published promo: https://t.me/arabsniper_signals/{sent['message_id']}")


if __name__ == "__main__":
    main()
