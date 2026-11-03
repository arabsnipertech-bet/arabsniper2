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
SLOTS = ("12:45", "17:15", "21:15")
# Explicit UTC schedules: CET/CEST primary plus 13-minute fallbacks.
# Only the currently due Europe/Rome slot may send.
UTC_CRONS = {
    "45 10 * * *", "58 10 * * *", "45 11 * * *", "58 11 * * *",
    "15 15 * * *", "28 15 * * *", "15 16 * * *", "28 16 * * *",
    "15 19 * * *", "28 19 * * *", "15 20 * * *", "28 20 * * *",
}
MESSAGES = (
    "🔎 ARABSNIPERBET GOLD · COSA OSSERVIAMO?\\n\\n"
    "Statistiche, evoluzione delle quote, segnali di mercato e selezioni Gold/Platinum "
    "vengono confrontati prima del calcio d'inizio. Ogni segnale richiede conferme: "
    "nessun risultato è garantito.\\n\\n"
    "🎁 Stiamo preparando l'accesso dimostrativo Gold gratuito per 7 giorni, "
    "senza carta, addebiti né rinnovo automatico. Annunceremo l'attivazione dopo il collaudo.\\n"
    "🌐 https://arabsniperbet.com\\n📲 https://t.me/arabsniper_signals\\n"
    "18+ · Gioca responsabilmente.",
    "📊 ARABSNIPERBET · LA QUOTA NON È UN PRONOSTICO\\n\\n"
    "L'apertura e la quota aggiornata raccontano come cambia il mercato. "
    "ArabSniper studia i movimenti insieme alle statistiche, all'OCI e all'aggio. "
    "Se mancano conferme attendibili, nessun segnale Gold viene forzato.\\n\\n"
    "🎁 Prova Gold gratuita di 7 giorni in preparazione: "
    "nessuna carta, nessun addebito e nessun rinnovo automatico quando sarà attivata.\\n"
    "🌐 https://arabsniperbet.com\\n"
    "18+ · Le analisi non garantiscono risultati.",
    "💛 ARABSNIPERBET GOLD · STUDIA PRIMA DI SCEGLIERE\\n\\n"
    "Le opportunità più interessanti nascono dall'incrocio tra statistiche "
    "e quote, non dal numero di pronostici pubblicati. "
    "Seguici per gli aggiornamenti Gold/Platinum e per i riscontri verificati.\\n\\n"
    "🎁 Accesso dimostrativo Gold di 7 giorni senza carta, addebiti o rinnovo: "
    "offerta in preparazione e non ancora disponibile tramite il bot.\\n"
    "🌐 https://arabsniperbet.com\\n"
    "18+ · Gioca responsabilmente."
)


def selected(now, event, schedule, recovery_slot=None):
    """Publish only due Rome-local slots, including a guarded watchdog recovery."""
    if event == "schedule":
        if schedule not in UTC_CRONS:
            return None
    elif event == "workflow_dispatch":
        if recovery_slot not in SLOTS:
            return None
    else:
        return None
    now = now.astimezone(ROME)
    for slot in SLOTS:
        if event == "workflow_dispatch" and slot != recovery_slot:
            continue
        h, m = map(int, slot.split(":"))
        expected = datetime.combine(now.date(), time(h, m), tzinfo=ROME)
        if timedelta(0) <= now - expected <= timedelta(minutes=40):
            return slot
    return None


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
    slot = selected(now, os.environ.get("GITHUB_EVENT_NAME"), os.environ.get("GITHUB_EVENT_SCHEDULE"), os.environ.get("RECOVERY_SLOT"))
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
    index = SLOTS.index(slot)
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
