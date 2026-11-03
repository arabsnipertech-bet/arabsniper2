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
    "🎁 ARABSNIPERBET GOLD · PROVA 7 GIORNI!\\n\\n"
    "Scopri statistiche, quote di apertura e aggiornate, OCI e mercati analizzati da ArabSniperBet.\\n\\n"
    "✅ 7 giorni gratuiti · 0 €\\n"
    "✅ Nessuna carta o pagamento anticipato\\n"
    "✅ Nessun rinnovo automatico: la prova termina da sola\\n\\n"
    "👉 Apri https://t.me/ArabSniperBot e scrivi /prova in PRIVATO.\\n"
    "Inserisci la tua email e conferma il codice ricevuto.\\n"
    "🌐 https://arabsniperbet.com/gold\\n"
    "Una prova per email/account Telegram. 18+ · Gioca responsabilmente.",
    "⚽ VUOI PROVARE L'AREA GOLD?\\n\\n"
    "Non una lista di pronostici casuali: ogni partita viene studiata incrociando dati statistici, quote e movimenti dei bookmaker.\\n\\n"
    "🎁 Richiedi 7 giorni gratis tramite il bot: https://t.me/ArabSniperBot\\n"
    "Scrivi /prova in una chat privata, verifica l'email con il codice.\\n\\n"
    "💳 Carta NON richiesta · costo 0 €\\n"
    "🔒 Alla scadenza accesso terminato automaticamente, senza addebiti o rinnovo.\\n"
    "18+ · Analisi statistiche, non risultati garantiti.",
    "💛 ARABSNIPERBET GOLD · 7 GIORNI SENZA IMPEGNO\\n\\n"
    "Puoi richiedere una settimana di accesso Gold e vedere in dettaglio i mercati e le variazioni di quota delle singole partite.\\n\\n"
    "✅ Gratis, senza inserire una carta\\n"
    "✅ Nessun addebito alla fine dei 7 giorni\\n"
    "✅ Nessuna disdetta necessaria\\n\\n"
    "📲 https://t.me/ArabSniperBot — invia /prova in privato e conferma l'email.\\n"
    "🌐 https://arabsniperbet.com/gold-dashboard-live.html\\n"
    "Un solo accesso di prova per email/account Telegram. Solo 18+."
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
