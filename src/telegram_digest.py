"""Publish the public ArabSniper picks and follow the same fixtures in Rome time."""
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
CHANNEL = "@arabsniper_signals"
SLOTS = {"0 13 * * *": "13:00", "30 17 * * *": "17:30",
         "0 19 * * *": "19:00", "0 20 * * *": "20:00"}
# UTC GitHub Actions primary/backup triggers for both CET and CEST.
# UTC events only qualify when their actual Rome-local slot is due.
UTC_CRONS = {
    "0 11 * * *", "13 11 * * *", "0 12 * * *", "13 12 * * *",
    "30 15 * * *", "43 15 * * *", "30 16 * * *", "43 16 * * *",
    "0 17 * * *", "13 17 * * *", "0 18 * * *", "13 18 * * *",
    "0 19 * * *", "13 19 * * *",
}
WINDOWS = {"17:30": ("18:00", "19:00"), "19:00": ("19:30", "21:00"),
           "20:00": ("20:30", "22:00")}
MARKETS = {"over15_ht": ("ht", 2), "over25_ht": ("ht", 3), "gg_ht": ("ht", "gg"),
           "over25_ft": ("ft", 3), "over35_ft": ("ft", 4)}
FINAL = {"FT", "AET", "PEN"}
LIVE = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE"}

def instant(value):
    try:
        t = datetime.fromisoformat(value)
        return t if t.tzinfo else None
    except (ValueError, TypeError):
        return None

def picks(feed, now, window=None, limit=3, exclude=None):
    """Rank valid, unstarted fixtures and optionally restrict their Rome kickoff."""
    if feed.get("date") != now.date().isoformat():
        return []
    excluded = set(exclude or [])
    result = {}
    for row in feed.get("selections", []):
        policy = row.get("opportunity") or {}
        private = row.get("source_tier") in ("platinum", "gold")
        start, at = instant(row.get("kickoff")), instant(row.get("captured_at"))
        market = row.get("market") if private else policy.get("goal_market")
        if (not start or not at or not at <= now < start or now-at > timedelta(minutes=90) or
                start.astimezone(ROME).date() != now.date() or
                (not private and not policy.get("eligible")) or market not in MARKETS or
                row.get("quote_executable") is False or
                not isinstance(row.get("odd"), (int, float)) or row["odd"] <= 1 or
                not row.get("fixture_id")):
            continue
        kickoff_local = start.astimezone(ROME)
        if window and not (window[0] <= kickoff_local.strftime("%H:%M") <= window[1]):
            continue
        fixture = str(row["fixture_id"])
        if fixture in excluded:
            continue
        candidate = dict(id=fixture, match=row["match"], label=row["label"],
                         market=market, odd=row["odd"], kickoff=row["kickoff"],
                         captured_at=row["captured_at"],
                         score=float(row.get("score") if private else policy.get("playability_score") or 0),
                         signal=row.get("signal") if private else policy.get("signal") or "unconfirmed",
                         tier=row.get("source_tier") or "gold",
                         strict=bool(row.get("strict") if private else policy.get("strict_eligible", False)),
                         stable=bool(row.get("stable")) if private else (policy.get("stability") or {}).get("status") == "strict",
                         market_confirmed=bool(row.get("market_confirmed")) if private else row.get("market_confirmation") == "confirmed",
                         model_probability=row.get("model_probability") if private else row.get("statistical_pct"),
                         first_odd=row.get("first_odd"), delta_pct=row.get("delta_pct"),
                         oci=row.get("oci"), ev_pct=row.get("ev_pct"),
                         margin_pct=row.get("margin_pct"),bookmaker=row.get("bookmaker"),
                         movement_confirmed=row.get("movement_confirmed"),
                         bookmakers_count=row.get("bookmakers_count"))
        if fixture not in result or candidate["score"] > result[fixture]["score"]:
            result[fixture] = candidate
    return sorted(result.values(), key=lambda r:(
        r["tier"] == "platinum", r["strict"], r["stable"], r["market_confirmed"],
        r["signal"] == "confirmed", r["score"], -instant(r["kickoff"]).timestamp()
    ), reverse=True)[:limit]

def finite_number(value):
    return type(value) in (int, float) and math.isfinite(value)


def evening_rationale(p):
    """Explain observed statistical and market evidence without fabricated signals.

    Kept concise for Telegram photo's 1,024-character caption limit.
    """
    lines = []
    probability = p.get("model_probability")
    odd = p.get("odd")
    if finite_number(probability) and 0 <= probability <= 100 and finite_number(odd) and odd > 1:
        implied = 100 / odd
        lines.append(f"📊 Modello {probability:.1f}% · quota implica {implied:.1f}% (aggio escluso)")
        lines.append(f"Scarto stimato {probability-implied:+.1f} p.p.; non è una certezza")
    else:
        lines.append("📊 Probabilità del modello non verificabile nel feed")
    opening = p.get("first_odd")
    delta = p.get("delta_pct")
    if (finite_number(opening) and opening > 1 and finite_number(odd) and
        finite_number(delta) and abs(delta-100*(odd/opening-1)) <= 0.3):
        direction = "in calo" if delta < -0.05 else "in rialzo" if delta > 0.05 else "stabile"
        confirmation = "confermato" if p.get("movement_confirmed") is True else "non confermato"
        lines.append(f"📉 Quota {opening:.2f} → {odd:.2f} ({delta:+.1f}%, {direction}; flusso {confirmation})")
    else:
        lines.append("📉 Apertura/variazione non verificata: nessun trend attribuito")
    oci = p.get("oci")
    margin = p.get("margin_pct")
    details = []
    if finite_number(oci):
        details.append(f"OCI {oci:+.1f}")
    if finite_number(margin) and 0 <= margin <= 100:
        details.append(f"aggio {margin:.1f}%")
    if p.get("market_confirmed"):
        details.append("concordanza modello/mercato")
    elif not p.get("movement_confirmed"):
        details.append("conferma mercato assente")
    if details:
        lines.append("🔎 " + " · ".join(details))
    if p.get("stable"):
        lines.append("✓ Stabilità del segnale rilevata")
    bookmaker = p.get("bookmaker")
    if isinstance(bookmaker, str) and bookmaker.strip():
        lines.append(f"Fonte quota: {bookmaker.strip()[:55]}")
    return lines


def verdict(pick, score, now):
    if now < instant(pick["kickoff"]):
        return "In programma"
    status = score.get("status")
    if status in {"PST", "CANC", "ABD", "SUSP", "INT", "AWD", "WO"}:
        return f"Da verificare ({status})"
    if status not in FINAL | LIVE:
        return "In attesa di aggiornamento"
    observed = instant(score.get("observed_at"))
    if status in LIVE and (not observed or not timedelta(0) <= now-observed <= timedelta(minutes=8) or score.get("refresh_failed")):
        return "Live non aggiornato"
    period, target = MARKETS[pick["market"]]
    ended = status in FINAL or period == "ht" and status in {"HT", "2H", "ET", "BT", "P"}
    if status == "LIVE" and period == "ht":
        return "Periodo live da verificare"
    field = ("halftime" if period == "ht" else "fulltime") if ended else "goals"
    if period == "ft" and status in {"ET", "BT", "P"}:
        field, ended = "fulltime", True
    value = score.get(field) or {}
    h, a = value.get("home"), value.get("away")
    if not all(type(x) is int and x >= 0 for x in (h,a)):
        return "Punteggio da verificare"
    hit = min(h,a) > 0 if target == "gg" else h+a >= target
    state = ("✅ Centrato" if hit else "❌ Non centrato") if ended else ("🟢 Soglia raggiunta live" if hit else "In corso")
    minute = score.get("elapsed")
    phase = ("PT" if period == "ht" else "FT") if ended else (f"{minute}′" if isinstance(minute,int) else status)
    return f"{state} · {phase} {h}–{a}"

def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)

def artifact(name):
    repo = os.environ["GITHUB_REPOSITORY"]
    data = json.loads(gh("api", f"repos/{repo}/actions/artifacts?name={name}&per_page=100"))
    return next((a for a in data["artifacts"] if a["name"] == name and not a["expired"]), None)

def prior_ids(day, slot):
    """Load earlier confirmed posts to prevent repeating the same fixture."""
    previous = ["13:00", "17:30", "19:00"]
    seen = set()
    for previous_slot in previous:
        if previous_slot == slot:
            break
        name = f"tg-{day}-{previous_slot.replace(':','')}-sent"
        record = artifact(name)
        if not record:
            continue
        folder = Path("prior") / previous_slot.replace(":", "")
        folder.mkdir(parents=True, exist_ok=True)
        gh("run", "download", str(record["workflow_run"]["id"]),
           "-n", name, "-D", str(folder))
        for p in json.loads((folder / "post.json").read_text())["picks"]:
            seen.add(p["id"])
    return seen


def slot_for_schedule(now, event, cron):
    """Allow only timely GitHub scheduled runs matching a due local slot."""
    if event != "schedule" or cron not in UTC_CRONS:
        return None
    now = now.astimezone(ROME)
    for slot in SLOTS.values():
        due = datetime.combine(now.date(), datetime.strptime(slot, "%H:%M").time(), ROME)
        if timedelta(0) <= now-due < timedelta(minutes=25):
            return slot
    return None


def prepare():
    now = datetime.now(ROME)
    slot = slot_for_schedule(now, os.environ.get("EVENT"), os.environ.get("CRON"))
    if slot is None:
        print("No due Rome-local Telegram slot, or not a scheduled event; skip publication")
        return
    day = now.date().isoformat()
    name = f"tg-{day}-{slot.replace(':','')}"
    if artifact(name+"-claim") or artifact(name+"-sent"):
        print("Slot already reserved or posted")
        return
    if slot == "13:00":
        # Dedicated public-feed spunti workflow owns this slot. Keep evening Gold unchanged.
        print("13:00 handled by independent public analytical spunti workflow")
        return
    from telegram_private_feed import fetch_current_private
    private_choices = fetch_current_private(now)
    feed = {"date":day, "selections": private_choices}
    print(f"Private daily board: {len(private_choices)} Gold/Platinum candidates")
    if slot == "13:00":
        chosen = picks(feed, now, limit=3)
        if len(chosen) != 3:
            print("Insufficient verified upcoming picks for a triple")
            return
        lines = [f"ArabSniper · Tripla del {now:%d/%m}", ""]
        total = 1
        for p in chosen:
            total *= p["odd"]
            lines.append(f"{instant(p['kickoff']).astimezone(ROME):%H:%M} · {p['match']}\n{p['label']} · quota {p['odd']:.2f}\n")
        lines.append(f"Quota combinata indicativa {total:.2f}")
    else:
        chosen = picks(feed, now, window=WINDOWS[slot], limit=1,
                       exclude=prior_ids(day, slot))
        if not chosen:
            print(f"No qualifying pre-match pick for {slot}")
            return
        p = chosen[0]
        lines = [f"ArabSniper · {p['tier'].upper()} · Singolo delle {slot}", "",
                 f"{instant(p['kickoff']).astimezone(ROME):%H:%M} · {p['match']}",
                 f"{p['label']} · quota osservata {p['odd']:.2f}",
                 "PERCHÉ SELEZIONATA:"]
        lines.extend(evening_rationale(p))
    lines.extend(["", "🎯 ArabSniperBet · Analisi, non garanzie.",
                  "👑 https://arabsniperbet.com/gold",
                  "18+ · Gioca responsabilmente."])
    Path("post.json").write_text(json.dumps(
        dict(picks=chosen, text="\n".join(lines).strip()), ensure_ascii=False))
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        print(f"name={name}", file=out)

def send():
    """Send the image and its caption as one verified Telegram message."""
    token = os.environ["BOT_TOKEN"]
    if not token:
        raise SystemExit("Missing bot token")
    post = json.loads(Path("post.json").read_text())
    image = Path("telegram-card.png")
    if not image.is_file():
        raise SystemExit("Missing Telegram image card")
    caption = post["text"]
    if len(caption) > 1024:
        raise SystemExit("Telegram photo caption exceeds 1024 characters")
    import requests
    try:
        with image.open("rb") as handle:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data={"chat_id": CHANNEL, "caption": caption},
                files={"photo": ("card.png", handle, "image/png")},
                timeout=30)
        if response.status_code != 200:
            raise SystemExit(f"Telegram photo HTTP {response.status_code}")
        payload = response.json()
        message = payload["result"]
    except (requests.RequestException, ValueError, KeyError):
        raise SystemExit("Telegram photo delivery unconfirmed") from None
    if not payload.get("ok") or message["chat"].get("username","").lower()!=CHANNEL[1:] or not message.get("photo"):
        raise SystemExit("Unexpected Telegram photo destination")
    print(f"Telegram photo message {message['message_id']}")

if __name__ == "__main__":
    {"prepare":prepare,"send":send}[sys.argv[1]]()
