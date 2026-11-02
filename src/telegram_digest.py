"""Publish the public ArabSniper picks and follow the same fixtures in Rome time."""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
CHANNEL = "@arabsniper_signals"
SLOTS = {"0 13 * * *": "13:00", "30 17 * * *": "17:30",
         "0 19 * * *": "19:00", "0 20 * * *": "20:00"}
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
        start, at = instant(row.get("kickoff")), instant(row.get("captured_at"))
        market = policy.get("goal_market")
        if (not start or not at or not at <= now < start or
                start.astimezone(ROME).date() != now.date() or
                not policy.get("eligible") or market not in MARKETS or
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
                         score=float(policy.get("playability_score") or 0),
                         signal=policy.get("signal") or "unconfirmed")
        if fixture not in result or candidate["score"] > result[fixture]["score"]:
            result[fixture] = candidate
    return sorted(result.values(), key=lambda r:(r["score"],r["kickoff"]), reverse=True)[:limit]

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


def prepare():
    now = datetime.now(ROME)
    if os.environ.get("EVENT") != "schedule":
        print("Manual/push run: validation only, no channel publication")
        return
    slot = SLOTS.get(os.environ.get("CRON"))
    if slot is None:
        raise SystemExit("Unrecognized Telegram schedule")
    due = datetime.combine(now.date(), datetime.strptime(slot, "%H:%M").time(), ROME)
    if not timedelta(0) <= now-due < timedelta(minutes=25):
        print("Outside the publication window")
        return
    day = now.date().isoformat()
    name = f"tg-{day}-{slot.replace(':','')}"
    if artifact(name+"-claim") or artifact(name+"-sent"):
        print("Slot already reserved or posted")
        return
    feed = json.loads(Path("data/arab_quote_homepage.json").read_text())
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
        lines = [f"ArabSniper · Pronostico singolo {slot}", "",
                 f"{instant(p['kickoff']).astimezone(ROME):%H:%M} · {p['match']}",
                 f"{p['label']} · quota {p['odd']:.2f}",
                 f"Segnale: {p['signal']}"]
    lines.extend(["", "🎯 Prono gratuito da ArabSniperBet.com",
                  "Studiamo quote, statistiche e movimenti di mercato.",
                  "👑 Area Gold: https://arabsniperbet.com/gold",
                  "📲 Canale: https://t.me/arabsniper_signals",
                  "Le analisi non garantiscono il risultato."])
    Path("post.json").write_text(json.dumps(
        dict(picks=chosen, text="\n".join(lines).strip()), ensure_ascii=False))
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        print(f"name={name}", file=out)

def send():
    token = os.environ["BOT_TOKEN"]
    if not token:
        raise SystemExit("Missing bot token")
    post = json.loads(Path("post.json").read_text())
    try:
        data = urlencode(dict(chat_id=CHANNEL,text=post["text"])).encode()
        with urlopen(f"https://api.telegram.org/bot{token}/sendMessage",data=data,timeout=20) as response:
            message = json.load(response)["result"]
    except HTTPError as exc:
        raise SystemExit(f"Telegram HTTP {exc.code}") from None
    except Exception:
        raise SystemExit("Telegram delivery unconfirmed") from None
    if message["chat"].get("username","").lower() != CHANNEL[1:]:
        raise SystemExit("Unexpected destination")
    print(f"Telegram message {message['message_id']}")

if __name__ == "__main__":
    {"prepare":prepare,"send":send}[sys.argv[1]]()
