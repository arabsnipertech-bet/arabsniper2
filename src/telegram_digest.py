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
SLOTS = {"0 13 * * *": "13:00", "30 17 * * *": "17:30", "0 19 * * *": "19:00",
         "30 20 * * *": "20:30", "0 22 * * *": "22:00"}
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

def picks(feed, now):
    if feed.get("date") != now.date().isoformat():
        return []
    result = {}
    for row in feed.get("selections", []):
        policy = row.get("opportunity") or {}
        start, at = instant(row.get("kickoff")), instant(row.get("captured_at"))
        market = policy.get("goal_market")
        if (not start or not at or not at <= now < start or start.astimezone(ROME).date() != now.date()
                or not policy.get("eligible") or market not in MARKETS or row.get("quote_executable") is False
                or not isinstance(row.get("odd"), (int, float)) or row["odd"] <= 1 or not row.get("fixture_id")):
            continue
        result.setdefault(str(row["fixture_id"]), dict(id=str(row["fixture_id"]), match=row["match"],
            label=row["label"], market=market, odd=row["odd"], kickoff=row["kickoff"], captured_at=row["captured_at"]))
    return list(result.values())[:3]

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

def prepare():
    now = datetime.now(ROME)
    dry = os.environ.get("EVENT") != "schedule"
    slot = "13:00" if dry else SLOTS[os.environ["CRON"]]
    due = datetime.combine(now.date(), datetime.strptime(slot,"%H:%M").time(), ROME)
    if not dry and not timedelta(0) <= now-due < timedelta(minutes=85):
        return
    day = now.date().isoformat()
    name = f"tg-{day}-{slot.replace(':','')}"
    if not dry and artifact(name+"-claim"):
        print("Slot already reserved")
        return
    if slot == "13:00":
        chosen = picks(json.loads(Path("data/arab_quote_homepage.json").read_text()),now)
        if not chosen:
            print("No eligible upcoming picks")
            return
        lines = [f"ArabSniper · Scelte del {now:%d/%m}", ""]
        for p in chosen:
            price = f"{p['odd']:.2f}".replace(".",",")
            lines.append(f"{instant(p['kickoff']).astimezone(ROME):%H:%M} · {p['match']}\n{p['label']} · quota {price}\n")
    else:
        receipt = artifact(f"tg-{day}-1300-sent")
        if not receipt:
            print("No confirmed midday post")
            return
        gh("run","download",str(receipt["workflow_run"]["id"]),"-n",receipt["name"],"-D","midday")
        chosen = json.loads(Path("midday/post.json").read_text())["picks"]
        with urlopen("https://arabsniperbet.com/api/live-scores",timeout=20) as response:
            scores = json.load(response)["fixtures"]
        lines = [f"ArabSniper · Aggiornamento {now:%d/%m %H:%M}", ""]
        for p in chosen:
            lines.append(f"{p['match']} · {p['label']}\n{verdict(p,scores.get(p['id'],{}),now)}\n")
    Path("post.json").write_text(json.dumps(dict(picks=chosen,text="\n".join(lines).strip()),ensure_ascii=False))
    if not dry:
        with open(os.environ["GITHUB_OUTPUT"],"a") as out:
            print(f"name={name}",file=out)

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
