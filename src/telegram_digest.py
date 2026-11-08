"""Publish the public ArabSniper picks and follow the same fixtures in Rome time."""
import json
import math
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
CHANNEL = "@arabsniper_signals"
from telegram_slots import WINDOWS, EDITORIAL, CHANNEL_SLOTS

UTC_CRONS = {"30 22,23,2,3,10,11,15,16,17 * * *", "0 11,12,18,19,20,21 * * *",
             "45 8,9,10,11,14,15 * * *", "15 15,16 * * *"}
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
                         provider_updated_at=row.get("provider_updated_at"),
                         quote_status=row.get("quote_status"),
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
    observed = instant(p.get("provider_updated_at"))
    if observed:
        local = observed.astimezone(ROME)
        lines.append(f"🕒 Quota rilevata {local:%d/%m alle %H:%M} (da verificare prima di giocare)")
        if p.get("quote_status") == "cached_stable":
            lines.append("Fonte: ultima quota conservata, non aggiornamento in tempo reale")
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
    previous = sorted(set(WINDOWS) | {"13:00", "19:00"})
    seen = set()
    midday = f"tg-spunti-{day}-1300-sent"
    record = artifact(midday)
    if record:
        folder = Path("prior/spunti")
        folder.mkdir(parents=True, exist_ok=True)
        gh("run", "download", str(record["workflow_run"]["id"]), "-n", midday, "-D", str(folder))
        seen.update(str(fid) for fid in json.loads((folder/"spunti-receipt.json").read_text())["fixtures"])
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


def slot_for_schedule(now, event, cron, recovery_slot=None):
    """Allow only due Rome-local slots, scheduled or backup-dispatched."""
    if event == "schedule":
        if cron not in UTC_CRONS:
            return None
    elif event == "workflow_dispatch":
        # An external watchdog may recover evening slots but cannot manufacture
        # a signal, change market eligibility or publish after the due window.
        if recovery_slot not in CHANNEL_SLOTS:
            return None
    else:
        return None
    now = now.astimezone(ROME)
    for slot in CHANNEL_SLOTS:
        if event == "workflow_dispatch" and slot != recovery_slot:
            continue
        due = datetime.combine(now.date(), datetime.strptime(slot, "%H:%M").time(), ROME)
        if event == "schedule":
            utc = due.astimezone(timezone.utc)
            minute, hours, *_ = cron.split()
            if int(minute) != utc.minute or str(utc.hour) not in hours.split(","):
                continue
        if timedelta(0) <= now-due < timedelta(minutes=25):
            return slot
    return None


def prepare():
    now = datetime.now(ROME)
    slot = slot_for_schedule(now, os.environ.get("EVENT"), os.environ.get("CRON"), os.environ.get("RECOVERY_SLOT"))
    if slot is None:
        print("No due Rome-local Telegram slot, or not a scheduled event; skip publication")
        return
    day = now.date().isoformat()
    name = f"tg-{day}-{slot.replace(':','')}"
    if artifact(name+"-sent"):
        print("Slot already reserved or posted")
        return
    previous_claim = artifact(name+"-claim")
    if previous_claim:
        from telegram_reconcile import recover
        recover(previous_claim, name, now)
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            print('name=' + name, file=out)
        return
    if slot in EDITORIAL:
        legacy = (f"telegram-evidence-{day}-{'audit' if slot == '10:45' else 'market'}-sent" if slot in ("10:45", "16:45")
                  else f"telegram-gold-promo-{day}-{slot.replace(':','')}-sent")
        if artifact(legacy):
            print("Editorial already delivered by previous schedule")
            return
        post = {"slot":slot, "picks":[], "day":day}
        if EDITORIAL[slot] == 'audit':
            from telegram_evidence import load_audit, audit_message
            expected = (now.date() - timedelta(days=1)).isoformat()
            report = load_audit(expected)
            if report and report.get('date') == expected and audit_message(report):
                post['audit'] = report
        if slot in ('12:45','17:15'):
            try:
                from promo_successes import load_yesterday
                report=load_yesterday(now)
                if report: post['successes']=report
            except Exception as error:
                print('Verified successes unavailable; standard promotion:',type(error).__name__)
        reserve(name, post)
        return
    from gold_storage import client
    from homepage_preview import social_board, stamp

    s3 = client()
    def load_board(key):
        obj = s3.get_object(Bucket=os.environ["R2_BUCKET"], Key=key)
        return json.load(obj["Body"])
    board = load_board("arab_quote_day1.json")
    if slot == "00:30" and board.get("date") != day:
        board = load_board("arab_quote_day2.json")
    excluded = prior_ids(day, slot)
    now = datetime.now(ROME)
    first, last, limit = WINDOWS[slot]
    start = datetime.fromisoformat(f"{day}T{first}").replace(tzinfo=ROME)
    end = datetime.fromisoformat(f"{day}T{last}").replace(tzinfo=ROME)
    updated = stamp(board.get("updated_at"))
    if board.get("date") != day or not updated or not timedelta(0) <= now-updated <= timedelta(minutes=90):
        scheduled = datetime.fromisoformat(f"{day}T{slot}").replace(tzinfo=ROME)
        if now-scheduled < timedelta(minutes=14):
            raise RuntimeError("Missing current private board; retry this slot")
        reserve(name, dict(slot=slot, picks=[], first=first, last=last, stale=True))
        return
    from social_analysis import select
    chosen = select(board, now, start, end, excluded)
    post = dict(slot=slot, picks=chosen, reserved=[], first=first, last=last, analysis_only=True)
    try:
        from sports_daily_live import reserve_publication
        reserve_publication(post, now)
    except Exception as error:
        post.pop('sports_live', None)
        print('Sports live reservation unavailable:', type(error).__name__)
    reserve(name, post)


def reserve(name, post):
    post.setdefault("day", datetime.now(ROME).date().isoformat())
    post["text"] = slot_message(post)
    Path("post.json").write_text(json.dumps(post, ensure_ascii=False))
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        print(f"name={name}", file=out)

def refresh_reserved(post, now):
    """Remove expired previews before each channel sends its image."""
    if post.get('live') and (not post.get('picks') or instant(post['live']['kickoff']) <= now):
        post.pop('live')
    if post.get('sports_live') and (not post.get('picks') or instant(post['sports_live']['kickoff']) <= now):
        post.pop('sports_live')
    public_ids = {str(p.get("id", p.get("fixture_id"))) for p in post.get("picks", [])}
    seen = set(public_ids)
    valid = []
    for row in post.get("reserved", []):
        start = instant(row.get("kickoff"))
        fid = str(row.get("id", ""))
        if start and start > now and fid and fid not in seen:
            valid.append(dict(id=fid, kickoff=row["kickoff"]))
            seen.add(fid)
    post["reserved"] = valid[:2] if post.get("picks") else []
    return post


def social_cta(post):
    lines = ["🎯 Questa è una delle selezioni gratuite del canale."]
    if post.get("reserved"):
        lines.append("🔒 Altre partite selezionabili nell’area Gold: scopri le analisi riservate.")
    else:
        lines.append("🔎 Esplora l’area Gold per consultare le analisi disponibili e i prossimi aggiornamenti.")
    lines.append("🎁 Gold gratis 7 giorni: scrivi /prova a @ArabSniperBot")
    if post.get('live'):
        live=post['live'];start=instant(live['kickoff']).astimezone(ROME)
        kind='dalla Gold · pronostico riservato' if live['origin']=='gold' else 'della singola gratuita'
        lines.append(f"🟠 LIVE DEL GIORNO {kind}: {live['match']} · {start:%H:%M}. Seguiremo gol ed esito su Telegram.")
        lines.append('Selezione registrata prima del via · rif. '+live['commitment'][:12])
    return "\n".join(lines)


def slot_message(post):
    if post.get("analysis_only"):
        from social_analysis import caption
        return caption(post)
    slot = post["slot"]
    picks = post["picks"]
    if slot in EDITORIAL:
        if EDITORIAL[slot] == 'audit':
            from telegram_evidence import audit_message
            expected = (datetime.fromisoformat(post['day']).date() - timedelta(days=1)).isoformat()
            report = post.get('audit') or {}
            message = audit_message(report) if report.get('date') == expected else None
            return message or f"ArabSniper · Riscontri del {expected} in verifica. Nessun risultato non verificato viene pubblicato."
        if post.get('successes') and EDITORIAL[slot]=='promo':
            from promo_successes import message
            return message(post['successes'])
        if EDITORIAL[slot] == "education":
            from telegram_evidence import educational_message
            return educational_message(datetime.fromisoformat(post["day"]).date(), slot=slot)
        from telegram_gold_promo import MESSAGES
        return MESSAGES[0 if slot == "12:45" else 1]
    if not picks:
        if post.get("stale"):
            note = f"Controllo delle {slot}: dati Gold non aggiornati per questa fascia."
            if slot != "22:00":
                return note
            from telegram_gold_promo import MESSAGES
            return note + "\n\n" + MESSAGES[-1]
        if slot == "22:00":
            from telegram_gold_promo import MESSAGES
            return MESSAGES[-1]
        return (f"ArabSniper · Controllo delle {slot} completato. "
                f"Nessuna nuova selezione valida da pubblicare per le gare {post['first']}–{post['last']}.")
    title = "Tripla" if slot == "13:00" and len(picks) == 3 else "Singola" if len(picks) == 1 else "Selezioni singole"
    lines = [f"ArabSniper · {title} delle {slot}"]
    for pick in picks:
        start = instant(pick["kickoff"]).astimezone(ROME)
        at = instant(pick["provider_updated_at"]).astimezone(ROME)
        tier = "BLU · segnale di mercato valido" if pick["tier"] == "Blu" else pick["tier"].upper()
        from social_labels import competition
        lines += ["", competition(pick), f"{tier} · {start:%H:%M} · {pick['match']}",
                  f"{pick['label']} · quota {pick['odd']:.2f} rilevata alle {at:%H:%M}"]
        if len(picks)==1:
            lines.extend(evening_rationale(pick))
    # Reserve caption space for the free/Gold distinction and the trial CTA.
    footer = "\n\n" + social_cta(post) + "\n18+ · Le analisi non garantiscono risultati."
    while lines and len(("\n".join(lines) + footer).encode("utf-16-le")) // 2 > 1024:
        lines.pop()
    return "\n".join(lines) + footer

def send():
    """Send the current Gold analysis image and caption as one verified message."""
    token = os.environ["BOT_TOKEN"]
    if not token:
        raise SystemExit("Missing bot token")
    post = json.loads(Path("post.json").read_text())
    if post.get('message_id'):
        print('Existing Telegram receipt recovered; delivery already confirmed')
        return
    if "slot" in post:
        import requests
        from social_card import render
        post["picks"] = [p for p in post["picks"] if instant(p["kickoff"]) > datetime.now(ROME)]
        refresh_reserved(post, datetime.now(ROME))
        post["text"] = slot_message(post)
        render(post, "social-card.jpg")
        caption = post["text"]
        from telegram_reconcile import caption as bounded_caption
        caption = bounded_caption(caption)
        try:
            with open("social-card.jpg", "rb") as photo:
                response = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto",
                                         data={"chat_id":CHANNEL,"caption":caption},
                                         files={"photo":("social-card.jpg",photo,"image/jpeg")},timeout=30)
            payload = response.json()
        except (requests.RequestException,ValueError) as exc:
            raise SystemExit("Telegram photo delivery unconfirmed; category="+type(exc).__name__) from None
        result = payload.get("result") or {}
        if not payload.get("ok") or not result.get("photo"):
            raise SystemExit("Telegram photo not confirmed")
        if result.get("chat", {}).get("username", "").lower() != CHANNEL[1:] or not result.get("message_id"):
            raise SystemExit("Unexpected Telegram receipt")
        post["message_id"] = result["message_id"]
        Path("post.json").write_text(json.dumps(post, ensure_ascii=False))
        print("Confirmed Telegram message", post["message_id"])
        return
    if any(instant(p["kickoff"]) <= datetime.now(ROME) for p in post["picks"]):
        raise SystemExit("Kickoff already reached; do not send")
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
