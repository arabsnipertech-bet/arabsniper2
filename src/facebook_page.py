"""Publish verified ArabSniper selections and daily results as the Facebook Page."""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROME = ZoneInfo("Europe/Rome")
PAGE = "604216843376166"
SLOTS = {"00:30", "04:30", "12:30", "17:30", "18:30", "20:00", "22:00"}


def moment(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Timestamp without timezone")
    return result


def selection(receipt, now):
    slot = receipt.get("slot")
    if not receipt.get("message_id") or slot not in SLOTS or receipt["day"] != now.date().isoformat():
        return
    scheduled = moment(f"{receipt['day']}T{slot}{now.strftime('%z')}")
    if not timedelta(0) <= now - scheduled < timedelta(minutes=30):
        return
    rows = []
    for pick in receipt["picks"]:
        start = moment(pick["kickoff"]).astimezone(ROME)
        if start <= now or pick["tier"].lower() not in {"platinum", "gold", "blu"}:
            continue
        at = moment(pick["provider_updated_at"]).astimezone(ROME)
        label = "Blu · Solo Studio" if pick["tier"].lower() == "blu" else pick["tier"].title()
        rows.append(f"{label} · {start:%H:%M} · {pick['match']}\n{pick['label']} · quota {pick['odd']:.2f} (rilevata {at:%d/%m %H:%M})")
    if not rows:
        if slot != "22:00":
            return
        rows = ["Scopri il cruscotto Gold e confronta statistiche e movimenti delle quote: https://arabsniperbet.com/gold"]
    return f"ArabSniperBet · {now:%d/%m/%Y} · fascia {slot}", "\n\n".join(rows)


def results(feed, now):
    day = now.date() - timedelta(days=1)
    if feed.get("date") != day.isoformat() or feed.get("available") is not True:
        raise ValueError("Yesterday's certified results unavailable")
    outcomes = {}
    for row in feed["all_results"]:
        if not row.get("forecast_sha256") or moment(row["captured_at"]) >= moment(row["kickoff"]):
            raise ValueError("Unverified prematch record")
        key = (row["fixture_id"], row["market"], row["label"])
        outcome = row["outcome"]
        if key in outcomes and outcomes[key] != outcome:
            raise ValueError("Conflicting results")
        outcomes[key] = outcome
    counts = Counter(outcomes.values())
    pending = len(outcomes) - counts["won"] - counts["lost"]
    return f"ArabSniperBet · Riscontri del {day:%d/%m/%Y}", f"Selezioni registrate: {len(outcomes)}\nVinte: {counts['won']} · Perse: {counts['lost']} · Altri esiti o da verificare: {pending}"


def graph(path, data=None):
    request = Request("https://graph.facebook.com/v25.0/" + path,
                      data=urlencode(data).encode() if data else None,
                      headers={"Authorization": "Bearer " + os.environ["FACEBOOK_PAGE_ACCESS_TOKEN"]})
    with urlopen(request, timeout=25) as response:
        return json.load(response)


def main(mode):
    now = datetime.now(ROME)
    if mode == "results":
        if now.hour != 11:
            return
        post = results(json.loads(Path("data/arab_quote_results_public.json").read_text()), now)
    else:
        receipts = list(Path("receipts").glob("**/post.json"))
        if len(receipts) > 1:
            raise ValueError("Ambiguous slot receipts")
        post = selection(json.loads(receipts[0].read_text()), now) if receipts else None
    if not post:
        print("No eligible Facebook post")
        return
    if graph("me?fields=id")["id"] != PAGE:
        raise ValueError("Token belongs to another identity")
    title, body = post
    posts = graph(PAGE + "/published_posts?fields=message&limit=100")
    if any(p.get("message", "").split("\n")[0] == title for p in posts["data"]):
        print("Already published")
        return
    response = graph(PAGE + "/feed", {"message": title + "\n\n" + body + "\n\n18+ · Le analisi non garantiscono risultati."})
    post_id = response["id"]
    if not post_id.startswith(PAGE + "_") or graph(post_id + "?fields=id")["id"] != post_id:
        raise ValueError("Facebook publication not confirmed")
    print("Confirmed Facebook post", post_id)


if __name__ == "__main__":
    main(sys.argv[1])
