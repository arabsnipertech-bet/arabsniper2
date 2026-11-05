"""Publish verified ArabSniper selections and daily results as the Facebook Page."""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

from telegram_slots import CHANNEL_SLOTS, WINDOWS

ROME = ZoneInfo("Europe/Rome")
PAGE = "604216843376166"



def moment(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Timestamp without timezone")
    return result


def selection(receipt, now):
    slot = receipt.get("slot")
    if not receipt.get("message_id") or slot not in CHANNEL_SLOTS or receipt["day"] != now.date().isoformat():
        return
    scheduled = moment(f"{receipt['day']}T{slot}{now.strftime('%z')}")
    if not timedelta(0) <= now - scheduled < timedelta(minutes=30):
        return
    text = receipt["text"]
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Telegram receipt has no message text")
    picks = receipt["picks"]
    future = [p for p in picks if moment(p["kickoff"]) > now]
    if picks and not future:
        return
    from telegram_digest import refresh_reserved, slot_message
    before = receipt.get("reserved", [])
    refresh_reserved(receipt, now)
    if len(future) != len(picks) or before != receipt.get("reserved", []):
        text = slot_message(dict(receipt, picks=future))
    return f"ArabSniperBet · {now:%d/%m/%Y} · fascia {slot}", text


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
    try:
        with urlopen(request, timeout=25) as response:
            return json.load(response)
    except HTTPError as exc:
        try:
            error = json.load(exc).get("error", {})
        except ValueError:
            error = {}
        message = str(error.get("message", "")).lower()
        reason = next((label for needle, label in (
            ("expired", "expired"), ("cannot parse", "malformed"), ("invalidated", "invalidated"),
            ("revoked", "revoked"), ("permission", "permissions")) if needle in message), "unspecified")
        code, subcode = error.get("code"), error.get("error_subcode")
        raise RuntimeError(f"Meta HTTP {exc.code}; code={code if type(code) is int else None}; "
                           f"subcode={subcode if type(subcode) is int else None}; reason={reason}") from None


def main(mode):
    now = datetime.now(ROME)
    card = None
    if mode == "results":
        if now.hour != 11:
            return
        post = results(json.loads(Path("data/arab_quote_results_public.json").read_text()), now)
    else:
        receipts = list(Path("receipts").glob("**/post.json"))
        if len(receipts) > 1:
            raise ValueError("Ambiguous slot receipts")
        receipt = json.loads(receipts[0].read_text()) if receipts else None
        post = selection(receipt, now) if receipt else None
        if post:
            card = receipts[0].with_name("social-card.jpg")
            if not card.exists() or post[1] != receipt["text"] or "reserved" in receipt:
                from social_card import render
                current = dict(receipt, text=post[1], picks=[p for p in receipt["picks"] if moment(p["kickoff"]) > now])
                render(current, card)
    if not post:
        print("No eligible Facebook post")
        return
    if graph("me?fields=id")["id"] != PAGE:
        raise ValueError("Token belongs to another identity")
    title, body = post
    message = body if mode != "results" else f"{title}\n\n{body}\n\n18+ · Le analisi non garantiscono risultati."
    since = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    posts = graph(f"{PAGE}/published_posts?fields=id,message&limit=100&since={since}")
    original = receipt['text'] if mode != 'results' else message
    slot_heading = original.split('\n')[0] if mode != 'results' and receipt['slot'] in WINDOWS else None
    existing = next((p for p in posts['data'] if p.get('message') == message
                     or p.get('message') == original
                     or (slot_heading and p.get('message', '').split('\n')[0] == slot_heading)
                     or p.get('message', '').split('\n')[0] == title), None)
    if existing:
        record_receipt(existing['id'], receipt if mode != 'results' else None, now)
        print("Already published", existing['id'])
        return
    if card:
        import requests
        try:
            with card.open("rb") as image:
                upload = requests.post(f"https://graph.facebook.com/v25.0/{PAGE}/photos",
                    headers={"Authorization":"Bearer "+os.environ["FACEBOOK_PAGE_ACCESS_TOKEN"]},
                    data={"message":message},files={"source":("social-card.jpg",image,"image/jpeg")},timeout=30)
            upload.raise_for_status()
            post_id = upload.json()["post_id"]
        except (requests.RequestException, ValueError, KeyError):
            raise SystemExit("Facebook photo publication unconfirmed") from None
    else:
        post_id = graph(PAGE + "/feed", {"message": message})["id"]
    if not post_id.startswith(PAGE + "_") or graph(post_id + "?fields=id")["id"] != post_id:
        raise ValueError("Facebook publication not confirmed")
    record_receipt(post_id, receipt if mode != 'results' else None, now)
    print("Confirmed Facebook post", post_id)


def record_receipt(post_id, receipt, now):
    if not isinstance(post_id, str) or not post_id.startswith(PAGE + '_'):
        raise ValueError('Invalid Facebook receipt')
    if receipt is None:
        return
    name = f"fb-{receipt['day']}-{receipt['slot'].replace(':', '')}-sent"
    Path('facebook-receipt.json').write_text(json.dumps({
        'day': receipt['day'], 'slot': receipt['slot'], 'post_id': post_id,
        'telegram_message_id': receipt['message_id'], 'verified_at': now.isoformat()}))
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            print('name=' + name, file=out)


if __name__ == "__main__":
    main(sys.argv[1])
