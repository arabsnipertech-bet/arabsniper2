"""One-time late Telegram triple for Nov 3: fresh, unstarted Gold/Platinum only."""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from telegram_digest import artifact, picks
from telegram_private_feed import fetch_current_private

ROME=ZoneInfo("Europe/Rome")
DAY="2026-11-03"
NAME="tg-2026-11-03-extra-triple"


def prepare(now):
    now=now.astimezone(ROME)
    if now.date().isoformat()!=DAY:
        print("This request applies only to 2026-11-03; no send")
        return False
    # Protect both the original 13:00 slot and this one-time recovery.
    if any(artifact(x) for x in (
        f"tg-{DAY}-1300-sent",f"tg-{DAY}-1300-claim",
        NAME+"-sent",NAME+"-claim")):
        print("The original triple or late replacement was already reserved/sent")
        return False
    feed={"date":DAY,"selections":fetch_current_private(now)}
    selected=picks(feed,now,limit=3)
    if len(selected)!=3:
        print(f"Only {len(selected)} fresh, unstarted, qualifying fixtures; no triple posted")
        return False
    # No prediction may have already kicked off or be stale when preparing the post.
    for item in selected:
        kickoff=datetime.fromisoformat(item["kickoff"])
        observed=datetime.fromisoformat(item["captured_at"])
        if not observed<=now<kickoff or now-observed>timedelta(minutes=90):
            raise ValueError("Unverified or started fixture: abort")
    lines=[f"ArabSniper · TRIPLA STRAORDINARIA · {now:%d/%m %H:%M}",
           "Pubblicazione fuori orario: quote osservate adesso, prima delle partite.",""]
    total=1.0
    for item in selected:
        kickoff=datetime.fromisoformat(item["kickoff"]).astimezone(ROME)
        total*=item["odd"]
        lines.extend([f"{kickoff:%H:%M} · {item['match']}",
                      f"{item['label']} · quota osservata {item['odd']:.2f}",""])
    lines.extend([f"Quota combinata indicativa: {total:.2f}",
                  "Selezioni Gold/Platinum prepartita, non retroattive.",
                  "🎁 Gold 7 giorni senza carta e senza rinnovo: https://t.me/ArabSniperBot",
                  "18+ · Le analisi non garantiscono risultati."])
    Path("post.json").write_text(json.dumps({"picks":selected,"text":"\n".join(lines)},ensure_ascii=False))
    return True


if __name__=="__main__":
    import sys
    now=datetime.now(ROME)
    if prepare(now):
        with open(os.environ["GITHUB_OUTPUT"],"a") as out:
            print("name="+NAME,file=out)
