"""Wake due Telegram publications from the external ten-minute Fast watchdog.

Independent from GitHub schedule events. No sports predictions are invented here:
each target workflow retains its own validation, dedup and freshness checks.
"""
import json
import os
from datetime import datetime, timedelta, timezone, time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROME=ZoneInfo("Europe/Rome")
REPO="arabsnipertech-bet/arabsniper2"
from telegram_slots import CHANNEL_SLOTS

SLOTS = tuple(("telegram-digest.yml", clock, clock, 24) for clock in CHANNEL_SLOTS)
# Allow GitHub's primary cron a moment to start. External watchdog runs every
# ten minutes, so a single check within the window is sufficient.
MINIMUM_DELAY=timedelta(minutes=3)


def due_slots(now):
    local=now.astimezone(ROME)
    for workflow,clock,slot,max_minutes in SLOTS:
        h,m=map(int,clock.split(":"))
        due=datetime.combine(local.date(),time(h,m),tzinfo=ROME)
        age=local-due
        if MINIMUM_DELAY<=age<=timedelta(minutes=max_minutes):
            yield workflow,slot,due


class GitHub:
    def __init__(self,token):
        if not token:raise RuntimeError("Missing GITHUB_TOKEN")
        self.token=token
    def request(self,path,body=None):
        url=f"https://api.github.com/repos/{REPO}/actions/{path}"
        req=Request(url,data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization":"Bearer "+self.token,
                     "Accept":"application/vnd.github+json",
                     "X-GitHub-Api-Version":"2022-11-28",
                     "User-Agent":"ArabSniper-Telegram-Watchdog",
                     "Content-Type":"application/json"},
            method="GET" if body is None else "POST")
        try:
            with urlopen(req,timeout=18) as res:
                if res.status not in (200,201,204):
                    raise RuntimeError("GitHub unexpected response")
                payload=res.read()
                return json.loads(payload) if payload else {}
        except HTTPError as exc:
            raise RuntimeError("GitHub HTTP "+str(exc.code)) from None
        except (URLError,TimeoutError,ValueError) as exc:
            raise RuntimeError("GitHub backup request unavailable") from None
    def runs(self,workflow,from_time):
        query=urlencode({"branch":"main","created":">="+from_time.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "per_page":100})
        payload=self.request(f"workflows/{workflow}/runs?{query}")
        if not isinstance(payload,dict) or not isinstance(payload.get("workflow_runs"),list):
            raise RuntimeError("GitHub returned invalid run listing")
        if int(payload.get("total_count",-1))>len(payload["workflow_runs"]):
            raise RuntimeError("GitHub run listing incomplete")
        return payload["workflow_runs"]
    def slot_recorded(self, day, slot):
        names = [f"tg-{day}-{slot.replace(':','')}-sent"]
        if slot in ("12:45","17:15"):
            names.append(f"telegram-gold-promo-{day}-{slot.replace(':','')}-sent")
        elif slot == "16:45":
            names.append(f"telegram-evidence-{day}-market-sent")
        for name in names:
            result = self.request("artifacts?" + urlencode({"name":name}))
            if any(a["name"] == name and not a["expired"] for a in result["artifacts"]):
                return True
        return False

    def dispatch(self,workflow,slot):
        self.request(f"workflows/{workflow}/dispatches",
                     {"ref":"main","inputs":{"recovery_slot":slot}})


def wake(api,now,dry_run=False):
    planned=[]
    for workflow,slot,due in due_slots(now):
        attempts=api.runs(workflow,due-timedelta(minutes=1))
        active = any(row.get("status") in ("queued","in_progress","waiting","pending")
                     for row in attempts if row.get("head_branch") == "main")
        already = active or api.slot_recorded(due.date().isoformat(),slot)
        if already:
            planned.append({"workflow":workflow,"slot":slot,"action":"already_attempted"})
            continue
        if dry_run:
            planned.append({"workflow":workflow,"slot":slot,"action":"would_dispatch"})
            continue
        api.dispatch(workflow,slot)
        planned.append({"workflow":workflow,"slot":slot,"action":"dispatched"})
    return planned


def main():
    if os.environ.get("GITHUB_REPOSITORY")!=REPO or os.environ.get("GITHUB_REF")!="refs/heads/main":
        raise RuntimeError("Production main only")
    result=wake(GitHub(os.environ.get("GITHUB_TOKEN")),datetime.now(ROME),
                dry_run=os.environ.get("FAST_BACKUP_DRY_RUN")=="true")
    print(json.dumps({"telegram_watchdog":result},ensure_ascii=False))
    path=os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path,"a",encoding="utf8") as file:
            file.write("Telegram backup: "+(", ".join(
                r["slot"]+" "+r["action"] for r in result) if result else "no due slot")+"\\n")


if __name__=="__main__":
    main()
