"""Wake due Telegram publications from the external ten-minute Fast watchdog.

Independent from GitHub schedule events. No sports predictions are invented here:
each target workflow retains its own validation, dedup and freshness checks.
"""
import json
import os
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

ROME=ZoneInfo("Europe/Rome")
REPO="arabsnipertech-bet/arabsniper2"
from telegram_slots import CHANNEL_SLOTS

SLOTS = tuple(("telegram-digest.yml", clock, clock, 24) for clock in CHANNEL_SLOTS)
# Check during the first external tick at :05/:15/:25/:35/:45/:55.
# Active runs, receipts and the publisher's reservation prevent duplicates.
MINIMUM_DELAY=timedelta(seconds=1)


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
        elif slot == "10:45":
            names.append(f"telegram-evidence-{day}-audit-sent")
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

    def facebook_recorded(self, day, slot):
        name = f"fb-{day}-{slot.replace(':', '')}-sent"
        result = self.request('artifacts?' + urlencode({'name': name}))
        return any(a['name'] == name and not a['expired'] for a in result['artifacts'])


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


PUBLISHER_WORKFLOW = 'publisher-v71.yml'
PUBLISHER_INTERVAL = timedelta(minutes=17)
LIVE_INTERVAL = timedelta(minutes=9)
PUBLISHER_FEED = f'https://raw.githubusercontent.com/{REPO}/publisher-public-v71/homepage.json'


def publisher_live_active(now):
    """Read the public coverage state from the same Publisher."""
    try:
        with urlopen(PUBLISHER_FEED, timeout=5) as response:
            feed = json.loads(response.read(100_001))
        if feed.get('publisher') != 'v7.1':
            return False
        live = feed.get('live') or {}
        if live.get('status') not in ('Prematch', 'In corso', 'Primo tempo', 'Intervallo', 'Secondo tempo'):
            return False
        start = datetime.fromisoformat(live['kickoff'].replace('Z', '+00:00'))
        return start.tzinfo is not None and start <= now < start + timedelta(hours=5)
    except (AttributeError, KeyError, TypeError, URLError, TimeoutError, ValueError):
        return False


def wake_publisher(api, now, *, dry_run=False):
    """Wake the existing Publisher if neither its cron nor Fast woke it recently."""
    path = 'workflows/' + PUBLISHER_WORKFLOW
    if api.request(path).get('state') != 'active':
        return 'workflow_disabled'
    runs = api.runs(PUBLISHER_WORKFLOW, now - timedelta(minutes=45))
    if any(run['status'] != 'completed' for run in runs):
        return 'already_running'
    if runs:
        created = max(datetime.fromisoformat(run['created_at'].replace('Z', '+00:00')) for run in runs)
        age = now - created
        if age < LIVE_INTERVAL or (age < PUBLISHER_INTERVAL and not publisher_live_active(now)):
            return 'recent_cycle'
    if dry_run:
        return 'would_dispatch'
    api.request(path + '/dispatches', {'ref': 'main', 'inputs': {'dry_run': 'false'}})
    return 'dispatched'


def main():
    if os.environ.get("GITHUB_REPOSITORY")!=REPO or os.environ.get("GITHUB_REF")!="refs/heads/main":
        raise RuntimeError("Production main only")
    from publisher_v71.cutover import ENABLED
    if ENABLED:
        public_path = Path(__file__).resolve().parents[1] / 'data/arab_quote_homepage.json'
        with public_path.open(encoding='utf-8') as source:
            safe = json.load(source).get('publisher') == 'v7.1'
        result = (wake_publisher(GitHub(os.environ.get('GITHUB_TOKEN')), datetime.now(ROME),
                                 dry_run=os.environ.get('FAST_BACKUP_DRY_RUN') == 'true')
                  if safe else 'awaiting_safe_public_projection')
        print(json.dumps({'publisher_watchdog': result}))
        return
    result=wake(GitHub(os.environ.get("GITHUB_TOKEN")),datetime.now(ROME),
                dry_run=os.environ.get("FAST_BACKUP_DRY_RUN")=="true")
    print(json.dumps({"telegram_watchdog":result},ensure_ascii=False))
    path=os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path,"a",encoding="utf8") as file:
            file.write("Telegram backup: "+(", ".join(
                r["slot"]+" "+r["action"] for r in result) if result else "no due slot")+"\\n")


def wake_facebook(api, now, dry_run=False):
    """A Telegram receipt is the input, never proof of Facebook completion."""
    result = []
    for _, slot, due in due_slots(now):
        day = due.date().isoformat()
        if not api.slot_recorded(day, slot) or api.facebook_recorded(day, slot):
            continue
        runs = api.runs('facebook-recovery.yml', due-timedelta(minutes=1))
        if any(r.get('status') in ('queued', 'in_progress', 'waiting', 'pending') for r in runs):
            continue
        if not dry_run:
            api.dispatch('facebook-recovery.yml', slot)
        result.append({'slot': slot, 'action': 'would_dispatch' if dry_run else 'dispatched'})
    return result


def facebook_main():
    from publisher_v71.cutover import ENABLED
    if ENABLED:
        print('Editorial recovery is owned by Publisher V7.1')
        return
    if os.environ.get('GITHUB_REPOSITORY') != REPO or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise RuntimeError('Production main only')
    result = wake_facebook(GitHub(os.environ.get('GITHUB_TOKEN')), datetime.now(ROME),
                           dry_run=os.environ.get('FAST_BACKUP_DRY_RUN') == 'true')
    print(json.dumps({'facebook_watchdog': result}))


if __name__=="__main__":
    import sys
    facebook_main() if sys.argv[1:] == ['facebook'] else main()
