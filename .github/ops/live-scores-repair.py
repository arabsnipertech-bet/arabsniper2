"""Bounded reuse of the approved daily recovery; never loops or rewrites forecasts."""
import argparse
from datetime import date, datetime, timedelta, timezone
import json
import os
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

REPO = 'arabsnipertech-bet/arabsniper2'
WORKFLOW = 'recover-daily-results.yml'
ROME = ZoneInfo('Europe/Rome')

def permitted(day, now, runs):
    age = (now.astimezone(ROME).date() - date.fromisoformat(day)).days
    if not 1 <= age <= 3:
        return False
    for run in runs:
        if run.get('status') != 'completed':
            return False
        if run.get('conclusion') == 'skipped':
            continue
        created = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
        if now - created < timedelta(hours=12):
            return False
    return True

def run(day):
    if os.environ.get('GITHUB_REPOSITORY') != REPO or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('Recovery is restricted to production main')
    def api(path, data=None):
        payload = json.dumps(data).encode() if data is not None else None
        request = Request('https://api.github.com/repos/' + REPO + path, data=payload,
            headers={'Authorization':'Bearer ' + os.environ['GH_TOKEN'],
                     'Accept':'application/vnd.github+json','Content-Type':'application/json'})
        with urlopen(request, timeout=20) as response:
            body = response.read()
            return json.loads(body) if body else None
    path = '/actions/workflows/' + WORKFLOW
    runs = api(path + '/runs?branch=main&per_page=100')['workflow_runs']
    if not permitted(day, datetime.now(timezone.utc), runs):
        print('Recovery deferred: active/recent recovery or date outside the bounded window.')
        return
    api(path + '/dispatches', {'ref':'main','inputs':{'date':day,'refresh':'true'}})
    print('Requested existing immutable-forecast recovery for ' + day)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('date')
    args = parser.parse_args()
    run(args.date)
