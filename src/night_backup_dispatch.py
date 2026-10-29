"""External Night wake-up: GitHub metadata only, never football or Gold data.

cron-job.org dispatches the small backup workflow. This module checks active
Night jobs and successful runs before dispatching the existing guarded Night.
The authoritative once-per-Rome-date check remains in night_scan_guard.py.
"""
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


REPOSITORY = 'arabsnipertech-bet/arabsniper2'
WORKFLOW = 'night_scan.yml'
ROME = ZoneInfo('Europe/Rome')
ACTIVE = ('in_progress', 'queued', 'waiting', 'pending', 'requested')


def instant(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        raise ValueError('Invalid GitHub run timestamp') from None
    if result.tzinfo is None:
        raise ValueError('GitHub run timestamp has no timezone')
    return result


class GitHub:
    def __init__(self, token):
        if not token:
            raise ValueError('Missing Actions token')
        self.token = token

    def request(self, suffix, body=None):
        request = Request(
            f'https://api.github.com/repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/{suffix}',
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': f'Bearer {self.token}',
                     'Accept': 'application/vnd.github+json',
                     'X-GitHub-Api-Version': '2022-11-28',
                     'User-Agent': 'ArabSniper-Night-Backup',
                     'Content-Type': 'application/json'},
            method='GET' if body is None else 'POST')
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except HTTPError as error:
            # Do not print upstream content, request headers, or credentials.
            raise RuntimeError(f'GitHub HTTP {error.code}; no automatic retry') from None
        except (URLError, TimeoutError, json.JSONDecodeError):
            raise RuntimeError('GitHub unavailable or invalid response; no automatic retry') from None

    def runs(self, status):
        payload = self.request('runs?' + urlencode({
            'branch': 'main', 'status': status,
            'per_page': 100 if status == 'success' else 1}))
        if (not isinstance(payload, dict) or not isinstance(payload.get('workflow_runs'), list)
                or not isinstance(payload.get('total_count'), int)
                or payload['total_count'] < len(payload['workflow_runs'])
                or (payload['total_count'] and not payload['workflow_runs'])):
            raise RuntimeError('Invalid GitHub run listing; no dispatch')
        return payload['workflow_runs']

    def dispatch(self):
        self.request('dispatches', {'ref': 'main', 'inputs': {'force_night': False}})


def wake_night(api, now=None, dry_run=False):
    now = (now or datetime.now(ROME)).astimezone(ROME)
    result = {'date': now.date().isoformat(), 'observed_at': now.isoformat(),
              'dry_run': dry_run, 'football_calls': 0, 'dispatched': False}
    for status in ACTIVE:
        runs = api.runs(status)
        if runs:
            return dict(result, action='already_active', run_id=runs[0].get('id'))
    for run in api.runs('success'):
        if not isinstance(run, dict) or run.get('head_branch') != 'main':
            raise RuntimeError('Unexpected GitHub run; no dispatch')
        started = instant(run.get('run_started_at'))
        finished = instant(run.get('updated_at'))
        if finished < started or finished > now:
            raise RuntimeError('Inconsistent GitHub run times; no dispatch')
        if (run.get('status') == 'completed' and run.get('conclusion') == 'success'
                and started.astimezone(ROME).date() == now.date()
                and finished.astimezone(ROME).date() == now.date()):
            return dict(result, action='already_completed', run_id=run.get('id'))
    # Daytime connection tests and delayed calls cannot start an extra scan.
    if not 2 <= now.hour < 6:
        return dict(result, action='outside_night_window')
    if dry_run:
        return dict(result, action='would_dispatch')
    api.dispatch()
    return dict(result, action='dispatched', dispatched=True)


def main():
    if (os.getenv('GITHUB_REPOSITORY') != REPOSITORY
            or os.getenv('GITHUB_REF') != 'refs/heads/main'):
        raise RuntimeError('Backup may run only on the production main branch')
    result = wake_night(GitHub(os.getenv('GITHUB_TOKEN')),
                       dry_run=os.getenv('NIGHT_BACKUP_DRY_RUN') == 'true')
    print(json.dumps(result, sort_keys=True))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as output:
            output.write('Night backup: **' + result['action'] + '**\n\n'
                         + 'Rome date: ' + result['date'] + '. '
                         + 'This check makes no API-Football calls.\n')


if __name__ == '__main__':
    main()
