"""External Fast watchdog: inspect GitHub, dispatch only a due guarded scan."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

REPOSITORY = 'arabsnipertech-bet/arabsniper2'
WORKFLOW = 'fast_scan.yml'
ROME = ZoneInfo('Europe/Rome')
ACTIVE = ('in_progress', 'queued', 'waiting', 'pending', 'requested')
WRITERS = {'.github/workflows/' + name for name in (
    'fast_scan.yml', 'night_scan.yml', 'freeze-audit-source.yml', 'gold-storage-check.yml',
    'manual-auditor.yml', 'odds-probe.yml', 'repair-matrix-quotes.yml',
    'run-auditor.yml', 'storage-cleanup.yml')}
INTERVAL = timedelta(minutes=60)
RETRY_DELAY = timedelta(minutes=10)
MAX_FAILED_ATTEMPTS = 2


def instant(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is not None:
            return result
    except (AttributeError, TypeError, ValueError):
        pass
    raise ValueError('Invalid GitHub timestamp')


def fresh_for_hour(started, now):
    """Keep one collection per Rome hour, with a 50-minute floor at boundaries."""
    start = started.astimezone(timezone.utc)
    end = now.astimezone(timezone.utc)
    age = end - start
    same_hour = (start.replace(minute=0, second=0, microsecond=0) ==
                 end.replace(minute=0, second=0, microsecond=0))
    return timedelta() <= age < INTERVAL and (same_hour or age < timedelta(minutes=50))


class IncompleteListing(RuntimeError):
    pass


class GitHub:
    def __init__(self, token):
        if not token:
            raise ValueError('Missing Actions token')
        self.token = token

    def request(self, suffix, body=None):
        request = Request(f'https://api.github.com/repos/{REPOSITORY}/actions/{suffix}',
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': f'Bearer {self.token}', 'Accept': 'application/vnd.github+json',
                'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'ArabSniper-Fast-Backup',
                'Content-Type': 'application/json'}, method='GET' if body is None else 'POST')
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except HTTPError as error:
            raise RuntimeError(f'GitHub HTTP {error.code}; no dispatch retry') from None
        except (URLError, TimeoutError, json.JSONDecodeError):
            raise RuntimeError('GitHub unavailable or invalid response; no dispatch retry') from None

    def listing(self, suffix, key):
        # Actions can change state between counting and returning the rows.
        # Only repeat read requests. Never retry a dispatch with an unknown outcome.
        for delay in (0, 1, 2):
            if delay:
                sleep(delay)
            try:
                return self._read_listing(suffix, key)
            except IncompleteListing:
                if delay == 2:
                    raise

    def _read_listing(self, suffix, key):
        rows = []
        total = None
        for page in range(1, 6):
            query = suffix if page == 1 else f'{suffix}&page={page}'
            data = self.request(query)
            if (not isinstance(data, dict) or not isinstance(data.get(key), list)
                    or type(data.get('total_count')) is not int or data['total_count'] < 0):
                raise RuntimeError('Invalid GitHub listing; no dispatch')
            if total is None:
                total = data['total_count']
            if total != data['total_count'] or total > 500:
                break
            batch = data[key]
            rows.extend(batch)
            if len(rows) == total:
                ids = [row.get('id') for row in rows if isinstance(row, dict)]
                if len(ids) == total and all(type(i) is int and i > 0 for i in ids) and len(set(ids)) == total:
                    return rows
                break
            if len(batch) != 100 or len(rows) > total:
                break
        raise IncompleteListing('Incomplete GitHub listing; no dispatch')

    def active_runs(self, status):
        return self.listing('runs?' + urlencode({'status': status, 'per_page': 100}), 'workflow_runs')

    def recent_fast_runs(self, now):
        since = (now - timedelta(hours=24)).astimezone(timezone.utc).isoformat()
        return self.listing(f'workflows/{WORKFLOW}/runs?' + urlencode({
            'branch': 'main', 'created': '>=' + since, 'per_page': 100}), 'workflow_runs')

    def jobs(self, run):
        run_id = run.get('id')
        if type(run_id) is not int or run_id < 1:
            raise ValueError('Invalid run id')
        return self.listing(f'runs/{run_id}/jobs?filter=latest&per_page=100', 'jobs')

    def dispatch(self):
        self.request(f'workflows/{WORKFLOW}/dispatches', {'ref': 'main', 'inputs': {'force_fast': False}})


def published_start(jobs):
    for job in jobs:
        steps = {s.get('name'): s for s in job.get('steps', [])}
        good = lambda name: steps.get(name, {}).get('conclusion') == 'success'
        if (good('Run Fast Scan') and good('Record verified Fast completion')
                and good('Validate generated files')
                and (good('Save verified state in private storage') or good('Publish Fast Scan safely'))):
            # A later public homepage push failure does not undo published Gold.
            return instant(steps['Run Fast Scan'].get('started_at'))
    return None


def wake_fast(api, now=None, dry_run=False):
    now = (now or datetime.now(ROME)).astimezone(ROME)
    result = {'observed_at': now.isoformat(), 'dry_run': dry_run,
              'football_calls': 0, 'dispatched': False}
    if not 6 <= now.hour <= 23:
        return dict(result, action='outside_fast_window')
    failures = []
    for run in api.recent_fast_runs(now):
        if not isinstance(run, dict) or run.get('head_branch') != 'main':
            raise RuntimeError('Unexpected Fast run; no dispatch')
        if run.get('status') != 'completed':
            return dict(result, action='fast_active', run_id=run.get('id'))
        finished = instant(run.get('updated_at'))
        if finished > now:
            raise RuntimeError('Future GitHub run; no dispatch')
        if now - finished >= INTERVAL:
            continue
        jobs = api.jobs(run)
        started = published_start(jobs)
        if started:
            if not started <= finished:
                raise RuntimeError('Inconsistent Fast times; no dispatch')
            if started.astimezone(ROME).date() == now.date() and fresh_for_hour(started, now):
                return dict(result, action='already_published', run_id=run.get('id'),
                            collection_started_at=started.isoformat())
        elif run.get('conclusion') in ('failure', 'cancelled', 'timed_out', 'startup_failure', 'action_required'):
            failures.append(finished)
    if len(failures) >= MAX_FAILED_ATTEMPTS:
        return dict(result, action='retry_limit', failed_attempts=len(failures))
    if failures and now - max(failures) < RETRY_DELAY:
        return dict(result, action='retry_cooldown')
    # Do not replace a pending writer: all these workflows share one mutex.
    for status in ACTIVE:
        for run in api.active_runs(status):
            if not isinstance(run, dict) or not isinstance(run.get('path'), str):
                raise RuntimeError('Invalid active run; no dispatch')
            if run['path'].split('@')[0] in WRITERS:
                return dict(result, action='writer_busy', run_id=run.get('id'))
    if dry_run:
        return dict(result, action='would_dispatch')
    api.dispatch()
    return dict(result, action='dispatched', dispatched=True)


def main():
    if os.getenv('GITHUB_REPOSITORY') != REPOSITORY or os.getenv('GITHUB_REF') != 'refs/heads/main':
        raise RuntimeError('Backup may run only on production main')
    result = wake_fast(GitHub(os.getenv('GITHUB_TOKEN')),
                       dry_run=os.getenv('FAST_BACKUP_DRY_RUN') == 'true')
    print(json.dumps(result, sort_keys=True))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as out:
            out.write('Fast backup: **' + result['action'] + '**\n\n'
                      + 'No API-Football calls are made by this check.\n')
    if result['action'] == 'retry_limit':
        raise RuntimeError('Fast recovery stopped after two failed attempts within 60 minutes')


if __name__ == '__main__':
    main()
