"""External Fast watchdog: inspect GitHub, dispatch only a due guarded scan."""
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from fast_schedule import (AFTERNOON, EXTENDED, FORCED, HALF_HOUR, ON_HOUR,
                           ORDINARY, TZ as ROME, explicit_slot, latest_slot)

REPOSITORY = 'arabsnipertech-bet/arabsniper2'
WORKFLOW = 'fast_scan.yml'
ACTIVE = ('in_progress', 'queued', 'waiting', 'pending', 'requested')
WRITERS = {'.github/workflows/' + name for name in (
    'fast_scan.yml', 'night_scan.yml', 'freeze-audit-source.yml', 'gold-storage-check.yml',
    'manual-auditor.yml', 'odds-probe.yml', 'repair-matrix-quotes.yml',
    'run-auditor.yml', 'recover-auditor.yml', 'storage-cleanup.yml', 'v7-maintenance.yml')}
GRACE = timedelta(minutes=15)
RETRY_DELAY = timedelta(minutes=10)
MAX_FAILED_ATTEMPTS = 2
FAILED = {'failure', 'cancelled', 'timed_out', 'startup_failure', 'action_required'}
RUN_TITLE = re.compile(r'^Fast Scan \[(ordinary|extended|forced)\](?: (.*))?$')
RECORD_STEP = re.compile(r'^Record verified Fast completion \[(ordinary|extended|forced) (.*)\]$')


def instant(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if result.tzinfo is not None:
            return result
    except (AttributeError, TypeError, ValueError):
        pass
    raise ValueError('Invalid GitHub timestamp')


class TemporaryReadError(RuntimeError):
    pass


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
            if body is None and error.code in (502, 503, 504):
                raise TemporaryReadError(f'GitHub HTTP {error.code} on read') from None
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
            except (IncompleteListing, TemporaryReadError):
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

    def dispatch(self, scope, slot):
        self.request(f'workflows/{WORKFLOW}/dispatches', {'ref': 'main', 'inputs': {
            'force_fast': False, 'fast_scope': scope, 'fast_slot': slot.isoformat()}})


def publication(jobs):
    for job in jobs:
        steps = {s.get('name'): s for s in job.get('steps', [])}
        good = lambda name: steps.get(name, {}).get('conclusion') == 'success'
        if not (good('Run Fast Scan') and good('Validate generated files')
                and (good('Save verified state in private storage') or good('Publish Fast Scan safely'))):
            continue
        started = instant(steps['Run Fast Scan'].get('started_at'))
        for name in steps:
            match = RECORD_STEP.fullmatch(name) if isinstance(name, str) else None
            if match and good(name):
                kind, raw = match.groups()
                return kind, explicit_slot(raw, kind, started) if raw else None, started
        if good('Record verified Fast completion'):
            # A two-day publication performed by the previous revision still covers today.
            return ORDINARY, latest_slot(started, ORDINARY), started
    return None


def run_slot(run, created):
    match = RUN_TITLE.fullmatch(run.get('display_title') or '')
    scope, detail = match.groups() if match else (ORDINARY, None)
    if scope == FORCED:
        return scope, None
    if not detail or detail in (ON_HOUR, HALF_HOUR, AFTERNOON):
        return scope, latest_slot(created, scope, detail)
    try:
        return scope, explicit_slot(detail, scope, created)
    except ValueError:
        return scope, None


def wake_fast(api, now=None, dry_run=False):
    now = (now or datetime.now(ROME)).astimezone(ROME)
    result = {'observed_at': now.isoformat(), 'dry_run': dry_run,
              'football_calls': 0, 'dispatched': False}
    if not 6 <= now.hour <= 23:
        return dict(result, action='outside_fast_window')
    targets = [(scope, latest_slot(now, scope)) for scope in (ORDINARY, EXTENDED)]
    due = {(scope, slot): [] for scope, slot in targets if slot and now - slot >= GRACE}
    if not due:
        return dict(result, action='waiting_for_schedule')
    covered = {}
    runs = api.recent_fast_runs(now)
    for run in runs:
        if not isinstance(run, dict) or run.get('head_branch') != 'main':
            raise RuntimeError('Unexpected Fast run; no dispatch')
        if run.get('status') != 'completed':
            return dict(result, action='fast_active', run_id=run.get('id'))
    first_due = min(slot for _, slot in due)
    for run in runs:
        finished = instant(run.get('updated_at'))
        if finished < first_due:
            continue
        created = instant(run.get('created_at'))
        target = run_slot(run, created)
        jobs = None
        if finished > now:
            # The run envelope can disagree with the runner timestamps. Use
            # the terminal step only when the Fast job independently confirms it.
            jobs = api.jobs(run)
            fast_jobs = [job for job in jobs if job.get('name') == 'fast-scan'
                         and job.get('status') == 'completed' and job.get('conclusion') == run.get('conclusion')]
            terminal = [step for job in fast_jobs for step in job.get('steps', [])
                        if step.get('name') == 'Complete job' and step.get('status') == 'completed']
            if len(terminal) != 1 or not terminal[0].get('completed_at'):
                raise RuntimeError('Future GitHub run; no dispatch')
            finished = instant(terminal[0]['completed_at'])
            if finished > now:
                raise RuntimeError('Future GitHub run; no dispatch')
        if jobs is None:
            jobs = api.jobs(run)
        published = publication(jobs)
        if published:
            scope, slot, started = published
            if not created <= started <= finished:
                raise RuntimeError('Inconsistent Fast times; no dispatch')
            if (scope, slot) in due and started.astimezone(ROME).date() == now.date():
                covered[(scope, slot)] = run['id']
        elif target in due:
            failed = run.get('conclusion') in FAILED
            collected = any(s.get('name') == 'Run Fast Scan' and s.get('conclusion') == 'success'
                            for job in jobs for s in job.get('steps', []))
            if failed or collected:
                due[target].append((finished, 'unpublished' if collected else 'failed'))

    blocked = []
    for (scope, slot), failures in sorted(due.items(), key=lambda entry: entry[0][1], reverse=True):
        if (scope, slot) in covered:
            continue
        details = dict(result, scope=scope, slot=slot.isoformat(), failed_attempts=len(failures))
        if len(failures) >= MAX_FAILED_ATTEMPTS:
            blocked.append(dict(details, action='retry_limit'))
        elif failures and now - max(failures)[0] < RETRY_DELAY:
            blocked.append(dict(details, action='retry_cooldown'))
        else:
            reason = max(failures)[1] if failures else 'missed_schedule'
            return recover(api, scope, slot, dict(details, reason=reason), dry_run)
    if blocked:
        return blocked[0]
    return dict(result, action='already_published', run_ids=list(covered.values()))


def recover(api, scope, slot, result, dry_run):
    # Do not replace a pending writer: all these workflows share one mutex.
    for status in ACTIVE:
        for run in api.active_runs(status):
            if not isinstance(run, dict) or not isinstance(run.get('path'), str):
                raise RuntimeError('Invalid active run; no dispatch')
            if run['path'].split('@')[0] in WRITERS:
                return dict(result, action='writer_busy', run_id=run.get('id'))
    if dry_run:
        return dict(result, action='would_dispatch')
    api.dispatch(scope, slot)
    return dict(result, action='dispatched', dispatched=True)


def main():
    if os.getenv('GITHUB_REPOSITORY') != REPOSITORY or os.getenv('GITHUB_REF') != 'refs/heads/main':
        raise RuntimeError('Backup may run only on production main')
    try:
        result = wake_fast(GitHub(os.getenv('GITHUB_TOKEN')),
                           dry_run=os.getenv('FAST_BACKUP_DRY_RUN') == 'true')
    except IncompleteListing:
        result = {'action': 'listing_incomplete_retry_next_check', 'dispatched': False}
    print(json.dumps(result, sort_keys=True))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as out:
            out.write('Fast backup: **' + result['action'] + '**\n\n'
                      + (f"Scope {result['scope']}, slot {result['slot']}, "
                         f"reason {result.get('reason', result['action'])}.\n\n" if 'slot' in result else '')
                      + 'No API-Football calls are made by this check.\n')
    if result['action'] == 'retry_limit':
        raise RuntimeError('Fast recovery stopped after two failed attempts for the same Rome slot')


if __name__ == '__main__':
    main()
