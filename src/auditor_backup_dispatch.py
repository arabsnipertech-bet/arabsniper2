"""External nightly watchdog; no archive, storage or football API work here."""
from datetime import datetime, time, timedelta, timezone
import json
import os
from pathlib import Path
from urllib.parse import urlencode

from auditor_schedule import ROME, START, admission
from fast_backup_dispatch import ACTIVE, WRITERS, REPOSITORY, GitHub as Actions, instant

WORKFLOW = 'run-auditor.yml'
BACKUP_START = time(0, 40)
RETRY_DELAY = timedelta(minutes=10)
MAX_FAILED_ATTEMPTS = 2
RUN_STEP = 'Run and publish the daily Auditor before 01:50 Rome'
VERIFIED_STEP = 'Record verified daily Auditor completion'
FAILED = {'failure', 'cancelled', 'timed_out', 'startup_failure', 'action_required'}


class GitHub(Actions):
    def recent_auditor_runs(self, now):
        since = datetime.combine(now.astimezone(ROME).date(), START, ROME)
        return self.listing(f'workflows/{WORKFLOW}/runs?' + urlencode({
            'branch': 'main', 'created': '>=' + since.astimezone(timezone.utc).isoformat(),
            'per_page': 100}), 'workflow_runs')

    def dispatch(self):
        self.request(f'workflows/{WORKFLOW}/dispatches', {'ref': 'main'})


def verified_completion(jobs, now):
    """A green admission-only run is not evidence that the Auditor ran."""
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get('steps'), list):
            raise RuntimeError('Invalid Auditor job; no dispatch')
        if any(not isinstance(step, dict) for step in job['steps']):
            raise RuntimeError('Invalid Auditor steps; no dispatch')
        steps = {step.get('name'): step for step in job['steps']}
        executed = steps.get(RUN_STEP, {})
        proof = steps.get(VERIFIED_STEP, {})
        if executed.get('conclusion') == proof.get('conclusion') == 'success':
            started = instant(executed.get('started_at'))
            completed = instant(proof.get('completed_at'))
            if not started <= completed <= now:
                raise RuntimeError('Inconsistent Auditor completion; no dispatch')
            if started.astimezone(ROME).date() == completed.astimezone(ROME).date() == now.date():
                return True
    return False


def wake_auditor(api, now=None, dry_run=False):
    now = now or datetime.now(ROME)
    gate = admission(now)  # Also rejects naive clocks.
    now = now.astimezone(ROME)
    result = {'observed_at': now.isoformat(), 'dry_run': dry_run,
              'football_calls': 0, 'dispatched': False}
    if not gate['run'] or now.time().replace(tzinfo=None) < BACKUP_START:
        return dict(result, action='outside_audit_backup_window')
    # This check has its own mutex: it must never occupy the data-writer queue.
    for status in ACTIVE:
        for run in api.active_runs(status):
            if not isinstance(run, dict) or not isinstance(run.get('path'), str):
                raise RuntimeError('Invalid active run; no dispatch')
            if run['path'].split('@')[0] in WRITERS:
                return dict(result, action='writer_busy', run_id=run.get('id'))
    failures = []
    since = datetime.combine(now.date(), START, ROME)
    for run in api.recent_auditor_runs(now):
        if (not isinstance(run, dict) or run.get('head_branch') != 'main'
                or run.get('path', '').split('@')[0] != '.github/workflows/' + WORKFLOW):
            raise RuntimeError('Unexpected Auditor run; no dispatch')
        created, finished = instant(run.get('created_at')), instant(run.get('updated_at'))
        if not created <= finished <= now:
            raise RuntimeError('Inconsistent Auditor run times; no dispatch')
        if run.get('status') != 'completed':
            return dict(result, action='auditor_active', run_id=run.get('id'))
        if created < since:
            continue
        if verified_completion(api.jobs(run), now):
            return dict(result, action='already_completed', run_id=run.get('id'))
        if run.get('conclusion') in FAILED:
            failures.append(finished)
    if len(failures) >= MAX_FAILED_ATTEMPTS:
        return dict(result, action='retry_limit', failed_attempts=len(failures))
    if failures and now - max(failures) < RETRY_DELAY:
        return dict(result, action='retry_cooldown')
    if dry_run:
        return dict(result, action='would_dispatch')
    api.dispatch()
    return dict(result, action='dispatched', dispatched=True)


def main():
    if os.getenv('GITHUB_REPOSITORY') != REPOSITORY or os.getenv('GITHUB_REF') != 'refs/heads/main':
        raise RuntimeError('Auditor backup may run only on production main')
    result = wake_auditor(GitHub(os.getenv('GITHUB_TOKEN')),
                         dry_run=os.getenv('AUDITOR_BACKUP_DRY_RUN') == 'true')
    print(json.dumps(result, sort_keys=True))
    if os.getenv('GITHUB_STEP_SUMMARY'):
        with Path(os.environ['GITHUB_STEP_SUMMARY']).open('a') as out:
            out.write('Auditor backup: **' + result['action'] + '**\n\n'
                      + 'No API-Football or private-storage calls are made by this check.\n')
    if result['action'] == 'retry_limit':
        raise RuntimeError('Auditor recovery stopped after two failed attempts this night')


if __name__ == '__main__':
    main()
