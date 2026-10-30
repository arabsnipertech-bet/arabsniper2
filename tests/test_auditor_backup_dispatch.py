from copy import deepcopy
from datetime import timedelta
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import auditor_backup_dispatch as backup
import nightly_auditor

NOW = backup.instant('2026-10-31T00:50:00+01:00')


def run(finished=None, conclusion='success', **values):
    return dict({'id': 12, 'head_branch': 'main', 'status': 'completed',
        'path': '.github/workflows/run-auditor.yml', 'conclusion': conclusion,
        'created_at': (NOW - timedelta(minutes=20)).isoformat(),
        'updated_at': (finished or NOW - timedelta(minutes=5)).isoformat()}, **values)


def jobs():
    return [{'name': 'auditor', 'steps': [
        {'name': backup.RUN_STEP, 'conclusion': 'success',
         'started_at': (NOW - timedelta(minutes=19)).isoformat()},
        {'name': backup.VERIFIED_STEP, 'conclusion': 'success',
         'completed_at': (NOW - timedelta(minutes=6)).isoformat()}]}]


class FakeAPI:
    def __init__(self, runs=None, job_list=None, active=None):
        self.runs = runs or []
        self.job_list = jobs() if job_list is None else job_list
        self.active = active or {}
        self.dispatched = 0

    def active_runs(self, status): return self.active.get(status, [])
    def recent_auditor_runs(self, now): return self.runs
    def jobs(self, run): return self.job_list
    def dispatch(self): self.dispatched += 1


class AuditorBackupTests(unittest.TestCase):
    def test_missing_nightly_scan_dispatches_and_dry_run_does_not(self):
        for dry in (True, False):
            api = FakeAPI()
            result = backup.wake_auditor(api, NOW, dry)
            self.assertEqual(result['action'], 'would_dispatch' if dry else 'dispatched')
            self.assertEqual(api.dispatched, int(not dry))
            self.assertEqual(result['football_calls'], 0)

    def test_before_backup_and_after_window_never_touch_network(self):
        for at in ('2026-10-31T00:30:00+01:00', '2026-10-31T00:39:59+01:00',
                   '2026-10-31T01:00:00+01:00', '2026-10-31T02:00:00+01:00',
                   '2026-10-31T18:00:00+01:00'):
            api = Mock()
            self.assertEqual(backup.wake_auditor(api, backup.instant(at))['action'],
                             'outside_audit_backup_window')
            self.assertEqual(api.mock_calls, [])

    def test_rome_summer_winter_and_utc_date_boundary(self):
        for at in ('2026-10-30T23:40:00Z', '2026-10-30T23:59:59Z',
                   '2026-07-30T22:40:00Z'):
            self.assertEqual(backup.wake_auditor(FakeAPI(), backup.instant(at))['action'], 'dispatched')
        with self.assertRaises(ValueError):
            backup.wake_auditor(FakeAPI(), NOW.replace(tzinfo=None))

    def test_any_active_writer_blocks_in_every_pending_state(self):
        for path in backup.WRITERS:
            for status in backup.ACTIVE:
                with self.subTest(path=path, status=status):
                    api = FakeAPI(active={status: [{'id': 9, 'path': path + '@refs/heads/main'}]})
                    self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'writer_busy')
                    self.assertEqual(api.dispatched, 0)

    def test_backup_and_ci_do_not_block_their_own_recovery(self):
        api = FakeAPI(active={'in_progress': [
            {'id': 8, 'path': '.github/workflows/auditor_backup.yml'},
            {'id': 9, 'path': '.github/workflows/policy_tests.yml'}]})
        self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'dispatched')

    def test_verified_completion_prevents_duplicate_even_if_later_job_fails(self):
        for conclusion in ('success', 'failure'):
            api = FakeAPI([run(conclusion=conclusion)])
            self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'already_completed')
            self.assertEqual(api.dispatched, 0)

    def test_green_skipped_admission_or_missing_completion_is_not_a_run(self):
        variants = [[{'name': 'admission', 'steps': []}], []]
        for index in (0, 1):
            value = jobs(); value[0]['steps'][index]['conclusion'] = 'skipped'
            variants.append(value)
        value = jobs(); value[0]['steps'].pop(); variants.append(value)
        for value in variants:
            with self.subTest(jobs=value):
                api = FakeAPI([run()], value)
                self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'dispatched')

    def test_previous_rome_day_does_not_lock_tonights_audit(self):
        yesterday = run(created_at=(NOW - timedelta(days=1, minutes=20)).isoformat(),
                        updated_at=(NOW - timedelta(days=1)).isoformat())
        api = FakeAPI([yesterday], [])
        self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'dispatched')

    def test_failed_attempt_cooldown_and_two_failure_limit(self):
        api = FakeAPI([run(conclusion='failure')], [])
        self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'retry_cooldown')
        failed = run(NOW - timedelta(minutes=10), 'failure')
        api = FakeAPI([failed], [])
        self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'dispatched')
        api = FakeAPI([failed, run(NOW - timedelta(minutes=15), 'timed_out', id=13)], [])
        self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'retry_limit')
        self.assertEqual(api.dispatched, 0)

    def test_run_becoming_active_between_queries_blocks_dispatch(self):
        for status in backup.ACTIVE:
            api = FakeAPI([run(status=status)])
            self.assertEqual(backup.wake_auditor(api, NOW)['action'], 'auditor_active')
            self.assertEqual(api.dispatched, 0)

    def test_invalid_runs_jobs_or_future_proofs_fail_closed(self):
        cases = [FakeAPI(active={'queued': [{}]}), FakeAPI([{}]),
                 FakeAPI([run(head_branch='other')]),
                 FakeAPI([run(NOW + timedelta(seconds=1))]),
                 FakeAPI([run()], [{}]), FakeAPI([run()], [{'steps': [None]}])]
        bad = deepcopy(jobs()); bad[0]['steps'][1]['completed_at'] = (NOW + timedelta(seconds=1)).isoformat()
        cases.append(FakeAPI([run()], bad))
        for api in cases:
            with self.subTest(api=api):
                with self.assertRaises((RuntimeError, ValueError)):
                    backup.wake_auditor(api, NOW)
                self.assertEqual(api.dispatched, 0)

    def test_dispatch_uses_protected_main_entry_without_force_flag(self):
        with patch.object(backup.GitHub, 'request', return_value={}) as request:
            backup.GitHub('test-placeholder').dispatch()
            request.assert_called_once_with('workflows/run-auditor.yml/dispatches', {'ref': 'main'})

    def test_history_query_uses_rome_nightly_start_and_rejects_truncation(self):
        with patch.object(backup.GitHub, 'request', return_value={'total_count': 0, 'workflow_runs': []}) as request:
            backup.GitHub('test-placeholder').recent_auditor_runs(NOW)
            path = urlsplit(request.call_args.args[0])
            self.assertEqual(path.path, 'workflows/run-auditor.yml/runs')
            self.assertEqual(parse_qs(path.query)['created'], ['>=2026-10-30T23:30:00+00:00'])
        with patch.object(backup.GitHub, 'request', return_value={'total_count': 101, 'workflow_runs': []}):
            with self.assertRaises(RuntimeError):
                backup.GitHub('test-placeholder').recent_auditor_runs(NOW)

    def test_nonproduction_branch_rejected_before_network(self):
        with patch.dict(os.environ, {'GITHUB_REPOSITORY': backup.REPOSITORY, 'GITHUB_REF': 'refs/heads/test'}):
            with patch.object(backup.GitHub, 'request') as request:
                with self.assertRaises(RuntimeError): backup.main()
                request.assert_not_called()

    def test_only_verified_runner_outcomes_emit_completion(self):
        for result in ('complete', 'already_completed', 'outside_audit_window'):
            with tempfile.TemporaryDirectory() as folder:
                output = Path(folder)/'outputs'
                with patch.dict(os.environ, {'GITHUB_OUTPUT': str(output)}), patch.object(nightly_auditor, 'run', return_value=result):
                    nightly_auditor.main()
                self.assertEqual(output.read_text(), 'completed=' + str(result != 'outside_audit_window').lower() + '\n')

    def test_failed_publication_never_emits_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)/'outputs'
            with patch.dict(os.environ, {'GITHUB_OUTPUT': str(output)}), patch.object(nightly_auditor, 'run', side_effect=TimeoutError):
                with self.assertRaises(TimeoutError): nightly_auditor.main()
            self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
