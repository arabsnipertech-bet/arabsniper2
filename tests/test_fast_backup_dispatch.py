import io
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import fast_backup_dispatch as backup

NOW = backup.instant('2026-10-30T10:05:00+01:00')


def run(finished=None, conclusion='success'):
    return {'id': 12, 'head_branch': 'main', 'status': 'completed', 'conclusion': conclusion,
            'updated_at': (finished or NOW - timedelta(minutes=5)).isoformat()}


def jobs(started=None, overrides=None):
    steps = [{'name': name, 'conclusion': 'success'} for name in (
        'Run Fast Scan', 'Record verified Fast completion', 'Validate generated files',
        'Save verified state in private storage')]
    steps[0]['started_at'] = (started or NOW - timedelta(minutes=40)).isoformat()
    for name, value in (overrides or {}).items():
        for step in steps:
            if step['name'] == name:
                step['conclusion'] = value
    return [{'steps': steps}]


class FakeAPI:
    def __init__(self, runs=None, job_list=None, active=None):
        self.runs = runs or []
        self.job_list = jobs() if job_list is None else job_list
        self.active = active or {}
        self.dispatched = 0

    def active_runs(self, status): return self.active.get(status, [])
    def recent_fast_runs(self, now): return self.runs
    def jobs(self, run): return self.job_list
    def dispatch(self): self.dispatched += 1


class FastBackupTests(unittest.TestCase):
    def test_missing_scan_dispatches_once_and_dry_run_does_not_dispatch(self):
        for dry in (True, False):
            api = FakeAPI()
            result = backup.wake_fast(api, NOW, dry)
            self.assertEqual(result['action'], 'would_dispatch' if dry else 'dispatched')
            self.assertEqual(api.dispatched, 0 if dry else 1)
            self.assertEqual(result['football_calls'], 0)

    def test_all_shared_writers_and_pending_states_block_dispatch(self):
        for path in backup.WRITERS:
            for status in backup.ACTIVE:
                with self.subTest(path=path, status=status):
                    api = FakeAPI(active={status: [{'path': path, 'id': 8}]})
                    self.assertEqual(backup.wake_fast(api, NOW)['action'], 'writer_busy')
                    self.assertEqual(api.dispatched, 0)

    def test_unrelated_ci_and_backup_do_not_block(self):
        api = FakeAPI(active={'in_progress': [{'path': '.github/workflows/fast_scan_backup.yml', 'id': 9}]})
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'dispatched')

    def test_recent_published_collection_is_skipped_even_if_public_homepage_push_fails(self):
        for conclusion in ('success', 'failure'):
            api = FakeAPI([run(conclusion=conclusion)])
            self.assertEqual(backup.wake_fast(api, NOW)['action'], 'already_published')
            self.assertEqual(api.dispatched, 0)

    def test_new_publication_of_old_collection_does_not_delay_next_scan(self):
        api = FakeAPI([run()], jobs(started=NOW - timedelta(minutes=61)))
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'dispatched')

    def test_skipped_cached_unvalidated_and_unpublished_runs_are_not_fresh(self):
        for name in ('Run Fast Scan', 'Record verified Fast completion',
                     'Validate generated files', 'Save verified state in private storage'):
            with self.subTest(name=name):
                api = FakeAPI([run()], jobs(overrides={name: 'skipped'}))
                self.assertEqual(backup.wake_fast(api, NOW)['action'], 'dispatched')

    def test_failed_publication_cools_down_then_retries_and_is_bounded(self):
        failed_jobs = jobs(overrides={'Save verified state in private storage': 'failure'})
        api = FakeAPI([run(conclusion='failure')], failed_jobs)
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'retry_cooldown')
        api = FakeAPI([run(NOW - timedelta(minutes=15), 'failure')], failed_jobs)
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'dispatched')
        api = FakeAPI([run(NOW - timedelta(minutes=15), 'failure'),
                       run(NOW - timedelta(minutes=40), 'timed_out')], failed_jobs)
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'retry_limit')
        self.assertEqual(api.dispatched, 0)

    def test_old_failures_do_not_permanently_lock_recovery(self):
        api = FakeAPI([run(NOW - timedelta(minutes=61), 'failure')], [])
        self.assertEqual(backup.wake_fast(api, NOW)['action'], 'dispatched')

    def test_window_rome_summer_winter_and_midnight(self):
        for hour in (0, 2, 5):
            api = FakeAPI()
            self.assertEqual(backup.wake_fast(api, NOW.replace(hour=hour))['action'], 'outside_fast_window')
            self.assertEqual(api.dispatched, 0)
        for at in ('2026-07-11T04:05:00Z', '2026-10-30T05:05:00Z', '2026-10-30T22:55:00Z'):
            self.assertEqual(backup.wake_fast(FakeAPI(), backup.instant(at))['action'], 'dispatched')

    def test_active_race_and_malformed_data_fail_closed(self):
        active = run(); active['status'] = 'queued'
        self.assertEqual(backup.wake_fast(FakeAPI([active]), NOW)['action'], 'fast_active')
        api = FakeAPI([run(NOW + timedelta(seconds=1))])
        with self.assertRaises(RuntimeError): backup.wake_fast(api, NOW)
        self.assertEqual(api.dispatched, 0)
        api = FakeAPI(active={'in_progress': [{}]})
        with self.assertRaises(RuntimeError): backup.wake_fast(api, NOW)
        self.assertEqual(api.dispatched, 0)

    def test_http_payload_does_not_force_or_change_branch(self):
        with patch.object(backup.GitHub, 'request', return_value={}) as request:
            backup.GitHub('test-placeholder').dispatch()
            request.assert_called_once_with('workflows/fast_scan.yml/dispatches',
                                           {'ref': 'main', 'inputs': {'force_fast': False}})

    def test_truncated_or_malformed_listings_are_rejected(self):
        for value in ({}, {'total_count': 101, 'workflow_runs': []},
                      {'total_count': False, 'workflow_runs': []}):
            with patch.object(backup.GitHub, 'request', return_value=value):
                with self.assertRaises(RuntimeError): backup.GitHub('test-placeholder').active_runs('queued')

    def test_transient_count_mismatch_repeats_the_get_only(self):
        api = backup.GitHub('test')
        busy = {'path': '.github/workflows/fast_scan.yml', 'id': 7}
        with patch.object(api, 'recent_fast_runs', return_value=[]), patch.object(api, 'request', side_effect=[
                {'total_count': 1, 'workflow_runs': []},
                {'total_count': 1, 'workflow_runs': [busy]}]) as get, patch.object(backup, 'sleep'):
            self.assertEqual(backup.wake_fast(api, NOW)['action'], 'writer_busy')
            self.assertEqual(get.call_count, 2)
            self.assertTrue(all(len(c.args) == 1 for c in get.call_args_list))

    def test_a_verified_fresh_fast_does_not_need_queue_listings(self):
        api = FakeAPI([run()])
        with patch.object(api, 'active_runs', side_effect=AssertionError('unnecessary queue read')):
            self.assertEqual(backup.wake_fast(api, NOW)['action'], 'already_published')

    def test_pagination_requires_all_distinct_jobs(self):
        api = backup.GitHub('test')
        first = {'total_count': 101, 'jobs': [{'id': n} for n in range(1, 101)]}
        with patch.object(api, 'request', side_effect=[first, {'total_count': 101, 'jobs': [{'id': 101}]}]) as get:
            self.assertEqual(len(api.jobs({'id': 42})), 101)
            self.assertIn('&page=2', get.call_args.args[0])
        with patch.object(api, 'request', side_effect=[first, {'total_count': 101, 'jobs': [{'id': 1}]}]*3), patch.object(backup, 'sleep'):
            with self.assertRaises(backup.IncompleteListing): api.jobs({'id': 42})

    def test_hourly_wake_does_not_drift_with_runner_setup(self):
        start = backup.instant('2026-10-31T06:09:14+01:00')
        self.assertFalse(backup.fresh_for_hour(start, backup.instant('2026-10-31T07:05:23+01:00')))
        self.assertTrue(backup.fresh_for_hour(start, backup.instant('2026-10-31T06:55:00+01:00')))
        late = backup.instant('2026-10-31T06:48:00+01:00')
        self.assertTrue(backup.fresh_for_hour(late, backup.instant('2026-10-31T07:05:00+01:00')))

    def test_auth_errors_do_not_print_tokens_or_response_bodies(self):
        error = HTTPError('https://api.github.com', 403, 'secret', {}, io.BytesIO(b'secret'))
        with patch.object(backup, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as raised:
                backup.GitHub('test-placeholder').active_runs('queued')
        self.assertEqual(str(raised.exception), 'GitHub HTTP 403; no dispatch retry')

    def test_nonproduction_main_is_rejected_before_network(self):
        with patch.dict(backup.os.environ, {'GITHUB_REPOSITORY': backup.REPOSITORY, 'GITHUB_REF': 'refs/heads/test'}):
            with patch.object(backup.GitHub, 'request') as request:
                with self.assertRaises(RuntimeError): backup.main()
                request.assert_not_called()


if __name__ == '__main__': unittest.main()
