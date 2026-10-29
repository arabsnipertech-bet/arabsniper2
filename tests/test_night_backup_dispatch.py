import io
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import night_backup_dispatch as backup


def successful(start='2026-10-29T00:55:00Z', end='2026-10-29T01:15:00Z'):
    return {'id': 123, 'head_branch': 'main', 'status': 'completed',
            'conclusion': 'success', 'run_started_at': start, 'updated_at': end}


class FakeAPI:
    def __init__(self, runs=None):
        self.by_status = runs or {}
        self.dispatched = 0

    def runs(self, status):
        return self.by_status.get(status, [])

    def dispatch(self):
        self.dispatched += 1


class NightBackupTests(unittest.TestCase):
    now = backup.instant('2026-10-29T02:20:00+01:00')

    def test_active_and_queued_night_are_never_replaced(self):
        for status in backup.ACTIVE:
            with self.subTest(status=status):
                api = FakeAPI({status: [{'id': 45}]})
                self.assertEqual(backup.wake_night(api, self.now)['action'], 'already_active')
                self.assertEqual(api.dispatched, 0)

    def test_complete_today_skips_without_a_second_scan(self):
        api = FakeAPI({'success': [successful()]})
        self.assertEqual(backup.wake_night(api, self.now)['action'], 'already_completed')
        self.assertEqual(api.dispatched, 0)

    def test_yesterday_or_cross_midnight_is_not_todays_valid_completion(self):
        for start, end in [('2026-10-28T00:55:00Z', '2026-10-28T01:15:00Z'),
                           ('2026-10-28T22:55:00Z', '2026-10-28T23:15:00Z')]:
            with self.subTest(start=start):
                api = FakeAPI({'success': [successful(start, end)]})
                self.assertEqual(backup.wake_night(api, self.now)['action'], 'dispatched')
                self.assertEqual(api.dispatched, 1)

    def test_rome_date_in_summer_and_both_autumn_hours(self):
        api = FakeAPI({'success': [successful('2026-07-10T22:10:00Z', '2026-07-10T23:50:00Z')]})
        self.assertEqual(backup.wake_night(api, backup.instant('2026-07-11T02:20:00+02:00'))['action'], 'already_completed')
        for offset in ('+02:00', '+01:00'):
            with self.subTest(offset=offset):
                api = FakeAPI()
                result = backup.wake_night(api, backup.instant('2026-10-25T02:20:00' + offset))
                self.assertEqual(result['date'], '2026-10-25')
                self.assertEqual(api.dispatched, 1)

    def test_failed_missing_and_cancelled_do_not_prevent_recovery(self):
        for runs in ({}, {'failure': [successful()]}, {'cancelled': [successful()]}):
            api = FakeAPI(runs)
            self.assertEqual(backup.wake_night(api, self.now)['action'], 'dispatched')
            self.assertEqual(api.dispatched, 1)

    def test_dry_run_and_daytime_cannot_dispatch(self):
        api = FakeAPI()
        self.assertEqual(backup.wake_night(api, self.now, True)['action'], 'would_dispatch')
        for hour in (0, 1, 6, 14, 23):
            self.assertEqual(backup.wake_night(api, self.now.replace(hour=hour))['action'], 'outside_night_window')
        self.assertEqual(api.dispatched, 0)

    def test_unknown_times_and_api_errors_stop_without_dispatch(self):
        for invalid in (None, 'not-a-date', '2026-10-29T02:15:00', '2026-10-30T01:15:00Z'):
            api = FakeAPI({'success': [successful(end=invalid)]})
            with self.assertRaises((ValueError, RuntimeError)):
                backup.wake_night(api, self.now)
            self.assertEqual(api.dispatched, 0)
        with patch.object(FakeAPI, 'runs', side_effect=RuntimeError('unavailable')):
            api = FakeAPI()
            with self.assertRaises(RuntimeError): backup.wake_night(api, self.now)
            self.assertEqual(api.dispatched, 0)

    def test_http_dispatch_targets_only_main_without_force(self):
        with patch.object(backup.GitHub, 'request', return_value={}) as request:
            backup.GitHub('test-placeholder').dispatch()
            request.assert_called_once_with('dispatches', {'ref': 'main', 'inputs': {'force_night': False}})

    def test_malformed_api_result_does_not_look_like_no_runs(self):
        for payload in ({}, [], {'workflow_runs': [], 'total_count': 1}, {'workflow_runs': None, 'total_count': 0}):
            with self.subTest(payload=payload), patch.object(backup.GitHub, 'request', return_value=payload):
                with self.assertRaises(RuntimeError): backup.GitHub('test-placeholder').runs('success')

    def test_credentials_and_error_response_are_not_reported(self):
        error = HTTPError('https://api.github.com', 403, 'private-text', {}, io.BytesIO(b'private-data'))
        with patch.object(backup, 'urlopen', side_effect=error):
            with self.assertRaises(RuntimeError) as raised:
                backup.GitHub('test-placeholder').runs('queued')
        self.assertEqual(str(raised.exception), 'GitHub HTTP 403; no automatic retry')

    def test_nonproduction_context_is_rejected_before_network(self):
        with patch.dict(backup.os.environ, {'GITHUB_REPOSITORY': backup.REPOSITORY, 'GITHUB_REF': 'refs/heads/test'}):
            with patch.object(backup.GitHub, 'request') as request:
                with self.assertRaises(RuntimeError): backup.main()
                request.assert_not_called()


if __name__ == '__main__':
    unittest.main()
