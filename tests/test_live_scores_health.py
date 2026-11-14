import importlib.util
import json
import sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from types import SimpleNamespace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
SCRIPT = ROOT / '.github/ops/live-scores-health.py'
SPEC = importlib.util.spec_from_file_location('live_scores_health', SCRIPT)
health = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(health)

NOW = datetime(2026, 11, 13, 15, 56, 21, tzinfo=timezone.utc)


def feed(status='provider_rate_limit', *, cron_age=2, overdue=0, missing_halftime=0,
         watchlist_status=None, limit_scope='unconfirmed', retry_minutes=3, stale_live=0,
         live_due=None):
    limited = status in ('provider_rate_limit', 'minute_quota_low')
    return {
        'enabled': True,
        'schema_version': 1,
        'status': status,
        'fixtures': {},
        'diagnostic': {'limit_scope': limit_scope, 'categories': ['rateLimit']} if limited else None,
        'health': {
            'last_cron_at': (NOW - timedelta(minutes=cron_age)).isoformat(),
            'last_successful_poll_at': NOW.isoformat(),
            'retry_after': (NOW + timedelta(minutes=retry_minutes)).isoformat() if limited else None,
            'overdue_fixtures': overdue,
            'missing_halftime': missing_halftime,
            'stale_live_fixtures': stale_live,
            'watchlist_status': watchlist_status,
            'live_tracking_fixtures': live_due,
        },
    }


def relayed_score(at=NOW, **values):
    return {'status': '2H', 'observed_at': at.isoformat(), 'source': 'API-Football',
            'transport': 'GitHub Actions/R2', 'goals': {'home': 1, 'away': 0}, **values}


class LiveScoresHealthTests(TestCase):
    def inspect(self, feeds, *, relay_finished_at=None):
        output = StringIO()
        waits = []

        def now():
            return NOW + timedelta(seconds=sum(waits))

        with (mock.patch.object(health, 'get_json', side_effect=feeds) as read,
              mock.patch.object(health, 'datetime') as clock,
              mock.patch.object(health, 'monotonic', side_effect=lambda: sum(waits)),
              mock.patch.object(health, 'sleep', side_effect=waits.append),
              mock.patch.object(health, 'provider_results') as provider,
              redirect_stdout(output)):
            clock.fromisoformat.side_effect = datetime.fromisoformat
            clock.now.side_effect = lambda _: now()
            _, summary, reasons = health.inspect_with_rate_limit_recheck(NOW, relay_finished_at)
        provider.assert_not_called()
        return summary, reasons, waits, read.call_count, output.getvalue()

    def test_recovered_limit_keeps_initial_evidence(self):
        summary, reasons, waits, reads, log = self.inspect([feed(), feed('ok')])
        self.assertEqual(reasons, [])
        self.assertEqual(summary['status'], 'ok')
        self.assertTrue(summary['rate_limit_recheck']['cleared'])
        self.assertEqual(summary['rate_limit_recheck']['diagnostic']['categories'], ['rateLimit'])
        self.assertIn('::warning', log)
        self.assertEqual((waits, reads), ([185], 2))

    def test_persistent_limit_stays_failed_with_a_bounded_recheck(self):
        summary, reasons, waits, reads, log = self.inspect([feed()] * 6)
        self.assertEqual(reasons, ['Collector state: provider_rate_limit'])
        self.assertFalse(summary['rate_limit_recheck']['cleared'])
        self.assertNotIn('::warning', log)
        self.assertEqual(sum(waits), 300)
        self.assertEqual(reads, 6)

    def test_existing_faults_are_reported_immediately(self):
        cases = (
            ('overdue fixture', feed(overdue=1)),
            ('stale live fixture', feed(stale_live=1)),
            ('stale cron', feed(cron_age=9)),
            ('stale watchlist', feed(watchlist_status='watchlist_stale')),
            ('daily limit', feed(limit_scope='daily')),
            ('long retry', feed(retry_minutes=8)),
            ('other collector failure', feed('provider_error')),
        )
        for name, current in cases:
            with self.subTest(name=name):
                summary, reasons, waits, reads, log = self.inspect([current])
                self.assertTrue(reasons)
                self.assertNotIn('rate_limit_recheck', summary)
                self.assertEqual((waits, reads), ([], 1))

    def test_stops_rechecking_when_a_result_becomes_overdue(self):
        summary, reasons, waits, reads, log = self.inspect([feed(), feed(overdue=1)])
        self.assertIn('Tracked fixtures overdue without a confirmed result', reasons)
        self.assertFalse(summary['rate_limit_recheck']['cleared'])
        self.assertEqual((waits, reads), ([185], 2))

    def test_worker_recovery_after_the_incident_backoff(self):
        summary, reasons, waits, reads, _ = self.inspect([feed(retry_minutes=2.5), feed(), feed('ok')])
        self.assertEqual(reasons, [])
        self.assertTrue(summary['rate_limit_recheck']['cleared'])
        self.assertEqual((waits, reads), ([155, 30], 3))

    def test_healthy_feed_is_read_once(self):
        summary, reasons, waits, reads, log = self.inspect([feed('idle', cron_age=0)])
        self.assertEqual(reasons, [])
        self.assertNotIn('rate_limit_recheck', summary)
        self.assertEqual((waits, reads), ([], 1))

    def test_successful_poll_with_stale_live_fixture_is_reported(self):
        summary, reasons, waits, reads, _ = self.inspect([feed('ok', stale_live=1)])
        self.assertEqual(summary['status'], 'ok')
        self.assertEqual(reasons, ['Tracked live scores missing or older than six minutes'])
        self.assertEqual((waits, reads), ([], 1))

    def test_recent_relay_delivery_keeps_the_limit_as_a_warning(self):
        current = feed()
        current['fixtures']['123'] = relayed_score()
        summary, reasons, waits, reads, log = self.inspect([current])
        self.assertEqual(reasons, [])
        self.assertEqual(summary['status'], 'provider_rate_limit')
        self.assertEqual(summary['delivery_during_provider_limit'], 'relay')
        self.assertIn('::warning', log)
        self.assertEqual((waits, reads), ([], 1))

    def test_followup_waits_for_the_worker_to_consume_the_relay(self):
        initial = feed(stale_live=21)
        updated = feed(cron_age=-0.25)
        updated['fixtures']['123'] = relayed_score()
        summary, reasons, waits, reads, log = self.inspect(
            [initial, updated], relay_finished_at=NOW - timedelta(seconds=10))
        self.assertEqual(reasons, [])
        self.assertEqual(summary['delivery_during_provider_limit'], 'relay')
        self.assertFalse(summary['rate_limit_recheck']['cleared'])
        self.assertIn('::warning', log)
        self.assertEqual((waits, reads), ([15], 2))

    def test_stale_scores_continue_to_fail_after_relay_followup(self):
        still_stale = feed(stale_live=21, cron_age=-0.25)
        summary, reasons, waits, reads, _ = self.inspect(
            [feed(stale_live=21), still_stale], relay_finished_at=NOW - timedelta(seconds=10))
        self.assertIn('Tracked live scores missing or older than six minutes', reasons)
        self.assertNotIn('delivery_during_provider_limit', summary)
        self.assertEqual((waits, reads), ([15], 2))

    def test_relay_without_a_new_worker_cycle_times_out(self):
        stale = feed(stale_live=21)
        summary, reasons, waits, reads, _ = self.inspect([stale] * 10, relay_finished_at=NOW)
        self.assertIn('Tracked live scores missing or older than six minutes', reasons)
        self.assertNotIn('delivery_during_provider_limit', summary)
        self.assertEqual((sum(waits), reads), (135, 10))

    def test_daily_limits_and_delivery_gaps_remain_errors(self):
        cases = (
            ('daily', feed(limit_scope='daily'), relayed_score()),
            ('overdue', feed(overdue=1, retry_minutes=8), relayed_score()),
            ('stale relay', feed(retry_minutes=8), relayed_score(NOW - timedelta(minutes=7))),
            ('stale worker', feed(retry_minutes=8),
             relayed_score(NOW - timedelta(minutes=7), transport='Worker')),
        )
        for name, current, row in cases:
            with self.subTest(name=name):
                current['fixtures']['123'] = relayed_score()
                current['fixtures']['124'] = row
                if name == 'stale relay':
                    del current['fixtures']['123']
                summary, reasons, waits, reads, log = self.inspect([current])
                self.assertTrue(reasons)
                self.assertNotIn('delivery_during_provider_limit', summary)
                self.assertNotIn('::warning', log)
                self.assertEqual((waits, reads), ([], 1))

    def test_throttling_with_no_live_matches_is_an_operational_warning(self):
        for status in ('provider_rate_limit', 'minute_quota_low'):
            with self.subTest(status=status):
                summary, reasons, waits, reads, log = self.inspect(
                    [feed(status, live_due=0, missing_halftime=2)])
                self.assertEqual(reasons, [])
                self.assertEqual(summary['delivery_during_provider_limit'], 'idle')
                self.assertIn('No live or overdue score updates are due', log)
                self.assertEqual((waits, reads), ([], 1))

    def test_idle_throttle_does_not_hide_a_stale_watchlist_or_raise_a_second_fault(self):
        summary, reasons, waits, reads, log = self.inspect([
            feed(live_due=0, watchlist_status='watchlist_stale')])
        self.assertEqual(reasons, ['Fixture watchlist: watchlist_stale'])
        self.assertEqual(summary['delivery_during_provider_limit'], 'idle')
        self.assertIn('::warning', log)
        self.assertEqual((waits, reads), ([], 1))

    def test_fresh_direct_scores_cover_a_limited_followup(self):
        current = feed(live_due=1, missing_halftime=2)
        current['fixtures']['123'] = relayed_score(transport=None)
        summary, reasons, waits, reads, _ = self.inspect([current])
        self.assertEqual(reasons, [])
        self.assertEqual(summary['delivery_during_provider_limit'], 'direct')
        self.assertEqual((waits, reads), ([], 1))

    def test_recent_relay_does_not_cover_a_missing_live_score(self):
        current = feed(live_due=2, retry_minutes=8)
        current['fixtures']['123'] = relayed_score()
        summary, reasons, waits, reads, _ = self.inspect([current])
        self.assertEqual(reasons, ['Collector state: provider_rate_limit'])
        self.assertNotIn('delivery_during_provider_limit', summary)
        self.assertEqual((waits, reads), ([], 1))

    def test_recent_official_not_started_status_covers_a_tracked_fixture(self):
        current = feed(live_due=2, missing_halftime=2)
        current['health']['live_tracking_fixture_ids'] = ['123', '124']
        current['fixtures']['123'] = relayed_score()
        current['fixtures']['124'] = relayed_score(status='NS')
        summary, reasons, waits, reads, _ = self.inspect([current])
        self.assertEqual(reasons, [])
        self.assertEqual(summary['delivery_during_provider_limit'], 'relay')
        self.assertEqual((waits, reads), ([], 1))

    def test_a_future_game_cannot_count_for_a_different_missing_tracked_fixture(self):
        current = feed(live_due=2, retry_minutes=8)
        current['health']['live_tracking_fixture_ids'] = ['123', '124']
        current['fixtures']['123'] = relayed_score()
        current['fixtures']['125'] = relayed_score(status='NS')
        summary, reasons, waits, reads, _ = self.inspect([current])
        self.assertEqual(reasons, ['Collector state: provider_rate_limit'])
        self.assertNotIn('delivery_during_provider_limit', summary)
        self.assertEqual((waits, reads), ([], 1))

    def test_completed_finals_with_unknown_halftimes_exit_successfully_and_report_coverage(self):
        current = feed('ok', missing_halftime=2)
        matches = {'1564928': 5, '1564933': 2}
        for fid, goals in matches.items():
            current['fixtures'][fid] = {
                'status': 'FT', 'fulltime': {'home': goals, 'away': 0},
                'halftime': {'home': None, 'away': None},
            }
        with TemporaryDirectory() as temp, redirect_stdout(StringIO()) as log:
            path = Path(temp) / 'summary.md'
            with (mock.patch.object(health, 'get_json', return_value=current),
                  mock.patch.object(health, 'datetime', wraps=datetime) as clock,
                  mock.patch.object(health, 'recent_audit', return_value={}),
                  mock.patch.object(health, 'client') as client,
                  mock.patch.dict(health.os.environ, {'GITHUB_STEP_SUMMARY': str(path), 'R2_BUCKET': 'test'})):
                clock.now.return_value = NOW
                storage = client.return_value
                storage.get_object.return_value = {'Body': BytesIO(b'{"files": {}}')}
                health.run([], True)
                storage.get_object.assert_called_once_with(Bucket='test', Key='state/current.json')
            block = path.read_text().split('```json\n', 1)[1]
            report = json.loads(block.split('\n```', 1)[0])
        self.assertEqual(report['missing_halftime_fixture_ids'], list(matches))
        self.assertEqual(report['health']['missing_halftime'], 2)
        self.assertEqual([x['date'] for x in report['auditor_days']], ['2026-11-12', '2026-11-11'])
        self.assertEqual([x['differences'] for x in report['auditor_days']], [0, 0])
        self.assertIn('::warning title=Gold halftime coverage::2 finished fixtures', log.getvalue())
        self.assertIn('Markets that require halftime remain unverified', log.getvalue())

    def test_automatic_monitor_reports_the_same_fault_once_and_rearms_after_recovery(self):
        class ClientError(Exception):
            response = {'Error': {'Code': 'NoSuchKey'}}

        s3 = mock.Mock()
        data = {}

        def get(**request):
            if request['Key'] not in data:
                raise ClientError()
            return {'Body': BytesIO(data[request['Key']])}

        def put(**request):
            data[request['Key']] = request['Body']

        s3.get_object.side_effect = get
        s3.put_object.side_effect = put
        fault = 'Tracked fixtures overdue without a confirmed result'
        summary = {'health': {'overdue_fixture_ids': ['1631640']}}
        def update(reasons):
            return health.update_incidents(s3, 'test', reasons, summary)

        with mock.patch.dict(sys.modules, {'botocore.exceptions': SimpleNamespace(ClientError=ClientError)}):
            self.assertEqual(update([fault, 'Collector state: results_pending']), [fault])
            self.assertEqual(update([fault, 'Collector state: provider_rate_limit']), [])
            self.assertEqual(s3.put_object.call_count, 1)
            summary['health']['overdue_fixture_ids'].append('1636033')
            self.assertEqual(update([fault]), [fault])
            self.assertEqual(update([fault, 'Cron Trigger missing or stale']), ['Cron Trigger missing or stale'])
            self.assertEqual(update([]), [])
            self.assertEqual(update([fault]), [fault])
