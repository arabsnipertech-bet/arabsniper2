import importlib.util
import sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from unittest import TestCase, mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
SCRIPT = ROOT / '.github/ops/live-scores-health.py'
SPEC = importlib.util.spec_from_file_location('live_scores_health', SCRIPT)
health = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(health)

NOW = datetime(2026, 11, 13, 15, 56, 21, tzinfo=timezone.utc)


def feed(status='provider_rate_limit', *, cron_age=2, overdue=0, missing_halftime=0,
         watchlist_status=None, limit_scope='unconfirmed', retry_minutes=3, stale_live=0):
    limited = status == 'provider_rate_limit'
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
        },
    }


class LiveScoresHealthTests(TestCase):
    def inspect(self, feeds):
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
            _, summary, reasons = health.inspect_with_rate_limit_recheck(NOW)
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
            ('missing halftime', feed(missing_halftime=1)),
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
        self.assertEqual(reasons, ['Tracked live scores missing or older than five minutes'])
        self.assertEqual((waits, reads), ([], 1))
