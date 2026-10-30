from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from history_cache import HistoryCache, TTL, ROME, descriptor, instant
from audit_storage import read_store, write_store
from test_policy_engine import engine

NOW = datetime.fromisoformat('2026-10-30T10:00:00+01:00')
TEAM = ('fixtures', {'team': 10, 'last': 32, 'status': 'FT'})
H2H = ('fixtures/headtohead', {'h2h': '10-20', 'last': 5})
LEAGUE = ('fixtures', {'league': 135, 'season': 2026, 'last': 50, 'status': 'FT'})


def fixture(fid=1, home=10, away=20, status='FT', at='2026-10-29T15:00:00+01:00', league=135):
    return {'fixture': {'id': fid, 'date': at, 'status': {'short': status}},
            'league': {'id': league, 'season': 2026, 'country': 'Italy', 'name': 'Serie A', 'type': 'League'},
            'teams': {'home': {'id': home, 'name': 'Home'}, 'away': {'id': away, 'name': 'Away'}},
            'goals': {'home': 3, 'away': 2},
            'score': {'halftime': {'home': 1, 'away': 1}, 'fulltime': {'home': 3, 'away': 2}}}


class Feed:
    def __init__(self, calendar=None):
        self.calendar = calendar or []
        self.history = [fixture()]
        self.calls = []
        self.failed_calendar = False
        self.failed_history = False

    def get(self, path, params):
        self.calls.append((path, dict(params)))
        if 'date' in params:
            rows = [row for row in self.calendar
                    if instant(row['fixture']['date']).astimezone(ROME).date().isoformat() == params['date']]
            return None if self.failed_calendar else {'response': deepcopy(rows)}
        return None if self.failed_history else {'response': deepcopy(self.history)}

    @property
    def histories(self):
        return [call for call in self.calls if descriptor(*call) is not None]


class PersistentHistoryTests(unittest.TestCase):
    def test_fresh_process_reuses_team_h2h_and_league_after_disk_reload(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'
            first, feed = HistoryCache(path, lambda: NOW), Feed()
            expected = [first.get(*query, feed.get) for query in (TEAM, H2H, LEAGUE)]
            first.save()
            second, fresh_feed = HistoryCache(path, lambda: NOW+timedelta(hours=1)), Feed()
            self.assertEqual([second.get(*query, fresh_feed.get) for query in (TEAM, H2H, LEAGUE)], expected)
            self.assertEqual(len(feed.histories), 3)
            self.assertEqual(fresh_feed.histories, [])
            self.assertEqual(second.report['hits'], 3)
            self.assertEqual(len(fresh_feed.calls), 2)  # Include overnight completions.

    def test_new_completed_game_invalidates_only_affected_histories(self):
        other = ('fixtures', {'team': 999, 'last': 32, 'status': 'FT'})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'
            cache, feed = HistoryCache(path, lambda: NOW), Feed()
            for query in (TEAM, H2H, LEAGUE, other): cache.get(*query, feed.get)
            cache.save()
            finished = fixture(2, away=30, at='2026-10-30T08:00:00+01:00')
            feed = Feed([finished]); feed.history.insert(0, finished)
            cache = HistoryCache(path, lambda: NOW+timedelta(hours=1))
            for query in (TEAM, H2H, LEAGUE, other): cache.get(*query, feed.get)
            self.assertEqual(len(feed.histories), 3)
            self.assertEqual(cache.report['invalidated'], 3)
            self.assertEqual(cache.report['hits'], 1)
            cache.save()
            again, feed = HistoryCache(path, lambda: NOW+timedelta(hours=2)), Feed([finished])
            for query in (TEAM, H2H, LEAGUE): again.get(*query, feed.get)
            self.assertFalse(feed.histories)

    def test_live_scheduled_or_postponed_match_does_not_invalidate(self):
        for status in ('NS', '1H', 'HT', '2H', 'PST', 'CANC'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                path = Path(directory)/'cache.json'; feed = Feed()
                first = HistoryCache(path, lambda: NOW)
                first.get(*TEAM, feed.get); first.save()
                feed = Feed([fixture(2, status=status)])
                second = HistoryCache(path, lambda: NOW+timedelta(hours=1))
                second.get(*TEAM, feed.get)
                self.assertEqual(feed.histories, [])

    def test_score_correction_invalidates_an_already_finished_fixture(self):
        raw = fixture(at='2026-10-30T06:00:00+01:00')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'; feed = Feed([raw]); feed.history = [raw]
            cache = HistoryCache(path, lambda: NOW); cache.get(*TEAM, feed.get); cache.save()
            raw = deepcopy(raw); raw['score']['halftime']['home'] = 2
            feed = Feed([raw]); feed.history = [raw]
            cache = HistoryCache(path, lambda: NOW+timedelta(hours=1)); cache.get(*TEAM, feed.get)
            self.assertEqual(cache.report['invalidated'], 1)

    def test_expiry_and_future_timestamp_cannot_reuse_history(self):
        for later in (NOW+TTL, NOW-timedelta(seconds=1)):
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)/'cache.json'; feed = Feed()
                cache = HistoryCache(path, lambda: NOW); cache.get(*TEAM, feed.get); cache.save()
                feed = Feed(); cache = HistoryCache(path, lambda: later)
                cache.get(*TEAM, feed.get)
                self.assertEqual(len(feed.histories), 1)
                self.assertEqual(cache.report['expired'], 1)

    def test_midnight_checks_previous_italian_day_and_handles_dst(self):
        for start, end in (('2026-10-30T22:30:00Z', '2026-10-31T00:30:00Z'),
                           ('2026-10-24T21:30:00Z', '2026-10-25T02:30:00Z')):
            before, after = datetime.fromisoformat(start.replace('Z','+00:00')), datetime.fromisoformat(end.replace('Z','+00:00'))
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory)/'cache.json'; feed = Feed()
                cache = HistoryCache(path, lambda: before); cache.get(*TEAM, feed.get); cache.save()
                cache, feed = HistoryCache(path, lambda: after), Feed()
                cache.get(*TEAM, feed.get)
                self.assertEqual(cache.report['validation_requests'], 3)
                self.assertEqual(len({params['date'] for _,params in feed.calls}), 3)
                self.assertFalse(feed.histories)

    def test_failed_or_incomplete_calendar_bypasses_persisted_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'; feed = Feed()
            cache = HistoryCache(path, lambda: NOW); cache.get(*TEAM, feed.get); cache.save()
            feed = Feed(); feed.failed_calendar = True
            cache = HistoryCache(path, lambda: NOW+timedelta(hours=1)); cache.get(*TEAM, feed.get)
            self.assertEqual(cache.report['validation_bypasses'], 1)
            self.assertEqual(len(feed.histories), 1)
            self.assertFalse(cache.observe({'response': [], 'paging': {'total': 2, 'current': 1}}))

    def test_errors_are_never_cached_and_callers_cannot_mutate_saved_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            cache, feed = HistoryCache(Path(directory)/'cache.json', lambda: NOW), Feed()
            feed.failed_history = True
            self.assertIsNone(cache.get(*TEAM, feed.get)); self.assertFalse(cache.entries)
            feed.failed_history = False
            one = cache.get(*TEAM, feed.get); one['response'][0]['goals']['home'] = 99
            two = cache.get(*TEAM, feed.get); self.assertEqual(two['response'][0]['goals']['home'], 3)
            two['response'].clear()
            self.assertTrue(cache.get(*TEAM, feed.get)['response'])

    def test_request_shape_sample_season_and_h2h_pair_stay_separate(self):
        queries = [TEAM, H2H, LEAGUE, ('fixtures', {**TEAM[1], 'last': 8}),
                   ('fixtures', {**LEAGUE[1], 'season': 2025}),
                   ('fixtures/headtohead', {'h2h': '10-30', 'last': 5})]
        with tempfile.TemporaryDirectory() as directory:
            cache, feed = HistoryCache(Path(directory)/'cache.json', lambda: NOW), Feed()
            for query in queries: cache.get(*query, feed.get)
            self.assertEqual(len(feed.histories), len(queries))
            self.assertEqual(len(cache.entries), len(queries))
        for path, params in [('odds', {'fixture': 100}), ('fixtures/statistics', {'fixture': 100}),
                             ('fixtures', {'team': 10, 'last': 5, 'status': 'NS'})]:
            self.assertIsNone(descriptor(path, params))

    def test_night_refreshes_once_then_primes_the_following_fast(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'; feed = Feed()
            cache = HistoryCache(path, lambda: NOW); cache.get(*TEAM, feed.get); cache.save()
            feed = Feed(); cache = HistoryCache(path, lambda: NOW+timedelta(hours=1), reuse=False)
            cache.get(*TEAM, feed.get); cache.get(*TEAM, feed.get); cache.save()
            self.assertEqual(len(feed.histories), 1)
            cache, feed = HistoryCache(path, lambda: NOW+timedelta(hours=2)), Feed()
            cache.get(*TEAM, feed.get); self.assertFalse(feed.histories)

    def test_private_checkpoint_roundtrip_preserves_cache_for_new_runner(self):
        from gold_storage import GOLD, publish, restore
        from test_gold_storage import Fake
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            root = Path(a); path = root/'data/arab_quote_history_cache.json'
            for name in GOLD: write_store(root/'data'/name, {'matches': []})
            cache, feed = HistoryCache(path, lambda: NOW), Feed()
            cache.get(*TEAM, feed.get); cache.save()
            storage = Fake(); publish(storage, 'test', root); restore(storage, 'test', b)
            cache, feed = HistoryCache(Path(b)/'data/arab_quote_history_cache.json', lambda: NOW+timedelta(hours=1)), Feed()
            cache.get(*TEAM, feed.get)
            self.assertFalse(feed.histories)

    def test_fresh_calendar_observed_mid_cycle_invalidates_in_memory_history(self):
        with tempfile.TemporaryDirectory() as directory:
            cache, feed = HistoryCache(Path(directory)/'cache.json', lambda: NOW), Feed()
            cache.get(*TEAM, feed.get)
            ended = fixture(2, at='2026-10-30T07:00:00+01:00')
            cache.observe({'response': [ended]}); feed.history.insert(0, ended)
            refreshed = cache.get(*TEAM, feed.get)
            self.assertEqual(len(feed.histories), 2)
            self.assertEqual(refreshed['response'][0]['fixture']['id'], 2)

    def test_broken_cache_file_falls_back_to_fresh_http(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'; path.write_text('invalid cache')
            cache, feed = HistoryCache(path, lambda: NOW), Feed()
            self.assertTrue(cache.get(*TEAM, feed.get)['response'])
            self.assertEqual(len(feed.histories), 1)
            cache.save()
            self.assertEqual(read_store(path)['schema_version'], 1)

    def test_previous_day_kickoff_finishing_after_midnight_invalidates_cache(self):
        before = datetime.fromisoformat('2026-10-31T00:15:00+01:00')
        pending = fixture(2, status='1H', at='2026-10-30T23:30:00+01:00')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'cache.json'; feed = Feed([pending])
            cache = HistoryCache(path, lambda: before); cache.get(*TEAM, feed.get); cache.save()
            finished = deepcopy(pending); finished['fixture']['status']['short'] = 'FT'
            feed = Feed([finished]); feed.history.insert(0, finished)
            cache = HistoryCache(path, lambda: before+timedelta(hours=2))
            self.assertEqual(cache.get(*TEAM, feed.get)['response'][0]['fixture']['id'], 2)
            self.assertEqual(cache.report['invalidated'], 1)

    def test_active_engine_uses_persistent_cache_and_always_requests_odds(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(engine, 'iso_now', return_value=NOW.isoformat()):
            path = Path(directory)/'cache.json'
            results = []
            for hour in (0, 1):
                feed = Feed(); client = engine.APIClient('test-placeholder', min_interval=0)
                client.history_cache = HistoryCache(path, lambda: NOW+timedelta(hours=hour))
                def http(url, headers, params, timeout):
                    payload = {'response': []} if url.endswith('/odds') else feed.get(url.rsplit('/',1)[-1] if not url.endswith('/fixtures/headtohead') else 'fixtures/headtohead', params)
                    return Mock(status_code=200, json=lambda: payload)
                client.session.get = Mock(side_effect=http)
                results.append([engine.get_team_recent_matches(client, 10),
                                engine.get_h2h_matches(client, 10, 20),
                                engine.get_league_baseline(client, 135, 2026)])
                client.get('odds', {'fixture': 100}); client.get('odds', {'fixture': 100})
                client.history_cache.save()
                self.assertEqual(len(feed.histories), 3 if hour == 0 else 0)
                self.assertEqual(client.request_count, 7 if hour == 0 else 4)
                self.assertEqual(sum(call.args[0].endswith('/odds') for call in client.session.get.call_args_list), 2)
            self.assertEqual(results[0], results[1])


if __name__ == '__main__':
    unittest.main()
