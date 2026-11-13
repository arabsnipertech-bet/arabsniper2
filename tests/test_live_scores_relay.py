import importlib.util
from pathlib import Path
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
import io
import json
import os
import sys
from types import SimpleNamespace
from urllib.error import HTTPError
spec=importlib.util.spec_from_file_location('relay',Path(__file__).resolve().parents[1]/'.github/ops/live-scores-relay.py')
relay=importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)
NOW=datetime(2026,11,13,9,tzinfo=timezone.utc)
def watch(n=1):
    return {'fixtures':[{'fixture_id':str(i+1),'kickoff':(NOW-timedelta(hours=3)).isoformat()} for i in range(n)]}
class RelayTests(unittest.TestCase):
    def test_missing_tracked_fixture_is_requested(self):
        self.assertEqual(relay.due(watch(),{},NOW),['1'])
    def test_future_fixture_and_postponed_do_not_poll(self):
        future={'fixtures':[{'fixture_id':'1','kickoff':(NOW+timedelta(hours=1)).isoformat()}]}
        self.assertEqual(relay.due(future,{},NOW),[])
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':{'status':'PST'}}},NOW),[])
    def test_complete_zero_score_needs_no_provider_call(self):
        row={'status':'FT','halftime':{'home':0,'away':0},'fulltime':{'home':0,'away':0}}
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':row}},NOW),[])
    def test_fresh_live_is_deduplicated(self):
        row={'status':'1H','observed_at':NOW.isoformat()}
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':row}},NOW),[])
    def test_batch_is_bounded_and_retained_overnight_ids_survive(self):
        self.assertEqual(len(relay.due(watch(100),{},NOW)),20)
        self.assertEqual(relay.due({'fixtures':[]},{'health':{'overdue_fixture_ids':['1644690']}},NOW),['1644690'])
    def test_stale_live_fixture_from_retained_watchlist_has_priority(self):
        live={'health':{'stale_live_fixture_ids':['1234'],'overdue_fixture_ids':['1234']}}
        ids=relay.due(watch(30),live,NOW)
        self.assertEqual(ids[0],'1234')
        self.assertEqual(len(ids),20)
    def test_repeated_unresolved_results_do_not_starve_other_ids(self):
        rows={str(i+1):{'status':'NS','observed_at':(NOW-timedelta(minutes=5)).isoformat()} for i in range(20)}
        self.assertIn('21',relay.due(watch(21),{'fixtures':rows},NOW))
    def test_limited_collector_recovers_live_fixtures_before_the_regular_cutoff(self):
        observed=(NOW-timedelta(minutes=3)).isoformat()
        rows={str(i+1):{'status':'HT','observed_at':observed} for i in range(28)}
        live={'fixtures':rows,'status':'provider_rate_limit','diagnostic':{'limit_scope':'unconfirmed'}}
        self.assertEqual(relay.due(watch(28),{'fixtures':rows},NOW),[])
        self.assertEqual(set(relay.due(watch(28),live,NOW)),set(rows))
        self.assertEqual(len(relay.due(watch(50),live,NOW)),40)
    def test_midpoint_checks_only_affected_live_or_just_started_fixtures(self):
        candidate = {'fixtures': [{'fixture_id': '1', 'kickoff': (NOW-timedelta(minutes=10)).isoformat()}]}
        live = {'fixtures': {'1': {'status': '1H'}}, 'status': 'provider_rate_limit'}
        self.assertTrue(relay.needs_midpoint(candidate, live, NOW))
        self.assertTrue(relay.needs_midpoint(candidate, {**live, 'fixtures': {}}, NOW))
        self.assertTrue(relay.needs_midpoint(watch(0), {**live, 'health': {'stale_live_fixtures': 1}}, NOW))
        self.assertFalse(relay.needs_midpoint(candidate, {**live, 'status': 'ok'}, NOW))
        self.assertFalse(relay.needs_midpoint(candidate, {**live, 'diagnostic': {'limit_scope': 'daily'}}, NOW))
        self.assertFalse(relay.needs_midpoint(candidate, {**live, 'fixtures': {'1': {'status': 'FT'}}}, NOW))
        self.assertFalse(relay.needs_midpoint(candidate, {**live, 'fixtures': {}}, NOW+timedelta(minutes=10)))
    def test_cap_and_cooldown_include_failed_requests(self):
        self.assertFalse(relay.allowed({'day':'2026-11-13','calls':599},NOW))
        self.assertFalse(relay.allowed({'day':'2026-11-13','calls':2,'attempted_at':NOW.isoformat()},NOW))
        self.assertTrue(relay.allowed({'day':'2026-11-12','calls':600},NOW))
    def test_retry_after_is_honored_across_runs(self):
        retry=relay.retry_at({'Retry-After':'360'},NOW)
        self.assertEqual(retry,NOW+timedelta(minutes=6))
        self.assertEqual(relay.retry_at({'Retry-After':'Fri, 13 Nov 2026 09:06:00 GMT'},NOW),retry)
        self.assertIsNone(relay.retry_at({'Retry-After':'invalid'},NOW))
        state={'day':'2026-11-13','calls':2,'attempted_at':NOW.isoformat(),'retry_after':retry.isoformat()}
        self.assertFalse(relay.allowed(state,NOW+timedelta(minutes=5)))
        self.assertTrue(relay.allowed(state,retry))

    def test_second_batch_rate_limit_keeps_first_batch_and_persists_cooldown(self):
        fixtures = watch(28)
        observed = (NOW - timedelta(minutes=3)).isoformat()
        feed = {
            'status': 'provider_rate_limit', 'health': {'relay_fixture_capacity': 40},
            'fixtures': {f['fixture_id']: {'status': 'HT', 'observed_at': observed}
                         for f in fixtures['fixtures']},
        }
        quota = {'subscription': {'active': True}, 'requests': {'current': 50, 'limit_day': 10000}}
        first_ids = relay.due(fixtures, feed, NOW)[:20]
        body = [{'fixture': {'id': int(fid)}} for fid in first_ids]
        def response(data):
            stream = io.BytesIO(json.dumps({'response': data}).encode())
            stream.headers = {}
            return stream
        storage = mock.Mock()
        storage.get_object.return_value = {'Body': io.BytesIO(b'{}')}
        modules = {
            'boto3': SimpleNamespace(client=mock.Mock(return_value=storage)),
            'botocore.config': SimpleNamespace(Config=mock.Mock()),
            'botocore.exceptions': SimpleNamespace(ClientError=type('ClientError', (Exception,), {})),
        }
        limited = HTTPError(None, 429, 'rate limited', {'Retry-After': '600'}, None)
        env = dict.fromkeys(('R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY',
                             'R2_BUCKET', 'API_SPORTS_KEY'), 'fake')
        with (mock.patch.dict(sys.modules, modules), mock.patch.dict(os.environ, env),
              mock.patch.object(relay, 'datetime', wraps=datetime) as clock,
              mock.patch.object(relay.Path, 'read_text', return_value=json.dumps(fixtures)),
              mock.patch.object(relay.subprocess, 'run', return_value=SimpleNamespace(stdout=json.dumps(feed))),
              mock.patch.object(relay.time, 'sleep'),
              mock.patch.object(relay, 'urlopen', side_effect=[response(quota), response(body), limited]) as provider):
            clock.now.return_value = NOW
            with self.assertRaisesRegex(ValueError, 'Official provider HTTP 429'):
                relay.run()
        self.assertEqual(provider.call_count, 3)
        persisted = json.loads(storage.put_object.call_args.kwargs['Body'])
        self.assertEqual(persisted['calls'], 3)
        self.assertEqual([str(r['fixture']['id']) for r in persisted['fixtures']], first_ids)
        self.assertEqual(relay.stamp(persisted['retry_after']), NOW + timedelta(minutes=10))
if __name__=='__main__': unittest.main()
