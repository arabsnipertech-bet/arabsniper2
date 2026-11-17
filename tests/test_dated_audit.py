from datetime import timedelta
from io import BytesIO
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from test_daily_recovery import DAY, NOW, sources
from test_gold_storage import Fake

from audit_daily_evidence import receipt_path, verified_evidence
from audit_journal import capture_forecasts, ingest, journals_for_day
from audit_storage import read_store
from auditor_daily_priority import build_yesterday, ensure_priority, v7_ready
from current_audit import archive, fetch_results, settle_day
from audit_official_scores import needs_result, read_live_scores
from v7_archive import merge_verified_result
from gold_storage import checkpoint_day, checkpoint_results, previous_manifest, publish, restore_v7_priority
from recover_daily import next_recovery_day, recover_learning
from v7_learning import STATE, run as learn_v7


class DatedAuditTests(TestCase):
    def test_scan_archives_both_rome_dates_and_restores_just_the_target(self):
        with TemporaryDirectory() as folder, TemporaryDirectory() as dest:
            root = Path(folder)
            rows = [{'fixture_id': str(i), 'kickoff': f'{DAY}T{hour}:30:00Z'}
                    for i, hour in ((1, 22), (2, 23))]

            def capture(output, *, store, persist, observed_at):
                fixture = output['days'][0]['matches'][0]['fixture']
                store.setdefault('fixtures', {})[fixture['fixture_id']] = {
                    'fixture': fixture, 'captured_at': observed_at.isoformat(), 'probabilities_raw': {'p': 0.55}}

            now = NOW-timedelta(hours=15)
            ns = {'DATA_DIR': root/'data', 'now_rome': lambda: now, 'capture_prematch_predictions': capture}
            inputs = {'days': [{'matches': [{'fixture': fixture} for fixture in rows]}]}
            first, next_day = capture_forecasts(ns, inputs)
            self.assertEqual(list(read_store(first)['fixtures']), ['1'])
            self.assertEqual(list(read_store(next_day)['fixtures']), ['2'])
            self.assertEqual(list(journals_for_day([first, next_day], DAY)), [first])
            store = Fake()
            publish(store, 'test', root, checkpoint_only=True)
            restore_v7_priority(store, 'test', Path(dest), DAY)
            self.assertTrue((Path(dest)/first.relative_to(root)).is_file())
            self.assertFalse((Path(dest)/next_day.relative_to(root)).exists())
            # Both shards from this scan are archived in the same historical batch.
            self.assertEqual(ingest(root, limit=1), 2)
            ledger = read_store(root/'data/arab_quote_prematch_audit.json')
            self.assertEqual(set(ledger['fixtures']), {'1', '2'})

    def test_learning_interruption_leaves_a_durable_day_and_retry_reuses_it(self):
        with (TemporaryDirectory() as folder, TemporaryDirectory() as retry,
              patch.dict(os.environ, {'API_SPORTS_KEY': 'test'})):
            root, store = Path(folder), Fake()
            official = sources(store, root)

            def interrupted(*command):
                if command[0] == 'src/auditor_daily_priority.py':
                    build_yesterday(root, DAY, NOW, lambda *_: official)
                elif command[1] == 'checkpoint-results':
                    checkpoint_results(store, 'test', root, DAY)
                else:
                    self.assertEqual(command[1], 'learn-v7')
                    raise TimeoutError('learning interrupted')

            with self.assertRaises(TimeoutError):
                ensure_priority(root, NOW.date(), interrupted)
            index = previous_manifest(store, 'test')
            self.assertIn(receipt_path(DAY), index)
            self.assertNotIn(STATE, index)

            restored = Path(retry)
            restore_v7_priority(store, 'test', restored, DAY)
            receipt = verified_evidence(restored, DAY)
            self.assertEqual(receipt['fixtures']['1']['result']['status'], 'FT')
            self.assertFalse((restored/'data/arab_quote_prematch_audit.json').exists())

            def resume(*command):
                self.assertIn(command[1], ('learn-v7', 'checkpoint-day'))
                if command[1] == 'learn-v7':
                    learn_v7(restored, NOW)
                else:
                    checkpoint_day(store, 'test', restored, DAY)

            ensure_priority(restored, NOW.date(), resume)
            self.assertTrue(v7_ready(restored, NOW.date()))
            self.assertEqual(previous_manifest(store, 'test')[receipt_path(DAY)], index[receipt_path(DAY)])

    def test_id_lookup_checks_rome_date_before_certifying_a_final(self):
        moved = (DAY+timedelta(days=1)).isoformat()+'T00:30:00+01:00'
        stayed = (DAY-timedelta(days=1)).isoformat()+'T23:30:00+00:00'
        rows = [{'fixture': {'id': fid, 'date': kickoff, 'status': {'short': 'PEN'}},
                 'goals': {'home': 3, 'away': 2},
                 'score': {'fulltime': {'home': 2, 'away': 2}, 'halftime': {'home': 1, 'away': 1}}}
                for fid, kickoff in ((1, moved), (2, stayed))]
        response = BytesIO(json.dumps({'response': rows}).encode())
        response.headers = {}
        with patch('current_audit.urlopen', return_value=response):
            result = fetch_results(str(DAY), 'test', [1, 2], only_ids=True)
        first, second = [row['fixture']['result'] for row in result['days'][0]['matches']]
        self.assertEqual((first['status'], first['rescheduled_to'], first['home_ft']), ('PST', moved, None))
        self.assertFalse(needs_result(first))
        self.assertEqual(merge_verified_result(second, first), first)
        self.assertEqual((second['status'], second['home_ft'], second['away_ft']), ('PEN', 2, 2))

    def test_legacy_cache_needs_an_official_kickoff_and_moved_dates_keep_no_score(self):
        observed = NOW-timedelta(minutes=1)
        kickoff = (NOW-timedelta(days=1)).isoformat()
        elsewhere = (NOW-timedelta(hours=2)).isoformat()
        row = {'status': 'FT', 'fulltime': {'home': 2, 'away': 0}, 'source': 'API-Football',
               'observed_at': observed.isoformat()}
        fixtures = {'1': {**row, 'fixture_id': '1'}, '2': {**row, 'fixture_id': '2',
                                                         'provider_kickoff': elsewhere}}
        response = BytesIO(json.dumps({'enabled': True, 'schema_version': 1,
                                        'fixtures': fixtures}).encode())
        with patch('audit_official_scores.urlopen', return_value=response):
            results = read_live_scores({'1': kickoff, '2': kickoff}, NOW)
        self.assertEqual(list(results), ['2'])
        self.assertEqual((results['2']['status'], results['2']['rescheduled_to'], results['2']['home_ft']),
                         ('PST', elsewhere, None))

    def test_uses_stored_live_scores_and_requests_only_the_missing_fixture(self):
        with (TemporaryDirectory() as folder, TemporaryDirectory() as plan,
              patch.dict(os.environ, {'API_SPORTS_KEY': 'test'})):
            root, store = Path(folder), Fake()
            scores = sources(store, root, with_pending=True)
            live = scores['days'][0]['matches'][0]['fixture']['result']
            archive(root, NOW)
            requests = []

            def provider(request, timeout):
                requests.append(parse_qs(urlparse(request.full_url).query))
                response = BytesIO(json.dumps({'response': [
                    {'fixture': {'id': 2, 'status': {'short': 'FT'}},
                     'score': {'fulltime': {'home': 2, 'away': 1}}}]}).encode())
                response.headers = {}
                return response

            with (patch('audit_official_scores.read_live_scores', return_value={'1': live}),
                  patch('current_audit.urlopen', side_effect=provider)):
                self.assertTrue(settle_day(root, DAY, NOW, legacy_evidence=False))
            self.assertEqual(requests, [{'timezone': ['Europe/Rome'], 'ids': ['2']}])
            receipt = verified_evidence(root, DAY)
            self.assertFalse(receipt['fixtures']['1']['pending'])
            self.assertTrue(receipt['fixtures']['2']['pending'])
            self.assertEqual(receipt['fixtures']['2']['result']['home_ft'], 2)
            self.assertIsNone(receipt['fixtures']['2']['result']['home_ht'])
            checkpoint_results(store, 'test', root, DAY)
            self.assertEqual(next_recovery_day(store, 'test', Path(plan), NOW+timedelta(hours=21)), DAY)
            recover_learning(store, 'test', Path(plan), DAY, NOW)
            self.assertTrue(v7_ready(Path(plan), NOW.date()))
