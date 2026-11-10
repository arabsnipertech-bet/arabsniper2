import importlib.util
import io
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from audit_storage import read_store
from gold_storage import GOLD, publish, restore
from matrix_quotes import collect, ledger_offers
from quote_history import QuoteHistory, INDEX, PREFIX
from test_gold_storage import Fake
from test_quote_sources import feed, bet

AT = '2026-10-26T08:00:00+00:00'
START = '2026-10-29T18:00:00+00:00'


def payload(when=AT, odd=2., book=10):
    result = feed([bet(5, 2.5, odd, 1.9)], book=book, at=when)
    result['response'][0]['fixture']['date'] = START
    return result


def history(journal):
    return next(iter(next(iter(journal.days.values()))['fixtures']['100']['groups'].values()))


class PersistentPrices(unittest.TestCase):
    def test_all_sixty_observations_survive_hot_cache_cap_and_private_restore(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            data = Path(a)/'data'
            journal, state = QuoteHistory(data), {}
            for i in range(60):
                at = (datetime.fromisoformat(AT)+timedelta(minutes=i)).isoformat()
                raw = payload(at, 2.+i/1000)
                journal.observe(raw, at, {'fixture': 100})
                groups, _ = collect(raw, 100, at)
                ledger_offers(state, 100, groups, at, START)
            first = deepcopy(history(journal)['first'])
            self.assertEqual(len(history(journal)['observations']), 60)
            self.assertEqual(len(next(iter(state['matrix_quotes']['100']['groups'].values()))), 48)
            self.assertEqual(first['over']['odd'], 2.)
            self.assertAlmostEqual(history(journal)['observations'][-1]['delta_from_first_pct']['over'], 2.95)
            journal.flush()
            for name in GOLD:
                (data/name).write_text('{"matches":[]}')
            remote = Fake()
            publish(remote, 'test', a)
            manifest = json.loads(remote.objects['state/current.json'])
            self.assertIn('data/'+INDEX, manifest['files'])
            restore(remote, 'test', b)
            restored = QuoteHistory(Path(b)/'data')
            restored.observe(payload('2026-10-26T10:00:00Z', 2.1), '2026-10-26T10:00:00Z', {'fixture': 100})
            self.assertEqual(history(restored)['first'], first)
            self.assertEqual(len(history(restored)['observations']), 61)
            restored.flush()
            self.assertEqual(len(read_store(Path(b)/'data'/(PREFIX+'20261026.json'))['fixtures']['100']['groups'].popitem()[1]['observations']), 61)

    def test_identical_price_from_new_http_is_saved_but_does_not_manufacture_oci_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal, state = QuoteHistory(tmp), {}
            for at in (AT, '2026-10-26T09:00:00Z'):
                raw = payload()
                journal.observe(raw, at, {'fixture': 100})
                groups, _ = collect(raw, 100, at)
                ledger_offers(state, 100, groups, at, START)
            self.assertEqual(len(history(journal)['observations']), 2)
            self.assertEqual(len(next(iter(state['matrix_quotes']['100']['groups'].values()))), 1)
            self.assertEqual(history(journal)['observations'][1]['delta_from_first_pct']['over'], 0.)
            journal.observe(payload(), '2026-10-26T09:00:00Z', {'fixture': 100})
            self.assertEqual(len(history(journal)['observations']), 2, 'Retrying the exact same acquisition is idempotent')

    def test_bootstrap_uses_original_timestamps_and_keeps_books_and_lines_separate(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = {}
            groups, _ = collect(payload(), 100, AT)
            ledger_offers(state, 100, groups, AT, START)
            original = deepcopy(state)
            journal = QuoteHistory(tmp)
            journal.seed_matrix(state)
            journal.observe(payload('2026-10-26T10:00:00Z', 1.8), '2026-10-26T10:00:00Z', {'fixture': 100})
            journal.observe(payload('2026-10-26T10:00:00Z', 4., book=20), '2026-10-26T10:00:00Z', {'fixture': 100})
            self.assertEqual(state, original)
            first = history(journal)
            self.assertEqual(first['first']['over'], {'odd': 2., 'observed_at': AT, 'provider_updated_at': AT})
            self.assertEqual(first['observations'][0]['source']['endpoint'], 'legacy_matrix_ledger')
            self.assertAlmostEqual(first['observations'][-1]['delta_from_first_pct']['over'], -10)
            self.assertEqual(len(journal.days['20261026']['fixtures']['100']['groups']), 2)
            journal.flush()
            restored = QuoteHistory(tmp)
            restored.seed_matrix(state)
            self.assertEqual(len(history(restored)['observations']), 2, 'Restoring cannot import duplicate historical observations')

    def test_cache_refresh_retains_actual_acquisition_time_and_does_not_write_journal(self):
        raw = payload()
        raw['response'][0]['_observed_at'] = AT
        groups, _ = collect(raw, 100, '2026-10-26T10:00:00Z')
        self.assertEqual(next(iter(groups.values()))['observed_at'], AT)
        raw['response'][0]['_observed_at'] = '2026-10-26T11:00:00Z'
        self.assertFalse(collect(raw, 100, '2026-10-26T10:00:00Z')[0])

    def test_incomplete_future_and_postkickoff_cannot_create_baselines(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = QuoteHistory(tmp)
            for raw, at in [(payload(), '2026-10-26T07:00:00Z'),
                            (payload('2026-10-29T19:00:00Z'), '2026-10-29T19:00:00Z'),
                            ({**payload(), 'collection': {'complete': False}}, AT)]:
                journal.observe(raw, at, {'fixture': 100})
            self.assertFalse(journal.index['fixtures'])
            self.assertFalse(journal.days)

    def test_reschedule_keeps_immutable_first_in_the_original_partition(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = QuoteHistory(tmp)
            journal.observe(payload(), AT, {'fixture': 100})
            raw = payload('2026-10-27T08:00:00Z', 1.8)
            raw['response'][0]['fixture']['date'] = '2026-10-30T18:00:00Z'
            journal.observe(raw, '2026-10-27T08:00:00Z', {'fixture': 100})
            self.assertEqual(journal.index['fixtures']['100'], '20261026')
            self.assertEqual(history(journal)['first']['over']['odd'], 2.)
            self.assertAlmostEqual(history(journal)['observations'][-1]['delta_from_first_pct']['over'], -10)

    def test_missing_referenced_archive_fails_instead_of_resetting_first_price(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = QuoteHistory(tmp)
            journal.observe(payload(), AT, {'fixture':100})
            journal.flush()
            (Path(tmp)/(PREFIX+'20261026.json')).unlink()
            restored = QuoteHistory(tmp)
            with self.assertRaisesRegex(ValueError, 'Missing referenced'):
                restored.observe(payload('2026-10-26T10:00:00Z', 1.8),'2026-10-26T10:00:00Z',{'fixture':100})


class RawAcquisition(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('quote_history_engine', ROOT/'src/3appdays.py')
        cls.e = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.e
        spec.loader.exec_module(cls.e)

    def test_general_and_targeted_are_archived_before_any_market_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = self.e.APIClient('test-placeholder')
            client.odds_observer = QuoteHistory(tmp)
            client._get_page = Mock(return_value=payload())
            with patch.object(self.e, 'iso_now', return_value=AT):
                client.get('odds', {'fixture': 100})
            with patch.object(self.e, 'iso_now', return_value='2026-10-26T09:00:00Z'):
                client.get('odds', {'fixture': 100, 'bet': 5})
            points = history(client.odds_observer)['observations']
            self.assertEqual(len(points), 2)
            self.assertEqual(points[-1]['source']['query'], {'fixture': 100, 'bet': 5})
            with patch.object(self.e, 'iso_now', return_value='2026-10-26T09:00:00Z'):
                client.get('odds', {'fixture': 100, 'bet': 5})
            self.assertEqual(len(history(client.odds_observer)['observations']), 3,
                             'Separate HTTP acquisitions in the same second must both survive')

    def test_can_restore_the_exact_immutable_checkpoint_after_newer_publication(self):
        from gold_storage import sha
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            data = Path(a)/'data'
            data.mkdir()
            for name in GOLD:
                (data/name).write_text('{"matches":[]}')
            remote = Fake()
            publish(remote, 'test', a)
            checkpoint = sha(remote.objects['state/current.json'])
            (data/GOLD[0]).write_text('{"matches":[],"newer":true}')
            publish(remote, 'test', a)
            restore(remote, 'test', b, checkpoint)
            self.assertNotIn('newer', read_store(Path(b)/'data'/GOLD[0]))
            self.assertIn('newer', json.loads(remote.objects['state/current.json']) and read_store(data/GOLD[0]))

    def test_new_collector_and_journal_changes_require_actual_fast_collection(self):
        from refresh_opportunities import cached_refresh_safe
        for changed in ('src/odds_collection.py', 'src/quote_history.py', 'src/quote_cache.py'):
            self.assertFalse(cached_refresh_safe([changed, 'src/matrix_quotes.py']))
            self.assertIn("- '"+changed+"'", (ROOT/'.github/workflows/fast_scan.yml').read_text())

    def test_schedule_acquisition_precedes_failed_statistics_and_gold_keeps_real_1x2(self):
        from contextlib import ExitStack
        with tempfile.TemporaryDirectory() as tmp:
            e = self.e
            raw = payload()
            raw['response'][0]['bookmakers'][0]['bets'].append({'id':1,'name':'Match Winner', 'values':[
                {'value':'Home','odd':'2.0'},{'value':'Draw','odd':'3.3'},{'value':'Away','odd':'4.0'}]})
            fixture = {'fixture':{'id':100,'date':START,'status':{'short':'NS'}},
                'league':{'id':135,'season':2026,'name':'Serie A','country':'Italy','type':'League'},
                'teams':{'home':{'id':10,'name':'Home'},'away':{'id':20,'name':'Away'}}}
            requests = []
            def page(endpoint, params, *args, **kw):
                requests.append((endpoint,dict(params)))
                if endpoint == 'odds':return deepcopy(raw)
                if endpoint == 'fixtures' and 'date' in params:return {'response':[fixture]}
                raise RuntimeError('Statistics temporarily unavailable')
            client=e.APIClient('test-placeholder')
            client._get_page=Mock(side_effect=page)
            client.odds_observer=QuoteHistory(tmp)
            state={'version':e.VERSION,'fixtures':{}}
            with ExitStack() as stack:
                for name, value in [('iso_now',lambda:AT),('_existing_day_payload',lambda *a:{}),
                    ('read_json',lambda path,default=None:state if path==e.STATE_FILE else default),
                    ('atomic_write_json',lambda *a:None),('save_xg_cache',lambda:None)]:
                    stack.enter_context(patch.object(e,name,value))
                stack.enter_context(patch('scan_audit.update_audit',return_value={'cohorts':[],'status':'experimental'}))
                output=e.scan_dates(client,['2026-10-29'],[])
            first_odds=next(i for i,(endpoint,params) in enumerate(requests) if endpoint=='odds')
            first_stats=next(i for i,(endpoint,params) in enumerate(requests) if endpoint=='fixtures' and 'team' in params)
            self.assertLess(first_odds,first_stats)
            rows=output['days'][0]['matches'][0]['multimarket']['matrix']['rows']
            result=[r for r in rows if r['spec']['kind']=='result' and r['spec']['period']=='ft']
            self.assertEqual({r['spec']['side']:r['odd'] for r in result},{'1':2.,'x':3.3,'2':4.})
            self.assertTrue(all(not r['opportunity']['eligible'] for r in result))
            self.assertTrue(client.odds_observer.index['fixtures'])
