from datetime import date, datetime, timedelta
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]

from audit_storage import read_store, write_store
from current_audit import forecast_day
from gold_storage import sha, write_manifest
from lateral_matrix import market_id, settle_spec, specification
from matrix_quotes import collect
from v7_archive import ROME, WEIGHTS, quote_evidence, results_for_day, valid_snapshot
from v7_decision import COMBOS, GG, GOAL, capture, quote_rows, route
from v7_learning import STATE, active_weight, deduplicated, fit, run
from v7_maintenance import MARKER, complete_price_pages, consolidate, price_window
from v7_probability import BASE_WEIGHT, blend_rates, market_rates, price_usable, raw_rates, result_probabilities, score_model

AT = '2026-11-09T22:30:00+01:00'
START = '2026-11-09T23:45:00+01:00'
META = dict(fixture_id=321, kickoff=START, status_short='NS')
RATES = dict(home_ft=1.25, away_ft=1.20, home_ht=.55, away_ht=.50)
FT = GOAL['over25_ft']
HT = GOAL['over15_ht']
HIGH = GOAL['over35_ht']


def row(spec, odd=2.50, p=.45, fair=.40):
    return dict(id=market_id(spec), spec=spec, quality='supported',
                statistical_pct=p*100, odd=odd, first_odd=odd, quote_status='recent',
                quote_observed_at=AT, provider_updated_at=AT, quote_executable=True,
                bookmaker_id='1', selected_margin_pct=5, selected_market_fair_pct=fair*100)


def assessment():
    return dict(rates=RATES, statistics_as_of=AT, matrix=dict(rows=[
        row(json.loads(FT)), row(json.loads(HT)), row(json.loads(GG), 6., .20, .17)]))


class MathAndRouting(unittest.TestCase):
    def test_draw_changes_fit_and_strong_favorite_gains_probability(self):
        baseline = raw_rates({'rates': RATES})
        balanced = market_rates(baseline, {}, (.65, .20, .15))
        fewer_draws = market_rates(baseline, {}, (.715, .12, .165))
        self.assertGreater(sum(fewer_draws[s+'_ft'] for s in ('home','away')),
                           sum(balanced[s+'_ft'] for s in ('home','away')))
        corrected = blend_rates(baseline, balanced, .65)
        original = score_model(baseline)
        result = specification('result', 'ft', '1')
        self.assertGreater(score_model(corrected).estimate(result)['p'], original.estimate(result)['p'])
        for team in ('home', 'away'):
            self.assertAlmostEqual(corrected[team+'_ft'], corrected[team+'_ht']+corrected[team+'_st'])
        self.assertAlmostEqual(sum(result_probabilities(2.9, .71)), 1.)

    def test_combos_share_the_same_score_events_and_zero_second_half(self):
        rates = dict(home_ft=1.3, away_ft=.85, home_ht=1.3, away_ht=.85)
        model = score_model(rates)
        p = lambda spec: model.estimate(spec)['p']
        self.assertAlmostEqual(p(COMBOS[0]), p(specification('total','ht','over',2.5)), places=10)
        self.assertAlmostEqual(p(specification('halves_total','ft','over',.5,.5)), 0.)
        self.assertLess(p(COMBOS[1]), p(json.loads(GG)))
        self.assertLessEqual(p(json.loads(GG)), p(json.loads(HT)))
        self.assertAlmostEqual(p(json.loads(HIGH)) + p(specification('total','ht','under',3.5)), 1., places=10)
        score = dict(status='PEN', home_ht=2, away_ht=2, home_ft=2, away_ft=2,
                     extra_time={'home': 4, 'away': 2}, penalties={'home':5,'away':4})
        self.assertEqual(settle_spec(json.loads(HIGH), score), (1.,0.))
        self.assertEqual(settle_spec(specification('result','ft','x'), score), (1.,0.))
        self.assertIsNone(settle_spec(json.loads(HIGH), dict(score,status='SUSP')))

    def test_real_and_asian_prices_share_one_market_and_future_is_ineligible(self):
        spec = json.loads(HIGH)
        classic = row(spec, 7.)
        asian = row(dict(spec, kind='asian_total'), 8.)
        picks, _ = quote_rows([classic, asian], AT, START)
        self.assertEqual(list(picks), [HIGH])
        self.assertEqual(picks[HIGH][0]['odd'], 8.)
        self.assertFalse(price_usable(dict(asian, quote_observed_at=START), AT, START))
        self.assertFalse(price_usable(dict(asian, quote_executable=False), AT, START))
        odds = {'response': [{'fixture': {'id': 321}, 'update': AT, 'bookmakers': [
            {'id': 1, 'name': 'Book', 'bets': [{'id': 6, 'name': 'Goals Over/Under First Half',
                'values': [{'value':'Over 3.5','odd':'7.0'}, {'value':'Under 3.5','odd':'1.10'}]}]}]}]}
        groups, _ = collect(odds, 321, AT)
        self.assertEqual(next(iter(groups.values()))['specs']['over'], spec)

    def test_conversion_and_colpaccio_have_independent_eligibility(self):
        def r(p, odd, delta, edge=True):
            return dict(calibrated=p, statistics=p, odd=odd, ev=p*odd-1,
                        features=dict(delta_pct=delta, oci=-63),
                        edge_exclusions=[] if edge else ['missing_price'],
                        supported=True, spec_kind='total', side='over')
        rows = {FT:r(.60,2.1,-4), GG:r(.25,6,8), HT:r(.62,2.2,0), HIGH:r(.21,7,0,False)}
        result = route(rows, None)
        self.assertEqual(result['lineare']['market_id'], FT)
        self.assertEqual(result['alternativa']['market_id'], GG)
        self.assertEqual(result['radar_conversione']['market_id'], FT)
        self.assertIsNone(result['colpaccio'])
        rows[HIGH] = r(.21,7,0)
        self.assertEqual(route(rows, None)['colpaccio']['market_id'], HIGH)

    def test_snapshot_hash_survives_serialization_and_rejects_postkickoff(self):
        snapshot = json.loads(json.dumps(capture(assessment(), META, AT)))
        self.assertTrue(valid_snapshot(snapshot, META, AT))
        self.assertFalse(valid_snapshot(snapshot, META, START))
        snapshot['markets'][FT]['calibrated'] = .99
        self.assertFalse(valid_snapshot(snapshot, META, AT))
        self.assertIsNone(capture(assessment(), META, START))
        self.assertIsNone(capture(assessment(), dict(META,result={'status':'FT'}), AT))


class ArchiveAndWorkflow(unittest.TestCase):
    def test_italian_midnight_partition_and_separate_results(self):
        snap = capture(assessment(), META, AT)
        record = dict(fixture=META, captured_at=AT, publication_snapshot=True, model_version='v5',
                      multimarket=dict(matrix=dict(v7=snap)))
        key = f'v5::321::publication::{AT}'
        now = datetime.fromisoformat('2026-11-10T00:01:00+01:00')
        self.assertEqual(forecast_day('analysis_updates',key,record,now),'2026-11-09')
        later = dict(record, fixture=dict(META,kickoff='2026-11-10T00:00:00+01:00'))
        self.assertEqual(forecast_day('analysis_updates',key,later,now),'2026-11-10')
        store = {'analysis_updates':{key:record}}
        pending = results_for_day(store, date(2026,11,9), now)
        self.assertEqual(pending['fixtures']['321']['status'], 'pending')
        self.assertFalse(pending['samples'])
        self.assertTrue(all(x is None for x in pending['fixtures']['321']['outcomes'].values()))
        final = results_for_day(store, date(2026,11,9), now+timedelta(hours=4),
                                {'321':dict(status='FT',home_ht=2,away_ht=0,home_ft=2,away_ft=1)}, pending)
        self.assertEqual(final['fixtures']['321']['outcomes'][FT]['win'], 1.)
        self.assertEqual(final['fixtures']['321']['forecast_sha256'], snap['sha256'])
        self.assertNotIn('settlement', record)
        cancelled = results_for_day(store, date(2026,11,9), now, {'321':{'status':'CANC'}})
        self.assertEqual(cancelled['fixtures']['321']['status'], 'void')

    def test_archive_price_points_exclude_future(self):
        snapshot = capture(assessment(), META, AT)
        valid = dict(observed_at=AT,provider_updated_at=AT,odds={'over':2.2},quote_executable=True,complete=False)
        future = dict(valid,observed_at=START,odds={'over':4.})
        evidence = quote_evidence(dict(kickoff=START,groups={'group|book':[valid,future]}), snapshot)
        self.assertEqual(len(evidence['groups']['group|book']), 1)
        self.assertEqual(evidence['groups']['group|book'][0]['prices']['over'], 2.2)

    def test_consolidation_is_idempotent_and_retains_archive_objects(self):
        class Storage:
            objects = {}
            def put_object(self, **kw):
                self.objects[kw['Key']] = kw['Body']
            def get_object(self, **kw):
                return {'Body':io.BytesIO(self.objects[kw['Key']])}
        storage = Storage()
        original = {'auditordata/prematch_2026-11-09.json':dict(sha256=sha(b'{}'),bytes=2)}
        write_manifest(storage, 'test', original)
        now = datetime(2026,11,10,0,1,tzinfo=ROME)
        self.assertEqual(consolidate(storage,'test',now),'consolidated_pending_verification')
        self.assertEqual(consolidate(storage,'test',now),'already_consolidated')
        index = json.loads(storage.objects['state/current.json'])['files']
        self.assertEqual(index[next(iter(original))], next(iter(original.values())))
        digest = index['data/arab_quote_v7_close_20261109.json']['sha256']
        payload = json.loads(storage.objects['state/objects/'+digest])
        self.assertEqual(payload['cutoff_utc'], '2026-11-09T23:00:00+00:00')
        self.assertEqual(payload['status'], 'pending_verification')

    def test_price_window_and_no_truncated_pages(self):
        now = datetime(2026,11,9,12,32,tzinfo=ROME)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/MARKER
            write_store(path,dict(status='complete',started_at=(now-timedelta(minutes=28)).isoformat(),
                                  completed_at=(now-timedelta(minutes=11)).isoformat()))
            self.assertTrue(price_window(tmp,now))
            self.assertFalse(price_window(tmp,now+timedelta(minutes=30)))
        class API:
            def get(self, name, params, use_cache=False):
                return {'paging':{'current':1,'total':2}, 'response':[{'fixture':321}]}
        self.assertIsNone(complete_price_pages(API(),321,lambda: False))
        self.assertIsNone(complete_price_pages(API(),321,lambda: True))


class Learning(unittest.TestCase):
    def test_future_outcomes_cannot_enter_training_and_update_is_bounded(self):
        cutoff = datetime(2026,11,29,1,tzinfo=ROME)
        rows = []
        for offset in range(1,16):
            when = cutoff-timedelta(days=offset,hours=-12)
            for i in range(24):
                rows.append(dict(fixture_id=f'{offset}:{i}', market=FT, date=str(when.date()),
                    kickoff=when.isoformat(), observed_at=(when+timedelta(hours=2)).isoformat(),
                    outcome=1, raw=.50, corrected=.65, calibrated=.65, v51=.60, fair=.64,
                    variants={str(w):w for w in WEIGHTS}))
        future = dict(rows[0], fixture_id='future', observed_at=(cutoff+timedelta(minutes=1)).isoformat())
        self.assertEqual(len(deduplicated([*rows,rows[0],future],cutoff)),len(rows))
        report = fit(rows,cutoff,.65)
        self.assertEqual(report['weight'],.70)
        self.assertTrue(report['applied'])
        self.assertTrue(set(report['train_dates']).isdisjoint(report['holdout_dates']))
        self.assertEqual(fit(rows[:72],cutoff,.65)['reason'],'insufficient_disjoint_samples')
        blocked = [dict(r,fair=.99) for r in rows]
        self.assertFalse(fit(blocked,cutoff,.65)['applied'])

    def test_state_is_idempotent_and_cannot_apply_before_training(self):
        cutoff = datetime(2026,11,9,0,42,tzinfo=ROME)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            first=run(root,cutoff)
            saved=read_store(root/STATE)
            second=run(root,cutoff+timedelta(minutes=30))
            self.assertEqual(first,second)
            self.assertEqual(saved,read_store(root/STATE))
            self.assertEqual(active_weight(cutoff-timedelta(minutes=1),root),(BASE_WEIGHT,'baseline'))
            self.assertFalse(first['update']['improvement_demonstrated'])


if __name__ == '__main__':
    unittest.main()
