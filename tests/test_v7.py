from datetime import date, datetime, timedelta
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]

from audit_storage import read_store, write_store
from current_audit import forecast_day
from gold_storage import sha, write_manifest
from lateral_matrix import FAMILIES, build_matrix, market_id, settle_spec, specification
from matrix_pipeline import build_board
from matrix_quotes import collect
from v7_archive import ROME, WEIGHTS, day_path, quote_evidence, results_for_day, valid_snapshot
from v7_decision import (AWAY, BOTH_GOALS, COMBOS, GG, GG_BOTH, GG_CARRY, GG_FT, GOAL,
                         OVER_CARRY, capture, quote_rows, route, stored_offers)
from v7_learning import STATE, active_weight, deduplicated, fit, run
from v7_maintenance import MARKER, complete_price_pages, consolidate, price_window
from v7_probability import BASE_WEIGHT, blend_rates, market_rates, price_usable, raw_rates, result_probabilities, score_model

AT = '2026-11-09T22:30:00+01:00'
START = '2026-11-09T23:45:00+01:00'
META = dict(fixture_id=321, kickoff=START, status_short='NS')
RATES = dict(home_ft=1.25, away_ft=1.20, home_ht=.55, away_ht=.50)
PROFILES = {side: {f'for_{period}': dict(sample=8, mean=1.5, without_last=1.5, without_peak=1.)
                   for period in ('ft', 'ht')} for side in ('home', 'away')}
PATTERNS = dict(conversion=False, rebound_gg=False)

FT = GOAL['over25_ft']
HT = GOAL['over15_ht']
HIGH = GOAL['over35_ht']


def row(spec, odd=2.50, p=.45, fair=.40):
    return dict(id=market_id(spec), spec=spec, quality='supported', family=FAMILIES[spec['kind']], period=spec['period'],
                statistical_pct=p*100, odd=odd, first_odd=odd, quote_status='recent',
                quote_observed_at=AT, provider_updated_at=AT, quote_executable=True,
                bookmaker_id='1', selected_margin_pct=5, selected_market_fair_pct=fair*100)


def assessment():
    return dict(rates=RATES, statistics_as_of=AT, profiles=PROFILES, matrix=dict(rows=[
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
        self.assertAlmostEqual(p(json.loads(OVER_CARRY)), p(specification('total','ht','over',2.5)), places=10)
        self.assertAlmostEqual(p(specification('halves_total','ft','over',.5,.5)), 0.)
        self.assertLess(p(json.loads(GG_CARRY)), p(json.loads(GG)))
        self.assertEqual(p(json.loads(GG_BOTH)), 0.)
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

    def test_conversion_observes_a_market_even_when_triade_has_no_eligible_price(self):
        p, odd = .42, 2.
        records = {FT: dict(source_market_id=FT, label='Over 2,5 FT', calibrated=p,
                            odd=odd, ev=p*odd-1, ev_low=-.2, horizon='FT',
                            tier='BLU', niche_id='bookmaker_shift_rebound', features={})}
        triad, signals = route(records, dict(PATTERNS, conversion=True))
        self.assertEqual(signals['radar_conversione']['market_id'], FT)
        self.assertTrue(all(pick is None for pick in triad.values()))

    def test_snapshot_hash_survives_serialization_and_rejects_postkickoff(self):
        snapshot = json.loads(json.dumps(capture(assessment(), META, AT)))
        self.assertTrue(valid_snapshot(snapshot, META, AT))
        self.assertFalse(valid_snapshot(snapshot, META, START))
        snapshot['markets'][FT]['calibrated'] = .99
        self.assertFalse(valid_snapshot(snapshot, META, AT))
        self.assertIsNone(capture(assessment(), META, START))
        self.assertIsNone(capture(assessment(), dict(META,result={'status':'FT'}), AT))

    def test_active_tiers_share_v7_and_started_snapshot_is_immutable(self):
        a = build_matrix(dict(rates=dict(RATES, home_ft=2.0, away_ft=1.3),
                              profiles=PROFILES, markets=[]), AT)
        matrix = a['matrix']
        over = next(r for r in matrix['rows'] if r['id'] == FT)
        baseline = over['statistical_pct']
        quote = row(json.loads(FT), 2.10, fair=.60)
        quote.pop('statistical_pct')
        over.update(quote, first_odd=2.10/.96, bookmaker='Test', oci=38,
                    market_fair_pct=60, delta_pct=-4, paired_books=4, agreeing_books=3,
                    movement_confirmed=True, fair_direction_coherent=True,
                    selected_movement=dict(delta_pct=-4, fair_delta_pp=2, persistence=.67, reversal=False))
        meta = dict(META, country='Italy', league='Serie A')
        fixture = dict(fixture=meta, multimarket=a)
        day = {'matches': [fixture]}
        with patch.dict('os.environ', {'GOLD_PRIVATE': 'true', 'V7_SHADOW_ONLY': 'true'}):
            build_board(day, AT)
            snapshot = matrix['v7']
            for policy in ('opportunity_policy', 'platinum_policy'):
                self.assertEqual(matrix[policy]['probability_model'], snapshot['version'])
                self.assertEqual(matrix[policy]['best_market_id'], FT)
            primary = snapshot['triad']['most_probable']
            self.assertEqual(primary['market_id'], FT)
            self.assertEqual(primary['tier'], 'PLATINUM')
            self.assertEqual(primary['badge'], '★ PLATINUM')
            self.assertEqual(primary['odd'], over['odd'])
            self.assertEqual(primary['horizon'], 'FT')
            self.assertEqual(fixture['display']['primary_choice'], primary)
            self.assertEqual(fixture['triad'], snapshot['triad'])
            self.assertEqual(list(stored_offers(a, meta, AT))[0][1], primary)
            self.assertAlmostEqual(over['statistical_pct']/100, snapshot['markets'][FT]['calibrated'], places=6)
            self.assertAlmostEqual(snapshot['markets'][FT]['v51'], baseline/100)
            self.assertAlmostEqual(over['ev_pct'], over['statistical_pct']*over['odd']-100, places=3)
            self.assertTrue(valid_snapshot(snapshot, meta, AT))
            build_board(day, AT)
            self.assertEqual(matrix['v7'], snapshot)
            build_board(day, START)
            self.assertEqual(matrix['v7'], snapshot)
            self.assertEqual(list(stored_offers(a, meta, START)), [])

    def test_triad_orders_by_probability_and_enforces_exact_role_limits(self):
        def priced(key, p, odd, tier='BLU', niche_id=None, delta=None):
            spec = json.loads(key)
            return dict(source_market_id=key, label=key, calibrated=p, odd=odd,
                        ev=p*odd-1, ev_low=p*odd-1.01, tier=tier, niche_id=niche_id,
                        horizon='PT+ST' if spec in COMBOS else {'ht':'PT','sh':'ST','ft':'FT'}[spec['period']],
                        features=dict(net_delta_pct=delta))
        records = {GG_FT:priced(GG_FT,.62,1.85), FT:priced(FT,.52,2.1),
                   BOTH_GOALS:priced(BOTH_GOALS,.54,2.25),
                   HT:priced(HT,.48,2.25),
                   GG:priced(GG,.26,5.), HIGH:priced(HIGH,.21,10.),
                   AWAY:priced(AWAY,.25,8.,delta=-3.)}
        triad, _ = route(records, PATTERNS)
        self.assertEqual([pick['market_id'] for pick in triad.values()], [GG_FT, HT, GG])
        records[GG]['calibrated'] = .15999
        self.assertIsNone(route(records, PATTERNS)[0]['high_odds_shot'])
        records[AWAY]['features']['net_delta_pct'] = 5.
        self.assertEqual(route(records, PATTERNS)[0]['high_odds_shot']['market_id'], AWAY)
        records[HT]['ev_low'] = -0.0001
        self.assertIsNone(route(records, PATTERNS)[0]['recommended_alternative'])

    def test_all_four_niches_require_observed_inputs(self):
        from context_analysis import congruence_triggers
        profiles = deepcopy(PROFILES)
        profiles['home']['against_ft'] = {'xg_mean': 1.}
        means = {side: {'st': {'sample': 4}} for side in ('home', 'away')}
        prices = dict(home={'odd': 1.70}, gg_ht={'odd': 5.50, 'features': {'net_delta_pct': 5., 'oci': -51}},
                      over25_ft={'odd': 1.75, 'features': {'net_delta_pct': -1}},
                      over15_ht={'odd': 3.50})
        rates = dict(home_ht=.75, away_ht=.60, home_st=.80, away_st=.80, home_ft=1.55, away_ft=1.40)
        observed = congruence_triggers(profiles, means, rates, prices, {}, {}, None)
        self.assertTrue(all(value for key, value in observed.items() if key != 'balanced_1x2_inversion'))
        self.assertFalse(observed['balanced_1x2_inversion'])
        balanced = dict(rates, home_ht=.73, away_ht=.62)
        opening, current = {'1':2.2,'2':2.8}, {'1':2.3,'2':2.7}
        self.assertTrue(congruence_triggers(profiles, means, balanced, prices, opening, current, .44)
                        ['balanced_1x2_inversion'])
        self.assertFalse(congruence_triggers(profiles, means, balanced, prices, opening, opening, .44)
                         ['balanced_1x2_inversion'])
        prices['gg_ht']['features']['oci'] = -50
        prices['home']['odd'] = 1.701
        self.assertFalse(congruence_triggers(profiles, means, rates, prices, {}, {}, None)['rebound_gg'])
        self.assertFalse(congruence_triggers(profiles, means, rates, prices, {}, {}, None)['fav_home_pt_vulnerable'])
        self.assertFalse(congruence_triggers(profiles, {}, rates, prices, {}, {}, None)['pt_st_symmetry_11_22'])

    def test_combo_without_direct_odd_has_a_model_reference_only(self):
        assessment = build_matrix(dict(rates=RATES, profiles=PROFILES), AT)
        rows = {r['id']: r for r in assessment['matrix']['rows']}
        for key in (GG, FT):
            rows[key].update(row(json.loads(key)))
        snapshot = capture(assessment, META, AT)
        reference = snapshot['markets'][GG_CARRY]
        self.assertIsNone(reference['odd'])
        self.assertIsNone(reference['ev'])
        self.assertEqual([leg['market_id'] for leg in reference['reference_legs']], [GG, FT])
        self.assertAlmostEqual(reference['model_fair_odd'], 1/reference['calibrated'], places=5)
        rows[FT]['quote_observed_at'] = START
        self.assertNotIn('reference_legs', capture(assessment, META, AT)['markets'][GG_CARRY])


class ArchiveAndWorkflow(unittest.TestCase):
    def test_price_writers_cannot_select_a_different_engine(self):
        for filename in ('v7-maintenance.yml', 'fast_scan.yml', 'night_scan.yml'):
            workflow = (ROOT/'.github/workflows'/filename).read_text()
            self.assertNotIn('V7_SHADOW_ONLY', workflow)

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
    def test_nightly_reports_roi_stress_by_signal_and_intermediate_persistence(self):
        cutoff = datetime(2026, 11, 10, 0, 42, tzinfo=ROME)
        day = date(2026, 11, 9)
        at = '2026-11-09T20:00:00+01:00'
        profits = [8., 5., 4., 2., -1., -1.]
        persistence = [.5, 1., .75, 0., .3, None]
        samples = [dict(fixture_id=str(i), market=HIGH, date=str(day), kickoff=at,
                        observed_at='2026-11-09T22:00:00+01:00', outcome=int(profit > 0),
                        calibrated=.25, roi=-1., roles=['colpaccio', 'radar_sorprese'] if i == 0 else ['colpaccio'],
                        role_roi={'colpaccio': profit, 'radar_sorprese': 3.},
                        policy_exclusions=[], features={'persistence': value}, benchmark=False)
                   for i, (profit, value) in enumerate(zip(profits, persistence))]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_store(day_path(root, day), dict(version='arabsniper-v7.0', date=str(day),
                        counts={'settled': 6}, samples=[*samples, samples[0]]))
            report = run(root, cutoff)
            self.assertEqual(report, read_store(root/f'auditarchive/prematch_{day}_v7_learning.json'))
        signals = report['diagnostics']['signals']
        colpaccio = signals['colpaccio']
        self.assertEqual(colpaccio['roi'], sum(profits)/6)
        self.assertEqual(colpaccio['roi_without_three_largest_wins'], 0.)
        self.assertEqual(colpaccio['top_wins_removed'], 3)
        self.assertEqual(colpaccio['n_without_three_largest_wins'], 3)
        intermediate = colpaccio['persistence_intermediate']
        self.assertEqual((intermediate['n'], intermediate['top_wins_removed']), (3, 2))
        self.assertEqual(intermediate['roi_without_three_largest_wins'], -1.)
        radar = signals['radar_sorprese']
        self.assertEqual(radar['roi'], 3.)
        self.assertEqual(radar['persistence_intermediate']['n'], 1)
        self.assertIsNone(radar['roi_without_three_largest_wins'])

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
