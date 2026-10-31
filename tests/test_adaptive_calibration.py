from copy import deepcopy
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from adaptive_calibration import Evidence, WEIGHTS, cohort, fit, neutral, nightly_state
from adaptive_pricing import apply_matrix, gate_passes
from audit_storage import write_store
from daily_audit import build_daily
from lateral_matrix import MODEL_ID, build_matrix, specification
from matrix_quotes import attach
from statistics_first import statistical_analysis
from test_quote_sources import feed, bet, NOW, START, META
from test_statistics_first import match

DAY = date(2026, 10, 31)
AT = '2026-10-31T08:00:00+01:00'
META_FUTURE = {'competition_tier': 'top', 'kickoff': '2026-10-31T20:00:00+01:00'}


def observations(n, *, wins=25, odd=1.9, clv=.04, market='over25_ft', ids=0):
    return [dict(fid=str(i+ids), cohort=cohort(market, odd, 'top'), market=market, side='over',
                 date='2026-10-30', at='2026-10-30T10:00:00Z', odd=odd, p=.4, base=.4,
                 y=int(i < wins), empirical=.7, fair=.55, clv=clv, pattern='odds_cross_reversal') for i in range(n)]


def matrix_row(market='over25_ft', p=.4, odd=1.9):
    spec = {'gg_ht': specification('btts','ht','yes'), 'over05_ht': specification('total','ht','over',.5),
            'over15_ht': specification('total','ht','over',1.5), 'over25_ft': specification('total','ft','over',2.5)}.get(market)
    return {'id':market, 'spec':spec or specification('result','ft',market),
            'statistical_pct':p*100,'win_weight':p,'loss_weight':1-p,'fair_odd':1/p,'sensitivity_weights':[[p,1-p]],
            'high_pct':p*100,'low_pct':p*100,'empirical':{'win_stake_pct':98.},'market_fair_pct':70.,
            'quote_status':'recent','odd':odd,'quality':'supported','family':'totals','period':'ft'}


def frozen(fid='1', at='2026-10-30T12:00:00Z', odd=2.1, book='7', source='bookmaker'):
    spec = specification('total','ft','over',2.5)
    row = {**matrix_row(odd=odd), 'spec':spec,'bookmaker_id':book,'quote_source':source,'quote_executable':source!='synthetic_combo',
           'quote_observed_at':at,'provider_updated_at':at,'selected_margin_pct':5.}
    return {'fixture':{'fixture_id':fid,'kickoff':'2026-10-30T20:00:00Z','competition_tier':'top'},
            'captured_at':at,'multimarket':{'model_id':MODEL_ID,'validation_from':'2026-10-26','matrix':{'rows':[row]}},
            'settlement':{'result':{'status':'AET','home_ft':2,'away_ft':1,'home_ht':1,'away_ht':0}}}


class AdaptiveCalibration(unittest.TestCase):
    def test_49_closed_fixtures_plus_duplicates_are_exactly_neutral(self):
        rows = observations(49, wins=49)
        rows += [*rows, {**rows[0], 'fid':'old','date':'2026-10-02'}, {**rows[0], 'fid':'today','date':'2026-10-31'}]
        state = fit(rows, DAY)
        d = next(iter(state['cohorts'].values()))
        self.assertEqual((d['n'], d['weights'], d['threshold_delta_pp'], d['favorite_trap_multiplier']),
                         (49, {'poisson':1.,'empirical':0.,'fair':0.}, 0, 1))
        a = {'matrix':{'rows':[matrix_row()]}}
        original = deepcopy(a)
        apply_matrix(a, META_FUTURE, AT, state)
        self.assertEqual(a, original)

    def test_metrics_shrinkage_and_ceiling_do_not_drift_on_retries(self):
        rows = observations(50, wins=10)
        state = fit(rows, DAY)
        key = rows[0]['cohort']
        d = state['cohorts'][key]
        self.assertAlmostEqual(d['roi_pct'], -62.)
        self.assertAlmostEqual(d['clv_pct'], 4.)
        self.assertAlmostEqual(d['brier'], .2*.6**2+.8*.4**2)
        self.assertGreater(d['log_loss'], 0)
        self.assertIn(d['threshold_delta_pp'], (.5,1.))
        self.assertEqual(fit(rows, DAY, state), state)
        for i in range(5):
            rows.extend(observations(1, wins=0, ids=100+i))
            state = fit(rows, DAY, state)
        self.assertEqual(state['cohorts'][key]['threshold_delta_pp'],2.5)
        good = fit(observations(60,wins=60), DAY)['cohorts'][key]
        self.assertEqual(good['threshold_delta_pp'], -.5)
        self.assertAlmostEqual(sum(good['weights'].values()),1.)
        self.assertGreaterEqual(good['weights']['poisson'],.97)
        self.assertLessEqual(good['weights']['empirical']+good['weights']['fair'],.030000001)
        self.assertEqual(good['favorite_trap_multiplier'],1.25)
        missing = fit(observations(70,wins=0,clv=None), DAY)['cohorts'][key]
        self.assertEqual((missing['threshold_delta_pp'],missing['clv_pct'],missing['clv_n']),(0,None,0))
        self.assertGreaterEqual(missing['favorite_trap_multiplier'],.5)
        self.assertLess(missing['favorite_trap_multiplier'],1.)

    def test_closed_prospective_execution_and_same_book_distinct_close_only(self):
        e = Evidence()
        first, close = frozen(), frozen(at='2026-10-30T19:00:00Z',odd=1.9)
        e.observe(close); e.observe(first); e.observe(deepcopy(first))
        e.observe(frozen(at='2026-10-30T19:30:00Z',book='9',odd=1.1))
        e.observe(frozen(at='2026-10-30T19:45:00Z',source='synthetic_combo',odd=1.15))
        e.observe({**first,'retrospective':True,'fixture':{**first['fixture'],'fixture_id':'2'}})
        e.observe({**frozen('3'),'captured_at':'2026-10-30T20:00:00Z'})
        e.observe({**frozen('4'),'settlement':{'result':{'status':'LIVE','home_ft':2,'away_ft':1}}})
        sample, = e.samples()
        self.assertEqual((sample['fid'],sample['odd'],sample['y']),('1',2.1,1))
        self.assertAlmostEqual(sample['clv'],2.1/1.9-1)
        repeated = frozen(at='2026-10-30T19:00:00Z')
        repeated['multimarket']['matrix']['rows'][0]['provider_updated_at']=first['captured_at']
        e = Evidence(); e.observe(first); e.observe(repeated)
        self.assertIsNone(e.samples()[0]['clv'])
        # Independent yet price-equivalent Asian half-lines are executable.
        asian = frozen(source='asian_halfline')
        asian['multimarket']['matrix']['rows'][0]['spec']['kind']='asian_total'
        e = Evidence(); e.observe(asian)
        self.assertEqual(len(e.samples()), 1)

    def test_nested_binary_events_and_three_way_sum_after_blending(self):
        state = fit(sum((observations(60,wins=60,market=m) for m in
                    ('gg_ht','over15_ht','over05_ht','1x2_ft')), []), DAY)
        a = {'matrix':{'rows':[matrix_row('gg_ht',.66),matrix_row('over15_ht',.66),
            matrix_row('over05_ht',.67),matrix_row('1',.49),matrix_row('x',.10),matrix_row('2',.41)]}}
        a['matrix']['rows'][0]['empirical']['win_stake_pct']=99.
        a['matrix']['rows'][1]['empirical']['win_stake_pct']=1.
        apply_matrix(a,META_FUTURE,AT,state)
        probs = [r['win_weight'] for r in a['matrix']['rows']]
        self.assertTrue(all(.01 <= p <= .99 for p in probs))
        self.assertLessEqual(probs[0],probs[1]); self.assertLessEqual(probs[1],probs[2])
        self.assertAlmostEqual(sum(probs[3:]),1.,places=9)
        self.assertNotAlmostEqual(probs[0],.66)
        self.assertAlmostEqual(a['matrix']['rows'][0]['ev_pct'],100*(probs[0]*1.9-1),places=3)
        first = deepcopy(a)
        apply_matrix(a,META_FUTURE,AT,state)
        self.assertEqual(a,first)
        for row in a['matrix']['rows']:
            self.assertFalse(gate_passes({**row,'ev_pct':0,'edge_pp':3.},0.))
        apply_matrix(a,META_FUTURE,AT,neutral())
        self.assertEqual(a['matrix']['rows'][0]['win_weight'],.66)
        self.assertNotIn('calibration',a['matrix'])

    def test_daily_auditor_handoff_and_first_half_leg_share_original_feed(self):
        e = Evidence()
        day = {'multimarket_evaluations':{'1':frozen()},'analysis_updates':{'later':frozen(at='2026-10-30T19:10:00Z',odd=1.9)}}
        build_daily(day,'2026-10-30',AT,observe=e.observe)
        with TemporaryDirectory() as d:
            root = Path(d)
            write_store(root/'auditarchive/prematch_2026-10-30_calibration.json', e.samples())
            write_store(root/'data/arab_quote_prematch_report.json', {'generated_at': AT,'groups':[{'n':1000000}]})
            computed = nightly_state(root, DAY)
            self.assertEqual(next(iter(computed['cohorts'].values()))['n'], 1)
            self.assertEqual(computed['report_generated_at'], AT)
            self.assertFalse((root/WEIGHTS).exists())
        a = build_matrix(statistical_analysis(match(),NOW),NOW)
        attach(a,feed([bet(6,.5)]),{},META,NOW)
        ref = a['matrix']['calibration_reference']
        self.assertEqual((len(a['matrix']['rows']),ref['quote_status'],ref['odd']),(58,'recent',1.9))

    def test_other_book_display_retains_a_same_contract_closing_offer(self):
        from adaptive_calibration import closing_prices
        at='2026-10-30T19:20:00Z'
        offered={'bookmaker_id':'7','complete':True,'quote_source':'bookmaker','observed_at':at,
                 'provider_updated_at':at,'odds':{'over':1.85,'under':2.05}}
        close=frozen(at=at,book='9',odd=2.05)
        close['multimarket']['matrix']['rows'][0]['calibration_closes']=closing_prices(
            [offered,{**offered,'bookmaker_id':'10','quote_source':'synthetic_combo'}],'over',at,close['fixture']['kickoff'])
        self.assertEqual(set(close['multimarket']['matrix']['rows'][0]['calibration_closes']),{'7'})
        e=Evidence(); e.observe(frozen()); e.observe(close)
        self.assertAlmostEqual(e.samples()[0]['clv'],2.1/1.85-1)

    def test_ineligible_and_future_state_do_not_leak_into_fast_or_history(self):
        from adaptive_calibration import load_weights
        state=fit(observations(60,wins=60),DAY)
        for date_value, trained in [('2026-11-01',None),('2026-10-27',None),('2026-10-31','2026-10-31T12:00:00+01:00')]:
            a={'matrix':{'rows':[matrix_row()]}}
            before=deepcopy(a)
            apply_matrix(a,META_FUTURE,AT,{**state,'as_of':date_value,'trained_at':trained})
            self.assertEqual(a,before)
        with TemporaryDirectory() as d, patch('adaptive_calibration.ROOT',Path(d)):
            load_weights.cache_clear()
            try:
                write_store(Path(d)/WEIGHTS,state)
                with patch.object(Path,'read_bytes',autospec=True,side_effect=Path.read_bytes) as read:
                    for _ in range(8): apply_matrix({'matrix':{'rows':[matrix_row()]}},META_FUTURE,AT)
                self.assertEqual(read.call_count,1)
            finally:
                load_weights.cache_clear()

    def test_gold_mature_and_platinum_use_bounded_gates(self):
        from adaptive_calibration import VERSION
        from adaptive_pricing import reprice
        from opportunity_policy import evaluate as gold
        from platinum_policy import evaluate as platinum
        from test_opportunity_policy import row
        from test_platinum_policy import CASES
        delta={'version':VERSION,'n':60,'threshold_delta_pp':1.}
        gold_row=row(ev_pct=.8,ev_low_pct=3.,edge_pp=.9)
        self.assertTrue(gold(gold_row)['eligible'])
        self.assertFalse(gold({**gold_row,'calibration':delta})['eligible'])
        mature=matrix_row(p=.501,odd=2.)
        mature.update(oci=50,movement_confirmed=True,fair_direction_coherent=True,quality='supported',
                      market_fair_pct=49.,calibration=delta)
        reprice(mature)
        self.assertEqual(mature['category'],'value_watch')
        high={**deepcopy(CASES['base']),'odd':2.7,'first_odd':2.9,'ev_pct':-15.,'edge_pp':2.}
        high['delta_pct']=100*(high['odd']/high['first_odd']-1)
        self.assertTrue(platinum(high,AT)['eligible'])
        self.assertFalse(platinum({**high,'calibration':delta},AT)['eligible'])
        relaxed={**high,'calibration':{**delta,'threshold_delta_pp':-.5}}
        self.assertTrue(platinum(relaxed,AT)['eligible'])
        self.assertFalse(platinum({**relaxed,'ev_pct':-15.1},AT)['eligible'])

    def test_pattern_class_requires_its_own_50_hits_and_absolute_cap(self):
        trials=observations(60,wins=60,market='gg_ht')
        for r in trials[:20]: r['pattern']='underdog_book_square'
        state=fit(trials,DAY)
        d=next(iter(state['cohorts'].values()))
        self.assertTrue(all(p['multiplier']==1. for p in d['by_market_pattern'].values()))
        for r in trials: r['pattern']='odds_cross_reversal'
        state=fit(trials,DAY)
        a={'matrix':{'rows':[matrix_row('gg_ht')]},'favorite_trap_pattern':{'classification':'odds_cross_reversal'},
           'favorite_trap_base':{'gg_ht':.038}}
        no_trap=deepcopy(a);no_trap['favorite_trap_base']={}
        apply_matrix(a,META_FUTURE,AT,state);apply_matrix(no_trap,META_FUTURE,AT,state)
        self.assertAlmostEqual(a['matrix']['rows'][0]['win_weight']-no_trap['matrix']['rows'][0]['win_weight'],.036)
