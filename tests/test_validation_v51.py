from copy import deepcopy
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from adaptive_calibration import fit, cohort, usable_entry, load_weights, calibration_status, WEIGHTS
from adaptive_pricing import apply_matrix
from statistics_first import market_anchored_rates
from lateral_matrix import build_matrix, specification
from calibration_benchmark import build_benchmark
from audit_storage import write_store
from test_adaptive_calibration import observations, frozen, matrix_row, AT, META_FUTURE, DAY


class ValidationV51(unittest.TestCase):
    def test_lambda_anchor_conserves_total_and_period_order_both_sides(self):
        rates = dict(home_ft=1.52, away_ft=1.9, home_ht=.65, away_ht=.8)
        prices = {'1':12.,'x':6.5,'2':1.26}
        changed, info = market_anchored_rates(rates, prices)
        self.assertLess(changed['home_ft'],rates['home_ft'])
        self.assertGreater(changed['away_ft'],rates['away_ft'])
        self.assertAlmostEqual(changed['home_ft']+changed['away_ft'],3.42)
        self.assertEqual(info['favorite'],'2')
        swapped = {k.replace('home','TEMP').replace('away','home').replace('TEMP','away'):v for k,v in rates.items()}
        reverse, _ = market_anchored_rates(swapped,{'1':1.26,'x':6.5,'2':12.})
        self.assertAlmostEqual(changed['home_ft'],reverse['away_ft'])
        for team in ('home','away'):
            self.assertGreaterEqual(changed[team+'_ft'],changed[team+'_ht'])
        zero, _ = market_anchored_rates(rates, prices, 0)
        self.assertEqual(zero,rates)
        maxed,_ = market_anchored_rates(rates,prices,5)
        capped,_ = market_anchored_rates(rates,prices,.35)
        self.assertEqual(maxed,capped)

    def test_missing_incomplete_balanced_and_invalid_market_leave_raw_rates(self):
        rates = dict(home_ft=1.5,away_ft=1.5,home_ht=.6,away_ht=.6)
        for prices in ({},{'1':1.4},{'1':2.8,'x':3.2,'2':2.8},{'1':1.1,'x':1.1,'2':1.1}):
            self.assertEqual(market_anchored_rates(rates,prices),(rates,None))

    def test_matrix_anchor_uses_fresh_paired_book_is_idempotent_and_freezes_base(self):
        at='2026-11-01T08:00:00+01:00'
        meta=dict(META_FUTURE,kickoff='2026-11-01T20:00:00+01:00')
        a=build_matrix(dict(rates=dict(home_ft=1.52,away_ft=1.9,home_ht=.65,away_ht=.8)),at)
        price={'1':12.,'x':6.5,'2':1.26}
        for r in a['matrix']['rows']:
            if r['spec']['kind']=='result' and r['period']=='ft':
                r.update(selected_market_odds=price,provider_updated_at=at,quote_status='recent')
        apply_matrix(a,meta,at)
        self.assertLess(a['rates']['home_ft'],1.52)
        self.assertEqual(a['pure_poisson_rates']['home_ft'],1.52)
        first=deepcopy(a)
        apply_matrix(a,meta,at)
        self.assertEqual(a,first)
        total=sum(r['win_weight'] for r in a['matrix']['rows'] if r['spec']['kind']=='result' and r['period']=='ft')
        self.assertAlmostEqual(total,1.,places=6)
        future=deepcopy(a)
        apply_matrix(future,dict(meta,result={'status':'FT'}),at)
        self.assertEqual(a,future)

    def test_macro_fallback_counts_unique_fixtures_and_uses_prior(self):
        rows=observations(25,wins=25)
        for i,r in enumerate(rows): r['cohort']=cohort('over25_ft',1.9,'top' if i%2 else 'second')
        s=fit(rows+rows,DAY)
        self.assertEqual(s['macro_cohorts']['over25_ft|1.8-2.49']['n'],25)
        for key in s['cohorts']:
            d=usable_entry(s,key)
            self.assertEqual(d['hierarchy_level'],'macro')
            self.assertLessEqual(d['weights']['fair']+d['weights']['empirical'],.06000001)
        self.assertEqual(len(s['sub_threshold_progress']),2)
        cold=fit(rows[:24],DAY)
        self.assertFalse(usable_entry(cold,rows[0]['cohort']))
        full=fit(observations(50,wins=50),DAY)
        self.assertEqual(usable_entry(full,rows[0]['cohort'].replace('second','top'))['hierarchy_level'],'micro')

    def test_six_point_budget_requires_brier_and_financial_confirmation(self):
        good=fit(observations(60,wins=60),DAY)
        self.assertEqual(next(iter(good['cohorts'].values()))['correction_cap'],.06)
        bad=fit(observations(60,wins=0,clv=-.1),DAY)
        self.assertEqual(next(iter(bad['cohorts'].values()))['correction_cap'],.03)

    def traps(self,n=25,y=0):
        return [dict(fid=str(i),type='studio_value',market='1x2_1',suppressed=True,
                     suppression_reasons=['model_market_divergence'],blocking_reasons=[],
                     date='2026-10-30',at='2026-10-30T10:00:00Z',p=.4,y=y,odd=8.,market_fair_pct=10.) for i in range(n)]

    def test_traps_adapt_without_retry_ratchet_and_do_not_inflate_sample(self):
        rows=self.traps()
        s=fit(rows+rows,DAY)
        self.assertGreater(s['mismatch_shrinkage_factor'],.15)
        self.assertEqual(s['mismatch_evidence']['n'],25)
        self.assertEqual(fit(rows,DAY,s),s)
        self.assertEqual(fit(self.traps(24),DAY)['mismatch_shrinkage_factor'],.15)
        self.assertEqual(fit(self.traps(y=1),DAY)['mismatch_shrinkage_factor'],.15)
        maxed=fit(rows,DAY,{'mismatch_shrinkage_factor':.349})
        self.assertEqual(maxed['mismatch_shrinkage_factor'],.35)

    def test_weight_reload_keeps_progress_and_detects_new_atomic_publication(self):
        with TemporaryDirectory() as temp,patch('adaptive_calibration.ROOT',Path(temp)):
            load_weights.cache_clear()
            s=fit(observations(24,wins=24),DAY)
            write_store(Path(temp)/WEIGHTS,s)
            self.assertEqual(len(load_weights()['cohorts']),1)
            self.assertEqual(calibration_status(as_of=AT)['warming_up_cohorts'],1)
            write_store(Path(temp)/WEIGHTS,fit(observations(50,wins=50),DAY))
            self.assertEqual(calibration_status(as_of=AT)['active_cohorts'],1)
            self.assertFalse(calibration_status(as_of='2026-11-10T12:00:00Z')['certified_current'])
            load_weights.cache_clear()

    def test_benchmark_pairs_identical_snapshots_deduplicates_and_reports_operations(self):
        record=frozen(odd=1.8)
        row=record['multimarket']['matrix']['rows'][0]
        row.update(pure_poisson={'win_weight':.4},statistical_pct=60,market_fair_pct=55,
                   opportunity={'eligible':True,'rank':1})
        close=deepcopy(record);close['captured_at']='2026-10-30T19:00:00Z'
        close['multimarket']['matrix']['rows'][0]['calibration_closes']={'7':['2026-10-30T18:59:00Z','2026-10-30T19:00:00Z',1.7]}
        missing=frozen('missing');missing['multimarket']['matrix']['rows'][0]['market_fair_pct']=None
        pending=frozen('pending');pending['settlement']={}
        store={'multimarket_evaluations':{'a':record,'b':record,'c':missing,'d':pending},'analysis_updates':{'a':close}}
        b=build_benchmark(store)
        self.assertEqual(b['matched_contracts'],1)
        self.assertEqual(b['excluded_incomplete_probabilities'],1)
        for group in b['levels'].values(): self.assertEqual(group['n'],1)
        self.assertAlmostEqual(b['levels']['poisson_base']['brier_score'],.36)
        self.assertAlmostEqual(b['levels']['calibrated_v51']['delta_brier_vs_base'],-.20)
        self.assertEqual(b['operational_selections']['gold_certificate']['band_1.70_1.85']['n'],1)
        self.assertEqual(b['by_odds_band']['1.70-2.09']['poisson_base']['n'],1)
        self.assertAlmostEqual(b['levels']['calibrated_v51']['clv_pct'],100*(1.8/1.7-1))
        self.assertEqual(build_benchmark(store),b)

    def test_suppressed_losses_and_forgone_wins_and_unpriced_studies(self):
        meta=frozen()['fixture']
        def rec(fid,y):
            return dict(fixture=dict(meta,fixture_id=fid),captured_at='2026-10-30T10:00:00Z',studio_ideas=[],
                suppressed_study_candidates=[dict(type='studio_value',market='1x2_1',spec=specification('result','ft','1'),
                    poisson_pct=40.,odd=8.,suppressed=True,suppression_reasons=['outsider_price_cap'])],
                settlement={'result':dict(status='FT',home_ft=y,away_ft=1-y,home_ht=0,away_ht=0)})
        b=build_benchmark({'study_observations':{'a':rec('1',0),'b':rec('2',1)}})
        g=b['operational_selections']['suppressed_traps']
        self.assertEqual((g['n'],g['saved_losses'],g['saved_loss_rate']),(2,1,.5))
        self.assertEqual(g['avoided_net_units'],-6.)
        self.assertEqual(g['roi_pct'],300.)

    def test_daily_and_cumulative_benchmarks_share_same_contracts(self):
        from daily_audit import build_daily,markdown
        from prematch_audit import build_report
        store={'multimarket_evaluations':{'a':frozen()}}
        daily=build_daily(store,'2026-10-30',AT)
        cumulative=build_report(store)
        self.assertEqual(daily['four_level_benchmark'],cumulative['four_level_benchmark'])
        self.assertIn('four_level_benchmark',markdown(daily))

    def test_weights_survive_private_checkpoint_round_trip(self):
        from gold_storage import collect,allowed
        with TemporaryDirectory() as temp:
            root=Path(temp)
            write_store(root/WEIGHTS,fit(observations(5),DAY))
            self.assertTrue(allowed(WEIGHTS))
            self.assertIn(WEIGHTS,collect(root))


class CheckpointAndFreezeRegression(unittest.TestCase):
    def test_private_roundtrip_keeps_newer_certified_git_weights(self):
        from test_gold_storage import Fake
        from gold_storage import publish, restore, GOLD
        with TemporaryDirectory() as source, TemporaryDirectory() as destination:
            a,b=Path(source),Path(destination)
            for name in GOLD: write_store(a/'data'/name,{'matches':[]})
            old=dict(fit(observations(5),DAY),trained_at='2026-10-31T01:00:00+01:00')
            new=dict(fit(observations(25),date(2026,11,1)),trained_at='2026-11-01T01:00:00+01:00')
            write_store(a/WEIGHTS,old)
            s3=Fake();publish(s3,'test',a)
            write_store(b/WEIGHTS,new)
            restore(s3,'test',b)
            from audit_storage import read_store
            self.assertEqual(read_store(b/WEIGHTS),new)
            # With no local publication, restore every warming-up metric verbatim.
            (b/WEIGHTS).unlink();restore(s3,'test',b)
            self.assertEqual(read_store(b/WEIGHTS),old)

    def test_no_new_trap_fixture_means_no_increment_when_window_rolls(self):
        rows=ValidationV51().traps(26)
        first=fit(rows,DAY)
        rolled=fit(rows[1:],date(2026,11,1),first)
        self.assertEqual(rolled['mismatch_shrinkage_factor'],first['mismatch_shrinkage_factor'])

    def test_reference_and_raw_baseline_survive_lambda_rebuild(self):
        from test_quote_sources import feed,bet,META
        from matrix_quotes import attach
        from statistics_first import statistical_analysis
        from test_statistics_first import match
        at='2026-11-01T08:00:00Z'; meta=dict(META,kickoff='2026-11-01T20:00:00Z')
        a=build_matrix(statistical_analysis(match(),at),at)
        # HT reference is deliberately outside the UI's core market catalogue.
        from adaptive_pricing import ht_reference
        a['matrix']['calibration_reference']=ht_reference(a,[],at,meta['kickoff'])
        for row in a['matrix']['rows']:
            if row['spec']['kind']=='result':
                row.update(quote_status='recent',provider_updated_at=at,selected_market_odds={'1':12.,'x':6.5,'2':1.26})
        apply_matrix(a,meta,at)
        ref=a['matrix']['calibration_reference']
        self.assertIn('pure_poisson',ref)
        self.assertGreater(ref['win_weight'],0)
        before=deepcopy(a)
        apply_matrix(a,meta,at)
        self.assertEqual(a,before)

if __name__=='__main__': unittest.main()
