import importlib.util
from datetime import datetime,timezone
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('factor_review',Path(__file__).parents[1]/'scripts/factor-combinations-review.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class Factors(unittest.TestCase):
    def setUp(self):
        self.at=datetime(2026,11,25,18,tzinfo=timezone.utc)
        self.rates=dict(home_ht=.6,away_ht=.7,home_ft=1.3,away_ft=1.4,home_st=.7,away_st=.7)
        self.key=m.market_id(m.specification('total','ht','over',1.5))
        self.row=dict(market=self.key,intensity=1.4,probability=.45,odd=2.5,ev=.1,ev_low=.03,
            price_identity=True,margin=6.,without_peak=True)

    def test_final_market_lambdas_cannot_fill_absent_native_statistical_lambdas(self):
        fixture=dict(measurement=dict(probability_origins=dict(final_v7=dict(rates=self.rates,observed_at=self.at.isoformat()))))
        self.assertEqual(m.rates_for(fixture,{},self.at)[0],{})
        fixture['measurement']['probability_origins']['pre_market_statistics']=dict(rates=self.rates,observed_at=self.at.isoformat())
        self.assertEqual(m.rates_for(fixture,{},self.at)[0],self.rates)

    def test_future_statistics_and_conflicting_origins_are_not_combined(self):
        native=dict(available=True,source='v7.statistics',rates=self.rates,observed_at='2026-11-26T01:00:00Z')
        self.assertEqual(m.rates_for({},dict(statistical_context=native),self.at)[0],{})
        fixture=dict(measurement=dict(probability_origins=dict(pre_market_statistics=dict(rates=self.rates,observed_at=self.at.isoformat()))))
        native=dict(native,observed_at=self.at.isoformat(),rates=dict(self.rates,home_ht=.9))
        self.assertEqual(m.rates_for(fixture,dict(statistical_context=native),self.at)[2],'conflicting_recorded_origins')

    def test_missing_zero_and_failed_conditions_remain_distinct(self):
        self.assertIsNone(m.passes(dict(self.row,intensity=None),'quattro_fattori'))
        self.assertFalse(m.passes(dict(self.row,margin=None),'quattro_fattori'))
        self.assertTrue(m.passes(dict(self.row,margin=0),'quattro_fattori'))
        key=m.market_id(m.specification('total','ht','under',1.5))
        self.assertTrue(m.passes(dict(self.row,market=key,intensity=0,probability=.8),'lambda'))
        key=m.market_id(m.specification('total','sh','over',1.5))
        self.assertIsNone(m.passes(dict(self.row,market=key),'prudente_senza_picco'))

    def test_fixed_choice_requires_original_capture_probability_and_snapshot_hash(self):
        base=dict(market_id=self.key,probability=.45,odd=2.5,captured_at=self.at.isoformat(),forecast_sha256='sealed')
        f=dict(forecast_sha256='sealed',triad=dict(recommended_alternative=base))
        row=dict(probability=.45,odd=2.5)
        self.assertEqual(m.selected_at_snapshot(f,{},self.key,row,self.at)[1],['recommended_alternative'])
        for changes in [dict(probability=None),dict(forecast_sha256='other'),dict(captured_at='2026-11-25T17:00:00Z')]:
            f['triad']['recommended_alternative']=dict(base,**changes)
            self.assertEqual(m.selected_at_snapshot(f,{},self.key,row,self.at)[1],[])

    def test_goal_lambdas_use_period_and_both_teams_for_btts(self):
        self.assertAlmostEqual(m.intensity(self.rates,m.specification('total','ht','over',1.5)),1.3)
        self.assertAlmostEqual(m.intensity(self.rates,m.specification('total','sh','over',1.5)),1.4)
        self.assertEqual(m.intensity(self.rates,m.specification('btts','ft','yes')),1.3)

    def test_missing_results_never_become_losses(self):
        rows=[dict(fixture_id='7',date='2026-11-25',outcome=None,selected=True,strict_fixed=False),
              dict(fixture_id='8',date='2026-11-25',outcome=1,selected=True,strict_fixed=False,probability=.4)]
        result=m.metrics(rows)
        self.assertEqual((result['n'],result['wins'],result['pending']),(1,1,1))
        self.assertEqual(result['mean_p'],.4)

    def test_quote_floor_does_not_confuse_frequent_low_price_unders_with_candidates(self):
        key=m.market_id(m.specification('total','ht','under',1.5))
        row=dict(self.row,market=key,intensity=1.,probability=.8,odd=1.3)
        self.assertTrue(m.passes(row,'quattro_fattori'))
        self.assertFalse(m.passes(row,'quattro_fattori_quota150'))

    def test_event_complements_do_not_claim_same_operator_price_evidence(self):
        base=dict(fixture_id='7',date='2026-11-25',config='frozen',selected=False,
            strict_fixed=False,quote_observed_at=self.at.isoformat(),price_identity=True,
            fair=.5,odd=1.9,ev=-.05)
        over=dict(base,market=self.key,probability=.4,outcome=0,bookmaker=1,selected=True)
        under=dict(base,market=m.market_id(m.specification('total','ht','under',1.5)),
            probability=.6,outcome=1,bookmaker=2)
        report=m.complementary_pairs([over,under])
        self.assertEqual(report['cells'],[])
        self.assertEqual(report['event_complements'][0]['alternative_under_when_over_selected']['wins'],1)
        self.assertEqual(report['excluded']['different_or_missing_operator'],1)
        under['bookmaker']='1'
        self.assertEqual(len(m.complementary_pairs([over,under])['cells']),1)
        under['outcome']=0
        with self.assertRaises(ValueError):m.complementary_pairs([over,under])


if __name__=='__main__':unittest.main()
