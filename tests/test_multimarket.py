import copy
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from multimarket_model import (COMBO, MODEL_ID, collect_quotes, update_ledger,
    quote_benchmarks, goal_probabilities, evaluate, freeze_evaluations)
from multimarket_audit import outcome, build_report
from prematch_audit import settle_predictions

NOW='2026-10-19T10:00:00+02:00'
KICKOFF='2026-10-20T15:00:00+02:00'


def payload():
    return {'response':[{'fixture':{'id':1,'date':KICKOFF},'update':NOW,
        'bookmakers':[{'id':bid,'name':f'Book {bid}','bets':[
            {'id':5,'name':'Goals Over/Under','values':[{'value':'Over 3.5','odd':3.1},{'value':'Under 3.5','odd':1.4}]},
            {'id':16,'name':'Total - Home','values':[{'value':'Over 1.5','odd':2.4},{'value':'Under 1.5','odd':1.6}]}
        ]} for bid in (1,2)]}]}


def match():
    return {'fixture':{'fixture_id':'1','kickoff':KICKOFF,'match':'Home - Away'},
            'model':{'reliability_pct':85,'lambdas':{
                'home_ft':1.5,'away_ft':1.3,'home_ht':.65,'away_ht':.55}},
            'statistics':{s:{'sample_all':12,'ht_sample_all':10} for s in ('home','away')}}


class ModelTests(unittest.TestCase):
    def test_research_does_not_use_post_kickoff_or_future_inputs(self):
        from model_research import research
        record={'input':match(),'captured_at':NOW,'statistics_at':NOW,
                'result':{'status':'FT','home_ht':1,'away_ht':0,'home_ft':2,'away_ft':0}}
        result=research({'records':[record]})
        self.assertEqual(result['summary']['fixtures'],1)
        self.assertTrue(all(c['period']=='retrospective' for c in result['cohorts']))
        record['statistics_at']='2026-10-21T10:00:00+02:00'
        self.assertEqual(research({'records':[record]})['summary']['fixtures'],0)

    def test_stale_quote_and_incoherent_lambda_are_not_candidates(self):
        m=match();quotes=update_ledger({},'1',collect_quotes(payload(),NOW)['1'],NOW,KICKOFF)
        m['model']['lambdas']['home_ht']=3
        result=evaluate(m,'2026-10-19T23:00:00+02:00',quotes)
        self.assertIsNone(result['best_candidate'])
        priced=next(r for r in result['markets'] if r['odd'])
        self.assertIn('lambda_tempi_incoerenti',priced['reasons'])
        self.assertIn('aggiornamento_quota_non_verificato',priced['reasons'])

    def test_joint_model_coherence_and_analytic_probability(self):
        for rates in ([.6,.4,.9,.7],[.01,.01,.01,.01],[4,3,2,2]):
            p=goal_probabilities(rates)
            for market in p.values():
                self.assertAlmostEqual(sum(market.values()),1,places=10)
                self.assertTrue(all(0<=v<=1 for v in market.values()))
            self.assertLessEqual(p['gg_ht']['yes'],p['over15_ht']['over'])
            self.assertLessEqual(p['over15_ht']['over'],p['over05_ht']['over'])
            self.assertLessEqual(p['over35_ft']['over'],p['over25_ft']['over'])
            self.assertLessEqual(p[COMBO]['yes'],p['over15_ht']['over'])
            self.assertAlmostEqual(p[COMBO]['yes'],p['over15_ht']['over']*p['over05_sh']['over'])
            # 1X2 symmetry when team intensities agree.
        p=goal_probabilities([.5,.5,1,1])
        self.assertAlmostEqual(p['1x2_ft']['1'],p['1x2_ft']['2'],places=10)

    def test_strict_market_id_line_and_operator(self):
        result=collect_quotes(payload(),NOW)['1']
        self.assertEqual(set(result),{'over35_ft','home15_ft'})
        bad=payload();bad['response'][0]['bookmakers'][0]['bets'][0]['id']=45
        self.assertEqual(len(collect_quotes(bad,NOW)['1']['over35_ft']),1)
        bad=payload();bad['response'][0]['bookmakers'][0]['bets'][0]['values'][1]['value']='Under 2.5'
        self.assertEqual(len(collect_quotes(bad,NOW)['1']['over35_ft']),1)
        bad=payload();bad['response'][0]['update']='2026-10-19T11:00:00+02:00'
        self.assertEqual(collect_quotes(bad,NOW),{})

    def test_changes_pair_same_book_not_changing_book_mix(self):
        state={};quotes=collect_quotes(payload(),NOW)['1']
        first=update_ledger(state,'1',quotes,NOW,KICKOFF)
        self.assertEqual(first['over35_ft'][0]['movement']['observations'],1)
        p=payload();p['response'][0]['bookmakers'][0]['id']=3
        p['response'][0]['bookmakers'][1]['bets'][0]['values'][0]['odd']=2.8
        p['response'][0]['update']='2026-10-19T11:00:00+02:00'
        later=update_ledger(state,'1',collect_quotes(p,p['response'][0]['update'])['1'],p['response'][0]['update'],KICKOFF)
        byid={b['bookmaker_id']:b for b in later['over35_ft']}
        self.assertEqual(byid['3']['movement']['observations'],1)
        self.assertEqual(byid['2']['movement']['observations'],2)
        self.assertGreater(byid['2']['movement']['drop_pct']['over'],0)

    def test_unpriced_joint_has_no_ev_and_no_recommendation(self):
        m=match();quotes=update_ledger({},'1',collect_quotes(payload(),NOW)['1'],NOW,KICKOFF)
        assessment=evaluate(m,NOW,quotes)
        combo=next(v for v in assessment['markets'] if v['market']==COMBO and v['selection']=='yes')
        self.assertIsNone(combo['odd']);self.assertIsNone(combo['expected_value_pct'])
        self.assertFalse(combo['research_candidate'])
        priced=next(v for v in assessment['markets'] if v['market']=='over35_ft' and v['selection']=='over')
        self.assertEqual(priced['odd'],3.1);self.assertEqual(priced['quote']['bookmaker_id'],'1')
        self.assertIsNotNone(priced['book_probability_pct'])

    def test_drought_does_not_automatically_boost_new_model(self):
        m=match();base=copy.deepcopy(m['model']['lambdas'])
        a=evaluate(m,NOW)
        m['model']['lambdas']['home_ft']=4
        m['model']['lambdas']['drought_layer']={'baseline_lambdas':base}
        self.assertEqual(a['rates'],evaluate(m,NOW)['rates'])

    def test_capture_immutable_versions_and_no_future_results(self):
        m=match();m['multimarket']=evaluate(m,NOW)
        store={};output={'days':[{'matches':[m]}]}
        freeze_evaluations(store,output,NOW)
        original=copy.deepcopy(store)
        m['multimarket']['markets'][0]['probability_pct']=99
        freeze_evaluations(store,output,NOW)
        self.assertEqual(store,original)
        freeze_evaluations(store,output,'2026-10-21T10:00:00+02:00')
        self.assertEqual(store,original)
        self.assertEqual(build_report(store)['cohorts'][0]['period'],'prospective')
        store['multimarket_evaluations'][f'{MODEL_ID}::1']['retrospective']=True
        self.assertEqual(build_report(store)['cohorts'][0]['period'],'retrospective')

    def test_settlement_missing_periods_and_regulation_time(self):
        result={'status':'AET','home_ht':1,'away_ht':1,'home_ft':1,'away_ft':1}
        self.assertFalse(outcome('over25_ft','over',result))
        self.assertFalse(outcome(COMBO,'yes',result))
        self.assertTrue(outcome('gg_ht','yes',result))
        result['home_ht']=None
        self.assertIsNone(outcome(COMBO,'yes',result))
        self.assertFalse(outcome('over25_ft','over',result))

    def test_matched_cohort_and_rejected_candidates_are_kept(self):
        m=match();q=update_ledger({},'1',collect_quotes(payload(),NOW)['1'],NOW,KICKOFF)
        m['multimarket']=evaluate(m,NOW,q);store={}
        freeze_evaluations(store,{'days':[{'matches':[m]}]},NOW)
        m['fixture']['result']={'status':'FT','home_ht':1,'away_ht':1,'home_ft':2,'away_ft':2}
        before=copy.deepcopy(next(iter(store['multimarket_evaluations'].values()))['multimarket'])
        settle_predictions(store,{'days':[{'matches':[m]}]})
        self.assertEqual(next(iter(store['multimarket_evaluations'].values()))['multimarket'],before)
        report=build_report(store)
        rows=[r for r in report['groups'] if r['market_selection']=='over35_ft::over' and r['group']=='matched_benchmark']
        self.assertEqual(len(rows),4);self.assertEqual({r['n'] for r in rows},{1})
        self.assertTrue(any(r['group']=='rejected' for r in report['groups']))


if __name__=='__main__': unittest.main()
