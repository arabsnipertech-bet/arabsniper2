import sys
import importlib.util
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
spec=importlib.util.spec_from_file_location('engine_policy_test',Path(__file__).resolve().parents[1]/'src'/'3appdays.py')
engine=importlib.util.module_from_spec(spec)
sys.modules[spec.name]=engine
spec.loader.exec_module(engine)

class EngineTests(unittest.TestCase):
    def gate(self,p=.4,reliability=60,odd=3,valid=True):
        return engine.recommendation_gate('over15_ht','over',{'over':p},{'over':p},
            {'current':{'odds':{'over':odd}}},{'usable_for_signals':valid,'bookmakers_found':3},reliability,
            {'competition_quality_score':64},'contrast',{})
    def test_threshold_edges_and_recovery(self):
        self.assertTrue(self.gate()['eligible'])
        self.assertTrue(self.gate()['recovered'])
        self.assertFalse(self.gate(p=.399)['eligible'])
        self.assertFalse(self.gate(reliability=59.9)['eligible'])
        self.assertFalse(self.gate(odd=2.5)['eligible'])
        self.assertFalse(self.gate(valid=False)['eligible'])
    def test_weight_and_egypt(self):
        self.assertNotIn('Egypt',engine.DEFAULT_EXCLUDED_COUNTRIES)
        args={'current':{'bookmakers_count':10},'book':{'strength':'strong'},'aggio':{'status':'normal'}}
        self.assertEqual(engine.market_calibration_weight(args,{'usable_for_signals':True}),.18)
        self.assertEqual(engine.market_calibration_weight(args,{'usable_for_signals':False}),0)
    def test_lambda_bonus_keeps_baseline(self):
        profile=engine.summarize_team_matches([], 'home')
        profile['goal_drought']={'all':{'bonus_relative':.04}}
        value=engine.build_lambdas(profile,profile,{}, {})
        baseline=value['drought_layer']['baseline_lambdas']
        self.assertAlmostEqual(value['home_ht'],baseline['home_ht']*1.04,places=2)
        self.assertAlmostEqual(value['total_ft'],value['home_ft']+value['away_ft'],places=2)

class PublicationTests(unittest.TestCase):
    def test_fixture_convergence_is_distinct_from_value_and_requires_ht_history(self):
        from copy import deepcopy
        from policy_integration import install
        market = {'selections':{'over':{'book_fair_probability_pct':50}},
                  'empirical_layer':{'poisson_positive':.5,'empirical_positive':.5},
                  'market_validation':{'usable_for_signals':True}}
        payload = {'fixture':{'fixture_id':'q'},
            'model':{'probabilities_raw':{'over15_ht':{'over':.5}},
                     'probabilities_calibrated':{'over15_ht':{'over':.5}},'reliability_pct':85},
            'book':{'markets':{'over15_ht':{'current':{'odds':{'over':1.8}}}},
                    'market_validation':{'over15_ht':{'usable_for_signals':True}}},
            'display':{'markets':{'over15_ht':market}},
            'statistics':{'home':{'ht_sample_all':8},'away':{'ht_sample_all':8}}}
        ns=dict(engine.__dict__)
        ns['build_fixture_output']=lambda *args:deepcopy(payload)
        install(ns)
        result=ns['build_fixture_output'](None,None,None,None)
        signal=result['display']['first_half_assessment']
        self.assertTrue(signal['eligible'])
        self.assertFalse(signal['value_eligible'])
        self.assertEqual(signal['expected_value_pct'],-10)
        payload['statistics']['away']['ht_sample_all']=0
        self.assertFalse(ns['build_fixture_output'](None,None,None,None)['display']['first_half_assessment']['eligible'])

    def test_uncapped_rankings_written_to_local_day_file(self):
        import tempfile
        import json
        from datetime import datetime, timezone, timedelta
        from policy_integration import install
        with tempfile.TemporaryDirectory() as folder:
            now=datetime.now(timezone.utc)
            rows=[]
            for i in range(7):
                rows.append({'fixture':{'fixture_id':str(i),'kickoff':(now+timedelta(hours=2)).isoformat()},
                    'display':{'market_candidates':[{'market':'over15_ht','fixture_id':str(i),'gate':{'eligible':True},'ranking_score':i,'probability_pct':45}]}})
            payload={'days':[{'day':1,'date':now.date().isoformat(),'matches':rows}]}
            kickoff=(now+timedelta(hours=1)).isoformat()
            rows.append({'fixture':{'fixture_id':'probability_only','kickoff':kickoff},
                'display':{'market_candidates':[{'market':'over15_ht','fixture_id':'probability_only',
                    'kickoff':kickoff,'gate':{'eligible':False},'ranking_score':0,'probability_pct':45,
                    'probability_assessment':{'eligible':True,'value_eligible':False}}]}})
            ns=dict(engine.__dict__)
            ns['DATA_DIR']=Path(folder)
            ns['PREMATCH_AUDIT_FILE']=Path(folder)/'frozen.json'
            ns['OUTPUT_FILE']=Path(folder)/'dashboard.json'
            ns['scan_dates']=lambda *args:payload
            install(ns)
            result=ns['scan_dates'](None,[],[])
            ranks=result['days'][0]['market_rankings']['over15_ht']
            self.assertEqual(len(ranks),7)
            self.assertEqual(ranks[0]['fixture_id'],'6')
            day=json.loads((Path(folder)/'arab_quote_day1.json').read_text())
            self.assertEqual(len(day['market_rankings']['over15_ht']),7)
            self.assertEqual(day['probability_rankings']['over15_ht'][0]['fixture_id'],'probability_only')
            self.assertFalse(day['probability_rankings']['over15_ht'][0]['gate']['eligible'])


class FirstHalfHistoryTests(unittest.TestCase):
    def fixture(self, halftime=None):
        return {'fixture': {'id': 1, 'date': '2026-10-17T15:00:00+02:00',
                            'status': {'short': 'FT'}},
                'teams': {'home': {'id': 10}, 'away': {'id': 20}},
                'goals': {'home': 3, 'away': 1},
                'score': {'halftime': halftime or {}}}

    def test_missing_half_is_unknown_and_excluded_from_ht_denominator(self):
        unknown = engine._match_row(self.fixture(), 10)
        valid = engine._match_row(self.fixture({'home': 2, 'away': 0}), 10)
        zero = engine._match_row(self.fixture({'home': 0, 'away': 0}), 10)
        self.assertIsNone(unknown['total_ht'])
        self.assertIsNone(unknown['team_goals_2h'])
        self.assertFalse(unknown['halftime_available'])
        profile = engine.summarize_team_matches([unknown, valid, zero], 'home')
        self.assertEqual(profile['sample_all'], 3)
        self.assertEqual(profile['ht_sample_all'], 2)
        self.assertEqual(profile['ht_sample_context'], 2)
        self.assertEqual(profile['ht_missing_count'], 1)
        self.assertAlmostEqual(profile['metrics']['over15ht_rate'], .5)
        self.assertAlmostEqual(profile['metrics']['goals_scored_ht'], 1)
        self.assertEqual(profile['metrics']['goals_scored_ft'], 3)

    def test_league_and_h2h_use_only_available_ht_results(self):
        rows = [self.fixture(), self.fixture({'home': 2, 'away': 0})]
        class Client:
            def get(self, *args, **kwargs):
                return {'response': rows}
        league = engine.get_league_baseline(Client(), 1, 2026)
        self.assertEqual(league['sample'], 2)
        self.assertEqual(league['ht_sample'], 1)
        self.assertEqual(league['avg_total_ht'], 2)
        self.assertEqual(league['over15ht_rate'], 1)
        h2h = engine.summarize_h2h([engine._match_row(r) for r in rows])
        self.assertEqual(h2h['ht_sample'], 1)
        self.assertEqual(h2h['over15ht_rate'], 1)
        self.assertEqual(h2h['avg_total_ft'], 4)

    def test_absent_ht_does_not_supply_empirical_zero_or_large_sample(self):
        team = engine.summarize_team_matches([engine._match_row(self.fixture(), 10)], 'home')
        self.assertEqual(team['ht_sample_context'], 0)
        self.assertIsNone(team['metrics']['goals_scored_ht'])
        p = engine._binary_empirical(team, team,
                {'ht_sample': 40, 'over15ht_rate': .35}, {'ht_sample': 0}, 'over15ht_rate')
        self.assertAlmostEqual(p, .35)
