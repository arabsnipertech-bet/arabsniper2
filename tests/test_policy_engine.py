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
