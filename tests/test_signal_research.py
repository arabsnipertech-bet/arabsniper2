import copy, importlib.util, json, sys, tempfile, unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from signal_research import capture,favourite,team_history,settle,build_report,event_results,COLLECTION
from multimarket_model import evaluate
from test_multimarket import match
NOW='2026-10-20T10:00:00+02:00'
START='2026-10-20T20:00:00+02:00'
def quotes(current=1.56):
    return {'1x2_ft':[{'bookmaker_id':str(i),'bookmaker':f'Book {i}','bet_id':1,'line':None,
        'observed_at':NOW,'provider_updated_at':NOW,'odds':{'1':current,'x':4,'2':5},
        'movement':{'first_observed_at':'2026-10-19T10:00:00+02:00',
        'first_odds':{'1':1.6,'x':4,'2':5}}} for i in (1,2)]}
def profile():
    return {'last_matches':[{'fixture_id':i,'league_id':7,'date':f'2026-10-{19-i:02d}',
        'research_ft':{'scored':0 if i<2 else 2,'conceded':1}} for i in range(8)]}
def sample(current=1.56):
    m=match();m['fixture'].update(kickoff=START,league_id=7)
    m['statistics']={s:profile() for s in ('home','away')}
    m['multimarket']={'model_id':'coherent-markets-v1','quotes':quotes(current),'markets':[]}
    return {'days':[{'matches':[m]}]}
class SignalTests(unittest.TestCase):
    def test_boundaries_and_same_operator(self):
        for price,expected in [(1.568,'drop'),(1.632,'rise'),(1.59,'stable')]:
            self.assertEqual(favourite(quotes(price),NOW,START)['group'],expected)
        q=quotes();q['1x2_ft'][1]['movement']['first_observed_at']=NOW
        self.assertEqual(favourite(q,NOW,START)['status'],'unavailable')
        q=quotes();q['1x2_ft'][1]['bookmaker_id']='1'
        self.assertEqual(favourite(q,NOW,START)['reason'],'duplicate_operator')
    def test_future_stale_and_incomplete_quotes(self):
        for field,value in [('observed_at','2026-10-20T11:00:00+02:00'),('provider_updated_at','2026-10-19T10:00:00+02:00'),('provider_updated_at',None)]:
            q=quotes();q['1x2_ft'][0][field]=value
            self.assertEqual(favourite(q,NOW,START)['status'],'unavailable')
        q=quotes();del q['1x2_ft'][0]['odds']['x']
        self.assertEqual(favourite(q,NOW,START)['status'],'unavailable')
    def test_history_prior_league_verified_and_deduplicated(self):
        p=profile();p['last_matches'].append({'fixture_id':99,'league_id':7,'date':'2026-10-20','research_ft':{'scored':20,'conceded':20}})
        x=team_history(p,7,NOW,START)
        self.assertEqual(x['mean_scored'],1.5);self.assertTrue(x['two_without_scoring'])
        self.assertFalse(x['two_without_conceding'])
        p['last_matches'][0]['league_id']=9
        self.assertEqual(team_history(p,7,NOW,START)['status'],'unavailable')
        p=profile();del p['last_matches'][0]['research_ft']
        self.assertEqual(team_history(p,7,NOW,START)['reason'],'unverified_regulation_scores')
        p=profile();p['last_matches'][1]=copy.deepcopy(p['last_matches'][0])
        self.assertEqual(team_history(p,7,NOW,START)['n'],7)
    def test_immutable_and_no_live_capture(self):
        store={};o=sample();capture(store,o,NOW);original=copy.deepcopy(store)
        o['days'][0]['matches'][0]['multimarket']['quotes']=quotes(1.9)
        capture(store,o,NOW);self.assertEqual(store,original)
        capture(store,o,START);self.assertEqual(store,original)
    def test_first_usable_not_best_no_repeat_count(self):
        store={};capture(store,sample(1.6),NOW)
        capture(store,sample(1.5),'2026-10-20T11:00:00+02:00');before=copy.deepcopy(store)
        settle(store,{'1':{'status':'FT','home_ht':1,'away_ht':0,'home_ft':2,'away_ft':1}})
        r=build_report(store)
        self.assertEqual(r['coverage'],[{'period':'prospective','group':'stable','fixtures':1}])
        row=next(x for x in r['groups'] if x['family']=='favourite' and x['event']=='over0.5_ht')
        self.assertEqual((row['n'],row['wins']),(1,1))
        for k,v in store[COLLECTION].items():
            self.assertEqual({a:b for a,b in v.items() if a!='settlement'},before[COLLECTION][k])
    def test_missing_pair_can_gain_evidence(self):
        store={};o=sample();o['days'][0]['matches'][0]['multimarket']['quotes']={}
        capture(store,o,NOW);later='2026-10-20T11:00:00+02:00';capture(store,sample(),later)
        r=build_report(store)['fixtures'][0]
        self.assertEqual(r['captured_at'],later);self.assertEqual(r['team_baseline_at'],NOW)
    def test_missing_outcomes_and_combinations(self):
        r={'status':'FT','home_ht':None,'away_ht':None,'home_ft':3,'away_ft':1}
        self.assertIsNone(event_results(r)['over0.5_ht']);self.assertTrue(event_results(r)['over3.5_ft'])
        r.update(home_ht=1,away_ht=1);self.assertTrue(event_results(r)['over1.5_ht_over1.5_sh'])
        r['home_ht']=4;self.assertIsNone(event_results(r)['over0.5_ht_over0.5_sh'])
    def test_no_model_probability_changes(self):
        m=match();a=evaluate(m,NOW);m['statistics']['home'].update(profile());b=evaluate(m,NOW)
        self.assertEqual(a['rates'],b['rates']);self.assertEqual(a['markets'],b['markets'])
    def test_daily_archive_settlement_roundtrip(self):
        spec=importlib.util.spec_from_file_location('signal_auditor',ROOT/'auditor/current_audit.py')
        audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)
        store={'fixtures':{}};capture(store,sample(),NOW)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'data').mkdir();(root/'data/arab_quote_prematch_audit.json').write_text(json.dumps(store))
            audit.archive(root,datetime.fromisoformat(NOW))
            path=root/'auditordata/prematch_2026-10-20.json';before=path.read_text()
            result={'days':[{'matches':[{'fixture':{'fixture_id':'1','result':{'status':'FT','home_ht':1,'away_ht':0,'home_ft':2,'away_ft':0}}}]}]}
            with patch.dict('os.environ',{'API_SPORTS_KEY':'test-only'}):
                audit.settle(root,datetime.fromisoformat('2026-10-21T10:00:00+02:00'),lambda *args:result)
            report=json.loads((root/'auditarchive/prematch_last_report.json').read_text())
            self.assertTrue(report['signal_research']['fixtures'][0]['outcomes']['over0.5_ht'])
            self.assertEqual(path.read_text(),before)
if __name__=='__main__':unittest.main()
