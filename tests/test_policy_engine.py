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
    def test_complete_scan_keeps_quote_history_and_runs_v5(self):
        from unittest.mock import patch
        from test_statistics_first import NOW, KICKOFF
        from contextlib import ExitStack
        def fixture(fid, date, home=10, away=20, status='FT'):
            return {'fixture':{'id':fid,'date':date,'status':{'short':status}},
                'league':{'id':135,'season':2026,'name':'Serie A','country':'Italy','type':'League'},
                'teams':{'home':{'id':home,'name':'Home'},'away':{'id':away,'name':'Away'}},
                'goals':{'home':3,'away':2},'score':{'fulltime':{'home':3,'away':2},'halftime':{'home':1,'away':1}}}
        class Client:
            last_error=None
            request_count=0
            odds_enricher=None
            def get(self, endpoint, params, **kwargs):
                if endpoint=='fixtures' and params.get('date'):
                    return {'response':[fixture(100,KICKOFF,status='NS'),
                                        fixture(101,'2026-10-26T05:00:00Z',status='FT')]}
                if endpoint=='fixtures':
                    team=params.get('team',10)
                    return {'response':[fixture(team*100+i,f'2026-10-{25-i%8:02}T07:00:00Z',team if i%2==0 else 999,999 if i%2==0 else team) for i in range(8 if 'team' in params else 30)]}
                return {'response':[]}
        state={'version':'1.4.0','fixtures':{},'quote_ledger':{'sentinel':'keep'}}
        written={}
        with ExitStack() as stack:
            for name, value in [('iso_now',lambda:NOW),('_existing_day_payload',lambda *a:{}),
                ('read_json',lambda path,default=None:state if path==engine.STATE_FILE else default),
                ('atomic_write_json',lambda path,payload:written.update({str(path):payload})),
                ('save_xg_cache',lambda:None),('enrich_team_matches_with_xg',lambda c,r,t:r)]:
                stack.enter_context(patch.object(engine,name,value))
            stack.enter_context(patch('scan_audit.update_audit',return_value={'cohorts':[],'status':'experimental'}))
            output=engine.scan_dates(Client(),['2026-10-26'],[])
        self.assertEqual(output['days'][0]['fresh_matches'],1)
        self.assertEqual(output['days'][0]['matches'][0]['multimarket']['model_id'],'lateral-matrix-v5.0')
        self.assertEqual(len(output['days'][0]['matches']),2)
        started=next(m for m in output['days'][0]['matches'] if m['fixture']['fixture_id']=='101')
        self.assertEqual(started['multimarket']['status'],'no_prematch_snapshot')
        self.assertEqual(started['multimarket']['matrix']['rows'],[])
        self.assertIsNone(started['display']['primary_choice'])
        self.assertEqual(state['quote_ledger'],{'sentinel':'keep'})
        self.assertIn('selection_board',output['days'][0])
        self.assertEqual(output['method']['active_analysis']['book_weight_in_ranking'],0)
        self.assertNotIn('ft_bridge',output['method'])

    def test_kickoff_during_scan_preserves_already_published_forecast(self):
        import tempfile
        from copy import deepcopy
        from unittest.mock import patch
        from policy_integration import install
        from statistics_first import build_selection_board
        from test_statistics_first import NOW, evaluated
        old={'date':'2026-10-26','matches':[evaluated()]}
        old['matches'][0]['fixture']['kickoff']='2026-10-26T09:00:00Z'
        build_selection_board(old,NOW)
        after=deepcopy(old)
        after['day']=1
        for row in after['matches'][0]['multimarket']['markets']:row['published_selection']=False
        with tempfile.TemporaryDirectory() as folder:
            ns=dict(engine.__dict__)
            ns.update(DATA_DIR=Path(folder), OUTPUT_FILE=Path(folder)/'out.json',
                iso_now=lambda:'2026-10-26T10:00:00Z', read_json=lambda *a:old,
                _scan_dates_before_prematch_audit=lambda *a:{'days':[after]})
            install(ns)
            with patch('scan_audit.update_audit',return_value={'cohorts':[],'status':'experimental'}):
                out=ns['scan_dates'](None,['2026-10-26'],[])
        self.assertEqual(out['days'][0]['matches'][0]['multimarket'],old['matches'][0]['multimarket'])
        self.assertEqual(out['days'][0]['selection_board']['markets']['over25_ft']['started_selections'],1)

    def test_active_engine_and_independent_statistics(self):
        from statistics_first import CONFIG
        from lateral_matrix import MODEL_ID
        self.assertEqual(engine.ACTIVE_ANALYSIS_ENGINE, MODEL_ID)
        self.assertNotIn('Egypt', engine.DEFAULT_EXCLUDED_COUNTRIES)
        self.assertEqual(CONFIG['book_weight_in_statistics'], 0)
        self.assertEqual(CONFIG['drought_probability_bonus'], 0)

    def test_real_fixture_pipeline_collects_statistics_before_odds(self):
        from unittest.mock import patch
        from statistics_first import statistical_analysis
        from lateral_matrix import MODEL_ID
        from test_statistics_first import NOW, KICKOFF
        calls=[]
        def raw(fid, home, away, date, status='FT'):
            return {'fixture':{'id':fid,'date':date,'status':{'short':status}},
                    'league':{'id':135,'season':2026,'name':'Serie A','country':'Italy','type':'League'},
                    'teams':{'home':{'id':home,'name':'Home'},'away':{'id':away,'name':'Away'}},
                    'goals':{'home':3,'away':2},
                    'score':{'fulltime':{'home':3,'away':2},'halftime':{'home':1,'away':1}}}
        upcoming=raw(100,10,20,KICKOFF,'NS')
        class Client:
            odds_enricher=None
            def get(self, endpoint, params, **kwargs):
                calls.append(endpoint)
                if endpoint=='odds':
                    self.assert_statistics_called()
                    return {'response':[]}
                if endpoint=='fixtures/headtohead':return {'response':[]}
                team=params.get('team',10)
                return {'response':[raw(team*100+i,team if i%2==0 else 999,999 if i%2==0 else team,
                      f'2026-10-{25-i%8:02}T07:00:00+00:00') for i in range(8 if 'team' in params else 30)]}
            def assert_statistics_called(self):
                if 'statistical_analysis' not in calls:raise AssertionError('odds requested before statistics')
        def analyze(*args):calls.append('statistical_analysis');return statistical_analysis(*args)
        with patch.object(engine,'iso_now',return_value=NOW), patch.object(engine,'enrich_team_matches_with_xg',side_effect=lambda c,r,t:r), patch('statistics_integration.statistical_analysis',side_effect=analyze), patch.object(engine,'build_lambdas',side_effect=AssertionError('legacy prediction path')), patch.object(engine,'extract_bookmaker_markets_with_audit',side_effect=AssertionError('duplicate quote parsing')), patch('statistics_integration.collect_quotes',side_effect=AssertionError('duplicate OCI path')):
            value=engine.build_fixture_output(Client(),upcoming,{}, {})
        self.assertEqual(value['multimarket']['model_id'],MODEL_ID)
        self.assertEqual(value['multimarket']['data_quality']['home']['sample'],8)
        self.assertEqual(value['multimarket']['data_quality']['away']['ht_sample'],4)
        self.assertEqual(value['model']['probabilities_raw'],value['model']['probabilities_calibrated'])
        self.assertFalse(value['model']['calibration_applied'])
        self.assertIsNone(value['multimarket']['markets'][0]['odd'])
        self.assertLess(calls.index('statistical_analysis'),calls.index('odds'))
        self.assertAlmostEqual(value['model']['lambdas']['total_ft'],value['model']['lambdas']['home_ft']+value['model']['lambdas']['away_ft'])

    def test_scan_publishes_board_before_single_audit_pass(self):
        import tempfile, json
        from unittest.mock import patch
        from policy_integration import install
        from test_statistics_first import NOW, evaluated
        from lateral_matrix import build_matrix
        matches=[evaluated(str(i)) for i in range(7)]
        for m in matches: m['multimarket']=build_matrix(m['multimarket'],NOW)
        payload={'days':[{'day':1,'date':'2026-10-26','matches':matches}]}
        observed=[]
        def audit(ns, output, client):
            observed.append(len(output['days'][0]['matches']))
            assert output['days'][0]['selection_board']['max_per_market'] is None
            assert all(len(m['multimarket']['matrix']['rows'])==55 for m in output['days'][0]['matches'])
            return {'cohorts':[], 'status':'experimental'}
        with tempfile.TemporaryDirectory() as folder:
            ns=dict(engine.__dict__)
            ns.update(DATA_DIR=Path(folder), OUTPUT_FILE=Path(folder)/'dashboard.json',
                      iso_now=lambda:NOW, _scan_dates_before_prematch_audit=lambda *args:payload)
            install(ns)
            with patch('scan_audit.update_audit',side_effect=audit):
                value=ns['scan_dates'](None,[],[])
            self.assertEqual(observed,[7])
            day=json.loads((Path(folder)/'arab_quote_day1.json').read_text())
            self.assertEqual(len(day['matches']),7)
            self.assertIsNone(day['selection_board']['max_per_market'])
            self.assertEqual(day['selection_board']['book_weight_in_ranking'],0)
            self.assertEqual(value['method']['active_analysis']['model_id'],'lateral-matrix-v5.0')


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
