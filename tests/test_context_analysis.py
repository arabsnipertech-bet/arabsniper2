import sys
import unittest
from copy import deepcopy
from pathlib import Path
from datetime import datetime, timedelta, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from context_analysis import select_history,profile,correction,evaluate,movement,favorite_path
from analysis_history import freeze_update,latest_store
from multimarket_model import fair
from multimarket_audit import settle,build_report

NOW='2026-10-20T08:00:00+00:00'
def history(side='home'):
    return [{'fixture_id':i,'kickoff':f'2026-10-{19-i:02d}T10:00:00+00:00','season':2026,'league':'Serie A',
        'side':side if i%2==0 else ('away' if side=='home' else 'home'),
        'research_ft':{'scored':1+i%3,'conceded':i%2},'halftime_available':True,
        'team_goals_ht':i%2,'team_conceded_ht':(i+1)%2,'team_xg_for':1.5,'team_xg_against':.8} for i in range(10)]
def match():
    return {'fixture':{'fixture_id':'100','season':2026,'kickoff':'2026-10-20T20:00:00+00:00'},
        'statistics':{'home':{'last_matches':history()},'away':{'last_matches':history('away')},
        'league':{'sample':20,'ht_sample':20,'avg_total_ft':2.6,'avg_total_ht':1.1,
        'avg_home_goals':1.5,'avg_away_goals':1.1,'avg_home_ht':.65,'avg_away_ht':.45}}}
def book(name,points,side='over'):
    traces=[{'observed_at':f'2026-10-20T0{5+i}:00:00+00:00','odds':{'over':v,'under':2.0}} for i,v in enumerate(points)]
    p,m=fair(traces[-1]['odds'])
    return {'bookmaker':name,'bookmaker_id':name,'observed_at':NOW,'provider_updated_at':NOW,'odds':traces[-1]['odds'],'fair':p,'overround_pct':m,'trace':traces}

class ContextTests(unittest.TestCase):
    def test_window_season_friendlies_future_missing(self):
        h=history();h[0]['friendly']=True;h[1]['season']=2025;h[2]['kickoff']='2026-10-21T12:00:00+00:00'
        chosen=select_history(h,NOW,2026)
        self.assertEqual([r['fixture_id'] for r in chosen],[3,4,5,6,7,8,9])
        self.assertEqual(len(select_history(history(),NOW,2026)),8)
        h=select_history(history(),NOW,2026);h[0]['halftime_available']=False
        p=profile(h,'home');self.assertEqual([r['fixture_id'] for r in p['ht_rows']],[1,2,3,4])

    def test_books_do_not_change_statistics_and_coherence(self):
        a=evaluate(match(),NOW);b=evaluate(match(),NOW,{'over25_ft':[book('A',[2.5,2.0]),book('B',[2.4,1.9])]})
        self.assertEqual(a['rates'],b['rates'])
        self.assertEqual([r['statistical_pct'] for r in a['markets']],[r['statistical_pct'] for r in b['markets']])
        p={(r['market'],r['selection']):r['probability_pct'] for r in a['markets']}
        self.assertLessEqual(p['gg_ht','yes'],p['over15_ht','over'])
        self.assertLessEqual(p['over15_ht','over'],p['over05_ht','over'])
        self.assertLessEqual(p['over35_ft','over'],p['over25_ft','over'])
        for key in {r['market'] for r in a['markets']}:
            self.assertAlmostEqual(sum(r['probability_pct'] for r in a['markets'] if r['market']==key),100,places=2)
        self.assertEqual(sum(r['research_candidate'] for r in a['markets']),1)

    def test_missing_halves_indirect_or_unavailable_but_ft_survives(self):
        m=match()
        for s in ('home','away'):
            for r in m['statistics'][s]['last_matches']: r['halftime_available']=False
        a=evaluate(m,NOW)
        self.assertTrue(a['weights']['home_ht']['indirect']);self.assertEqual(a['profiles']['home']['ht_sample'],0)
        m['statistics']['league']={};a=evaluate(m,NOW)
        self.assertIsNone(next(r for r in a['markets'] if r['market']=='gg_ht')['statistical_pct'])
        self.assertIsNotNone(next(r for r in a['markets'] if r['market']=='over25_ft')['statistical_pct'])

    def test_correction_reference_excludes_last_and_no_extra_bonus(self):
        h=select_history(history(),NOW,2026)
        for r in h: r['team_goals_ft']=1.5;r['team_conceded_ft']=.2
        h[0]['team_goals_ft']=4;h[0]['team_conceded_ft']=0
        p=profile(h,'home');c=correction(p,'for')
        self.assertEqual(c['reference_mean'],1.5);self.assertLess(c['relative'],0);self.assertGreaterEqual(c['relative'],-.05)
        self.assertEqual(correction(p,'against')['relative'],0)

    def test_single_operator_disagreement_and_reversal(self):
        self.assertFalse(movement([book('A',[2.5,2])],'over')['confirmed'])
        self.assertFalse(movement([book('A',[2.5,2]),book('B',[2,2.5])],'over')['confirmed'])
        m=movement([book('A',[2.5,2,2.4]),book('B',[2.5,2,2.4])],'over')
        self.assertTrue(m['confirmed']);self.assertEqual(m['arrow'],'↑');self.assertTrue(m['has_direction_reversal']);self.assertEqual(m['color'],'red')
        self.assertAlmostEqual(m['amplitude_pct'],20)

    def test_favorite_switch_is_distinct_from_direction_reversal(self):
        def team_book(name, pairs):
            b=book(name,[2]*len(pairs))
            for h,(home,away) in zip(b['trace'],pairs):
                h['odds']={'1':home,'x':3.5,'2':away}
            return b
        pairs=[(1.8,2.8),(2.2,2.2),(2.8,1.8)]
        a,b=team_book('A',pairs),team_book('B',pairs)
        m=movement([a,b],'1')
        self.assertTrue(m['favorite_switch']['confirmed'])
        self.assertEqual((m['favorite_switch']['from'],m['favorite_switch']['to']),('1','2'))
        self.assertFalse(m['has_direction_reversal'])
        self.assertEqual(len(m['operators'][0]['favorite_path']['changes']),1)
        self.assertFalse(movement([a],'1')['favorite_switch']['confirmed'])
        other=team_book('B',[(2.8,1.8),(2.2,2.2),(1.8,2.8)])
        self.assertFalse(movement([a,other],'1')['favorite_switch']['confirmed'])
        returned=team_book('A',[(1.8,2.8),(2.8,1.8),(1.8,2.8)])
        p=favorite_path(returned['trace'])
        self.assertEqual(len(p['changes']),2);self.assertFalse(p['net_switch'])
        tied=team_book('A',[(1.8,2.8),(2.2,2.2),(1.8,2.8)])
        self.assertEqual(favorite_path(tied['trace'])['changes'],[])
        end_tie=team_book('A',[(1.8,2.8),(2.2,2.2)])
        self.assertFalse(favorite_path(end_tie['trace'])['net_switch'])

    def test_margin_fair_and_previous_oci_are_observed(self):
        m=movement([book('A',[2.5,2.2,2]),book('B',[2.5,2.2,2])],'over')
        row=m['operators'][0]
        self.assertAlmostEqual(row['fair_initial_pct'],100*(1/2.5)/(1/2.5+1/2))
        self.assertAlmostEqual(row['fair_current_pct'],50)
        self.assertAlmostEqual(row['margin_initial_pct'],-10)
        self.assertAlmostEqual(row['margin_current_pct'],0)
        self.assertAlmostEqual(row['net_delta_pct'],-20)
        self.assertIsNotNone(m['oci_previous'])
        m=movement([book('A',[2.5,2]),book('B',[2.5,2])],'over')
        self.assertIsNone(m['oci_previous'])

    def test_stale_quotes_excluded_and_book_only_available(self):
        q=book('A',[2,2.1]);q['provider_updated_at']='2026-10-19T00:00:00+00:00'
        a=evaluate(match(),NOW,{'over25_ft':[q]})
        self.assertIsNone(next(r for r in a['markets'] if r['market']=='over25_ft')['book_probability_pct'])
        a=evaluate({'fixture':match()['fixture']},NOW,{'over25_ft':[book('A',[2,2.1])]})
        self.assertEqual(a['best_candidate']['source'],'analisi bookmaker')

    def test_unlike_opportunity_units_do_not_sum(self):
        m=match()
        for i,r in enumerate(m['statistics']['home']['last_matches']):r['team_xg_for']=1.6 if i<4 else 1.5
        for i,r in enumerate(m['statistics']['away']['last_matches']):
            r.pop('team_xg_for');r['team_shots_on_target_for']=3 if i<4 else 4
        a=evaluate(m,NOW)
        event=next(r for r in a['markets'] if r['market']=='over25_ft' and r['selection']=='over')
        self.assertEqual(event['evidence_blocks']['opportunities'],0)

    def test_immutable_updates_first_latest_and_no_postkickoff(self):
        m=match();a=evaluate(m,NOW);s={}
        freeze_update(s,m['fixture'],a,NOW)
        frozen=deepcopy(s)
        a['markets'][0]['statistical_pct']=99
        freeze_update(s,m['fixture'],a,'2026-10-20T09:00:00+00:00')
        freeze_update(s,m['fixture'],a,'2026-10-20T21:00:00+00:00')
        self.assertEqual(len(s['analysis_updates']),2)
        self.assertEqual(next(iter(s['analysis_updates'].values())),next(iter(frozen['analysis_updates'].values())))
        self.assertEqual(next(iter(latest_store(s)['multimarket_evaluations'].values()))['captured_at'],'2026-10-20T09:00:00+00:00')
        settle(s,{'100':{'status':'FT','home_ft':2,'away_ft':1,'home_ht':1,'away_ht':0}})
        report=build_report(s)
        self.assertEqual(report['latest_prematch']['cohorts'][0]['settled_fixtures'],1)
        import importlib.util,tempfile,json
        spec=importlib.util.spec_from_file_location('context_archive',Path(__file__).resolve().parents[1]/'auditor/current_audit.py')
        auditor=importlib.util.module_from_spec(spec);spec.loader.exec_module(auditor)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'data').mkdir();s['fixtures']={}
            (root/'data/arab_quote_prematch_audit.json').write_text(json.dumps(s))
            summary=auditor.archive(root,datetime.fromisoformat('2026-10-20T12:00:00+00:00'))
            self.assertEqual(summary['new_forecasts'],2)
            self.assertEqual(summary['rejected_without_valid_prematch_evidence'],0)
            self.assertEqual(auditor.archive(root,datetime.fromisoformat('2026-10-20T12:00:00+00:00'))['new_forecasts'],0)

if __name__=='__main__':unittest.main()

class IntegrationTests(unittest.TestCase):
    def test_real_engine_keeps_statistical_fixture_without_odds(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('context_engine_test',Path(__file__).resolve().parents[1]/'src/3appdays.py')
        e=importlib.util.module_from_spec(spec);sys.modules[spec.name]=e;spec.loader.exec_module(e)
        e.XG_MAX_CALLS_PER_RUN=0
        now=datetime.now(timezone.utc)
        def fixture(i,day,team=10,season=2026):
            return {'fixture':{'id':i,'date':day.isoformat(),'status':{'short':'FT' if day<now else 'NS'}},
                'league':{'id':135,'name':'Serie A','country':'Italy','season':season,'type':'League'},
                'teams':{'home':{'id':team,'name':'Home'},'away':{'id':20 if team==10 else 10,'name':'Away'}},
                'goals':{'home':2,'away':1},'score':{'halftime':{'home':1,'away':0},'fulltime':{'home':2,'away':1}}}
        class Client:
            def get(self,endpoint,params=None,**kwargs):
                if endpoint=='fixtures':return {'response':[fixture(i,now-timedelta(days=i+1),team=(params or {}).get('team',10)) for i in range(10)]}
                return {'response':[]}
        result=e.build_fixture_output(Client(),fixture(100,now+timedelta(hours=2)),{}, {})
        self.assertIsNotNone(result)
        self.assertEqual(result['multimarket']['model_id'],'context-goals-v3.4')
        self.assertEqual(result['multimarket']['profiles']['home']['sample'],8)
        self.assertEqual(result['multimarket']['best_candidate']['source'],'analisi statistica')
        class Empty:
            def get(self,*args,**kwargs):return {'response':[]}
        empty=e.build_fixture_output(Empty(),fixture(101,now+timedelta(hours=2)),{}, {})
        self.assertIsNotNone(empty)
        self.assertIsNone(empty['multimarket']['best_candidate'])

    def test_horizons_fixed_and_national_cup_policy(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('runner',Path(__file__).resolve().parents[1]/'src/3appdays_runner.py')
        r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
        self.assertIn('--fast',r.build_command('fast'))
        self.assertEqual(r.build_command('night',days=1)[-2:],['--days','5'])
        from selection_policy import competition
        self.assertFalse(competition({'name':'Coppa Italia','country':'Italy','type':'Cup'})['allowed'])
        self.assertTrue(competition({'name':'UEFA Champions League','country':'World','type':'Cup'})['allowed'])


class ComparableWindowTests(unittest.TestCase):
    def older(self, name='A', points=(3.,2.5,2.4,2.)):
        b=book(name,points)
        for row,hour in zip(b['trace'],(0,5,6,7)):
            row['observed_at']=f'2026-10-20T{hour:02d}:00:00+00:00'
        return b

    def test_recovers_older_start_using_real_shared_observations(self):
        books=[self.older(),book('B',[2.5,2.4,2.])];before=deepcopy(books)
        result=movement(books,'over')
        self.assertEqual(result['paired_books'],2)
        self.assertEqual(result['recovered_books'],1)
        self.assertTrue(result['confirmed'])
        self.assertEqual(result['operators'][0]['sequence'],[2.5,2.4,2.])
        self.assertNotEqual(result['operators'][0]['first_observed_fair_pct'],result['operators'][0]['fair_initial_pct'])
        self.assertEqual(books,before)

    def test_recovery_does_not_force_agreement(self):
        result=movement([self.older(),book('B',[2.,2.2,2.5])],'over')
        self.assertEqual(result['paired_books'],2)
        self.assertFalse(result['confirmed'])

    def test_stale_or_nonoverlapping_history_not_recovered(self):
        old=self.older();old['trace']=old['trace'][:1]+old['trace'][1:2]
        result=movement([old,book('B',[2.5,2.4,2.]),book('C',[2.5,2.4,2.])],'over')
        self.assertEqual(result['paired_books'],2)
        self.assertEqual(result['recovered_books'],0)
        self.assertEqual({r['bookmaker'] for r in result['operators']},{'B','C'})
