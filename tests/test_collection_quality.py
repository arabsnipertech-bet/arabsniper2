import sys, unittest, importlib.util
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from statistics_coverage import cache_reusable, profile_coverage
from context_analysis import evaluate, PRIMARY_MARKETS, select_proposal, pick_goal_proposal
from test_context_analysis import match, NOW
from test_directional_selection import row

class CoverageTests(unittest.TestCase):
    def test_zero_is_observed_missing_is_not(self):
        q=profile_coverage([{'team_xg_for':0,'team_shots_on_target_for':0},{}, {'team_xg_for':float('nan')}])
        self.assertEqual(q['xg_for'],1);self.assertEqual(q['opportunity_matches'],1)
    def test_errors_and_empty_cache_retry(self):
        r={'stats_schema':2,'source':'api_statistics_unavailable','checked_at':'2026-10-20T08:00:00+00:00'}
        self.assertTrue(cache_reusable(r,'2026-10-20T08:30:00+00:00'))
        self.assertFalse(cache_reusable(r,'2026-10-20T09:01:00+00:00'))
        r['source']='api_no_statistics'
        self.assertTrue(cache_reusable(r,'2026-10-20T10:00:00+00:00'))
        self.assertFalse(cache_reusable(r,'2026-10-21T09:00:00+00:00'))
        r['checked_at']='broken';self.assertFalse(cache_reusable(r,NOW))
    def test_complete_zero_statistics_persist(self):
        r={'stats_schema':2, 'home_stats':{'xg':0,'shots_on_target':0},'away_stats':{'xg':1,'shots_on_target':3}}
        self.assertTrue(cache_reusable(r,NOW))
    def test_wolves_conflict_does_not_depend_on_sixty_percent(self):
        rows=[row('1x2_ft','1',32.394,59.45),row('1x2_ft','x',29.624,22.769),row('1x2_ft','2',37.983,17.78),row('over25_ft','over',35.9,54.1)]
        _,d=select_proposal(rows)
        self.assertEqual(d['status'],'major_conflict')
        self.assertEqual(rows[-1]['price_assessment']['reference_source'],'book_only')
    def test_five_markets_only_and_all_outcomes_retained(self):
        a=evaluate(match(),NOW)
        self.assertIn((a['best_candidate']['market'],a['best_candidate']['selection']),PRIMARY_MARKETS)
        self.assertTrue(any(r['market']=='home15_ft' for r in a['markets']))
        self.assertEqual(sum(r['research_candidate'] for r in a['markets']),1)
        self.assertEqual(a['data_quality']['home']['xg_for'],8)
    def test_engine_failed_request_not_cached_forever(self):
        path=Path(__file__).resolve().parents[1]/'src/3appdays.py'
        spec=importlib.util.spec_from_file_location('collection_engine',path)
        e=importlib.util.module_from_spec(spec);sys.modules[spec.name]=e;spec.loader.exec_module(e)
        e._XG_STORE={'fixtures':{'1':{'stats_schema':2,'source':'api_statistics_unavailable','checked_at':'2026-01-01T00:00:00+00:00'}}}
        e.XG_ENABLED=True;e._XG_RUNTIME_CALLS=0;e.XG_MAX_CALLS_PER_RUN=1
        class Client:
            calls=0
            def get(self,*a,**kw):
                self.calls+=1
                return {'response':[]}
        c=Client();r=e.fetch_fixture_xg(c,1,10,20)
        self.assertEqual(c.calls,1);self.assertEqual(r['source'],'api_no_statistics')
        e.fetch_fixture_xg(c,1,10,20);self.assertEqual(c.calls,1)

    def test_negative_returns_do_not_promote_rare_high_price(self):
        rs=[row('over25_ht','over',3.2,15.6),row('over25_ft','over',35.9,54.1)]
        for r,o in zip(rs,[6,1.73]):r['odd']=o
        # Strong 1X2 conflict makes both estimates diagnostic.
        rs[:0]=[row('1x2_ft','1',32.4,59.45),row('1x2_ft','x',29.6,22.77),row('1x2_ft','2',38,17.78)]
        candidates,_=select_proposal(rs)
        chosen=pick_goal_proposal(candidates,1.5)
        self.assertEqual(chosen['market'],'over25_ft')
        self.assertEqual(chosen['selection_mode'],'likelihood_without_demonstrated_edge')
        self.assertLess(chosen['price_assessment']['estimated_return_pct'],0)


class OddsPaginationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('pagination_engine',Path(__file__).resolve().parents[1]/'src/3appdays.py')
        cls.engine=importlib.util.module_from_spec(spec)
        sys.modules[spec.name]=cls.engine
        spec.loader.exec_module(cls.engine)

    def page(self, current=1, total=2, book=1):
        return {'paging':{'current':current,'total':total},'response':[
            {'fixture':{'id':100},'bookmakers':[{'id':book}]}]}

    def client(self, responses):
        from unittest.mock import Mock
        client=self.engine.APIClient('test-only')
        client._get_page=Mock(side_effect=responses)
        return client

    def quota(self, used=2904):
        return {'response':{'requests':{'current':used,'limit_day':75000}}}

    def test_all_pages_combined_and_recovered_books_counted(self):
        c=self.client([self.page(),self.quota(),self.page(2,2,2)])
        result=c.get('odds',{'fixture':100})
        self.assertEqual(len(result['response']),2)
        self.assertTrue(result['collection']['complete'])
        self.assertEqual((c.odds_extra_pages,c.odds_books_recovered),(1,1))
        self.assertEqual(c._get_page.call_args.args,('odds',{'fixture':100,'page':2}))

    def test_single_page_needs_no_quota_call(self):
        c=self.client([self.page(total=1)])
        self.assertIsNotNone(c.get('odds',{'fixture':100}))
        self.assertEqual(c._get_page.call_count,1)

    def test_missing_or_inconsistent_page_never_publishes_partial(self):
        for page in (None,self.page(1,2,2),self.page(2,3,2)):
            with self.subTest(page=page):
                c=self.client([self.page(),self.quota(),page])
                self.assertIsNone(c.get('odds',{'fixture':100}))
                self.assertEqual(c.odds_pagination_incomplete,1)
                self.assertEqual(c.odds_books_recovered,0)

    def test_quota_reserve_or_unknown_quota_stops_extra_calls(self):
        for quota in (self.quota(74900),None):
            c=self.client([self.page(),quota])
            self.assertIsNone(c.get('odds',{'fixture':100}))
            self.assertEqual(c._get_page.call_count,2)

    def test_extra_page_budget_and_other_endpoints(self):
        c=self.client([self.page(),self.quota()]);c.odds_extra_pages=2000
        self.assertIsNone(c.get('odds',{'fixture':100}))
        c=self.client([{'response':[]}])
        self.assertEqual(c.get('fixtures',{'id':100}),{'response':[]})
        self.assertEqual(c._get_page.call_count,1)
