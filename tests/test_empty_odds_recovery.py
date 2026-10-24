import sys, unittest
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from odds_collection import OddsEnricher, coverage
from test_odds_collection import payload, NOW

class EmptyRecovery(unittest.TestCase):
    def collector(self, kickoff='2026-10-23T19:00:00+00:00'):
        client=Mock();client.request_count=0
        def get(path, params, **kw):
            client.request_count+=1
            if path=='status':return {'response':{'requests':{'current':1000,'limit_day':75000}}}
            return payload(2)
        client.get.side_effect=get
        c=OddsEnricher(client,{},now=NOW,max_calls=2)
        c.register_fixture({'fixture_id':100,'kickoff':kickoff,'league_id':1,'season':2026,'date':'2026-10-23','match':'Test'})
        return c,client
    def test_empty_baseline_recovers_and_keeps_exact_fixture(self):
        c,client=self.collector();base={'collection':{'complete':True},'response':[]}
        result=c.enrich(base,100)
        self.assertEqual(coverage(result,100,NOW),{('over25_ft','2')})
        self.assertEqual(base['response'],[])
        self.assertTrue(c.report[0]['recovered_from_empty'])
        self.assertEqual(client.get.call_args_list[1].args[1]['league'],1)
    def test_started_or_unknown_never_recovered(self):
        for kickoff in ('2026-10-23T08:00:00+00:00',None):
            c,client=self.collector(kickoff);c.enrich({'collection':{'complete':True},'response':[]},100)
            client.get.assert_not_called()
    def test_empty_fixture_not_blocked_by_other_fixtures_route_health(self):
        c,client=self.collector()
        c.state['route_health']={'bet':{'paused_at':NOW},'league_date':{'paused_at':NOW}}
        c.enrich({'collection':{'complete':True},'response':[]},100)
        self.assertEqual(c.calls,2)
    def test_unrelated_elapsed_time_does_not_use_request_budget(self):
        c,client=self.collector()
        with patch('odds_collection.time.monotonic',return_value=999999999):
            self.assertTrue(c.allowed())
        c.elapsed=c.max_seconds
        self.assertFalse(c.allowed())
    def test_wrong_fixture_and_incomplete_baseline_rejected(self):
        c,client=self.collector();client.get.side_effect=lambda path,params,**kw: ({'response':{'requests':{'current':1000,'limit_day':75000}}} if path=='status' else payload(2,fid=200))
        result=c.enrich({'collection':{'complete':True},'response':[]},100)
        self.assertEqual(coverage(result,100,NOW),set())
        c,client=self.collector();c.enrich({'collection':{'complete':False},'response':[]},100);client.get.assert_not_called()
