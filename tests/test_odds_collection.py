import sys, unittest, importlib.util
from pathlib import Path
from copy import deepcopy
from unittest.mock import Mock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from odds_collection import OddsEnricher, merge_payload, coverage
NOW='2026-10-23T09:00:00+00:00'

def payload(book=1, values=None, updated='2026-10-23T08:00:00+00:00', fid=100):
    return {'collection':{'complete':True},'response':[{'fixture':{'id':fid,'date':'2026-10-23T19:00:00+00:00'},
            'update':updated,'bookmakers':[{'id':book,'name':f'Book {book}','bets':[{'id':5,'name':'Goals Over/Under',
            'values':values if values is not None else [{'value':'Over 2.5','odd':'1.8'},{'value':'Under 2.5','odd':'2.1'}]}]}]}]}

class MergeTests(unittest.TestCase):
    def test_adds_new_book_and_preserves_timestamp(self):
        a=payload();b=payload(2);result=merge_payload(a,b,100,NOW)
        self.assertEqual(coverage(result,100,NOW),{('over25_ft','1'),('over25_ft','2')})
        self.assertEqual(len(a['response'][0]['bookmakers']),1)
    def test_never_manufactures_complete_pair(self):
        a=payload(values=[{'value':'Over 2.5','odd':'1.8'}])
        b=payload(values=[{'value':'Under 2.5','odd':'2.1'}],updated='2026-10-23T08:30:00+00:00')
        self.assertEqual(coverage(merge_payload(a,b,100,NOW),100,NOW),set())
    def test_stale_future_wrong_fixture_ignored(self):
        for b in (payload(2,updated='2026-10-22T08:00:00+00:00'),payload(2,updated='2026-10-23T10:00:00+00:00'),payload(2,fid=200)):
            self.assertEqual(coverage(merge_payload(payload(),b,100,NOW),100,NOW),{('over25_ft','1')})
    def test_older_quote_does_not_replace_newer(self):
        a=payload(updated='2026-10-23T08:30:00+00:00'); b=payload(values=[])
        self.assertEqual(coverage(merge_payload(a,b,100,NOW),100,NOW),{('over25_ft','1')})

class RecoveryTests(unittest.TestCase):
    def collector(self,response,quota=1000, state=None):
        client=Mock();client.request_count=0
        def get(path,params,**kwargs):
            client.request_count+=1
            if path=='status':return {'response':{'requests':{'current':quota,'limit_day':75000}}}
            return deepcopy(response)
        client.get.side_effect=get
        return OddsEnricher(client,state or {},now=NOW,max_calls=2),client
    def test_counts_real_recovery_and_caps_calls(self):
        c,client=self.collector(payload(2));result=c.enrich(payload(),100)
        self.assertEqual(c.stats['odds_targeted_books_recovered'],1)
        self.assertEqual(c.stats['odds_targeted_markets_recovered'],1)
        self.assertEqual(c.stats['odds_targeted_requests'],2)
        self.assertEqual(len(coverage(result,100,NOW)),2)
    def test_unknown_or_low_quota_does_not_probe(self):
        c,client=self.collector(payload(2),quota=74900)
        c.enrich(payload(),100)
        self.assertEqual(c.calls,0)
        c,client=self.collector(payload(2));client.get.side_effect=lambda *a,**k:None
        c.enrich(payload(),100);self.assertEqual(c.calls,0)
    def test_cooldown_persists_and_failed_requests_not_cached(self):
        c,client=self.collector({'response':[]});c.enrich(payload(),100)
        state=c.finish();self.assertEqual(len(state['queries']),2)
        c2,client2=self.collector(None,state=state);c2.enrich(payload(),100)
        self.assertGreaterEqual(c2.stats['odds_targeted_cooldown_skips'],2)
        self.assertEqual(len(c2.finish()['queries']),2)
    def test_incomplete_and_started_baseline_do_not_probe(self):
        for a in ({'collection':{'complete':False}}, {'response':[]},payload()):
            if a.get('response'):a['response'][0]['fixture']['date']='2026-10-23T08:00:00+00:00'
            c,client=self.collector(payload(2));self.assertEqual(c.enrich(a,100),a);client.get.assert_not_called()
    def test_partial_targeted_response_does_not_remove_complete_market(self):
        c,client=self.collector(payload(values=[],updated='2026-10-23T08:30:00+00:00'))
        r=c.enrich(payload(),100);self.assertEqual(coverage(r,100,NOW),{('over25_ft','1')})
    def test_retention_prunes_old_queries_and_cursors(self):
        c,client=self.collector(None,state={'queries':{'old:bet:5':'2026-09-01T00:00:00+00:00'},'cursors':{'old':9}})
        self.assertEqual(c.finish()['cursors'],{})

class CatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('deep_catalog_engine',Path(__file__).resolve().parents[1]/'src/3appdays.py')
        cls.e=importlib.util.module_from_spec(spec);sys.modules[spec.name]=cls.e;spec.loader.exec_module(cls.e)
    def test_refresh_filters_null_names_and_does_not_erase_on_error(self):
        from unittest.mock import patch
        e=self.e;client=Mock();client.get.return_value={'response':[{'id':1,'name':'Match Winner'},{'id':2,'name':None}]}
        with patch.object(e,'atomic_write_json') as write:
            self.assertEqual(len(e.fetch_bet_catalog(client)['bets']),1);write.assert_called_once()
        client.get.return_value=None
        with patch.object(e,'atomic_write_json') as write, patch.object(e,'read_json',return_value={'bets':[{'id':1}]}):
            self.assertEqual(e.fetch_bet_catalog(client)['bets'],[{'id':1}]);write.assert_not_called()

if __name__=='__main__':unittest.main()
