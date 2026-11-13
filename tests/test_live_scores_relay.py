import importlib.util
from pathlib import Path
import unittest
from datetime import datetime, timedelta, timezone
spec=importlib.util.spec_from_file_location('relay',Path(__file__).resolve().parents[1]/'.github/ops/live-scores-relay.py')
relay=importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)
NOW=datetime(2026,11,13,9,tzinfo=timezone.utc)
def watch(n=1):
    return {'fixtures':[{'fixture_id':str(i+1),'kickoff':(NOW-timedelta(hours=3)).isoformat()} for i in range(n)]}
class RelayTests(unittest.TestCase):
    def test_missing_tracked_fixture_is_requested(self):
        self.assertEqual(relay.due(watch(),{},NOW),['1'])
    def test_future_fixture_and_postponed_do_not_poll(self):
        future={'fixtures':[{'fixture_id':'1','kickoff':(NOW+timedelta(hours=1)).isoformat()}]}
        self.assertEqual(relay.due(future,{},NOW),[])
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':{'status':'PST'}}},NOW),[])
    def test_complete_zero_score_needs_no_provider_call(self):
        row={'status':'FT','halftime':{'home':0,'away':0},'fulltime':{'home':0,'away':0}}
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':row}},NOW),[])
    def test_fresh_live_is_deduplicated(self):
        row={'status':'1H','observed_at':NOW.isoformat()}
        self.assertEqual(relay.due(watch(),{'fixtures':{'1':row}},NOW),[])
    def test_batch_is_bounded_and_retained_overnight_ids_survive(self):
        self.assertEqual(len(relay.due(watch(100),{},NOW)),20)
        self.assertEqual(relay.due({'fixtures':[]},{'health':{'overdue_fixture_ids':['1644690']}},NOW),['1644690'])
    def test_repeated_unresolved_results_do_not_starve_other_ids(self):
        rows={str(i+1):{'status':'NS','observed_at':(NOW-timedelta(minutes=5)).isoformat()} for i in range(20)}
        self.assertIn('21',relay.due(watch(21),{'fixtures':rows},NOW))
    def test_cap_and_cooldown_include_failed_requests(self):
        self.assertFalse(relay.allowed({'day':'2026-11-13','calls':599},NOW))
        self.assertFalse(relay.allowed({'day':'2026-11-13','calls':2,'attempted_at':NOW.isoformat()},NOW))
        self.assertTrue(relay.allowed({'day':'2026-11-12','calls':600},NOW))
if __name__=='__main__': unittest.main()
