import sys,unittest
from copy import deepcopy
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'auditor'))
from weekend_export import retain,eligible,compact,parse_dates

def record(at='2026-10-23T16:00:00+02:00'):
    return {'captured_at':at,'fixture':{'fixture_id':1,'kickoff':'2026-10-23T20:00:00+02:00'},'multimarket':{'model_id':'v1','markets':[]}}
class ExportTests(unittest.TestCase):
    def test_first_latest_two_hours_are_separate_and_prematch(self):
        best={}
        for at in ('16:00','19:00','17:00','20:00','21:00'):
            retain(best,record('2026-10-23T'+at+':00+02:00'))
        self.assertEqual(len(best),3)
        self.assertIn('16:00',best['v1','1','first']['captured_at'])
        self.assertIn('19:00',best['v1','1','latest']['captured_at'])
        self.assertIn('17:00',best['v1','1','two_hours']['captured_at'])
    def test_retrospective_missing_timezone_and_outside_dates_rejected(self):
        r=record();r['retrospective']=True;self.assertFalse(eligible(r))
        r=record('2026-10-23T16:00:00');self.assertFalse(eligible(r))
        r=record();r['fixture']['kickoff']='2026-10-25T20:00:00+01:00';self.assertFalse(eligible(r))
    def test_compact_preserves_input_and_missing_results(self):
        r=record();before=deepcopy(r);out=compact(r,'latest',{})
        self.assertIsNone(out['result']);self.assertEqual(before,r)
        self.assertEqual(len(out['forecast_sha256']),64)
    def test_dates_are_explicit_and_validated(self):
        dates=parse_dates('2026-10-25,2026-10-24,2026-10-25')
        self.assertEqual(dates,('2026-10-24','2026-10-25'))
        for value in ('', '2026-02-30', '2026-1-1', '2026-10-24;echo nope'):
            with self.assertRaises(ValueError):parse_dates(value)
        r=record('2026-10-25T16:00:00+01:00');r['fixture']['kickoff']='2026-10-25T20:00:00+01:00'
        self.assertTrue(eligible(r,dates));self.assertFalse(eligible(r))
    def test_compact_retention_keeps_hashes_not_raw_quote_history(self):
        r=record();r['multimarket']['quotes']={'large_history':'x'*100000}
        expected=compact(r,None,{})
        best={};retain(best,r,transform=lambda x:compact(x,None,{}))
        self.assertEqual(len(best),3)
        self.assertEqual(best['v1','1','latest'],expected)
        self.assertNotIn('quotes',expected['assessment'])
        self.assertIn('quotes',r['multimarket'])
if __name__=='__main__':unittest.main()
