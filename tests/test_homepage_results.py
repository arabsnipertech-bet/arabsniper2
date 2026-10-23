import sys, unittest
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from homepage_results import build, MODEL
NOW=datetime(2026,10,23,8,tzinfo=timezone.utc)
def row(fid,won,**extra):
 return dict(snapshot='latest',model_id=MODEL,period='prospective',primary=True,fixture_id=str(fid),fixture={'kickoff':'2026-10-22T18:00:00+02:00','match':'A - B'},captured_at='2026-10-22T12:00:00+02:00',market='over25_ft',outcome=won,odd=1.9,**extra)
class Results(unittest.TestCase):
 def test_denominator_does_not_use_showcase(self):
  rows=[row(i,i<20) for i in range(40)]+[row(50,None)]
  p=build({'2026-10-22':{'markets':rows}},NOW)
  self.assertEqual(p['daily'],dict(wins=20,losses=20,pending=1,settled=40,hit_rate=50.0))
  self.assertEqual(len(p['examples']),10)
  self.assertEqual(len(p['all_results']),41)
 def test_excludes_backtests_late_capture_other_model_and_duplicates(self):
  rows=[row(1,True),row(1,True),dict(row(2,True),period='retrospective'),dict(row(3,True),captured_at='2026-10-22T18:00:00+02:00'),dict(row(4,True),model_id='other'),dict(row(5,True),snapshot='first')]
  self.assertEqual(build({'2026-10-22':{'markets':rows}},NOW)['daily']['settled'],1)
 def test_empty_not_seventy(self):
  p=build({},NOW);self.assertFalse(p['available']);self.assertIsNone(p['daily']['hit_rate']);self.assertIsNone(p['history']['hit_rate'])
 def test_showcase_only_winning_overs_with_recorded_odds(self):
  rows=[dict(row(1,True),odd=2.5),dict(row(2,True),odd=3.1),dict(row(3,False),odd=8),dict(row(4,True),odd=None),dict(row(5,True),market='gg_ht',odd=5)]
  p=build({'2026-10-22':{'markets':rows}},NOW)
  self.assertEqual([r['fixture_id'] for r in p['examples']],['2','1'])
  self.assertEqual(len(p['all_results']),5)
 def test_rome_date(self):
  r=row(1,True);r['fixture']['kickoff']='2026-10-21T23:00:00Z';r['captured_at']='2026-10-21T20:00:00Z'
  self.assertEqual(build({'2026-10-22':{'markets':[r]}},NOW)['daily']['wins'],1)
if __name__=='__main__':unittest.main()
