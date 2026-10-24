import unittest,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from home_odds_band_audit import analyze

def record(fid,odd,at='2026-10-23T10:00:00+00:00',settled=True):
 return {'fixture':{'fixture_id':fid,'kickoff':'2026-10-23T15:00:00+00:00'},'captured_at':at,
 'multimarket':{'model_id':'context-goals-v3.4','markets':[{'market':'1x2_ft','selection':'1','odd':odd},{'market':'over15_ht','selection':'over','odd':3.0}]},
 'settlement':{'result':{'status':'FT','home_ht':1,'away_ht':1,'home_ft':2,'away_ft':1} if settled else {}}}
class BandTests(unittest.TestCase):
 def test_boundaries_and_missing(self):
  store={'multimarket_evaluations':{str(i):record(i,o) for i,o in enumerate([1.57,1.58,1.75,1.76,None])}}
  out=analyze(store)
  g=next(g for g in out['groups'] if g['snapshot']=='first' and g['date']=='all_dates' and g['band']=='1.58-1.75' and g['market']=='over15_ht')
  self.assertEqual(g['settled'],2);self.assertEqual(g['wins'],2);self.assertEqual(g['paper_roi_pct'],200)
  self.assertEqual(out['excluded']['first_missing_home_quote'],1)
 def test_no_duplicate_or_postmatch_selection(self):
  store={'multimarket_evaluations':{'a':record(1,1.6)},'analysis_updates':{'b':record(1,1.9,'2026-10-23T14:00:00+00:00'),'c':record(1,1.6,'2026-10-23T16:00:00+00:00')}}
  out=analyze(store)
  groups=[g for g in out['groups'] if g['date']=='all_dates' and g['market']=='over25_ft' and g['band']!='all_priced_home']
  self.assertEqual([(g['snapshot'],g['band'],g['settled']) for g in groups],[('first','1.58-1.75',1),('latest','outside',1)])
 def test_pending_is_not_a_loss(self):
  out=analyze({'multimarket_evaluations':{'a':record(1,1.6,settled=False)}})
  self.assertTrue(all(g['settled']==0 and g['pending']==1 for g in out['groups']))
if __name__=='__main__':unittest.main()
