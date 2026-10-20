import sys, unittest
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from context_analysis import select_proposal,pick_for_price
from analysis_history import freeze_update

def row(m,s,p,b,score=0):
 return dict(market=m,selection=s,statistical_pct=p,book_probability_pct=b,bookmakers_count=7,coverage=1,
             pros=[],cons=[],evidence_blocks={'goals':score,'book':1,'form':0},support_score=score)

class DirectionalTests(unittest.TestCase):
 def test_close_minority_is_not_agreement_but_remains_eligible(self):
  rs=[row('over25_ft','under',45,35.7,3),row('over25_ft','over',55,64.3,-1)]
  picks,_=select_proposal(rs)
  self.assertEqual(picks[0]['selection'],'over')
  self.assertTrue(rs[0]['primary_eligible'])
  self.assertNotEqual(rs[0]['source'],'statistica e bookmaker')
 def test_dortmund_conflict_routes_to_book_without_changing_statistics(self):
  rs=[row('1x2_ft','1',44.887,10.781),row('1x2_ft','x',24.508,16.114),row('1x2_ft','2',30.605,73.105),
      row('over25_ft','under',44.996,35.663,3),row('over25_ft','over',55.004,64.337,-1)]
  before=[r['statistical_pct'] for r in rs];picks,d=select_proposal(rs)
  self.assertEqual(d['status'],'major_conflict');self.assertEqual(picks[0]['selection'],'2')
  self.assertEqual(picks[0]['source'],'analisi bookmaker');self.assertEqual(before,[r['statistical_pct'] for r in rs])
 def test_no_guard_from_single_book_or_missing_book(self):
  rs=[row('1x2_ft','1',20,70),row('1x2_ft','x',30,20),row('1x2_ft','2',50,10)]
  for r in rs:r['bookmakers_count']=1
  _,d=select_proposal(rs);self.assertNotEqual(d['status'],'major_conflict')
  rs=[row('over25_ft','over',55,None),row('over25_ft','under',45,None)]
  picks,_=select_proposal(rs);self.assertEqual(picks[0]['selection'],'over')
 def test_market_relative_not_common_probability_threshold(self):
  # A 40% 1X2 outcome can lead its market; no universal 50/60% gate.
  rs=[row('1x2_ft','1',40,42),row('1x2_ft','x',30,30),row('1x2_ft','2',30,28)]
  picks,_=select_proposal(rs);self.assertEqual(picks[0]['selection'],'1')
  self.assertEqual(picks[0]['source'],'statistica e bookmaker')
 def test_old_snapshots_unchanged(self):
  store={};meta={'fixture_id':'1','kickoff':'2026-10-20T18:45:00+02:00'}
  old={'model_id':'context-goals-v3','best_candidate':{'market':'over25_ft','selection':'under'}}
  freeze_update(store,meta,old,'2026-10-20T11:00:00+02:00');saved=deepcopy(store)
  new={'model_id':'context-goals-v3.1','best_candidate':{'market':'1x2_ft','selection':'2'}}
  freeze_update(store,meta,new,'2026-10-20T12:45:00+02:00')
  for k,v in saved['analysis_updates'].items():self.assertEqual(store['analysis_updates'][k],v)
  self.assertEqual(len(store['analysis_updates']),2)

class HalfTimeAndPriceTests(unittest.TestCase):
 def test_fragile_pt_keeps_raw_probability_but_uses_book_source(self):
  rs=[row('over15_ht','over',6.3,44),row('over15_ht','under',93.7,56)]
  for r in rs:r['halftime_sample_fragile']=True
  select_proposal(rs)
  self.assertEqual(rs[0]['statistical_pct'],6.3)
  self.assertEqual(rs[0]['statistical_reliability'],'fragile_halftime')
  self.assertEqual(rs[0]['source'],'analisi bookmaker')
 def test_price_floor_skips_one_point_zero_five(self):
  rs=[row('over25_ht','under',96,88),row('over25_ht','over',4,12),row('over25_ft','over',61,60),row('over25_ft','under',39,40)]
  for r,odd in zip(rs,[1.05,7,1.65,2.2]):r['odd']=odd
  picks,_=select_proposal(rs)
  chosen=pick_for_price(picks,1.5)
  self.assertEqual(chosen['market'],'over25_ft')
  self.assertGreaterEqual(chosen['odd'],1.5)
 def test_fragile_pt_without_book_is_not_primary(self):
  rs=[row('over15_ht','over',6.3,None),row('over15_ht','under',93.7,None),row('over25_ft','over',61,None),row('over25_ft','under',39,None)]
  for r in rs[:2]:r['halftime_sample_fragile']=True
  picks,_=select_proposal(rs)
  self.assertEqual(picks[0]['market'],'over25_ft')

class ProbabilityPriceTests(unittest.TestCase):
 def test_under_fifty_can_win_price_comparison(self):
  rs=[row('over15_ht','over',6.3,44.744),row('over15_ht','under',93.7,55.256)]
  for r,o in zip(rs,[2.12,1.67]):r['odd']=o;r['halftime_sample_fragile']=True
  picks,_=select_proposal(rs);chosen=pick_for_price(picks,1.5)
  self.assertEqual(chosen['selection'],'over')
  a=chosen['price_assessment'];self.assertEqual(a['status'],'market_comparison_only')
  self.assertFalse(a['validated_advantage']);self.assertLess(a['estimated_return_pct'],0)
  self.assertAlmostEqual(a['break_even_probability_pct'],100/2.12)
  self.assertEqual(chosen['statistical_pct'],6.3)
 def test_more_probable_is_not_necessarily_better_priced(self):
  rs=[row('over25_ft','over',60,62),row('over25_ft','under',40,38)]
  for r,o in zip(rs,[1.4,3]):r['odd']=o
  picks,_=select_proposal(rs)
  self.assertEqual(picks[0]['selection'],'under')
  self.assertEqual(picks[0]['price_assessment']['status'],'unvalidated_positive_estimate')
  self.assertFalse(picks[0]['price_assessment']['validated_advantage'])
 def test_fallback_below_preferred_price_is_explicit(self):
  rs=[row('over25_ft','over',70,70)];rs[0]['odd']=1.3
  picks,_=select_proposal(rs);chosen=pick_for_price(picks,2)
  self.assertIsNotNone(chosen);self.assertFalse(chosen['price_preference_met'])
  self.assertEqual(chosen['requested_min_odd'],2)
 def test_missing_price_never_gets_invented_return(self):
  picks,_=select_proposal([row('over25_ft','over',70,None)])
  a=picks[0]['price_assessment'];self.assertIsNone(a['return_index']);self.assertIsNone(a['break_even_probability_pct'])
