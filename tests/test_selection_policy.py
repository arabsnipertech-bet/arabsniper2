import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from selection_policy import competition, drought, movements
from prematch_audit import build_report

class PolicyTests(unittest.TestCase):
    def test_allowed_third_tiers_and_egypt(self):
        for country,name in [('Germany','3. Liga'),('Italy','Serie C - Girone A'),('Finland','Ykkönen'),('Japan','J3 League'),('Egypt','Second League'),('Brazil','Serie C')]:
            self.assertTrue(competition({'country':country,'name':name})['allowed'],(country,name))
        for country,name in [('Saudi-Arabia','Pro League'),('Italy','Serie D'),('Egypt','Third Division'),('Finland','Kakkonen'),('England','League Two'),('Italy','Coppa Italia Serie C')]:
            self.assertFalse(competition({'country':country,'name':name})['allowed'],(country,name))
        self.assertTrue(competition({'country':'World','name':'UEFA Champions League'})['allowed'])
        self.assertFalse(competition({'country':'England','name':'League Cup'})['allowed'])

    def test_drought_boundaries_cap_and_missing(self):
        goal={'date':'2026-01-01','team_goals_ft':1,'team_goals_ht':0}
        half={'date':'2026-01-02','team_goals_ft':1,'team_goals_ht':1}
        zero={'date':'2026-01-03','team_goals_ft':0,'team_goals_ht':0}
        self.assertEqual(drought([goal])['bonus_relative'],0)
        self.assertEqual(drought([half,goal])['bonus_relative'],.01)
        self.assertEqual(drought([zero,goal])['bonus_relative'],.02)
        self.assertEqual(drought([zero,half,goal])['bonus_relative'],.03)
        self.assertEqual(drought([zero]*10)['bonus_relative'],.04)
        self.assertEqual(drought([{'date':'2026-02-01','team_goals_ft':None}])['bonus_relative'],0)

    def test_quote_reversal_and_inversion(self):
        market={'open':{'odds':{'1':2,'2':3}},'current':{'odds':{'1':2.5,'2':1.9}},'history':[{'odds':{'1':2}},{'odds':{'1':1.8}},{'odds':{'1':2.5}}]}
        value=movements(market,'1')
        self.assertEqual(value['drop_pct'],-25)
        self.assertEqual(value['direction_reversals'],1)
        self.assertTrue(value['favorite_inversion'])
        self.assertIsNone(movements({},'1')['drop_pct'])

    def test_quote_report_and_recovery_split(self):
        store={'fixtures':{'1':{'probabilities_calibrated':{'over15_ht':{'over':.5}},'market_candidates':[{'market':'over15_ht','selection':'over','odd':2.1,'recovered':True,'gate':{'eligible':True}}],
          'quote_observation':{'1x2_ft':{'current':{'odds':{'1':2.0,'x':3,'2':4}},'movement_features':{'1':{'drop_pct':5}}}},
          'settlement':{'result':{'home_ft':2,'away_ft':1},'outcomes':{'over15_ht':True}}}}}
        report=build_report(store)
        self.assertTrue(any(r['relation']=='recovered' for r in report['groups']))
        home=next(r for r in report['quote_groups'] if r['selection']=='1' and r['signal']=='drop')
        self.assertEqual(home['wins'],1)
        self.assertEqual(home['roi_pct'],100)
