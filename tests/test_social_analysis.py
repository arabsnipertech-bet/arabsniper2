import unittest,sys,copy
from pathlib import Path
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from social_analysis import select,caption,model_probabilities,probability_lines
from sports_daily_live import advance
NOW=datetime(2026,11,8,12,30,tzinfo=ZoneInfo('Europe/Rome'))
def match(i,country='Italy',league='Serie A',sample=8):
    return {'fixture':{'fixture_id':i,'match':f'Squadra {i} – Ospite','country':country,'league':league,'kickoff':(NOW+timedelta(hours=1)).isoformat(),'status_short':'NS'},'multimarket':{'captured_at':NOW.isoformat(),'rates':{'home_ft':1.5,'away_ft':1.2,'home_ht':.6,'away_ht':.4},'profiles':{s:{'for_ft':{'sample':sample,'mean':1.4},'against_ft':{'mean':1.1},'for_ht':{'sample':sample}} for s in ('home','away')}}}
class AnalysisTest(unittest.TestCase):
    def board(self,rows):return dict(date='2026-11-08',updated_at=NOW.isoformat(),matches=rows)
    def choose(self,rows,exclude=()):return select(self.board(rows),NOW,NOW,NOW+timedelta(hours=4),exclude)
    def test_four_distinct_major_leagues_without_prices(self):
        rows=[match(1,'Romania','Liga I'),*[match(i) for i in range(2,7)],match(2)]
        result=self.choose(rows)
        self.assertEqual([r['id'] for r in result],['2','3','4','5'])
        self.assertTrue(all('odd' not in r and 'market' not in r for r in result))
    def test_stale_started_small_samples_and_previous_excluded(self):
        a=match(1);a['fixture']['kickoff']=NOW.isoformat()
        b=match(2);b['multimarket']['captured_at']=(NOW-timedelta(hours=2)).isoformat()
        self.assertEqual(self.choose([a,b,match(3,sample=3),match(4)],['4']),[])
    def test_missing_half_sample_does_not_invent_split(self):
        m=match(1);m['multimarket']['profiles']['home']['for_ht']['sample']=1
        self.assertIsNone(self.choose([m])[0]['analysis']['home']['pt'])
    def test_unavailable_not_backfilled(self):
        p=dict(slot='12:30',picks=self.choose([match(1)]),analysis_only=True)
        self.assertIn('1 schede su 4',caption(p));self.assertLess(len(caption(p).encode('utf-16-le'))//2,1024)
    def test_probabilities_are_existing_model_values(self):
        a={'matrix':{'rows':[{'spec':{'kind':'result','period':'ft','side':'1'},'statistical_pct':52.3},{'spec':{'kind':'total','period':'ft','side':'over','line':2.5},'statistical_pct':61.2},{'spec':{'kind':'btts','period':'ft','side':'yes'},'statistical_pct':None}]}}
        p=model_probabilities(a)
        self.assertEqual(p,{'1':52.3,'over25':61.2})
        self.assertIn('GG n.d.',probability_lines({'probabilities':p})[1])

    def test_same_study_option_as_gold_without_recomputing_probability(self):
        m=match(1)
        m['multimarket']['matrix']={'rows':[dict(id='o25',label='Over 2,5 FT',spec=dict(kind='total',period='ft',side='over',line=2.5),quality='supported',quote_executable=True,quote_status='recent',odd=1.92,statistical_pct=61.2,low_pct=55,provider_updated_at=NOW.isoformat(),quote_observed_at=NOW.isoformat())]}
        p=self.choose([m])[0]['proposal']
        self.assertEqual(p['label'],'Over 2,5 FT')
        self.assertEqual(p['odd'],1.92)
        self.assertEqual(p['probability'],61.2)
        self.assertIn('Studio',p['title'])
        m['multimarket']['matrix']['rows'][0]['provider_updated_at']=(NOW-timedelta(hours=2)).isoformat()
        self.assertIsNone(self.choose([m])[0]['proposal'])

    def test_sports_events_require_confirmation_and_correct_reversal(self):
        s={};o=dict(status='1H',home=0,away=0,minute=1)
        self.assertIsNone(advance(s,o,0));self.assertIsNotNone(advance(s,o,46))
        goal=dict(o,home=1,minute=10)
        self.assertIsNone(advance(s,goal,60));self.assertIn('PUNTEGGIO',advance(s,goal,106))
        self.assertIsNone(advance(s,o,120));self.assertIn('RETTIFICA',advance(s,o,166))
        ft=dict(o,status='FT');self.assertIsNone(advance(s,ft,180));self.assertIn('FINALE',advance(s,ft,226));self.assertTrue(s['done'])
if __name__=='__main__':unittest.main()
