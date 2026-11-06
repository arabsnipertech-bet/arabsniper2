import sys,unittest
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from promo_successes import build,message

class PromoTest(unittest.TestCase):
    def record(self,fid,odd=2.5,tier='Gold'):
        row=dict(id='over25',label='Over 2,5 FT',odd=odd,quote_executable=True,
                 spec=dict(kind='total',period='ft',side='over',line=2.5),
                 opportunity=dict(eligible=tier=='Gold'),platinum=dict(eligible=tier=='Platinum'))
        return dict(fixture=dict(fixture_id=str(fid),kickoff='2026-11-05T18:00:00+01:00',
                    match=f'A{fid} - B{fid}',country='Italy',league='Serie A'),
                    captured_at='2026-11-05T16:00:00+01:00',multimarket=dict(matrix=dict(rows=[row])),
                    settlement=dict(result=dict(status='FT',home_ft=3,away_ft=0,home_ht=1,away_ht=0)))
    def store(self,n):return dict(multimarket_evaluations={str(i):self.record(i,2+i/10) for i in range(n)})
    def test_minimum_maximum_and_higher_odds(self):
        self.assertFalse(build(self.store(4),'2026-11-05')['ready'])
        self.assertEqual(len(build(self.store(5),'2026-11-05')['examples']),5)
        out=build(self.store(12),'2026-11-05')
        self.assertEqual(len(out['examples']),10)
        self.assertEqual(out['examples'][0]['fixture_id'],'11')
        self.assertIn('non è il bilancio completo',message(out))
        self.assertLess(len(message(out).encode('utf-16-le'))//2,1024)
    def test_exclude_retrospective_late_unknown_and_losses(self):
        store=self.store(8);rows=store['multimarket_evaluations']
        rows['0']['retrospective']=True
        rows['1']['captured_at']='2026-11-05T19:00:00+01:00'
        rows['2']['settlement']={}
        rows['3']['settlement']['result']['home_ft']=0
        self.assertFalse(build(store,'2026-11-05')['ready'])
    def test_duplicate_and_ineligible_markets_never_inflate(self):
        store=self.store(5)
        store['analysis_updates']={'duplicate':deepcopy(store['multimarket_evaluations']['0'])}
        duplicate=store['analysis_updates']['duplicate']
        duplicate['captured_at']='2026-11-05T17:00:00+01:00'
        duplicate['multimarket']['matrix']['rows'][0]['odd']=99
        out=build(store,'2026-11-05');self.assertEqual(len(out['examples']),5)
        self.assertLess(max(r['odd'] for r in out['examples']),3)
        for r in store['multimarket_evaluations'].values():r['multimarket']['matrix']['rows'][0]['opportunity']['eligible']=False
        duplicate['multimarket']['matrix']['rows'][0]['opportunity']['eligible']=False
        self.assertFalse(build(store,'2026-11-05')['ready'])
    def test_studio_and_platinum_remain_labelled(self):
        store=self.store(5)
        row=self.record(9,tier='Platinum');store['analysis_updates']={'p':row}
        studio=self.record(10);studio['studio_ideas']=[dict(studio['multimarket']['matrix']['rows'][0])]
        store['study_observations']={'s':studio}
        out=build(store,'2026-11-05')
        self.assertIn('Platinum',[r['tier'] for r in out['examples']])
        self.assertIn('Studio',[r['tier'] for r in out['examples']])
