import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from gold_cohort_analysis import analyze, gold, metrics

def row(fid, market, p, b, outcome, primary=False, snapshot='latest'):
    return dict(model_id='context-goals-v3.4', period='prospective', selection='over',
                snapshot=snapshot, fixture_id=fid, market=market, candidate_present=True,
                primary=primary, statistical_probability=p, book_probability_pct=b,
                outcome=outcome, odd=2.0, fixture={'country':'Test','league':'Test'})

class GoldStudy(unittest.TestCase):
    def test_exact_gold_boundary(self):
        self.assertEqual(gold({'statistical_probability':.6,'book_probability_pct':50}), 'no_gold')
        self.assertEqual(gold({'statistical_probability':.61,'book_probability_pct':51}), 'gold')
        self.assertEqual(gold({'statistical_probability':None,'book_probability_pct':51}), 'unknown')

    def test_pt_inherits_ft_badge_and_snapshots_stay_separate(self):
        rows=[row('1','over25_ft',.6,55,True),row('1','over15_ht',.3,30,False,True),
              row('1','over25_ft',.4,40,True,snapshot='first'),
              row('1','over15_ht',.7,65,False,True,snapshot='first')]
        r=analyze({'2026-10-23':{'audit_date':'2026-10-23','markets':rows}},'2026-10-23')
        ht=[g for g in r['groups'] if g['market']=='over15_ht' and g['scope']=='primary_only']
        self.assertEqual([(g['snapshot'],g['gold'],g['losses']) for g in ht], [('first','no_gold',1),('latest','gold',1)])
        self.assertEqual(r['joint'][1]['counts'],{'FT_1_HT_0':1})

    def test_pending_not_loss(self):
        m=metrics([{'outcome':True,'odd':2},{'outcome':False,'odd':2},{'outcome':None,'odd':2}])
        self.assertEqual((m['settled'],m['wins'],m['losses'],m['pending'],m['hit_rate']), (2,1,1,1,50))
        self.assertEqual(m['paper_roi_pct'],0)

    def test_duplicate_is_error(self):
        r=row('1','over25_ft',.6,55,True)
        with self.assertRaises(ValueError): analyze({'2026-10-23':{'audit_date':'2026-10-23','markets':[r,r]}},'2026-10-23')

if __name__=='__main__': unittest.main()
