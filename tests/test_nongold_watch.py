import sys
import unittest
from copy import deepcopy
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from selection_studies import build_nongold_ft_watch

class NonGoldWatch(unittest.TestCase):
    def row(self, p=.4,b=55,odd=1.8,outcome=True):
        return dict(model_id='context-goals-v3.4',period='prospective',market='over25_ft',selection='over',candidate_present=True,snapshot='latest',primary=True,statistical_probability=p,book_probability_pct=b,odd=odd,outcome=outcome)
    def test_gold_and_missing_excluded_not_relabelled(self):
        rows=[self.row(),self.row(.6,55),self.row(.4,None),self.row(.4,55,None),self.row(.4,45,outcome=False)]
        before=deepcopy(rows)
        report=build_nongold_ft_watch({'audit_date':'2026-10-23','markets':rows})
        self.assertEqual(report['phase'],'exploratory')
        g=[g for g in report['groups'] if g['scope']=='all_candidates']
        self.assertEqual(sum(g['total'] for g in g),2)
        self.assertEqual(next(x for x in g if x['membership']=='selected')['statistics']['wins'],1)
        self.assertEqual(rows,before)
    def test_future_validation_and_pending(self):
        report=build_nongold_ft_watch({'audit_date':'2026-10-24','markets':[self.row(outcome=None)]})
        self.assertEqual(report['phase'],'validation')
        self.assertEqual(report['groups'][0]['pending'],1)
        self.assertEqual(report['groups'][0]['statistics']['n'],0)
    def test_no_ht_or_retrospective(self):
        a=self.row();a['market']='over15_ht'
        b=self.row();b['period']='retrospective'
        report=build_nongold_ft_watch({'audit_date':'2026-10-24','markets':[a,b]})
        self.assertEqual(report['groups'],[])

if __name__=='__main__':unittest.main()
