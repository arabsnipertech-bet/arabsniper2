import copy
import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from selection_studies import classify, build_studies


def row():
    return dict(statistical_probability=.60,book_probability_pct=55,odd=1.7,
                price_assessment={'reference_probability_pct':55},market='over25_ft',selection='over',
                primary=True,period='prospective',snapshot='first',model_id='context-goals-v3.4',
                fixture_id='1',fixture={'country':'Japan','league_id':98},outcome=True)


class StudiesTests(unittest.TestCase):
    def test_membership_ignores_result(self):
        r=row();a=classify(r);r['outcome']=False
        self.assertEqual(a,classify(r))
        self.assertTrue(a['agreement_below_break_even'])
        self.assertFalse(a['agreement_above_break_even'])

    def test_thresholds_and_missing(self):
        r=row();r['statistical_probability']=.5
        self.assertFalse(classify(r)['both_over_50'])
        r['statistical_probability']=.65
        self.assertTrue(classify(r)['agreement_10pp'])
        r['statistical_probability']=.651
        self.assertFalse(classify(r)['agreement_10pp'])
        r['odd']=None
        self.assertIsNone(classify(r)['agreement_below_break_even'])
        r['book_probability_pct']=None
        self.assertIsNone(classify(r))

    def test_phase_pending_matched_and_no_mutation(self):
        r=row();pending=copy.deepcopy(r);pending.update(fixture_id='2',outcome=None)
        report={'audit_date':'2026-10-21','markets':[r,pending]};original=copy.deepcopy(report)
        a=build_studies(report)
        self.assertEqual(report,original)
        g=next(g for g in a['groups'] if g['scope']=='primary_only' and g['country']=='*' and g['rule']=='agreement_10pp')
        self.assertEqual((g['fixtures'],g['pending'],g['statistics']['n'],g['book']['n']),(2,1,1,1))
        self.assertEqual(g['phase'],'exploratory')
        report['audit_date']='2026-10-22'
        self.assertTrue(all(g['phase']=='validation' for g in build_studies(report)['groups']))

    def test_missing_data_not_a_rejection(self):
        r=row();r['book_probability_pct']=None
        a=build_studies({'audit_date':'2026-10-22','markets':[r]})
        self.assertTrue(all(g['membership']=='missing_data' and g['statistics']['n']==0 for g in a['groups']))
