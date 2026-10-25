import unittest
from copy import deepcopy
from test_context_analysis import evaluate,match,NOW
from context_analysis import over25_signals

class OciSelectionTests(unittest.TestCase):
    def rows(self):
        def row(m,s,oci,previous):
            return dict(market=m,selection=s,movement=dict(confirmed=True,paired_books=3,agreeing_books=2,oci=oci,oci_previous=previous,direction='down',amplitude_pct=3))
        return [row('1x2_ft','1',2,1),row('1x2_ft','2',1,2),row('over25_ft','over',0,0)]
    def test_rules_and_missing_previous(self):
        rows=self.rows();before=deepcopy(rows)
        self.assertEqual(over25_signals(rows,NOW)['rules'],['A','B','C'])
        self.assertEqual(rows,before)
        for patch in [dict(oci_previous=None),dict(oci_previous=1),dict(confirmed=False),dict(paired_books=4)]:
            r=deepcopy(rows);r[1]['movement'].update(patch)
            self.assertEqual(over25_signals(r,NOW)['rules'],[])
        rows[2]['movement']['amplitude_pct']=2.99
        self.assertEqual(over25_signals(rows,NOW)['rules'],['A','B'])
        rows[0]['movement']['oci_previous']=None
        self.assertEqual(over25_signals(rows,NOW)['rules'],['A'])
    def test_one_operator_for_complete_1x2_and_frozen_metadata(self):
        books=[dict(bookmaker=name,observed_at=NOW,provider_updated_at=NOW,odds=odds) for name,odds in [('A',{'1':1.5,'x':4,'2':5}),('B',{'1':1.6,'x':5,'2':3}),('C',{'1':1.7,'x':3,'2':4})]]
        a=evaluate(match(),NOW,{'1x2_ft':books})
        rows=[r for r in a['markets'] if r['market']=='1x2_ft']
        self.assertEqual({r['quote']['bookmaker'] for r in rows},{'B'})
        self.assertEqual([r['odd'] for r in rows],[1.6,5,3])
        over=next(r for r in a['markets'] if r['market']=='over25_ft' and r['selection']=='over')
        self.assertEqual(over['over25_signals'],a['over25_signals'])
        self.assertEqual(a['over25_signals']['probability_bonus'],0)
