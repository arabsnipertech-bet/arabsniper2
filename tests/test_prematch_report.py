import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from prematch_audit import event_outcome, settle_predictions, build_report

class ReportTests(unittest.TestCase):
    def test_missing_and_real_zero(self):
        self.assertIsNone(event_outcome('gg_ht', {'home_ht': None, 'away_ht': 0}))
        self.assertFalse(event_outcome('over15_ht', {'home_ht': 0, 'away_ht': 0}))
        self.assertTrue(event_outcome('over25_ft', {'home_ft': 2, 'away_ft': 1}))

    def test_settlement_preserves_forecast_and_report_counts(self):
        record = {'probabilities_calibrated': {'over15_ht': {'over': .6}, 'gg_ht': {'yes': .3}},
                  'ranked_choices': [{'market': 'over15_ht', 'selection': 'over', 'relation': 'confirmed', 'odd': 2.0}]}
        store = {'fixtures': {'1': record}}
        result = {'status': 'FT', 'home_ht': 1, 'away_ht': 1, 'home_ft': 2, 'away_ft': 1}
        output = {'days': [{'matches': [{'fixture': {'fixture_id': 1, 'result': result}}]}]}
        settle_predictions(store, output); settle_predictions(store, output)
        rows = build_report(store)['groups']
        row = next(x for x in rows if x['market']=='over15_ht' and x['relation']=='all')
        self.assertEqual(row['n'], 1)
        self.assertEqual(row['roi_pct'], 100)
        self.assertEqual(row['brier_score'], .16)
        self.assertEqual(record['probabilities_calibrated']['over15_ht']['over'], .6)
        self.assertEqual(build_report({'fixtures': {}})['groups'], [])

    def test_unfrozen_results_and_live_results_excluded(self):
        store = {'fixtures': {'1': {}}}
        settle_predictions(store, {'days': [{'matches': [{'fixture': {'fixture_id': 1, 'result': {'status': 'HT'}}}]}]})
        self.assertNotIn('settlement', store['fixtures']['1'])
        self.assertEqual(build_report(store)['groups'], [])

if __name__ == '__main__':
    unittest.main()
