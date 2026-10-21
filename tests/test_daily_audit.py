import copy
import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from daily_audit import build_daily, fingerprint, markdown


def record(captured='2026-10-20T10:00:00+00:00'):
    return {'fixture': {'fixture_id': 1, 'kickoff': '2026-10-20T18:00:00+00:00'},
            'captured_at': captured, 'model_version': 'context-goals-v3.4',
            'multimarket': {'model_id': 'context-goals-v3.4',
                'best_candidate': {'market': 'over15_ht', 'selection': 'over'},
                'markets': [{'market': 'over15_ht', 'selection': 'over', 'odd': 2.5,
                             'probabilities': {'statistics': .4}, 'book_probability_pct': 38}]},
            'settlement': {'result': {'status': 'FT', 'home_ht': 1, 'away_ht': 1,
                                      'home_ft': 2, 'away_ft': 1}}}


class DailyTests(unittest.TestCase):
    def report(self, r):
        return build_daily({'multimarket_evaluations': {'v::1': r}}, '2026-10-20', 'now')

    def test_no_mutation_and_hash_excludes_only_settlement(self):
        r = record(); before = copy.deepcopy(r)
        d = self.report(r)
        self.assertEqual(r, before)
        self.assertEqual(len(d['fixtures']), 2)
        changed = copy.deepcopy(r); changed['settlement'] = {}
        self.assertEqual(fingerprint(r), fingerprint(changed))
        changed['multimarket']['markets'][0]['odd'] = 3
        self.assertNotEqual(fingerprint(r), fingerprint(changed))
        self.assertIn('Primary pending', markdown(d))

    def test_missing_ht_is_pending_not_loss(self):
        r = record(); r['settlement']['result']['home_ht'] = None
        rows = [x for x in self.report(r)['markets'] if x['primary']]
        self.assertTrue(all(x['outcome'] is None and x['paper_profit'] is None for x in rows))
        self.assertEqual(rows[0]['pending_reason'], 'missing_score')

    def test_missing_quote_excluded_from_roi(self):
        r = record(); r['multimarket']['markets'][0]['odd'] = None
        for group in self.report(r)['groups']:
            self.assertEqual(group['priced_n'], 0)
            self.assertIsNone(group['paper_roi_pct'])
            self.assertEqual(group['wins'], 1)

    def test_updates_deduplicated_and_late_capture_excluded(self):
        r = record(); later = record('2026-10-20T17:00:00+00:00')
        late = record('2026-10-20T18:00:00+00:00')
        store = {'multimarket_evaluations': {'v::1': r, 'duplicate': r},
                 'analysis_updates': {'a': later, 'b': later, 'c': late}}
        d = build_daily(store, '2026-10-20', 'now')
        self.assertEqual(len(d['fixtures']), 2)
        self.assertEqual(len(d['excluded_records']), 1)
        self.assertEqual(d['fixtures'][1]['captured_at'], later['captured_at'])
        self.assertTrue(all(g['n'] == 1 for g in d['groups']))

    def test_no_pick_and_retrospective_separate(self):
        r = record(); r['retrospective'] = True; r['multimarket']['best_candidate'] = None
        d = self.report(r)
        self.assertTrue(all(f['primary_market'] is None and f['period']=='retrospective' for f in d['fixtures']))
        self.assertFalse(any(x['primary'] for x in d['markets']))

    def test_live_score_does_not_settle(self):
        r = record(); r['settlement']['result']['status'] = '2H'
        self.assertTrue(all(x['outcome'] is None for x in self.report(r)['markets']))

if __name__ == '__main__': unittest.main()
