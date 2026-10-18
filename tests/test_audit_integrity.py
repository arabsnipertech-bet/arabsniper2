"""Offline regression tests; extract pure helpers without API dependencies."""
import ast
import json
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import *

SOURCE = Path(__file__).resolve().parents[1] / 'src' / '3appdays.py'

class AuditTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 18, 7, tzinfo=timezone.utc)
        self.store = {}
        self.ns = dict(globals(), MARKET_O05HT='over05_ht', MARKET_O15HT='over15_ht',
                       MARKET_GGHT='gg_ht', MARKET_1X2='1x2_ft', MARKET_O25='over25_ft',
                       VERSION='test', PREMATCH_AUDIT_FILE='unused',
                       safe_int=lambda x, default=0: int(x) if x is not None else default,
                       iso_now=lambda: self.now.isoformat(), now_rome=lambda: self.now,
                       read_json=lambda *args: self.store,
                       atomic_write_json=lambda path, data: self.store.update(data),
                       parse_iso=lambda x: datetime.fromisoformat(x) if x else None)
        for node in ast.parse(SOURCE.read_text()).body:
            if isinstance(node, ast.FunctionDef) and node.name in {
                '_result_payload', '_selection_won', 'capture_prematch_predictions'}:
                if node.name == '_selection_won' and '_selection_won' in self.ns:
                    self.ns['_selection_won_v113'] = self.ns['_selection_won']
                exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), self.ns)

    def test_missing_ht_is_not_zero(self):
        result = self.ns['_result_payload']({'fixture': {'status': {'short': 'FT'}},
                  'score': {'fulltime': {'home': 2, 'away': 1}, 'halftime': {}}})
        self.assertIsNone(result['home_ht'])
        for market in ('over05_ht', 'over15_ht', 'gg_ht'):
            self.assertIsNone(self.ns['_selection_won'](market, 'over', result))
        self.assertTrue(self.ns['_selection_won']('over25_ft', 'over', result))

    def test_true_zero_and_positive_scores(self):
        f = self.ns['_selection_won']
        self.assertFalse(f('over15_ht', 'over', {'home_ht': 0, 'away_ht': 0}))
        self.assertTrue(f('gg_ht', 'yes', {'home_ht': 1, 'away_ht': 1}))

    def test_snapshot_frozen_and_late_capture_rejected(self):
        fixture = {'fixture_id': 1, 'kickoff': (self.now+timedelta(hours=1)).isoformat()}
        match = {'fixture': fixture, 'model': {'probabilities_raw': {'gg_ht': {'yes': .3}}}}
        payload = {'days': [{'matches': [match]}]}
        capture = self.ns['capture_prematch_predictions']
        capture(payload)
        original = json.dumps(self.store['fixtures']['1'], sort_keys=True)
        match['model']['probabilities_raw']['gg_ht']['yes'] = .9
        capture(payload)
        self.assertEqual(original, json.dumps(self.store['fixtures']['1'], sort_keys=True))
        fixture['fixture_id'] = 2
        fixture['kickoff'] = self.now.isoformat()
        capture(payload)
        self.assertNotIn('2', self.store['fixtures'])
        fixture['fixture_id'] = 3
        fixture['kickoff'] = None
        capture(payload)
        self.assertNotIn('3', self.store['fixtures'])

if __name__ == '__main__':
    unittest.main()
