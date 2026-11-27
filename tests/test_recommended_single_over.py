import json
import unittest

from test_v7_surgical_quality import record
from v7_decision import GOAL, OVER_CARRY, replace_recommended_over_combo


class RecommendedSingleOver(unittest.TestCase):
    def test_each_half_line_uses_its_own_contract(self):
        for name in ('over15_ht', 'over25_ht', 'over15_sh', 'over25_sh'):
            key = GOAL[name]
            _, row, _ = record(json.loads(key), .46, 2.5)
            triad = {'recommended_alternative': {'market_id': OVER_CARRY, 'odd': 7.}}
            signals = {}
            replace_recommended_over_combo(triad, signals, {key: row})
            pick = triad['recommended_alternative']
            self.assertEqual((pick['market_id'], pick['source_market_id'], pick['odd'], pick['probability']),
                             (key, key, 2.5, .46))
            self.assertIs(pick, signals['consigliata'])

    def test_highest_probability_and_other_roles(self):
        keys = [GOAL[name] for name in ('over15_ht','over15_sh','over25_ht')]
        rows = {key: record(json.loads(key), p, odd)[1] for key,p,odd in zip(keys,(.48,.52,.56),(2.6,2.2,4.))}
        shot = {'market_id': keys[2]}
        triad = {'recommended_alternative': {'market_id': OVER_CARRY}, 'high_odds_shot': shot}
        replace_recommended_over_combo(triad, {}, rows)
        self.assertEqual(triad['recommended_alternative']['market_id'], keys[1])
        self.assertIs(triad['high_odds_shot'], shot)
        self.assertFalse(replace_recommended_over_combo(triad, {}, rows))

    def test_unpriced_single_does_not_inherit_combo_price(self):
        key = GOAL['over15_ht']
        row = record(json.loads(key), .48, 2.5)[1]
        row['odd'] = None
        triad = {'recommended_alternative': {'market_id': OVER_CARRY, 'odd': 6.}}
        signals = {'consigliata': triad['recommended_alternative']}
        replace_recommended_over_combo(triad, signals, {key: row})
        self.assertIsNone(triad['recommended_alternative'])
        self.assertIsNone(signals['consigliata'])
