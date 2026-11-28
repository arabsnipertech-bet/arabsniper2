import json
import unittest

from test_v7_surgical_quality import record
from v7_decision import (GOAL, OVER_CARRY, complete_prematch_slots, prefer_reliable_half_over,
                         replace_recommended_over_combo)


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
        row.update(odd=None, model_fair_odd=1/.48)
        triad = {'recommended_alternative': {'market_id': OVER_CARRY, 'odd': 6.}}
        signals = {'consigliata': triad['recommended_alternative']}
        replace_recommended_over_combo(triad, signals, {key: row})
        complete_prematch_slots(triad, {key: row}, replacing_combo=True)
        selected = triad['recommended_alternative']
        self.assertEqual(selected['market_id'], key)
        self.assertEqual(selected['quote_state'], 'model_estimate')
        self.assertIsNone(selected['odd'])
        self.assertIsNone(selected['ev'])
        self.assertIsNone(signals['consigliata'])

    def test_complete_all_three_without_book_prices(self):
        keys = [GOAL[k] for k in ('over15_ht','over25_ht','over15_sh','over25_sh','over25_ft')]
        rows = {k: record(json.loads(k), .45, 2.3)[1] for k in keys}
        for r in rows.values():
            r.update(odd=None,model_fair_odd=1/.45)
        triad = dict.fromkeys(('most_probable','recommended_alternative','high_odds_shot'))
        complete_prematch_slots(triad, rows)
        self.assertEqual(len({p['market_id'] for p in triad.values()}), 3)
        self.assertTrue(all(p['study_only'] and p['odd'] is None for p in triad.values()))

    def test_three_goal_recommendation_compares_available_singles_after_completion(self):
        role = 'recommended_alternative'
        high, low = GOAL['over25_ht'], GOAL['over15_ht']
        current = {'market_id': high, 'probability': .092}
        triad = {role: current}
        rows = {low: record(json.loads(low), .29, 3.1)[1]}
        signals = {'consigliata': current}
        prefer_reliable_half_over(triad, signals, {})
        self.assertIs(triad[role], current)
        prefer_reliable_half_over(triad, signals, rows)
        self.assertEqual((triad[role]['market_id'], triad[role]['odd']), (low, 3.1))
        self.assertIsNone(signals['consigliata'])

    def test_carpi_same_half_improves_both_probability_and_ev_as_observation(self):
        role = 'recommended_alternative'
        high, low, other = (GOAL[x] for x in ('over25_ht', 'over15_ht', 'over15_sh'))
        rows = {k: record(json.loads(k), p, odd)[1] for k, p, odd in
                ((high, .11959262, 7.), (low, .33583621, 2.63), (other, .43608709, 1.95))}
        for key in (low, other):
            rows[key]['opposite_conflict_flag'] = True
        pick = {'market_id': high, 'probability': rows[high]['calibrated']}
        triad, signals = {role: pick}, {'consigliata': pick}
        prefer_reliable_half_over(triad, signals, rows)
        self.assertEqual((triad[role]['market_id'], triad[role]['odd']), (low, 2.63))
        self.assertTrue(triad[role]['study_only'] and triad[role]['opposite_conflict_flag'])
        self.assertIsNone(signals['consigliata'])
        rows[low]['ev'] = rows[high]['ev'] - .01
        triad[role] = pick
        prefer_reliable_half_over(triad, signals, rows)
        self.assertIs(triad[role], pick)
