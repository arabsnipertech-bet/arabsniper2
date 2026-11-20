"""The reference price, settlement and dependent markets must agree."""
from copy import deepcopy
from math import exp
from pathlib import Path
import random
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from adaptive_pricing import reprice
from lateral_matrix import specification
from v7_decision import GG, route
from v7_probability import VERSION, goal_structure, pricing_evidence, raw_rates, score_model


class SelectedPriceEvidence(unittest.TestCase):
    def row(self):
        return dict(probability_model=VERSION, quote_status='recent', quality='supported', family='btts',
                    win_weight=.28, loss_weight=.72, odd=4., first_odd=4.,
                    sensitivity_weights=[[.26, .74], [.28, .72]],
                    market_fair_pct=30., selected_margin_pct=10.,
                    selected_market_fair_pct=100/4/1.10)

    def test_selected_operator_edge_not_consensus_and_ev_not_charged_twice(self):
        row = self.row()
        reprice(row)
        self.assertAlmostEqual(row['ev_pct'], 12.)
        self.assertAlmostEqual(row['ev_low_pct'], 4.)
        self.assertAlmostEqual(row['edge_pp'], 28.-100/4/1.10)
        self.assertGreater(row['edge_pp'], 0)
        self.assertEqual(row['market_fair_pct'], 30.)
        self.assertEqual(row['edge_fair_basis'], 'selected_bookmaker')
        self.assertEqual((row['win_weight'], row['loss_weight'], row['odd']), (.28, .72, 4.))

    def test_consensus_cannot_replace_a_missing_same_operator_fair(self):
        row = self.row()
        row['selected_market_fair_pct'] = None
        reprice(row)
        self.assertIsNone(row['edge_pp'])
        self.assertEqual(row['ev_pct'], 12.)

    def test_legacy_pricing_keeps_its_original_reference(self):
        row = self.row()
        row.pop('probability_model')
        row['edge_fair_basis'] = 'selected_bookmaker'
        reprice(row)
        self.assertAlmostEqual(row['edge_pp'], -2.)
        self.assertNotIn('edge_fair_basis', row)

    def test_related_ev_estimates_use_one_price_without_changing_inputs(self):
        row = self.row()
        p = dict(p=.23, win_weight=.23, loss_weight=.77,
                 push_probability=0., half_win_probability=0., half_loss_probability=0.)
        raw, stats = dict(p, p=.28, win_weight=.28, loss_weight=.72), dict(p, p=.27, win_weight=.27, loss_weight=.73)
        original = deepcopy((row, p, raw, stats))
        proof = pricing_evidence(row, p, raw, stats, True)
        self.assertEqual(proof['basis'], 'binary')
        self.assertEqual(proof['break_even_probability'], .25)
        self.assertAlmostEqual(proof['raw_ev'], .12)
        self.assertAlmostEqual(proof['statistical_ev'], .08)
        self.assertAlmostEqual(proof['fair_identity_error_pp'], 0)
        self.assertAlmostEqual(p['win_weight']*(row['odd']-1)-p['loss_weight'], -.08)
        self.assertEqual((row, p, raw, stats), original)

    def test_unusable_price_cannot_supply_evidence(self):
        p = dict(win_weight=.3, loss_weight=.7)
        for odd, usable in ((None, True), (4., False), (float('nan'), True), (1., True)):
            self.assertIsNone(pricing_evidence(dict(self.row(), odd=odd), p, p, p, usable))

    def test_push_and_quarter_lines_do_not_use_binary_probability_formula(self):
        model = score_model(raw_rates({'rates': dict(home_ft=1.5, away_ft=1., home_ht=.6, away_ht=.4)}))
        for spec in [specification('dnb', 'ft', '1'), specification('asian_total', 'ft', 'over', 2.25)]:
            estimate = model.estimate(spec)
            row = dict(self.row(), odd=2.)
            proof = pricing_evidence(row, estimate, estimate, estimate, True)
            self.assertEqual(proof['basis'], 'settlement_weights')
            self.assertIsNone(proof['break_even_probability'])
            expected = estimate['win_weight']-estimate['loss_weight']
            self.assertAlmostEqual(proof['raw_ev'], expected)
            self.assertGreater(abs(expected-(estimate['p']*2-1)), .01)


class GoalStructureAndCoherence(unittest.TestCase):
    def model(self, home, away, home_st=.8, away_st=.7):
        rates = dict(home_ht=home, away_ht=away, home_st=home_st, away_st=away_st,
                     home_ft=home+home_st, away_ft=away+away_st)
        return rates, score_model(rates)

    def test_same_pt_volume_equal_over_probability_different_gg(self):
        balanced, a = self.model(1., 1.)
        asymmetric, b = self.model(1.6, .4)
        over, gg = specification('total', 'ht', 'over', 1.5), specification('btts', 'ht', 'yes')
        self.assertAlmostEqual(a.estimate(over)['p'], b.estimate(over)['p'], places=10)
        self.assertGreater(a.estimate(gg)['p'], b.estimate(gg)['p'])
        self.assertEqual(goal_structure(balanced)['pt_symmetry'], 1.)
        self.assertAlmostEqual(goal_structure(asymmetric)['pt_symmetry'], .64)
        # Asymmetry alone cannot imply that the offered price has no value.
        self.assertGreater(b.estimate(gg)['p']*4.2-1, 0)

    def test_zero_missing_and_tiny_inputs_are_distinguished(self):
        rates, _ = self.model(0., 0.)
        self.assertIsNone(goal_structure(rates)['pt_symmetry'])
        self.assertEqual(goal_structure(rates)['pt_share'], 0.)
        tiny = dict(home_ht=1e-200, away_ht=1e-200, home_st=1e-200, away_st=1e-200)
        self.assertEqual(goal_structure(tiny)['pt_symmetry'], 1.)
        self.assertEqual(goal_structure(tiny)['pt_share'], .5)
        for bad in (None, -1., float('nan'), True):
            self.assertIsNone(goal_structure(dict(tiny, home_ht=bad)))

    def test_nested_and_joint_markets_follow_one_distribution(self):
        rng = random.Random(20261120)
        for _ in range(48):
            rates, m = self.model(*(rng.uniform(.01, 2.8) for _ in range(4)))
            structure = goal_structure(rates)
            self.assertAlmostEqual(structure['lambda_ft'], rates['home_ft']+rates['away_ft'])
            self.assertTrue(0 <= structure['pt_share'] <= 1)
            self.assertTrue(0 <= structure['pt_symmetry'] <= 1)
            def p(kind, period, side, line=None, extra=None):
                return m.estimate(specification(kind, period, side, line, extra))['p']
            self.assertLessEqual(p('btts', 'ht', 'yes'), p('total', 'ht', 'over', 1.5)+1e-10)
            for period, lines in [('ht', [.5, 1.5, 2.5]), ('ft', [1.5, 2.5, 3.5])]:
                probs = [p('total', period, 'over', x) for x in lines]
                self.assertTrue(all(a+1e-10 >= b for a,b in zip(probs, probs[1:])))
            joint = p('pt_ft_total', 'ft', 'over', 1.5, 2.5)
            self.assertLessEqual(joint, p('total', 'ht', 'over', 1.5)+1e-10)
            self.assertLessEqual(joint, p('total', 'ft', 'over', 2.5)+1e-10)
        # GG PT already implies Over 1.5 PT; multiplying them is wrong.
        _, m = self.model(.8, .7)
        gg = m.estimate(specification('btts', 'ht', 'yes'))['p']
        over = m.estimate(specification('total', 'ht', 'over', 1.5))['p']
        self.assertGreater(gg, gg*over)


class ColpaccioAdmissionUnchanged(unittest.TestCase):
    def test_25_percent_gg_at_420_is_already_a_valid_blue_colpaccio(self):
        record = dict(source_market_id=GG, label='GG PT', calibrated=.25, odd=4.2,
            ev=.05, ev_low=.02, tier='BLU', niche_id=None, horizon='PT',
            spec_kind='btts', model_fair_odd=4., quote_state='moving',
            quote={'first_odd':4.2}, features={})
        observed = dict(conversion=False, rebound_gg=False)
        triad, _ = route({GG:record}, observed)
        self.assertEqual(triad['high_odds_shot']['market_id'], GG)
        self.assertEqual(triad['high_odds_shot']['tier'], 'BLU')
        # Neither retrospective returns nor the new related diagnostic EVs
        # may be additional votes or become a direct admission filter.
        for roi in [-1e6, 1e6]:
            changed = dict(record, roi=roi, pricing_evidence={'raw_ev':1e6, 'statistical_ev':-1e6})
            self.assertEqual(route({GG:changed}, observed)[0], triad)


if __name__ == '__main__':
    unittest.main()
