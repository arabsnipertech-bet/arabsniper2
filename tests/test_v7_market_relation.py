"""Exact price relations must explain, never inflate or independently vote."""
from copy import deepcopy
from math import log
from pathlib import Path
import json
import random
import sys
import unittest

sys.path[:0] = [str(Path(__file__).resolve().parents[1]/'src'), str(Path(__file__).resolve().parent)]
from context_analysis import movement
from lateral_matrix import market_id, specification
from matrix_quotes import analyse_prices
from quote_cache import mark_stable
from test_v7 import AT, META, RATES, assessment
from v7_decision import capture, market_origin
from v7_probability import goal_structure, movement_relation, pricing_evidence, value_relation

FROM = '2026-11-09T21:30:00+01:00'


def estimate(p):
    return dict(p=p, win_weight=p, loss_weight=1-p, push_probability=0.,
                half_win_probability=0., half_loss_probability=0.)


def price(q=4., margin=8.):
    return dict(odd=q, bookmaker_id='1', quote_status='recent', quote_observed_at=AT,
                provider_updated_at=AT, quote_executable=True,
                selected_margin_pct=margin, selected_market_fair_pct=100/q/(1+margin/100))


def moving(q0=2.16, q1=2.10, m0=5., m1=8.):
    r = price(q1, m1)
    r['selected_movement'] = dict(observed_from=FROM, observed_to=AT,
        delta_pct=100*(q1/q0-1), price_reference=dict(basis='same_book_complete_partition',
            bookmaker_id='1', odd_from=q0, odd_to=q1, fair_from=1/q0/(1+m0/100),
            fair_to=1/q1/(1+m1/100), margin_from_pct=m0, margin_to_pct=m1))
    return r


class BinaryValueRelation(unittest.TestCase):
    def test_above_fair_can_still_fall_short_of_margin(self):
        row, model = price(), estimate(.24)
        before = deepcopy((row, model))
        value = value_relation(row, model)
        self.assertEqual(value['status'], 'verified')
        self.assertGreater(value['edge_fair_pp'], 0)
        self.assertTrue(value['fair_advantage_below_margin'])
        self.assertAlmostEqual(value['ev_pct'], -4.)
        self.assertLess(value['relative_advantage_pct'], value['margin_pct'])
        self.assertAlmostEqual(value['identity_error_pp'], 0)
        self.assertEqual((row, model), before)

    def test_positive_ev_and_break_even_are_not_charged_twice(self):
        for p in [.25, .26, .80]:
            v = value_relation(price(), estimate(p))
            self.assertEqual(v['status'], 'verified')
            self.assertAlmostEqual(v['ev_pct'], 100*(p*4-1))
            self.assertEqual(v['break_even_probability'], .25)
            self.assertFalse(v['fair_advantage_below_margin'])

    def test_a_margin_expansion_at_fixed_price_does_not_raise_ev(self):
        low, high = (value_relation(price(margin=m), estimate(.26)) for m in [5., 10.])
        self.assertLess(low['edge_fair_pp'], high['edge_fair_pp'])
        self.assertEqual(low['ev_pct'], high['ev_pct'])
        self.assertEqual(low['break_even_probability'], high['break_even_probability'])

    def test_unknown_operator_missing_fair_and_inconsistent_fair_fail_closed(self):
        for changes, status in [(dict(bookmaker_id=None), 'same_book_fair_unavailable'),
                                (dict(selected_market_fair_pct=None), 'same_book_fair_unavailable'),
                                (dict(selected_market_fair_pct=35.), 'inconsistent_price_reference')]:
            with self.subTest(changes=changes):
                self.assertEqual(value_relation(dict(price(), **changes), estimate(.26))['status'], status)

    def test_push_and_partial_payouts_keep_settlement_weight_ev(self):
        e = dict(estimate(.5), win_weight=.45, loss_weight=.45, push_probability=.10)
        proof = pricing_evidence(price(), e, e, e, True, as_of=AT)
        self.assertEqual(proof['basis'], 'settlement_weights')
        self.assertEqual(proof['value_relation'], dict(status='settlement_weights_only'))
        self.assertEqual(proof['movement_relation'], dict(status='settlement_weights_only'))
        self.assertIsNone(proof['break_even_probability'])
        self.assertAlmostEqual(proof['raw_ev'], .45*3-.45)

    def test_related_goal_estimates_do_not_become_independent_votes(self):
        proof = pricing_evidence(price(), estimate(.26), estimate(.30), estimate(.29), True, as_of=AT)
        self.assertFalse(proof['statistical_votes_independent'])
        self.assertEqual(proof['relations_version'], 'V7_MARKET_RELATION_V1')
        self.assertIsNone(pricing_evidence(price(), estimate(.26), estimate(.30), estimate(.29), False))


class ComparableMovementRelation(unittest.TestCase):
    def test_margin_only_drop_is_not_fair_probability_confirmation(self):
        row = moving()
        before = deepcopy(row)
        proof = movement_relation(row, AT)
        self.assertEqual(proof['status'], 'verified')
        self.assertLess(proof['net_delta_pct'], 0)
        self.assertAlmostEqual(proof['fair_delta_pp'], 0)
        self.assertFalse(proof['fair_rises'])
        self.assertAlmostEqual(proof['margin_share_of_log_move'], 1)
        self.assertAlmostEqual(proof['identity_error'], 0)
        self.assertEqual(row, before)

    def test_constant_margin_drop_is_all_fair_movement(self):
        proof = movement_relation(moving(m1=5.), AT)
        self.assertTrue(proof['fair_rises'])
        self.assertEqual(proof['margin_delta_pp'], 0)
        self.assertEqual(proof['margin_share_of_log_move'], 0)
        self.assertAlmostEqual(proof['fair_log_move'], proof['implied_log_move'])

    def test_equal_price_can_hide_opposing_fair_and_margin_movements(self):
        proof = movement_relation(moving(q0=2.10), AT)
        self.assertEqual(proof['status'], 'verified')
        self.assertEqual(proof['net_delta_pct'], 0)
        self.assertIsNone(proof['margin_share_of_log_move'])
        self.assertLess(proof['fair_log_move'], 0)
        self.assertGreater(proof['margin_log_move'], 0)
        self.assertAlmostEqual(proof['identity_error'], 0)

    def test_whole_window_is_not_replaced_by_post_reversal_leg(self):
        r = moving(q0=2.20, q1=2.40, m0=5., m1=5.)
        r['selected_movement'].update(reversal=True, delta_pct=100*(2.4/2.6-1), fair_delta_pp=2.)
        proof = movement_relation(r, AT)
        self.assertGreater(proof['net_delta_pct'], 0)
        self.assertLess(proof['fair_delta_pp'], 0)
        self.assertLess(r['selected_movement']['delta_pct'], 0)

    def test_other_operator_or_legacy_missing_reference_is_unavailable(self):
        r = moving(); r['selected_movement']['price_reference']['bookmaker_id'] = '2'
        self.assertEqual(movement_relation(r, AT)['status'], 'inconsistent_bookmaker')
        self.assertEqual(movement_relation(price(), AT)['status'], 'comparable_reference_unavailable')

    def test_future_or_unordered_window_is_rejected(self):
        for changes in [dict(observed_from=AT), dict(observed_to='2026-11-10T12:00:00+01:00'),
                        dict(observed_from=None)]:
            r = moving(); r['selected_movement'].update(changes)
            self.assertEqual(movement_relation(r, AT)['status'], 'inconsistent_observation_window')

    def test_current_selected_price_must_match_the_window_endpoint(self):
        for changes in [dict(odd=2.20), dict(selected_margin_pct=9.), dict(selected_market_fair_pct=50.)]:
            self.assertEqual(movement_relation(dict(moving(), **changes), AT)['status'], 'inconsistent_price_reference')

    def test_cached_or_stable_quotes_do_not_revive_historical_live_flow(self):
        for changes in [dict(prematch_stable=True), dict(quote_status='cached_stable')]:
            self.assertEqual(movement_relation(dict(moving(), **changes), AT)['status'], 'stable_quote_no_live_flow')

    def test_exact_identity_over_a_reproducible_price_grid(self):
        rng = random.Random(7341)
        count = 0
        for _ in range(150):
            q0, q1, m0, m1 = rng.uniform(1.4, 7), rng.uniform(1.4, 7), rng.uniform(1, 15), rng.uniform(1, 15)
            proof = movement_relation(moving(q0, q1, m0, m1), AT)
            self.assertEqual(proof['status'], 'verified')
            self.assertAlmostEqual(proof['implied_log_move'], log(q0/q1))
            self.assertLess(abs(proof['identity_error']), 1e-12)
            count += 1
        self.assertEqual(count, 150)


class PipelineAndExplanations(unittest.TestCase):
    def test_existing_computed_partition_movements_are_preserved_without_new_collection(self):
        spec = specification('btts', 'ft', 'yes'); token = market_id(spec)
        other = market_id(specification('btts', 'ft', 'no'))
        odds0, odds1 = {token:2.16, other:1.65}, {token:2.10, other:1.68}
        trace = [dict(observed_at=FROM, odds=odds0), dict(observed_at=AT, odds=odds1)]
        offer = dict(complete=True, odds=odds1, token=token, group='gg-ft', odd=2.10,
                     first_odd=2.16, first_at=FROM, observed_at=AT, provider_updated_at=AT,
                     bookmaker_id='1', bookmaker='Book 1', bet_id=8, trace=trace)
        row = dict(price(), id=token, spec=spec, family='btts', period='ft', quality='supported',
                   statistical_pct=55., low_pct=50., high_pct=60., win_weight=.55, loss_weight=.45,
                   sensitivity_weights=[[.50,.50],[.55,.45]])
        original_offer = deepcopy(offer)
        before_movement = movement([offer], token)
        analyse_prices(row, [offer], AT, META['kickoff'])
        proof = movement_relation(row, AT)
        self.assertEqual(proof['status'], 'verified')
        self.assertAlmostEqual(proof['fair_delta_pp'], before_movement['operators'][0]['fair_delta_pp'])
        self.assertEqual(row['selected_movement']['delta_pct'], before_movement['operators'][0]['delta_pct'])
        self.assertEqual(offer, original_offer)
        mark_stable(row, META['kickoff'])
        self.assertEqual(movement_relation(row, AT)['status'], 'stable_quote_no_live_flow')

    def test_added_evidence_cannot_change_official_probability_ev_tier_or_triade(self):
        a, b = assessment(), assessment()
        for item in a['matrix']['rows']:
            item['selected_market_fair_pct'] = 100/item['odd']/1.05
        b = deepcopy(a)
        for item in b['matrix']['rows']:
            q = item['odd']; item['selected_movement'] = moving(q*1.02, q, 5., 5.)['selected_movement']
        baseline, revised = capture(a, META, AT), capture(b, META, AT)
        fields = ('raw','statistics','corrected','calibrated','fair','odd','ev','ev_low','tier')
        for key in baseline['markets']:
            self.assertEqual({f:baseline['markets'][key][f] for f in fields},
                             {f:revised['markets'][key][f] for f in fields})
        for role, old in baseline['triad'].items():
            new = revised['triad'][role]
            self.assertEqual(None if old is None else (old['market_id'],old['tier'],old['probability'],old['ev']),
                             None if new is None else (new['market_id'],new['tier'],new['probability'],new['ev']))
        selected = {pick['market_id'] for pick in revised['triad'].values() if pick}
        enriched = {key for key, record in revised['markets'].items()
                    if (record['pricing_evidence'] or {}).get('relations_version') == 'V7_MARKET_RELATION_V1'}
        self.assertEqual(enriched, selected)
        self.assertTrue(any((m['pricing_evidence'] or {}).get('movement_relation',{}).get('status')=='verified'
                            for m in revised['markets'].values()))
        json.dumps(revised, allow_nan=False)

    def test_tooltip_explains_an_uncovered_margin_without_claiming_positive_value(self):
        spec = specification('btts','ht','yes'); e = estimate(.24); r = moving(q0=4.1,q1=4.,m0=8.,m1=8.)
        record = dict(calibrated=.24, model_fair_odd=1/.24, niche_id=None, odd=4., ev=-.04, ev_low=-.06,
                      features={}, quote={}, tier='BLU', pricing_evidence=pricing_evidence(r,e,e,e,True,as_of=AT))
        rates = dict(RATES,home_st=RATES['home_ft']-RATES['home_ht'],away_st=RATES['away_ft']-RATES['away_ht'])
        origin = market_origin(spec,rates,record)
        self.assertIn('non copre interamente l’aggio',origin['origin_tooltip'])
        self.assertIn('Pareggio economico 25,0%',origin['origin_tooltip'])
        self.assertIn('Finestra stesso operatore',origin['origin_tooltip'])

    def test_period_structure_uses_the_same_rates_without_a_probability_bonus(self):
        rates=dict(home_ht=.7,away_ht=.7,home_st=.8,away_st=.3)
        s=goal_structure(rates)
        self.assertEqual(s['periods']['pt']['symmetry'],1.)
        self.assertAlmostEqual(s['periods']['st']['weaker_side_lambda'],.3)
        self.assertAlmostEqual(s['periods']['ft']['lambda_total'],s['lambda_ft'])
        self.assertAlmostEqual(s['periods']['ft']['lambda_home'],1.5)


if __name__ == '__main__': unittest.main()
