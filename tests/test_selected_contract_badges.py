"""The promotion pass may only decorate a pick already emitted by routing."""
from copy import deepcopy
import unittest
from unittest.mock import patch

from test_v7 import AT, META, assessment
from test_v7_surgical_quality import record
from lateral_matrix import specification
from opportunity_policy import evaluate
from v7_decision import capture, promote_selected_contract


class SelectedContractBadges(unittest.TestCase):
    def fixture(self, spec):
        key, row, source = record(spec, .55, 2.1)
        source.update(quality='supported', statistical_pct=55., ev_pct=15.5,
                      ev_low_pct=5., first_odd=2.2, bookmaker='A', oci=38,
                      movement_confirmed=True, fair_direction_coherent=True,
                      agreeing_books=2, paired_books=2, delta_pct=100*(2.1/2.2-1),
                      selected_movement=dict(delta_pct=100*(2.1/2.2-1), fair_delta_pp=1.,
                                             persistence=.75, reversal=False))
        source['opportunity'] = evaluate(source)
        row['confirmation'] = dict(qualification_type='documented_value', value_qualified=True)
        pick = dict(market_id=key, source_market_id=key, odd=2.1, tier='BLU')
        return pick, row, source

    def test_official_roles_keep_execution_contract(self):
        for spec in [specification('result','ft','1'), specification('result','ft','2'),
                     specification('btts','ft','yes'), specification('total','ht','under',.5)]:
            with self.subTest(spec=spec):
                pick, row, source = self.fixture(spec)
                immutable = (pick['market_id'], pick['odd'], row['calibrated'], row['ev'])
                source['agreeing_books'] = 1
                promote_selected_contract(pick, row, source, AT)
                self.assertEqual(pick['tier'], 'GOLD', pick)
                self.assertEqual((pick['market_id'],pick['odd'],row['calibrated'],row['ev']), immutable)
                self.assertEqual(row['tier'], 'GOLD')

    def test_platinum_and_sensitivity(self):
        pick, row, source = self.fixture(specification('result','ft','2'))
        promote_selected_contract(pick, row, source, AT)
        self.assertEqual(pick['tier'], 'PLATINUM', pick)
        pick['tier'] = 'BLU'
        row['ev_low'] = .0199
        promote_selected_contract(pick, row, source, AT)
        self.assertEqual(pick['tier'], 'BLU')
        self.assertIn('sensitivity',pick['selected_contract_badge']['gold_reasons'])

    def test_study_and_failed_checks_are_not_promoted(self):
        pick, row, source = self.fixture(specification('btts','ft','yes'))
        pick['study_only'] = True
        source['opportunity']['reasons'].append('history')
        promote_selected_contract(pick, row, source, AT)
        self.assertEqual(pick['tier'], 'BLU')
        self.assertCountEqual(pick['selected_contract_badge']['gold_reasons'], ['history','confirmation'])

    def test_routed_triad_identity_with_and_without_badge_pass(self):
        def contracts(snapshot):
            return {role: tuple(pick.get(key) for key in ('market_id','source_market_id','odd','probability','ev'))
                    if pick else None for role,pick in snapshot['triad'].items()}
        baseline = assessment()
        with patch('v7_decision.promote_selected_contract'):
            previous = capture(deepcopy(baseline), META, AT)
        current = capture(deepcopy(baseline), META, AT)
        self.assertTrue(any(previous['triad'].values()))
        self.assertEqual(contracts(previous),contracts(current))
