"""Study evidence and automatic recommendations have separate lifecycles."""
from copy import deepcopy
from importlib import import_module
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from lateral_matrix import build_matrix, ScoreModel, specification
from matrix_pipeline import build_board
from matrix_quotes import attach
from proposal_stability import seed_stability
from statistics_first import statistical_analysis
from test_lateral_matrix import payload
from test_statistics_first import match, NOW


class StudyObservationTests(unittest.TestCase):
    def test_rejected_legacy_gate_preserves_model_marker_and_original_poisson(self):
        engine = import_module('3appdays')
        probabilities = {
            market: {side: .72 if side == sides[0] else .28 / (len(sides)-1) for side in sides}
            for market, sides in engine.MARKET_SELECTIONS.items()
        }
        source = {**match(), 'model': {'probabilities_raw': deepcopy(probabilities), 'reliability_pct': 75}}
        with patch.object(engine, '_build_fixture_output_v112', return_value=source):
            output = engine._build_fixture_output_v113_final(None, {}, {}, {})
        display, model = output['display'], output['model']
        self.assertIsNone(display['primary_choice'])
        self.assertFalse(display['ranked_choices'])
        self.assertEqual(len(display['rejected_choices']), len(probabilities))
        key = engine.MARKET_O15HT
        self.assertEqual(model['probabilities_poisson'], probabilities)
        self.assertNotEqual(model['probabilities_raw'][key], probabilities[key])
        self.assertTrue(model['markers'][key]['study_only'])
        selection = display['markets'][key]['selections']['over']
        self.assertEqual(display['markets'][key]['model_pick'], 'over')
        self.assertTrue(selection['marker']['model'])
        self.assertNotEqual(selection['marker']['shape'], 'none')
        self.assertIsNone(selection['oci_as'])
        self.assertIsNone(selection['delta_odd'])

    def test_missing_or_high_margin_preserves_pure_statistics_while_both_tiers_reject(self):
        fixture = match()
        for t in fixture['statistics']['home']['last_matches']:
            t.update(team_xg_for=1.72, team_shots_on_target_for=4)
        base = build_matrix(statistical_analysis(fixture, NOW), NOW)
        target = specification('total', 'ft', 'over', 2.5)
        model = ScoreModel(base['rates']).estimate(target)
        for quotes in ({}, payload(over=1.65, under=1.65)):
            with self.subTest(has_quotes=bool(quotes)):
                item = deepcopy(fixture)
                item['multimarket'] = result = attach(deepcopy(base), quotes, {}, item['fixture'], NOW)
                build_board({'date':'2026-10-26', 'matches':[item]}, NOW)
                row = next(r for r in result['matrix']['rows'] if r['spec'] == target)
                pure = row.get('calibration_base') or row
                self.assertAlmostEqual(pure['statistical_pct'], model['p']*100, places=3)
                self.assertAlmostEqual(pure['fair_odd'], model['fair_odd'], places=6)
                profile = result['profiles']['home']['for_ft']
                self.assertAlmostEqual(profile['xg_mean'], 1.72)
                self.assertEqual(profile['shots_mean'], 4)
                self.assertFalse(row['opportunity']['eligible'])
                self.assertFalse(row['platinum']['eligible'])
                self.assertIsNone(row['oci'])
                self.assertEqual(row['quote_status'], 'recent' if quotes else 'missing')

    def test_last_measured_flow_survives_successive_scans_without_becoming_a_current_quote(self):
        from platinum_policy import evaluate as platinum
        from opportunity_policy import evaluate as gold
        def assessment(row):
            return {'matrix': {'rows': [row]}}
        first = {'id':'goal', 'quote_status':'recent', 'bookmaker':'Original', 'bookmaker_id':1,
                 'provider_updated_at':NOW, 'quote_observed_at':NOW, 'odd':2.05, 'first_odd':2.25,
                 'oci':0., 'paired_books':3, 'agreeing_books':2}
        current = {'id':'goal','spec':specification('total', 'ft', 'over', 2.5),
                   'quote_status':'recent','odd':2.15,'oci':None, 'bookmaker':'Second'}
        before = deepcopy(current)
        seed_stability(assessment(first), assessment(current))
        self.assertEqual(current['study_flow']['oci'], 0)
        self.assertEqual(current['study_flow']['bookmaker'], 'Original')
        self.assertEqual({k:v for k,v in current.items() if k != 'study_flow'}, before)
        for decision in (lambda r: platinum(r,NOW), gold):
            self.assertEqual(decision(before), decision(current))
            self.assertFalse(decision(current)['eligible'])
        stale = {**before, 'quote_status':'stale', 'odd':None}
        seed_stability(assessment(current), assessment(stale))
        self.assertEqual(stale['study_flow'], current['study_flow'])
        self.assertIsNone(stale['odd'])
        self.assertIsNone(stale['oci'])
        fresh = {**current, 'oci':31.}
        seed_stability(assessment(stale), assessment(fresh))
        self.assertNotIn('study_flow', fresh)
