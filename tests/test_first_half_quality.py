import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from first_half_quality import assess, evidence, PROFILE_ID
from filter_audit import build_filter_comparison
from test_filter_audit import record


def market(book=45, poisson=.4, empirical=.4):
    return {'selections': {'over': {'book_fair_probability_pct': book}},
            'empirical_layer': {'poisson_positive': poisson, 'empirical_positive': empirical},
            'market_validation': {'usable_for_signals': True}}


class FirstHalfQualityTests(unittest.TestCase):
    def test_convergence_includes_negative_ev_and_preserves_value_decision(self):
        gate = {'eligible': False, 'expected_value_pct': -10}
        signal = assess(market(), 60, gate)
        self.assertTrue(signal['eligible'])
        self.assertFalse(signal['value_eligible'])
        self.assertEqual(signal['expected_value_pct'], -10)
        self.assertEqual(signal['profile_id'], PROFILE_ID)
        self.assertEqual(gate, {'eligible': False, 'expected_value_pct': -10})

    def test_missing_invalid_and_unverified_inputs_do_not_pass(self):
        for row in (market(book=None), market(poisson=float('nan')),
                    market(empirical=.3999), market(book=44.99)):
            self.assertFalse(assess(row, 85)['eligible'])
        row = market(); row['market_validation']['usable_for_signals'] = False
        self.assertFalse(assess(row, 85)['eligible'])
        self.assertFalse(assess(market(), 59.99)['eligible'])

    def test_evidence_is_detached_and_missing_coverage_explicit(self):
        team = {'ht_sample_all': 7, 'ht_sample_context': 3,
                'first_half': {'recent5': {'sample': 5}}, 'metrics': {}}
        stats = {'home': copy.deepcopy(team), 'away': copy.deepcopy(team)}
        result = evidence(stats, {'total_ht': 1.6})
        stats['home']['first_half']['recent5']['sample'] = 0
        self.assertEqual(result['teams']['home']['first_half']['recent5']['sample'], 5)
        self.assertEqual(result['coverage_status'], 'available')
        self.assertEqual(evidence({}, {})['coverage_status'], 'unrecorded')
        stats['away']['ht_sample_all'] = 0
        self.assertEqual(evidence(stats, {})['coverage_status'], 'missing_team_ht')

    def test_whole_universe_profiles_and_frozen_discovery_cutoff(self):
        records = []
        for day in ('2026-10-18', '2026-10-19', '2026-10-20'):
            item = record(accepted=False, outcome=True, date=day)
            item['markets'] = {'over15_ht': market()}
            records.append(item)
        original = copy.deepcopy(records)
        comparison = build_filter_comparison({'fixtures': dict(enumerate(records))})['over15_ht_probability_comparison']
        rows = {r['profile']: r for r in comparison['profiles']}
        triple = rows['book_poisson_empirical_45_40_40']
        self.assertEqual(triple['selected']['n'], 3)
        self.assertEqual(triple['selected_from_current_rejected']['wins'], 3)
        self.assertEqual(triple['discovery']['n'], 1)
        self.assertEqual(triple['transition']['n'], 1)
        self.assertEqual(triple['prospective']['n'], 1)
        self.assertEqual(triple['sample_status'], 'insufficient_prospective_sample')
        self.assertEqual(rows['current_value_filter']['selected']['n'], 0)
        self.assertEqual(rows['convergence_quality_60']['selected']['n'], 0)
        self.assertEqual(records, original)


if __name__ == '__main__':
    unittest.main()
