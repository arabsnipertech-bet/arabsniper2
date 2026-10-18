import copy
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from filter_audit import build_filter_comparison


def record(accepted=True, outcome=True, date='2026-10-18', probability=.6, odd=2):
    return {'fixture': {'kickoff': date+'T15:00:00+02:00', 'league': 'test', 'country': 'Italy'},
        'captured_at': date+'T10:00:00+02:00', 'model_version': 'test', 'reliability_pct': 80,
        'probabilities_calibrated': {'over15_ht': {'over': probability}},
        'market_candidates': [{'market': 'over15_ht', 'selection': 'over', 'odd': odd,
            'relation': 'confirmed', 'gate': {'eligible': accepted,
                'reasons': [] if accepted else ['test_rejection'], 'book_disagreement_pp': 5}}],
        'settlement': {'outcomes': {'over15_ht': outcome}}}


class FilterAuditTest(unittest.TestCase):
    def market(self, records):
        return build_filter_comparison({'fixtures': dict(enumerate(records))})['markets']['over15_ht']

    def test_rejected_winners_remain_visible_and_input_is_immutable(self):
        rows = [record(True, False), record(False, True)]
        original = copy.deepcopy(rows)
        report = self.market(rows)
        profile = report['profiles'][0]
        self.assertEqual(profile['accepted']['hit_rate_pct'], 0)
        self.assertEqual(profile['rejected']['hit_rate_pct'], 100)
        self.assertEqual(report['all_predictions']['n'], 2)
        self.assertEqual(rows, original)

    def test_missing_result_not_a_loss_and_missing_candidate_not_accepted(self):
        missing = record(); missing['market_candidates'] = []
        report = self.market([record(outcome=None), missing])
        self.assertEqual(report['coverage']['pending_or_missing_result'], 1)
        self.assertEqual(report['coverage']['missing_candidate'], 1)
        self.assertEqual(report['all_predictions']['n'], 1)
        self.assertEqual(report['profiles'][0]['accepted']['n'], 0)

    def test_missing_odds_not_in_roi_and_markets_are_separate(self):
        report = self.market([record(odd=None), record(outcome=False, odd=2)])
        self.assertEqual(report['all_predictions']['n'], 2)
        self.assertEqual(report['all_predictions']['priced_n'], 1)
        self.assertEqual(report['all_predictions']['roi_pct'], -100)
        result = build_filter_comparison({'fixtures': {'a': record()}})
        self.assertEqual(result['markets']['gg_ht']['all_predictions']['n'], 0)

    def test_pre_match_evidence_and_later_dates(self):
        invalid = record(); invalid['captured_at'] = '2026-10-18T16:00:00+02:00'
        result = build_filter_comparison({'fixtures': {'a': invalid}})
        self.assertEqual(result['invalid_prematch_records_excluded'], 1)
        rows = [record(date='2026-10-16'), record(date='2026-10-17'),
                record(date='2026-10-18'), record(date='2026-10-18')]
        report = self.market(rows)
        self.assertEqual(report['later_period_start_date'], '2026-10-18')
        self.assertEqual(report['profiles'][0]['earlier_period']['n'], 2)
        self.assertEqual(report['profiles'][0]['later_period']['n'], 2)
        self.assertEqual(report['profiles'][0]['sample_status'], 'insufficient_sample')

    def test_probability_thresholds_compare_distinct_subsets(self):
        report = self.market([record(probability=.45), record(probability=.55), record(probability=.65)])
        profiles = {p['profile']: p for p in report['profiles']}
        self.assertEqual(profiles['current']['accepted']['n'], 3)
        self.assertEqual(profiles['probability_50']['accepted']['n'], 2)
        self.assertEqual(profiles['probability_60']['accepted']['n'], 1)


if __name__ == '__main__':
    unittest.main()
