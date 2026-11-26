"""Research validity checks; no provider or storage access."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('weekend_quality_review', Path(__file__).parents[1]/'scripts/weekend-quality-review.py')
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


class WeekendEvidence(unittest.TestCase):
    def fixture(self):
        return dict(captured_at='2026-11-25T18:00:00Z', kickoff='2026-11-25T19:00:00Z',
                    observed_at='2026-11-25T21:00:00Z', outcomes={review.OVER:dict(win=1,loss=0)})

    def test_future_and_postmatch_evidence_is_not_eligible(self):
        f = self.fixture()
        self.assertTrue(review.in_time(f))
        self.assertFalse(review.in_time(dict(f, observed_at='2026-11-26T08:00:00Z')))
        self.assertFalse(review.in_time(dict(f, captured_at=f['kickoff'])))
        self.assertFalse(review.in_time(dict(f, observed_at=None)))

    def test_original_probability_is_never_backfilled_from_a_later_forecast(self):
        f = self.fixture()
        f['triad'] = {'recommended_alternative':dict(market_id=review.OVER,odd=2.5,captured_at='2026-11-25T16:00:00Z')}
        f['measurement'] = {'markets':{review.OVER:{'probability':.9}}}
        before = deepcopy(f)
        row = review.original_choices('7', f, '2026-11-25')[0]
        self.assertIsNone(row['probability'])
        self.assertIsNone(row['original_ev'])
        self.assertEqual(row['roi'],1.5)
        f['triad']['recommended_alternative']['probability']=.42
        row = review.original_choices('7', f, '2026-11-25')[0]
        self.assertEqual(row['probability'],.42)
        self.assertAlmostEqual(row['original_ev'],.05)
        self.assertEqual(before['measurement'],f['measurement'])

    def test_zero_false_and_absence_remain_distinct(self):
        self.assertEqual([review.recorded_state(v) for v in (None,0,False,True)],
                         ['missing','neutral','failed','passed'])

    def test_missing_probability_does_not_become_zero_in_calibration(self):
        data=[dict(fixture_id='1',outcome=1,probability=None,roi=1),
              dict(fixture_id='2',outcome=0,probability=0.,roi=-1)]
        value=review.summary(data)
        self.assertEqual(value['settled'],2)
        self.assertEqual(value['probability_n'],1)
        self.assertEqual(value['missing_probability'],1)
        self.assertEqual(value['brier'],0.)


if __name__ == '__main__': unittest.main()
