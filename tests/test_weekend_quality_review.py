"""Research validity checks; no provider or storage access."""
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('weekend_quality_review', Path(__file__).parents[1]/'scripts/weekend-quality-review.py')
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


class WeekendEvidence(unittest.TestCase):
    def test_cached_reschedule_is_resolution_not_missing_final_and_exposes_join_identity(self):
        now=datetime(2026,11,26,10,tzinfo=timezone.utc)
        original='2026-11-25T19:00:00Z'
        cache=dict(schema_version=1,enabled=True,fixtures={'7':dict(fixture_id='7',source='API-Football',
            status='NS',provider_kickoff='2026-11-27T19:00:00Z',observed_at='2026-11-26T07:00:00Z')})
        archives=[dict(date='2026-11-25',fixtures={'7':dict(status='pending',kickoff=original)})]
        receipts={'2026-11-25':dict(fixtures={'7':dict(kickoff=original)})}
        with patch('audit_official_scores.cached_feed',return_value=cache),patch.object(review,'datetime') as clock:
            clock.now.return_value=now
            row=review.cache_recovery_probe(archives,receipts)['days']['2026-11-25']
        self.assertEqual(row['still_without_cached_final'],1)
        self.assertEqual(row['still_without_cached_resolution'],0)
        self.assertEqual(row['cached_void_or_rescheduled'],1)
        evidence=row['cached_administrative_evidence'][0]
        self.assertEqual(evidence['forecast_kickoff'],evidence['receipt_kickoff'])
        self.assertEqual(evidence['provider_kickoff'],'2026-11-27T19:00:00+00:00')

    def test_later_cutoff_is_explicit_bounded_and_retains_original_default(self):
        now=datetime(2026,11,26,11,tzinfo=timezone.utc)
        self.assertEqual(review.CUTOFF,datetime(2026,11,26,7,tzinfo=timezone.utc))
        self.assertEqual(review.checked_cutoff('2026-11-26T10:00:00Z',now).hour,10)
        for value in ('invalid','2026-11-27T00:00:00Z','2026-11-25T12:00:00Z'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                review.checked_cutoff(value,now)

    def test_newly_reconciled_result_uses_explicit_review_cutoff_without_filling_forecast(self):
        f=dict(self.fixture(),observed_at='2026-11-26T09:00:00Z')
        original=deepcopy(f)
        self.assertFalse(review.in_time(f))
        with patch.object(review,'CUTOFF',datetime(2026,11,26,10,tzinfo=timezone.utc)):
            self.assertTrue(review.in_time(f))
            self.assertFalse(review.in_time(f,datetime(2026,11,26,7,tzinfo=timezone.utc)))
        self.assertEqual(original,f)

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

    def test_postmatch_choice_cannot_contribute_a_win_or_original_probability(self):
        f=self.fixture()
        f['triad']={'recommended_alternative':dict(market_id=review.OVER,odd=2.5,
                    probability=.9,captured_at='2026-11-25T20:00:00Z')}
        row=review.original_choices('7',f,'2026-11-25')[0]
        self.assertIsNone(row['outcome'])
        self.assertIsNone(row['probability'])
        self.assertIsNone(row['roi'])

    def test_missing_probability_does_not_become_zero_in_calibration(self):
        data=[dict(fixture_id='1',outcome=1,probability=None,roi=1),
              dict(fixture_id='2',outcome=0,probability=0.,roi=-1)]
        value=review.summary(data)
        self.assertEqual(value['settled'],2)
        self.assertEqual(value['probability_n'],1)
        self.assertEqual(value['missing_probability'],1)
        self.assertEqual(value['brier'],0.)


if __name__ == '__main__': unittest.main()
