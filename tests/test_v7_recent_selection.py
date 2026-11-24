"""Regressions from #248: valid cache, last-hour data and the previous choice."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from v7_recent_selection import POLICY_ID, save
from v7_decision import capture, stored_offers
from v7_archive import valid_snapshot
from refresh_opportunities import cached_refresh_safe, selection_coverage
from test_v7 import assessment, META, AT, LATER, GG, FT

LATE = '2026-11-09T22:46:00+01:00'


def cached_assessment():
    a = assessment()
    for row in a['matrix']['rows']:
        row.update(quote_status='cached_stable', prematch_stable=True,
                   provider_updated_at='2026-11-09T15:00:00+01:00',
                   quote_observed_at=AT)
    return a


class LatestPrematch(unittest.TestCase):
    def test_a_valid_stable_quote_does_not_disappear_in_the_last_three_hours(self):
        a = cached_assessment()
        snapshot = capture(a, META, AT, commitments={})
        primary = snapshot['triad']['most_probable']
        self.assertIsNotNone(primary)
        self.assertEqual(primary['odd'], 2.5)
        self.assertNotEqual(primary['quote_state'], 'model_estimate')
        self.assertTrue(valid_snapshot(snapshot, META, AT))

    def test_first_analysis_in_last_hour_is_not_automatically_demoted(self):
        a = assessment()
        ordinary = capture(deepcopy(a), META, LATE)
        published = capture(deepcopy(a), META, LATE, commitments={})
        self.assertEqual(published['triad'], ordinary['triad'])
        self.assertEqual(published['signals'], ordinary['signals'])
        self.assertEqual(published['triad_timing']['id'], POLICY_ID)
        self.assertEqual(published['triad_timing']['status'], 'current')

    def test_previous_late_marker_and_study_flag_do_not_poison_a_new_election(self):
        old = {'most_probable':dict(market_id=GG, label='old', odd=6., horizon='PT',
            captured_at='2026-11-05T03:18:00Z', study_only=True,
            selection_timing=dict(id='TRIAD_RECENT_SELECTION_V1', status='late'))}
        original = deepcopy(old)
        a = cached_assessment()
        expected = capture(deepcopy(a), META, LATE, commitments={})
        actual = capture(deepcopy(a), META, LATE, commitments=old)
        self.assertEqual(actual['triad'], expected['triad'])
        self.assertEqual(actual['signals'], expected['signals'])
        self.assertEqual(old, original)
        self.assertEqual(actual['triad_commitments']['most_probable']['replaces'], old['most_probable'])

    def test_all_roles_follow_latest_election_even_after_repeated_changes(self):
        first = capture(assessment(), META, AT, commitments={})
        old = deepcopy(first['triad_commitments'])
        for slot in old.values():
            slot['market_id'] = GG
            slot['replaces'] = dict(original='kept')
        a = assessment()
        expected = capture(deepcopy(a), META, LATER, commitments={})
        actual = capture(deepcopy(a), META, LATER, commitments=old)
        self.assertEqual(actual['triad'], expected['triad'])
        for role, slot in actual['triad_commitments'].items():
            self.assertEqual(slot['captured_at'], actual['captured_at'])
            self.assertEqual(slot['market_id'], expected['triad'][role]['market_id'])

    def test_a_repeated_market_records_actual_new_confirmation_without_history_growth(self):
        first = capture(assessment(), META, AT, commitments={})
        old = deepcopy(first['triad_commitments'])
        second = capture(assessment(), META, LATER, commitments=old)
        self.assertEqual(first['triad_commitments'], old)
        for role, slot in second['triad_commitments'].items():
            if slot['market_id'] == old[role]['market_id']:
                self.assertNotIn('replaces', slot)
            self.assertEqual(slot['captured_at'], second['captured_at'])

    def test_election_never_changes_original_probability_price_or_ev(self):
        a = cached_assessment()
        before = capture(deepcopy(a), META, AT, commitments={}, recent_selection={'enabled':False})
        after = capture(deepcopy(a), META, AT, commitments={})
        fields = ('raw','statistics','corrected','calibrated','fair','odd','ev','ev_low')
        for key in before['markets']:
            self.assertEqual({f: before['markets'][key][f] for f in fields},
                             {f: after['markets'][key][f] for f in fields})

    def test_genuinely_expired_quotes_are_not_resurrected_by_the_repair(self):
        a = assessment()
        for row in a['matrix']['rows']:
            row.update(provider_updated_at='2026-11-01T15:00:00Z', quote_status='cached_stable')
        snapshot = capture(a, META, AT, commitments={})
        self.assertTrue(all(not pick or pick['odd'] is None for pick in snapshot['triad'].values()))
        a['matrix']['v7'] = snapshot
        self.assertEqual(list(stored_offers(a, META, AT)), [])

    def test_started_match_cannot_be_reanalysed_with_its_known_result(self):
        self.assertIsNone(capture(assessment(), META, META['kickoff'], commitments={}))
        self.assertIsNone(capture(assessment(), dict(META,result={'home_ft':5,'away_ft':0}), AT, commitments={}))

    def test_unpriced_estimates_never_receive_an_invented_committed_price(self):
        self.assertEqual(save({'most_probable':dict(market_id=FT,odd=None)}, AT, {}), {})

    def test_no_quote_age_or_provider_timestamp_is_refreshed_by_confirmation(self):
        a = cached_assessment()
        old = deepcopy(a['matrix']['rows'])
        snapshot = capture(a, META, AT, commitments={})
        for before, after in zip(old, a['matrix']['rows']):
            for field in ('odd','quote_observed_at','provider_updated_at'):
                self.assertEqual(before[field], after[field])
        self.assertTrue(snapshot['triad_commitments'])

    def test_cached_release_requires_no_provider_collection(self):
        self.assertTrue(cached_refresh_safe(['src/v7_decision.py','src/v7_recent_selection.py',
            'src/refresh_opportunities.py','tests/test_v7_recent_selection.py','web/lateral-matrix.js']))
        self.assertFalse(cached_refresh_safe(['src/v7_probability.py','src/v7_decision.py']))

    def test_live_quality_report_counts_real_priced_primary_and_estimates_separately(self):
        a = cached_assessment()
        a['matrix']['v7'] = capture(a, META, AT, commitments={})
        summary = selection_coverage({'matches':[{'fixture':META,'multimarket':a}]}, AT)
        self.assertEqual(summary['counts']['fixtures'], 1)
        self.assertEqual(summary['counts']['most_probable_quoted'], 1)
        self.assertEqual(summary['policies'], {POLICY_ID:1})
        self.assertEqual(summary['qualification_policies'], {'TRIAD_POSITIVE_ALTERNATIVES_V2':1})


if __name__ == '__main__':
    unittest.main()
