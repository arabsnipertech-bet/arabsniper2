"""Chronological publication tests: no result/ROI is an input to election."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from v7_recent_selection import POLICY, clock, recent_commitment, routing_records, save
from v7_decision import capture, stored_offers
from v7_archive import valid_snapshot
from test_v7 import assessment, META, AT, LATER, HT, GG

START = '2026-11-25T18:00:00+00:00'
FIRST = '2026-11-25T15:30:00+00:00'
NEXT = '2026-11-25T16:30:00+00:00'
LIMIT = '2026-11-25T17:00:00+00:00'
LATE = '2026-11-25T17:01:00+00:00'
ROLE = 'most_probable'


def record(at=FIRST, odd=1.9):
    return dict(odd=odd, calibrated=.6, ev=.14, ev_low=.10,
                quote=dict(bookmaker_id='1', quote_observed_at=at, provider_updated_at=at))


def signal(key='first', odd=1.9):
    return dict(market_id=key, label=key, odd=odd, horizon='FT', tier='GOLD')


def confirm(at=FIRST, previous=None, key='first', quote_at=None):
    prior = {} if previous is None else previous
    timing = clock(at, START, prior)
    picks = {ROLE: signal(key)}
    slots = save(picks, {key: record(quote_at or at)}, prior, timing)
    return slots, timing, picks


class RecentClock(unittest.TestCase):
    def test_every_role_can_be_re_elected_even_after_multiple_previous_changes(self):
        old = {ROLE: dict(market_id='days-old', captured_at='2026-11-17T03:18:00Z',
                         odd=2.0, replaces=dict(market_id='even-older'))}
        original = deepcopy(old)
        first, _, _ = confirm(previous=old)
        second, timing, _ = confirm(NEXT, first, 'second')
        self.assertTrue(timing['reselect'])
        self.assertEqual(second[ROLE]['market_id'], 'second')
        self.assertEqual(second[ROLE]['replaces'], first[ROLE])
        self.assertEqual(second[ROLE]['replaces']['replaces'], original[ROLE])
        self.assertEqual(old, original)

    def test_an_unchanged_choice_gets_a_real_new_confirmation_without_growing_history(self):
        first, _, _ = confirm()
        before = deepcopy(first)
        second, _, _ = confirm(NEXT, first)
        self.assertEqual(second[ROLE]['captured_at'], NEXT)
        self.assertNotIn('replaces', second[ROLE])
        self.assertEqual(first, before)

    def test_choice_at_exactly_one_hour_is_allowed_and_the_next_minute_is_frozen(self):
        first, _, _ = confirm()
        final, timing, _ = confirm(LIMIT, first, 'last-before-deadline')
        self.assertTrue(timing['reselect'])
        self.assertEqual(final[ROLE]['selection_timing']['status'], 'frozen')
        held, timing, _ = confirm(LATE, final, 'later-winner')
        self.assertFalse(timing['reselect'])
        self.assertEqual(held[ROLE]['market_id'], 'last-before-deadline')
        self.assertEqual(held[ROLE]['odd'], final[ROLE]['odd'])
        self.assertEqual(held[ROLE]['captured_at'], LIMIT)
        self.assertEqual(timing['status'], 'frozen')

    def test_a_missing_role_cannot_acquire_a_new_market_in_the_last_hour(self):
        first, _, _ = confirm()
        timing = clock(LATE, START, first)
        pool = routing_records({'first': record(), 'later': record()}, timing, first)
        self.assertEqual(set(pool), {'first'})

    def test_a_days_old_choice_is_never_certified_by_reading_it_again(self):
        old_at = '2026-11-17T03:18:00+00:00'
        old, _, _ = confirm(old_at, quote_at=old_at)
        held, timing, picks = confirm(LATE, old)
        self.assertEqual(held[ROLE]['captured_at'], old_at)
        self.assertEqual(timing['status'], 'stale')
        self.assertTrue(picks[ROLE]['study_only'])
        self.assertTrue(held[ROLE]['study_only'])

    def test_legacy_commitments_remain_history_until_a_real_new_election_occurs(self):
        old = {ROLE: dict(market_id='first', label='old', odd=1.9, horizon='FT', captured_at=FIRST)}
        original = deepcopy(old)
        held, timing, _ = confirm(LATE, old)
        self.assertEqual(timing['status'], 'stale')
        self.assertFalse(recent_commitment(held[ROLE], timing))
        self.assertEqual(old, original)

    def test_a_late_first_observation_never_claims_a_pre_deadline_confirmation(self):
        slots, timing, picks = confirm(LATE)
        self.assertEqual(timing['status'], 'late')
        self.assertTrue(picks[ROLE]['study_only'])
        self.assertEqual(slots[ROLE]['captured_at'], LATE)
        again, later, _ = confirm('2026-11-25T17:15:00Z', slots)
        self.assertEqual(later['status'], 'late')
        self.assertEqual(again[ROLE]['captured_at'], LATE)

    def test_recent_pool_rejects_old_and_future_prices_but_keeps_the_complete_matrix(self):
        rows = {'recent': record(), 'stale': record('2026-11-17T03:18:00Z'),
                'future': record('2026-11-25T16:00:00Z'), 'unpriced': record(odd=None)}
        original = deepcopy(rows)
        chosen = routing_records(rows, clock(FIRST, START, {}), {})
        self.assertEqual(set(chosen), {'recent', 'unpriced'})
        self.assertEqual(rows, original)

    def test_recent_observation_cannot_reset_an_old_provider_timestamp(self):
        row = record()
        row['quote']['provider_updated_at'] = '2026-11-17T03:18:00Z'
        self.assertEqual(routing_records({'old-provider': row}, clock(FIRST, START, {}), {}), {})

    def test_temporary_early_preview_is_not_a_final_recent_selection(self):
        early = '2026-11-22T15:30:00Z'
        slots, timing, _ = confirm(early)
        self.assertEqual(timing['status'], 'provisional')
        self.assertFalse(slots[ROLE]['selection_timing']['recent_quote'])
        self.assertFalse(recent_commitment(slots[ROLE], clock(LATE, START, slots)))

    def test_new_kickoff_invalidates_the_previous_freeze_proof(self):
        slots, _, _ = confirm()
        moved = clock('2026-11-25T19:01:00Z', '2026-11-25T20:00:00Z', slots)
        self.assertFalse(recent_commitment(slots[ROLE], moved))

    def test_recent_flag_does_not_replace_the_real_provider_time_evidence(self):
        slots, _, _ = confirm()
        slots[ROLE]['selection_timing']['provider_updated_at'] = '2026-11-17T03:18:00Z'
        self.assertFalse(recent_commitment(slots[ROLE], clock(LATE, START, slots)))

    def test_missing_current_row_does_not_leave_a_stale_operational_commitment(self):
        old = {ROLE: dict(market_id='first', odd=1.9, captured_at='2026-11-17T03:18:00Z')}
        timing = clock(LATE, START, old)
        saved = save({ROLE: None}, {}, old, timing)
        self.assertEqual(timing['status'], 'stale')
        self.assertTrue(saved[ROLE]['study_only'])
        self.assertNotIn('study_only', old[ROLE])


class PublishedIntegration(unittest.TestCase):
    def test_real_publication_ignores_old_locks_and_preserves_the_old_record(self):
        old = {ROLE: dict(market_id=GG, label='old', odd=6.0, horizon='PT',
                         captured_at='2026-11-05T03:18:00Z')}
        original = deepcopy(old)
        a = assessment()
        fresh = capture(deepcopy(a), META, AT, commitments={})
        revised = capture(deepcopy(a), META, AT, commitments=old)
        self.assertEqual(revised['triad'], fresh['triad'])
        self.assertEqual(old, original)
        self.assertTrue(valid_snapshot(revised, META, AT))
        for role, slot in revised['triad_commitments'].items():
            self.assertEqual(slot['captured_at'], revised['captured_at'])
            if role in old:
                self.assertEqual(slot['replaces'], old[role])

    def test_reranking_does_not_change_probabilities_prices_or_original_ev(self):
        a = assessment()
        old = capture(deepcopy(a), META, AT, commitments={}, recent_selection={'enabled': False})
        new = capture(deepcopy(a), META, AT, commitments={})
        fields = ('raw', 'statistics', 'corrected', 'calibrated', 'fair', 'odd', 'ev', 'ev_low')
        for key in old['markets']:
            self.assertEqual({f: old['markets'][key][f] for f in fields},
                             {f: new['markets'][key][f] for f in fields})

    def test_last_hour_freeze_holds_market_identity_and_its_selected_price(self):
        a = assessment()
        first = capture(deepcopy(a), META, AT, commitments={})
        at = '2026-11-09T22:46:00+01:00'
        changed = deepcopy(a)
        for row in changed['matrix']['rows']:
            row.update(odd=row['odd']*1.2, quote_observed_at=at, provider_updated_at=at)
        held = capture(changed, META, at, commitments=first['triad_commitments'])
        for role, slot in first['triad_commitments'].items():
            self.assertEqual(held['triad_commitments'][role]['market_id'], slot['market_id'])
            self.assertEqual(held['triad_commitments'][role]['odd'], slot['odd'])
            self.assertEqual(held['triad_commitments'][role]['captured_at'], slot['captured_at'])
        self.assertTrue(valid_snapshot(held, META, at))

    def test_late_first_publication_is_study_and_cannot_feed_operational_offers(self):
        at = '2026-11-09T22:46:00+01:00'
        a = assessment()
        snapshot = capture(a, META, at, commitments={})
        a['matrix']['v7'] = snapshot
        self.assertEqual(snapshot['triad_timing']['status'], 'late')
        self.assertTrue(all(not p or p['study_only'] for p in snapshot['triad'].values()))
        self.assertFalse(snapshot['signals']['gold'] or snapshot['signals']['platinum'])
        self.assertEqual(list(stored_offers(a, META, at)), [])


if __name__ == '__main__':
    unittest.main()
