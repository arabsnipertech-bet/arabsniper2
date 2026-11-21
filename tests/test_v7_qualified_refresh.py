"""Qualified alternatives and prematch repairs; no outcomes/ROI guide routing."""
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from lateral_matrix import market_id, specification
from v7_decision import (GG, GG_FT, GOAL, TRIAD_MARKETS, TRIAD_QUALIFIED_REFRESH_V1,
    capture, commit_priced_slots, ev_recovery_candidates, qualified_refresh_candidate,
    refresh_unqualified_slots, route)
from v7_probability import VERSION
from test_v7 import AT, LATER, START, META, PATTERNS, assessment, row

ROLE = 'recommended_alternative'
FT, HT, ST = (GOAL[key] for key in ('over25_ft', 'over15_ht', 'over15_sh'))
ALTERNATIVES = (ROLE,)


def priced(key, p, odd, low, *, tier='BLU'):
    spec = json.loads(key)
    return dict(source_market_id=key, label=key, calibrated=p, odd=odd,
                ev=p*odd-1, ev_low=low, tier=tier, niche_id=None,
                horizon={'ht': 'PT', 'sh': 'ST', 'ft': 'FT'}[spec['period']],
                spec_kind=spec['kind'], model_fair_odd=1/p, supported=True,
                study_supported=True, quote_state='moving', fair=1/odd/1.05,
                quote={'quote_status': 'recent', 'bookmaker_id': '1', 'first_odd': odd}, features={})


def fixture():
    return {FT: priced(FT, .60, 1.80, .03),
            HT: priced(HT, .331, 2.71, -.106, tier=None),
            GG_FT: priced(GG_FT, .48, 2.40, .06),
            GG: priced(GG, .28, 4.00, .03)}


def published(records, *, alternatives=(), recoveries=None):
    return route(records, PATTERNS, study=True, qualified_alternatives=alternatives,
                 ev_recoveries=recoveries)


def repair(records, previous, *, at=LATER, national=False, recoveries=None):
    return refresh_unqualified_slots(records, PATTERNS, previous, datetime.fromisoformat(at),
        datetime.fromisoformat(START), national=national, ev_recoveries=recoveries,
        alternative_roles=ALTERNATIVES)


class QualifiedAlternatives(unittest.TestCase):
    def test_qualified_market_replaces_only_the_unqualified_study_fallback(self):
        records = fixture()
        before = deepcopy(records)
        old, old_signals = published(records)
        new, signals = published(records, alternatives=ALTERNATIVES)
        self.assertTrue(old[ROLE]['study_only'])
        self.assertIsNone(old_signals['consigliata'])
        self.assertEqual(new[ROLE]['market_id'], GG_FT)
        self.assertNotIn('study_only', new[ROLE])
        self.assertEqual(new[ROLE]['tier'], 'BLU')
        self.assertIs(signals['consigliata'], new[ROLE])
        for role in ('most_probable', 'high_odds_shot'):
            self.assertEqual(new[role], old[role])
        self.assertEqual(records, before)

    def test_existing_positive_qualified_recommendation_keeps_its_priority(self):
        records = fixture()
        records[HT] = priced(HT, .45, 2.50, .04)
        self.assertEqual(published(records, alternatives=ALTERNATIVES), published(records))

    def test_a_higher_probability_study_does_not_hide_a_lower_probability_qualified_offer(self):
        records = fixture()
        records[HT] = priced(HT, .43, 2.15, -.10, tier=None)
        records[GG_FT] = priced(GG_FT, .40, 2.60, .02)
        old, _ = published(records)
        new, _ = published(records, alternatives=ALTERNATIVES)
        self.assertEqual(old[ROLE]['market_id'], HT)
        self.assertEqual(new[ROLE]['market_id'], GG_FT)
        self.assertGreater(new[ROLE]['ev_low'], old[ROLE]['ev_low'])

    def test_alternatives_keep_existing_probability_odds_and_role_ranking(self):
        records = fixture()
        records[ST] = priced(ST, .50, 2.35, .07)
        triad, _ = published(records, alternatives=ALTERNATIVES)
        self.assertEqual(triad[ROLE]['market_id'], ST)
        for changes in ({'odd': 2.14}, {'odd': 3.51}, {'calibrated': .31}):
            changed = {**records[ST], **changes}
            changed['ev'] = changed['calibrated']*changed['odd']-1
            records[ST] = changed
            self.assertNotEqual(published(records, alternatives=ALTERNATIVES)[0][ROLE]['market_id'], ST)
            records[ST] = priced(ST, .50, 2.35, .07)

    def test_incomplete_stale_unsupported_or_inconsistent_alternatives_are_rejected(self):
        original = fixture()
        for changes in ({'fair': None}, {'supported': False}, {'quote': None},
                        {'quote': {'quote_status': 'cached_stable', 'bookmaker_id': '1'}},
                        {'quote': {'quote_status': 'recent', 'bookmaker_id': None}},
                        {'ev': -.01}, {'ev_low': -.01}, {'ev': .9},
                        {'ev_low': .9}, {'tier': None}, {'spec_kind': 'handicap'}):
            with self.subTest(changes=changes):
                records = deepcopy(original)
                records[GG_FT].update(changes)
                triad, _ = published(records, alternatives=ALTERNATIVES)
                self.assertTrue(triad[ROLE]['study_only'])

    def test_no_duplicate_or_incompatible_market_can_be_assigned(self):
        home = market_id(specification('result', 'ft', '1'))
        away = market_id(specification('result', 'ft', '2'))
        records = {home: priced(home, .60, 1.80, .02),
                   away: priced(away, .48, 2.40, .05), HT: fixture()[HT]}
        triad, _ = published(records, alternatives=ALTERNATIVES)
        self.assertEqual(triad['most_probable']['market_id'], home)
        self.assertNotEqual(triad[ROLE]['market_id'], away)
        keys = [r['market_id'] for r in triad.values() if r]
        self.assertEqual(len(keys), len(set(keys)))

    def test_a_new_alternative_remains_available_when_its_slot_is_locked_later(self):
        records = fixture()
        triad, _ = published(records, alternatives=ALTERNATIVES)
        commitments = commit_priced_slots(triad, AT, {})
        locked = {role: r['market_id'] for role, r in commitments.items()}
        later, _ = route(records, PATTERNS, study=True, locked=locked,
                         qualified_alternatives=ALTERNATIVES)
        self.assertEqual(later, triad)
        self.assertEqual(commit_priced_slots(later, LATER, commitments), commitments)

    def test_no_qualification_is_invented_when_no_better_candidate_exists(self):
        records = fixture()
        del records[GG_FT]
        self.assertEqual(published(records, alternatives=ALTERNATIVES), published(records))

    def test_gold_and_platinum_tiers_are_preserved_not_manufactured(self):
        for tier in ('BLU', 'GOLD', 'PLATINUM'):
            with self.subTest(tier=tier):
                records = fixture()
                records[GG_FT]['tier'] = tier
                self.assertEqual(published(records, alternatives=ALTERNATIVES)[0][ROLE]['tier'], tier)

    def test_a_configured_market_whitelist_is_not_silently_expanded_by_default(self):
        records = fixture()
        triad, signals = route(records, PATTERNS, study=True)
        self.assertTrue(triad[ROLE]['study_only'])
        self.assertIsNone(signals['consigliata'])
        self.assertNotIn(GG_FT, TRIAD_MARKETS[ROLE])


class DeterioratedCommitments(unittest.TestCase):
    def previous(self, records):
        # The new alternative was not available at the original publication.
        original = {key: value for key, value in records.items() if key != GG_FT}
        return commit_priced_slots(published(original)[0], AT, {})

    def test_known_deterioration_can_be_repaired_and_the_original_remains_immutable(self):
        records = fixture()
        previous = self.previous(records)
        saved = deepcopy(previous)
        updated = repair(records, previous)
        self.assertEqual(previous, saved)
        self.assertEqual(updated[ROLE]['market_id'], GG_FT)
        self.assertEqual(updated[ROLE]['replaces'], saved[ROLE])
        self.assertEqual(updated[ROLE]['selection_policy']['action'], 'replace_unqualified')
        for role in ('most_probable', 'high_odds_shot'):
            self.assertEqual(updated[role], saved[role])

    def test_a_prior_upgrade_in_another_role_does_not_freeze_a_deteriorated_slot(self):
        records = fixture()
        previous = self.previous(records)
        previous['most_probable']['replaces'] = {'original': 'preserved'}
        updated = repair(records, previous)
        self.assertEqual(updated[ROLE]['market_id'], GG_FT)
        self.assertEqual(updated['most_probable'], previous['most_probable'])

    def test_no_switch_occurs_in_the_last_hour_or_after_kickoff(self):
        records = fixture()
        previous = self.previous(records)
        for at in ('2026-11-09T22:46:00+01:00', START, '2026-11-10T00:00:00+01:00'):
            self.assertIs(repair(records, previous, at=at), previous)
        self.assertNotEqual(repair(records, previous, at='2026-11-09T22:45:00+01:00'), previous)

    def test_missing_prices_or_evidence_do_not_rewrite_historical_commitments(self):
        initial = fixture()
        previous = self.previous(initial)
        for field in ('odd', 'calibrated', 'ev', 'ev_low'):
            records = deepcopy(initial)
            records[HT][field] = None
            self.assertIs(repair(records, previous), previous)
        records = deepcopy(initial)
        del records[HT]
        self.assertIs(repair(records, previous), previous)

    def test_a_healthy_qualified_slot_is_not_replaced_by_this_policy(self):
        records = fixture()
        records[HT] = priced(HT, .45, 2.50, .03)
        previous = self.previous(records)
        self.assertIs(repair(records, previous), previous)

    def test_existing_hysteresis_prevents_tiny_prudent_edge_churn(self):
        records = fixture()
        records[HT] = priced(HT, .331, 2.71, -.01, tier=None)
        records[GG_FT] = priced(GG_FT, .48, 2.40, .005)
        previous = self.previous(records)
        self.assertIs(repair(records, previous), previous)
        records[GG_FT]['ev_low'] = .02
        self.assertIs(repair(records, previous, national=True), previous)
        self.assertNotEqual(repair(records, previous), previous)

    def test_a_later_deterioration_retains_the_whole_chain_of_previous_choices(self):
        records = fixture()
        previous = self.previous(records)
        first = repair(records, previous)
        records[GG_FT].update(ev_low=-.02, tier=None)
        records[ST] = priced(ST, .50, 2.35, .06)
        second = repair(records, first)
        self.assertEqual(second[ROLE]['market_id'], ST)
        self.assertEqual(second[ROLE]['replaces'], first[ROLE])
        self.assertEqual(second[ROLE]['replaces']['replaces'], previous[ROLE])

    def test_the_policy_does_not_use_settlement_or_roi_to_choose(self):
        records = fixture()
        previous = self.previous(records)
        ordinary = repair(records, previous)
        for key, record in records.items():
            record.update(roi=-999 if key == GG_FT else 999, result='loss' if key == GG_FT else 'win')
        self.assertEqual(repair(records, previous), ordinary)

    def test_primary_and_shot_can_be_repaired_without_changing_the_other_roles(self):
        home = market_id(specification('result', 'ft', '1'))
        shot = GOAL['over25_ht']
        for role, incumbent, candidate in [('most_probable', FT, home), ('high_odds_shot', GG, shot)]:
            with self.subTest(role=role):
                records = fixture()
                del records[GG_FT]
                records[HT] = priced(HT, .46, 2.40, .03)
                records[incumbent]['tier'] = None
                records[incumbent]['ev_low'] = -.05
                previous = commit_priced_slots(published(records)[0], AT, {})
                self.assertEqual(previous[role]['market_id'], incumbent)
                records[candidate] = priced(candidate, .62 if role == 'most_probable' else .30,
                                            1.75 if role == 'most_probable' else 4.0, .04)
                updated = repair(records, previous)
                self.assertEqual(updated[role]['market_id'], candidate)
                for other in TRIAD_MARKETS:
                    if other != role:
                        self.assertEqual(updated.get(other), previous.get(other))


class RecoveryAndIntegration(unittest.TestCase):
    def test_valid_ev_recovery_is_preserved_with_its_original_probability_and_ev(self):
        records = fixture()
        recovered = priced(HT, .39, 2.50, -.04, tier=None)
        recovered.update(raw=.42, statistics=.42)
        records[HT] = recovered
        source = row(json.loads(HT), 2.50, .42, 1/2.50/1.05)
        source.update(v7_supported=True, probability_model=VERSION)
        sources = {HT: source}
        proofs = ev_recovery_candidates(records, sources, AT, START, AT)
        self.assertIn(HT, proofs[ROLE])
        old = published(records, recoveries=proofs)
        new = published(records, alternatives=ALTERNATIVES, recoveries=proofs)
        self.assertEqual(new, old)
        previous = commit_priced_slots(new[0], AT, {})
        fresh = ev_recovery_candidates(records, sources, LATER, START, AT, previous)
        self.assertIs(repair(records, previous, recoveries=fresh), previous)
        self.assertEqual(new[0][ROLE]['probability'], .39)
        self.assertEqual(new[0][ROLE]['ev'], recovered['ev'])

    def test_negative_outside_preferred_markets_cannot_be_promoted_by_the_new_pass(self):
        records = fixture()
        records[GG_FT] = priced(GG_FT, .39, 2.40, -.08, tier=None)
        new, signals = published(records, alternatives=ALTERNATIVES)
        self.assertTrue(new[ROLE]['study_only'])
        self.assertIsNone(signals['consigliata'])

    def test_capture_can_disable_the_new_policy_and_preserves_all_model_prices(self):
        original = assessment()
        disabled = capture(deepcopy(original), META, AT, triad_refresh={'enabled': False})
        enabled = capture(deepcopy(original), META, AT)
        fields = ('raw', 'statistics', 'corrected', 'calibrated', 'fair', 'odd', 'ev', 'ev_low', 'tier')
        for key in disabled['markets']:
            self.assertEqual({f: disabled['markets'][key][f] for f in fields},
                             {f: enabled['markets'][key][f] for f in fields})
        self.assertTrue(TRIAD_QUALIFIED_REFRESH_V1['enabled'])


if __name__ == '__main__':
    unittest.main()
