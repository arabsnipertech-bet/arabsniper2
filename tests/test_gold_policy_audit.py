import contextlib
from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from gold_policy_audit import (capture, policy_choices, attach_result, build_report,
    closing_price, metrics, main, requested_case, STUDY_ID, POLICY_ID)
from test_gold_scope_goals import example
from test_statistics_first import match, NOW
from opportunity_policy import annotate
from lateral_matrix import MODEL_ID
from audit_storage import read_store, write_store


def fixture(fid='100'):
    m = match(fid)
    m['fixture'].update(home='Home', away='Away')
    a = example()
    a.update(model_id=MODEL_ID, captured_at=NOW, profiles={}, data_quality={}, markets=[])
    for r in a['matrix']['rows']:
        r['bookmaker_id'] = '10'
        r['bookmaker'] = 'Synthetic'
    annotate(a['matrix'], NOW, a)
    m['multimarket'] = a
    return m


def report_row(report, strategy='published', view='latest', cohort='baseline_26'):
    return next(r for r in report['strategies'] if r['strategy'] == strategy and r['snapshot'] == view and r['cohort'] == cohort)


class GoldPolicyAudit(unittest.TestCase):
    def test_policy_versions_remain_separate_and_old_hashes_are_immutable(self):
        from hashlib import sha256
        store = {}; m = fixture(); capture(store, m, NOW)
        old = store['fixtures']['100']['snapshots'][0]
        old['policy_id'] = 'gold-goals-v2'
        old['choices'].pop('without_hysteresis')
        old.pop('proposal_stability'); old.pop('sha256')
        old['sha256'] = sha256(json.dumps(old, sort_keys=True, allow_nan=False).encode()).hexdigest()
        frozen = deepcopy(old)
        capture(store, m, '2026-10-26T08:30:00Z')
        previous = store['fixtures']['100']['snapshots'][1]
        previous['policy_id'] = 'gold-goals-v3'; previous.pop('sha256')
        previous['sha256'] = sha256(json.dumps(previous, sort_keys=True, allow_nan=False).encode()).hexdigest()
        previous_frozen = deepcopy(previous)
        capture(store, m, '2026-10-26T09:00:00Z')
        v4 = store['fixtures']['100']['snapshots'][2]
        v4['policy_id'] = 'gold-goals-v4'; v4.pop('sha256')
        v4['sha256'] = sha256(json.dumps(v4, sort_keys=True, allow_nan=False).encode()).hexdigest()
        frozen_v4 = deepcopy(v4)
        capture(store, m, '2026-10-26T09:30:00Z')
        v5 = store['fixtures']['100']['snapshots'][3]
        v5['policy_id'] = 'gold-goals-v5'; v5.pop('sha256')
        v5['sha256'] = sha256(json.dumps(v5, sort_keys=True, allow_nan=False).encode()).hexdigest()
        frozen_v5 = deepcopy(v5)
        capture(store, m, '2026-10-26T10:00:00Z')
        report = build_report(store, '2026-10-26T10:00:00Z')
        self.assertEqual(report['policy_ids'], ['gold-goals-v2', 'gold-goals-v3', 'gold-goals-v4', 'gold-goals-v5', POLICY_ID])
        self.assertEqual(v4, frozen_v4)
        self.assertEqual(v5, frozen_v5)
        self.assertTrue(report['production_policy_changed'])
        for version in report['policy_ids']:
            rows = [r for r in report['strategies'] if r['policy_id'] == version and r['strategy'] == 'published' and r['cohort'] == 'baseline_26']
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['observations'], rows[1]['observations'])
        self.assertEqual(old, frozen)
        self.assertEqual(previous, previous_frozen)

    def test_shadows_cannot_bypass_probability_price_history_or_coherence(self):
        m = fixture(); matrix = m['multimarket']['matrix']
        for change in ({'odd': 150.}, {'statistical_pct': 17.9}, {'quality': 'limited'},
                       {'quote_status': 'stale'}, {'selected_margin_pct': -2}):
            rows = deepcopy(matrix['rows'][:4])
            for r in rows: r.update(change)
            annotate({'rows': rows})
            self.assertTrue(all(v is None for v in policy_choices(rows, {}).values()), change)
        rows = deepcopy(matrix['rows'][:4]); rows[1]['statistical_pct'] = 99; rows[3]['statistical_pct'] = 99
        annotate({'rows': rows})
        self.assertTrue(all(v is None for v in policy_choices(rows, {}).values()))

    def test_two_predefined_relaxations_are_separate_and_never_modify_gold(self):
        m = fixture(); r = m['multimarket']['matrix']['rows'][0]
        r.update(ev_pct=3, ev_low_pct=.5, selected_margin_pct=6)
        annotate({'rows': [r]})
        before = deepcopy(r)
        choices = policy_choices([r], {})
        self.assertIsNone(choices['published'])
        self.assertEqual(choices['shadow_reduced_cushion'], r['id'])
        self.assertEqual(choices['shadow_sensitivity_minus3'], r['id'])
        self.assertEqual(r, before)
        r.update(ev_pct=7, ev_low_pct=-2)
        annotate({'rows': [r]})
        choices = policy_choices([r], {})
        self.assertIsNone(choices['published'])
        self.assertIsNone(choices['shadow_reduced_cushion'])
        self.assertEqual(choices['shadow_sensitivity_minus3'], r['id'])
        r['ev_low_pct'] = -3.001; annotate({'rows': [r]})
        self.assertIsNone(policy_choices([r], {})['shadow_sensitivity_minus3'])

    def test_freeze_is_prospective_policy_specific_immutable_and_idempotent(self):
        store = {}; m = fixture(); original = deepcopy(m)
        self.assertTrue(capture(store, m, NOW)); first = deepcopy(store)
        self.assertFalse(capture(store, m, NOW)); self.assertEqual(store, first)
        self.assertEqual(m, original)
        self.assertFalse(capture(store, m, '2026-10-26T20:00:00Z'))
        self.assertEqual(store, first)
        for change in ({'country': 'Cyprus'}, {'status_short': 'HT'}, {'result': {'status': 'FT'}},
                       {'kickoff': '2026-10-30T20:00:00Z'}):
            bad = deepcopy(m); bad['fixture'].update(change)
            self.assertFalse(capture({}, bad, NOW))
        m['multimarket']['matrix']['opportunity_policy']['id'] = 'old-policy'
        self.assertFalse(capture({}, m, NOW))

    def test_first_latest_and_variants_count_fixture_once_and_missing_ht_is_pending(self):
        store = {}; m = fixture()
        capture(store, m, NOW)
        capture(store, m, '2026-10-26T09:00:00Z')
        frozen = deepcopy(store['fixtures']['100']['snapshots'])
        result = {'status': 'FT', 'home_ft': 2, 'away_ft': 1}
        attach_result(store, '100', result, '2026-10-26T22:00:00Z', 'test')
        report = build_report(store, '2026-10-26T22:00:00Z')
        for view in ('first', 'latest'):
            row = report_row(report, view=view)
            self.assertEqual((row['observations'], row['settled'], row['pending']), (1, 0, 1))
        result.update(home_ht=1, away_ht=1)
        attach_result(store, '100', result, '2026-10-26T22:01:00Z', 'test')
        report = build_report(store, '2026-10-26T22:01:00Z')
        self.assertEqual((report_row(report)['settled'], report_row(report)['wins']), (1, 1))
        self.assertEqual(store['fixtures']['100']['snapshots'], frozen)
        store['fixtures']['100']['snapshots'][0]['rows'][0]['statistical_pct'] += 1
        with self.assertRaisesRegex(ValueError, 'snapshot mismatch'): build_report(store, NOW)

    def test_results_cannot_arrive_before_kickoff_or_use_bad_scores(self):
        store = {}; capture(store, fixture(), NOW)
        result = {'status': 'FT', 'home_ft': 2, 'away_ft': 1, 'home_ht': 1, 'away_ht': 0}
        self.assertFalse(attach_result(store, '100', result, NOW, 'test'))
        for bad in ({'home_ft': -1}, {'home_ht': 3}, {'away_ht': True}, {'status': 'HT'}, {'away_ft': 1.5}):
            self.assertFalse(attach_result(store, '100', {**result, **bad}, '2026-10-26T22:00:00Z', 'test'))
        self.assertNotIn('settlement', store['fixtures']['100'])

    def test_closing_pairs_require_same_market_book_and_prematch_window(self):
        store = {}; m = fixture(); capture(store, m, NOW)
        first = deepcopy(store['fixtures']['100']['snapshots'][0]['rows'][0])
        self.assertIsNone(closing_price(store['fixtures']['100'], first))
        m['multimarket']['matrix']['rows'][0]['odd'] = 1.7
        capture(store, m, '2026-10-26T19:00:00Z')
        self.assertIsNone(closing_price(store['fixtures']['100'], first))
        m['multimarket']['matrix']['rows'][0]['provider_updated_at'] = '2026-10-26T19:00:00Z'
        capture(store, m, '2026-10-26T19:01:00Z')
        self.assertEqual(closing_price(store['fixtures']['100'], first), 1.7)
        self.assertIsNone(closing_price(store['fixtures']['100'], {**first, 'bookmaker_id': '20'}))
        self.assertIsNone(closing_price(store['fixtures']['100'], {**first, 'id': 'different-line'}))

    def test_extra_shadow_losses_are_counted_and_roi_includes_all_settled_prices(self):
        store = {}; m = fixture()
        for r in m['multimarket']['matrix']['rows'][:4]: r.update(ev_pct=3, ev_low_pct=.5, selected_margin_pct=8)
        annotate(m['multimarket']['matrix'], NOW, m['multimarket'])
        capture(store, m, NOW)
        attach_result(store, '100', {'status': 'FT', 'home_ft': 0, 'away_ft': 0, 'home_ht': 0, 'away_ht': 0}, '2026-10-26T22:00:00Z', 'test')
        r = report_row(build_report(store, '2026-10-26T22:00:00Z'), 'shadow_reduced_cushion')
        self.assertEqual((r['losses'], r['paper_roi_pct']), (1, -100.))
        self.assertEqual(r['relative_to_published']['extra']['losses'], 1)

    def test_main_persists_audit_without_changing_day_and_reuses_saved_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root/'data').mkdir(); (root/'auditarchive').mkdir()
            m = fixture(); m['fixture'].update(home='Odense', match='Odense - Opponent', country='Denmark', league='Superliga')
            day = root/'data/arab_quote_day1.json'; write_store(day, {'date': '2026-10-26', 'matches': [m]})
            original = day.read_bytes()
            with contextlib.redirect_stdout(io.StringIO()), patch('socket.socket', side_effect=AssertionError('No network allowed')):
                first = main(root, NOW)
                self.assertEqual(first['frozen_fixtures'], 1)
                self.assertEqual(first['api_calls'], 0)
                saved_path = root/'data/arab_quote_gold_policy_audit.json'
                previous_store = read_store(saved_path, {})
                previous_store['policy_id'] = 'gold-goals-v3'
                write_store(saved_path, previous_store)
                result = {'status': 'FT', 'home_ft': 0, 'away_ft': 0, 'home_ht': 0, 'away_ht': 0}
                archive = {'fixtures': {'100': {'fixture': m['fixture'], 'settlement': {'result': result}}}}
                write_store(root/'auditarchive/prematch_2026-10-26_results.json', archive)
                second = main(root, '2026-10-27T10:00:00Z')
                self.assertEqual(read_store(saved_path, {})['policy_id'], 'gold-goals-v3')
                self.assertEqual(read_store(saved_path, {})['active_policy_id'], POLICY_ID)
            self.assertEqual(report_row(second)['losses'], 1)
            self.assertEqual(day.read_bytes(), original)
            self.assertTrue((root/'data/arab_quote_gold_policy_report.json').exists())
            case = requested_case(m, NOW)
            self.assertTrue(case['scope_allowed']); self.assertEqual(len(case['markets']), 4)
            self.assertFalse(any('statistical_pct' in r or 'odd' in r for r in case['markets']))

    def test_baseline_and_three_day_study_are_separate(self):
        store = {}; capture(store, fixture('100'), NOW)
        m = fixture('200'); m['fixture']['kickoff'] = '2026-10-27T20:00:00Z'
        capture(store, m, NOW)
        report = build_report(store, NOW)
        self.assertEqual(report_row(report)['observations'], 1)
        self.assertEqual(report_row(report, cohort='study_27_29')['observations'], 1)

    def test_ablation_and_book_baseline_use_frozen_comparable_forecasts(self):
        store = {}; m = fixture()
        m['multimarket']['markets'] = [{'market': 'over25_ft', 'probabilities': {'without_peak_control': .6}}]
        capture(store, m, NOW)
        snapshot = store['fixtures']['100']['snapshots'][0]
        self.assertEqual(snapshot['choices']['published'], 'ht15')
        self.assertEqual(snapshot['choices']['without_pt_priority'], 'ft25')
        attach_result(store, '100', {'status': 'FT', 'home_ft': 2, 'away_ft': 1, 'home_ht': 0, 'away_ht': 0},
                      '2026-10-26T22:00:00Z', 'test')
        report = build_report(store, NOW)
        self.assertEqual(report_row(report)['losses'], 1)
        ablation = report_row(report, 'without_pt_priority')
        self.assertEqual(ablation['wins'], 1)
        self.assertAlmostEqual(ablation['paper_profit_units'], 1.)
        self.assertEqual(ablation['matched_book_n'], 1)
        variant = next(r for r in report['model_variants'] if r['snapshot'] == 'latest')
        self.assertAlmostEqual(variant['brier'], .16)
        m['multimarket']['matrix']['opportunity_policy']['best_market_id'] = 'not-the-published-market'
        with self.assertRaisesRegex(ValueError, 'proposal mismatch'): capture({}, m, NOW)


if __name__ == '__main__': unittest.main()

