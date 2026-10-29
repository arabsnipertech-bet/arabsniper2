from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from platinum_policy import evaluate, annotate, POLICY_ID, LEGACY_POLICY_ID
from platinum_audit import capture, report
from gold_policy_audit import attach_result
from lateral_matrix import MODEL_ID
from gold_competitions import SCOPE_ID

CASES = json.loads((Path(__file__).parent/'platinum_cases.json').read_text())
NOW, KICKOFF = CASES['now'], CASES['kickoff']


def fixture():
    matrix = {'rows': [deepcopy(CASES['base'])],
              'opportunity_policy': {'id': 'gold-goals-v6.1', 'best_market_id': None}}
    meta = {'fixture_id': '100', 'home': 'Casa', 'away': 'Ospite', 'match': 'Casa – Ospite',
            'kickoff': KICKOFF, 'status_short': 'NS', 'country': 'Italy', 'league': 'Serie A'}
    annotate(matrix, meta, NOW)
    return {'gold_scope': {'policy_id': SCOPE_ID, 'allowed': True}, 'fixture': meta,
            'multimarket': {'model_id': MODEL_ID, 'captured_at': NOW, 'matrix': matrix}}


class PlatinumPolicy(unittest.TestCase):
    def test_shared_price_boundaries_and_no_statistical_veto(self):
        for case in CASES['cases']:
            with self.subTest(case=case['name']):
                row = {**deepcopy(CASES['base']), **case['patch']}; before = deepcopy(row)
                self.assertEqual(evaluate(row, NOW)['eligible'], case['eligible'])
                self.assertEqual(row, before)

    def test_ev_floor_reasons_and_invalid_values(self):
        row = deepcopy(CASES['base'])
        self.assertEqual(evaluate({**row, 'ev_pct': -15}, NOW)['qualification'], 'high_odds')
        self.assertEqual(evaluate({**row, 'ev_pct': -15.0001}, NOW)['reasons'], ['ev_floor'])
        for invalid in (float('nan'), float('inf'), float('-inf'), True, '-15'):
            with self.subTest(ev=invalid):
                self.assertEqual(evaluate({**row, 'ev_pct': invalid}, NOW)['reasons'], ['ev_unavailable'])
        row.pop('ev_pct'); row.pop('statistical_pct')
        self.assertTrue(evaluate(row, NOW)['eligible'])

    def test_excluded_primary_falls_back_without_changing_gold_or_combo(self):
        m = fixture(); matrix = m['multimarket']['matrix']
        rejected = matrix['rows'][0]
        rejected.update(oci=80, ev_pct=-15.0001, combo={'active': False, 'reasons': ['example']})
        alternate = {**deepcopy(CASES['base']), 'id': 'over25', 'ev_pct': -15,
                     'spec': {**CASES['base']['spec'], 'line': 2.5}}
        matrix['rows'].append(alternate)
        before = deepcopy(matrix)
        annotate(matrix, m['fixture'], NOW)
        self.assertEqual(matrix['platinum_policy']['best_market_id'], 'over25')
        self.assertEqual(matrix['platinum_policy']['eligible_markets'], 1)
        self.assertEqual(rejected['platinum']['reasons'], ['ev_floor'])
        self.assertIsNone(rejected['platinum']['rank'])
        self.assertEqual(alternate['platinum']['rank'], 1)
        for old, new in zip(before['rows'], matrix['rows']):
            self.assertEqual({k: v for k, v in old.items() if k != 'platinum'},
                             {k: v for k, v in new.items() if k != 'platinum'})
        self.assertEqual(matrix['opportunity_policy'], before['opportunity_policy'])

    def test_cache_window_never_renews(self):
        row = {**CASES['base'], 'quote_status': 'cached_stable',
               'cache_source_at': '2026-10-29T00:00:00Z', 'cache_expires_at': '2026-10-29T12:00:00Z'}
        self.assertTrue(evaluate(row, NOW)['eligible'])
        self.assertTrue(evaluate(row, row['cache_expires_at'])['eligible'])
        self.assertFalse(evaluate(row, '2026-10-29T12:00:01Z')['eligible'])
        self.assertFalse(evaluate(row, '2026-10-28T23:59:59Z')['eligible'])
        self.assertFalse(evaluate({**row, 'cache_expires_at': '2026-10-29T13:00:00Z'}, NOW)['eligible'])

    def test_annotation_preserves_gold_and_deduplicates_equivalent_markets(self):
        m = fixture(); matrix = m['multimarket']['matrix']
        alternate = {**deepcopy(CASES['base']), 'id': 'asian', 'oci': 40,
                     'spec': {**CASES['base']['spec'], 'kind': 'asian_total'}}
        matrix['rows'].append(alternate)
        before = deepcopy(matrix)
        annotate(matrix, m['fixture'], NOW)
        self.assertEqual(matrix['platinum_policy']['best_market_id'], 'asian')
        self.assertEqual(matrix['platinum_policy']['eligible_markets'], 1)
        for old, new in zip(before['rows'], matrix['rows']):
            self.assertEqual({k: v for k, v in old.items() if k != 'platinum'},
                             {k: v for k, v in new.items() if k != 'platinum'})
        self.assertEqual(matrix['opportunity_policy'], before['opportunity_policy'])

    def test_started_results_and_missing_kickoff_never_get_reclassified(self):
        for patch in ({'kickoff': NOW}, {'kickoff': None}, {'status_short': '1H'},
                      {'result': {'status': 'FT'}}):
            m = fixture(); matrix = m['multimarket']['matrix']; before = deepcopy(matrix)
            annotate(matrix, {**m['fixture'], **patch}, '2026-10-29T11:00:00Z')
            self.assertEqual(matrix, before)

    def test_prospective_audit_is_immutable_and_separate_from_gold(self):
        store = {}; m = fixture()
        self.assertTrue(capture(store, m, NOW)); before = deepcopy(store)
        self.assertFalse(capture(store, m, NOW)); self.assertEqual(store, before)
        row = m['multimarket']['matrix']['rows'][0]; row['odd'] = 99
        self.assertEqual(store, before)
        m = fixture(); m['multimarket']['matrix'].pop('platinum_policy')
        self.assertFalse(capture(store, m, NOW))
        self.assertFalse(capture(store, fixture(), KICKOFF))
        self.assertFalse(capture(store, fixture(), '2026-10-29T09:00:00Z'))
        self.assertEqual(report(store, NOW)['groups'][0]['pending'], 1)
        result = {'status': 'PEN', 'home_ft': 2, 'away_ft': 2, 'home_ht': 1, 'away_ht': 0}
        attach_result(store, '100', result, '2026-10-29T22:00:00Z', 'synthetic')
        out = report(store, '2026-10-29T22:00:00Z')
        for group in out['groups']:
            self.assertEqual(group['proposals'], 1); self.assertEqual(group['won'], 1)
            self.assertEqual(group['paper_roi_pct'], 150)
        self.assertEqual(store['fixtures']['100']['snapshots'], before['fixtures']['100']['snapshots'])

    def test_withdrawal_is_not_replaced_with_an_older_winner(self):
        store = {}; m = fixture(); capture(store, m, NOW)
        m['multimarket']['matrix']['rows'][0]['oci'] = 0
        annotate(m['multimarket']['matrix'], m['fixture'], '2026-10-29T11:00:00Z')
        capture(store, m, '2026-10-29T11:00:00Z')
        self.assertEqual([g['snapshot'] for g in report(store, NOW)['groups']], ['first'])

    def test_board_accepts_missing_statistics_without_promoting_gold(self):
        from matrix_pipeline import build_board
        from lateral_matrix import market_id
        m = fixture(); a = m['multimarket']; row = a['matrix']['rows'][0]
        row.update(id=market_id(row['spec']), provider_updated_at=NOW, low_pct=None, fair_odd=None)
        a['markets'] = [{'market': 'over35_ft', 'selection': 'over', 'probabilities': {}}]
        day = {'date': '2026-10-29', 'matches': [m]}
        build_board(day, NOW)
        self.assertEqual(day['matrix_summary']['upcoming_fixtures_with_platinum'], 1)
        self.assertEqual(day['matrix_summary']['upcoming_fixtures_with_proposal'], 0)
        self.assertIsNone(a['markets'][0]['probabilities']['statistics'])
        self.assertFalse(a['markets'][0]['published_selection'])

    def test_audit_continues_beyond_gold_study_end_without_api_calls(self):
        import contextlib, io, tempfile
        from platinum_audit import main
        from audit_storage import read_store, write_store
        m = fixture(); now = '2026-11-01T10:00:00Z'
        m['fixture']['kickoff'] = '2026-11-01T18:00:00Z'
        annotate(m['multimarket']['matrix'], m['fixture'], now)
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder); write_store(root/'data/arab_quote_day1.json', {'matches': [m]})
            result = main(root, now)
            self.assertEqual(result['frozen_fixtures'], 1); self.assertEqual(result['api_calls'], 0)
            self.assertEqual(result['groups'][0]['date'], '2026-11-01')
            first = read_store(root/'data/arab_quote_platinum_audit.json')
            main(root, now)
            self.assertEqual(read_store(root/'data/arab_quote_platinum_audit.json'), first)

    def test_audit_migrates_ledger_without_rewriting_legacy_snapshots(self):
        import contextlib, io, tempfile
        from platinum_audit import main
        from audit_storage import read_store, write_store
        store = {'schema_version': 1, 'policy_id': LEGACY_POLICY_ID}
        capture(store, fixture(), NOW)
        legacy = store['fixtures']['100']['snapshots'][0]
        legacy['policy_id'] = LEGACY_POLICY_ID
        legacy['choice']['platinum']['policy_id'] = LEGACY_POLICY_ID
        # Treat the earlier snapshot, including its stored digest, as immutable.
        before = deepcopy(legacy)
        m = fixture(); later = '2026-10-29T11:00:00Z'
        annotate(m['multimarket']['matrix'], m['fixture'], later)
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            root = Path(folder)
            write_store(root/'data/arab_quote_platinum_audit.json', store)
            write_store(root/'data/arab_quote_day1.json', {'matches': [m]})
            result = main(root, later)
            saved = read_store(root/'data/arab_quote_platinum_audit.json')
            self.assertEqual(saved['policy_id'], POLICY_ID)
            self.assertEqual(saved['fixtures']['100']['snapshots'][0], before)
            self.assertEqual(len(saved['fixtures']['100']['snapshots']), 2)
            self.assertEqual({(g['policy_id'], g['snapshot']) for g in result['groups']},
                             {(LEGACY_POLICY_ID, 'first'), (POLICY_ID, 'latest')})
            main(root, later)
            self.assertEqual(read_store(root/'data/arab_quote_platinum_audit.json'), saved)


if __name__ == '__main__': unittest.main()
