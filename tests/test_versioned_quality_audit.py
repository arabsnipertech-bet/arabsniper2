import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from prematch_audit import settle_predictions, build_report

spec = importlib.util.spec_from_file_location('quality_current_audit', ROOT/'auditor/current_audit.py')
auditor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auditor)


def data():
    original = {'fixture': {'fixture_id': '1', 'kickoff': '2026-10-20T15:00:00+02:00'},
        'captured_at': '2026-10-19T10:00:00+02:00', 'model_version': '1.1.8',
        'probabilities_calibrated': {'over15_ht': {'over': .3}}}
    quality = copy.deepcopy(original)
    quality.update(model_version='1.1.9', first_half_assessment={'profile_id': 'over15_ht_convergence_v1'},
                   probabilities_calibrated={'over15_ht': {'over': .6}})
    key = '1.1.9::over15_ht_convergence_v1::1'
    return {'fixtures': {'1': original}, 'quality_evaluations': {key: quality}}


def results():
    return {'days': [{'matches': [{'fixture': {'fixture_id': '1', 'result': {
        'status': 'FT', 'home_ht': 2, 'away_ht': 0, 'home_ft': 3, 'away_ft': 0}}}]}]}


class VersionedQualityAuditTests(unittest.TestCase):
    def test_both_forecasts_settle_without_replacing_probabilities(self):
        store = data()
        settle_predictions(store, results())
        self.assertTrue(store['fixtures']['1']['settlement']['outcomes']['over15_ht'])
        quality = next(iter(store['quality_evaluations'].values()))
        self.assertTrue(quality['settlement']['outcomes']['over15_ht'])
        self.assertEqual(store['fixtures']['1']['probabilities_calibrated']['over15_ht']['over'], .3)
        self.assertEqual(quality['probabilities_calibrated']['over15_ht']['over'], .6)
        report = build_report(store)
        self.assertEqual(report['first_half_quality_by_version'][0]['model_version'], '1.1.9')
        self.assertEqual(report['first_half_quality_by_version'][0]['comparison']['all_predictions']['n'], 1)

    def test_daily_archive_preserves_both_and_uses_one_result_request(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root/'data/arab_quote_prematch_audit.json'
            source.parent.mkdir()
            source.write_text(json.dumps(data()))
            now = datetime.fromisoformat('2026-10-21T09:00:00+02:00')
            auditor.archive(root, now)
            day_path = root/'auditordata/prematch_2026-10-20.json'
            before = json.loads(day_path.read_text())
            changed = data()
            next(iter(changed['quality_evaluations'].values()))['probabilities_calibrated']['over15_ht']['over'] = .99
            source.write_text(json.dumps(changed))
            auditor.archive(root, now)
            self.assertEqual(json.loads(day_path.read_text()), before)
            calls = []
            def fetcher(date, key):
                calls.append(date)
                return results()
            with patch.dict(os.environ, {'API_SPORTS_KEY': 'test-placeholder'}):
                auditor.settle(root, now, fetcher=fetcher)
            self.assertEqual(calls, ['2026-10-20'])
            stored = json.loads((root/'auditarchive/prematch_2026-10-20_results.json').read_text())
            self.assertTrue(next(iter(stored['quality_evaluations'].values()))['settlement']['outcomes']['over15_ht'])
            report = json.loads((root/'auditarchive/prematch_last_report.json').read_text())
            self.assertEqual(report['first_half_quality_by_version'][0]['comparison']['all_predictions']['n'], 1)
