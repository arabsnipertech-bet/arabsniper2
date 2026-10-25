import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from audit_ledger import AuditLedger, capture_ledger, settle_ledger
from audit_storage import read_store, write_store, validate_store
from daily_audit import fingerprint, build_daily
from prematch_audit import build_report, settle_predictions
from multimarket_audit import build_report as market_report


def forecast(fid, captured='2026-10-20T10:00:00+02:00'):
    return {'fixture': {'fixture_id': str(fid), 'kickoff': '2026-10-20T18:00:00+02:00'},
            'captured_at': captured, 'model_version': 'context-goals-v3.4',
            'probabilities_calibrated': {'over15_ht': {'over': .6}},
            'multimarket': {'model_id': 'context-goals-v3.4', 'status': 'experimental',
                           'markets': [{'market': 'over25_ft', 'selection': 'over',
                                        'probabilities': {'statistics': .6}, 'odd': 1.9,
                                        'book_probability_pct': 52}],
                           'best_candidate': {'market': 'over25_ft', 'selection': 'over'}},
            'unicode': 'è già 🟡', 'numeric': [0.0, -1.5, 1e-6, 2**53+1]}


class LedgerTests(unittest.TestCase):
    def test_empty_slate_keeps_a_valid_forecast_archive(self):
        from test_policy_engine import engine
        now = datetime.fromisoformat('2026-10-20T10:00:00+02:00')
        expected = engine.capture_prematch_predictions({}, store={'schema_version': 1},
                                                      persist=False, observed_at=now)
        with tempfile.TemporaryDirectory() as tmp, AuditLedger() as ledger:
            ledger.metadata['schema_version'] = 1
            capture_ledger(ledger, {}, engine.capture_prematch_predictions, now)
            path = Path(tmp)/'empty.json'
            ledger.write(path)
            self.assertEqual(read_store(path), expected)

    def test_lossless_roundtrip_and_report_parity(self):
        first, latest = forecast(1), forecast(1, '2026-10-20T17:00:00+02:00')
        first['padding'] = 'x'*5000
        store = {'schema_version': 1, 'fixtures': {'1': first}, 'quality_evaluations': {},
                 'multimarket_evaluations': {'context-goals-v3.4::1': first},
                 'analysis_updates': {'context-goals-v3.4::1::latest': latest},
                 'signal_observations': {}}
        output = {'days': [{'matches': [{'fixture': {'fixture_id': '1', 'result': {
            'status': 'FT', 'home_ht': 1, 'away_ht': 1, 'home_ft': 2, 'away_ft': 1}}}]}]}
        # JSON does not preserve Python aliasing between collections.
        expected = json.loads(json.dumps(store))
        settle_predictions(expected, output)
        for packed in (False, True):
            with self.subTest(packed=packed), tempfile.TemporaryDirectory() as tmp:
                source = Path(tmp)/'source.json'
                target = Path(tmp)/'result.json'
                write_store(source, store, chunk_bytes=100 if packed else 100000)
                source_before = source.read_bytes()
                with AuditLedger(source) as ledger:
                    self.assertEqual(dict(ledger['fixtures']), store['fixtures'])
                    settle_ledger(ledger, output)
                    self.assertEqual(build_report(ledger), build_report(expected))
                    self.assertEqual(market_report(ledger), market_report(expected))
                    self.assertEqual(build_daily(ledger, '2026-10-20', 'now'),
                                     build_daily(expected, '2026-10-20', 'now'))
                    ledger.write(target)
                self.assertEqual(read_store(target), expected)
                self.assertEqual(source.read_bytes(), source_before)
                self.assertEqual(fingerprint(read_store(target)['fixtures']['1']), fingerprint(first))

    def test_scan_preserves_history_and_matches_in_memory_capture(self):
        from test_policy_engine import engine
        from scan_audit import update_audit
        import contextlib
        import io
        now = datetime.fromisoformat('2026-10-20T10:00:00+02:00')
        initial = {'schema_version': 1, 'fixtures': {'old': forecast('old')},
                   'multimarket_evaluations': {'context-goals-v3.4::old': forecast('old')}}
        row = forecast('new')
        match = {'fixture': row['fixture'], 'multimarket': row['multimarket'],
                 'model': {'probabilities_calibrated': row['probabilities_calibrated']}}
        output = {'days': [{'matches': [match]}]}
        expected = json.loads(json.dumps(initial))
        engine.capture_prematch_predictions(output, store=expected, persist=False, observed_at=now)
        settle_predictions(expected, output)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root/'arab_quote_prematch_audit.json'
            write_store(source, initial)
            ns = {'PREMATCH_AUDIT_FILE': source, 'DATA_DIR': root,
                  'now_rome': lambda: now, 'parse_iso': engine.parse_iso,
                  'capture_prematch_predictions': engine.capture_prematch_predictions,
                  'atomic_write_json': write_store, '_result_payload': engine._result_payload}
            with contextlib.redirect_stdout(io.StringIO()):
                report = update_audit(ns, output, None)
            self.assertEqual(read_store(source), expected)
            self.assertEqual(report, market_report(expected))
            self.assertEqual(read_store(root/'arab_quote_prematch_report.json'), build_report(expected))

    def test_existing_forecasts_survive_merge_and_index_closes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'audit.json'
            with AuditLedger() as ledger:
                directory = Path(ledger.temp.name)
                self.assertEqual(ledger.put('fixtures', '1', forecast(1)), 1)
                changed = forecast(1); changed['probabilities_calibrated'] = {}
                self.assertEqual(ledger.put('fixtures', '1', changed, overwrite=False), 0)
                ledger.write(path)
            self.assertFalse(directory.exists())
            self.assertEqual(read_store(path)['fixtures']['1'], forecast(1))

    def test_corrupt_json_or_part_never_replaces_previous_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            source, target = Path(tmp)/'source.json', Path(tmp)/'target.json'
            target.write_text('{"unchanged":true}')
            source.write_text('{"fixtures":{"1":{}}} garbage')
            with self.assertRaises(Exception):
                with AuditLedger(source) as ledger:
                    ledger.write(target)
            self.assertEqual(target.read_text(), '{"unchanged":true}')
            write_store(source, {'fixtures': {'1': forecast(1)}}, chunk_bytes=100)
            manifest = json.loads(source.read_text())
            (source.parent/'_audit_parts'/(manifest['parts'][-1]['sha256']+'.json.gz')).unlink()
            with self.assertRaises(FileNotFoundError):
                validate_store(source)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Linux address-space limit')
    def test_large_archive_under_bounded_memory(self):
        # A real 96 MiB JSON file is imported, exported and validated under a
        # 160 MiB address-space ceiling. A whole-document JSON load cannot fit.
        code = '''
import json,resource,tempfile
from pathlib import Path
from audit_ledger import AuditLedger
from audit_storage import validate_store
resource.setrlimit(resource.RLIMIT_AS,(160*1024*1024,160*1024*1024))
with tempfile.TemporaryDirectory() as tmp:
    path=Path(tmp)/'large.json'; target=Path(tmp)/'packed.json'
    row=json.dumps({'fixture':{'fixture_id':'one'},'padding':'x'*(1024*1024)})
    with path.open('w') as f:
        f.write('{"fixtures":{')
        for i in range(96):
            if i:f.write(',')
            f.write(json.dumps(str(i))+':'+row)
        f.write('}}')
    del row
    with AuditLedger(path) as ledger:
        assert len(ledger['fixtures'])==96
        ledger.write(target)
    validate_store(target)
    print('bounded-memory-ok')
'''
        result = subprocess.run([sys.executable, '-c', code], cwd=ROOT,
                                env={**os.environ, 'PYTHONPATH': str(ROOT/'src')+os.pathsep+os.environ.get('PYTHONPATH','')},
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('bounded-memory-ok', result.stdout)


if __name__ == '__main__':
    unittest.main()
