import ast
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from audit_storage import read_store, FORMAT
from publication_storage import prepare


class PublicationTests(unittest.TestCase):
    def test_real_scan_writer_bounds_all_public_outputs(self):
        tree = ast.parse((ROOT / 'src/3appdays.py').read_text())
        nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('atomic_write_json', 'read_json')]
        ns = dict(Path=Path, Any=Any, log=lambda *a: None)
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'scan', 'exec'), ns)
        value = {'matches': [{'text': 'è' * (3 * 1024 * 1024), 'probability': 0.123456789}]}
        with tempfile.TemporaryDirectory() as d:
            for name in ['dashboard', 'day1', 'state', 'xg_cache']:
                p = Path(d) / f'arab_quote_{name}.json'
                ns['atomic_write_json'](p, value)
                self.assertEqual(json.loads(p.read_text())['_storage'], FORMAT)
                self.assertEqual(ns['read_json'](p, {}), value)
            part = next((Path(d) / '_audit_parts').iterdir())
            part.unlink()
            with self.assertRaises(FileNotFoundError):
                ns['read_json'](p, {})

    def test_legacy_recovery_preserves_all_records(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'arab_quote_dashboard.json'
            value = {'days': [{'matches': [{'id': 42, 'text': 'x' * (5 * 1024 * 1024)}]}]}
            p.write_text(json.dumps(value))
            prepare(d)
            self.assertEqual(read_store(p), value)
            self.assertLess(p.stat().st_size, 4096)
            prepare(d)
            self.assertEqual(read_store(p), value)
