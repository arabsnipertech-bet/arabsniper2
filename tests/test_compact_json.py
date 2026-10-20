"""Production JSON writers retain records without whitespace expansion."""
import ast
import json
import os
from pathlib import Path
import tempfile
import unittest
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

class CompactJsonTests(unittest.TestCase):
    def test_real_writers_preserve_nested_history(self):
        payload = {"fixtures": {"42": {"name": "Città", "p": 0.123456789,
                   "quotes": [None, True, {"odd": 2.15}]}},
                   "signal_observations": {"a": {"captured_at": "2026-10-19T12:00:00Z"}}}
        for source, name in [("src/3appdays.py", "atomic_write_json"),
                             ("auditor/current_audit.py", "write")]:
            with self.subTest(source=source), tempfile.TemporaryDirectory() as folder:
                tree = ast.parse((ROOT / source).read_text())
                node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
                ns = dict(json=json, os=os, Path=Path, Any=Any)
                exec(compile(ast.Module(body=[node], type_ignores=[]), source, "exec"), ns)
                target = Path(folder) / "data" / "audit.json"
                ns[name](target, payload)
                raw = target.read_text()
                self.assertEqual(json.loads(raw), payload)
                self.assertEqual(raw.strip(), json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                ns[name](target, {"previous": payload, "new": [1, 2]})
                self.assertEqual(json.loads(target.read_text()), {"previous": payload, "new": [1, 2]})
                self.assertFalse(list(target.parent.glob("*.tmp")))

if __name__ == "__main__":
    unittest.main()
