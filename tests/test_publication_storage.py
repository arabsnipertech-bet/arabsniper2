import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from audit_storage import read_store, write_store, FORMAT
from publication_storage import prepare, prepare_file


class PublicationTests(unittest.TestCase):
    def test_existing_manifest_is_verified_without_rewriting(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'arab_quote_state.json'
            value = {'rows': ['è' * 1000] * 3}
            write_store(path, value, chunk_bytes=257)
            original = path.read_bytes()
            with patch('publication_storage.write_store', side_effect=AssertionError('repacked')):
                prepare_file(path)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(read_store(path), value)
            manifest = json.loads(original)
            part = path.parent / '_audit_parts' / (manifest['parts'][0]['sha256'] + '.json.gz')
            part.write_bytes(b'broken')
            with self.assertRaises(Exception):
                prepare_file(path)

    def test_legacy_large_json_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'arab_quote_state.json'
            value = {'rows': ['è' * 1000] * 2500}
            path.write_text(json.dumps(value, ensure_ascii=False))
            prepare_file(path)
            self.assertEqual(json.loads(path.read_bytes())['_storage'], FORMAT)
            self.assertEqual(read_store(path), value)

    def test_pipeline_and_corruption_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'arab_quote_state.json'
            write_store(path, {'rows': ['a' * 1000]}, chunk_bytes=257)
            prepare(directory)
            self.assertTrue((Path(directory) / 'arab_quote_homepage.json').exists())
            manifest = json.loads(path.read_bytes())
            (path.parent / '_audit_parts' / (manifest['parts'][0]['sha256'] + '.json.gz')).unlink()
            with self.assertRaises(Exception):
                prepare(directory)


if __name__ == '__main__':
    unittest.main()
