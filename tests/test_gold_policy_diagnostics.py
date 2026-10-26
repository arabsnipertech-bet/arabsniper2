from copy import deepcopy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from audit_storage import write_store
from gold_storage import sha
from gold_policy_diagnostics import inspect_checkpoint
from test_gold_policy_audit import fixture
from test_statistics_first import NOW


class ReadOnlyStore:
    def __init__(self, split=False):
        self.objects, self.calls = {}, []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root/'data').mkdir()
            m = fixture(); m['fixture'].update(home='Odense', country='Denmark', league='Superliga', match='Odense - Opponent')
            for day in range(1, 6):
                write_store(root/f'data/arab_quote_day{day}.json', {'matches': [m] if day == 1 else []}, chunk_bytes=200 if split else 4*1024*1024)
            files = {}
            for path in (root/'data').rglob('*'):
                if not path.is_file(): continue
                raw = path.read_bytes(); digest = sha(raw)
                files[path.relative_to(root).as_posix()] = {'sha256': digest, 'bytes': len(raw)}
                self.objects['state/objects/'+digest] = raw
            self.objects['state/current.json'] = json.dumps({'version': 1, 'files': files}).encode()

    def get_object(self, Bucket, Key):
        self.calls.append(Key)
        return {'Body': io.BytesIO(self.objects[Key])}

    def put_object(self, **kwargs): raise AssertionError('Read-only diagnosis cannot write')
    def delete_object(self, **kwargs): raise AssertionError('Read-only diagnosis cannot delete')


class GoldPolicyDiagnostics(unittest.TestCase):
    def test_small_and_split_files_are_read_only_and_export_only_categorical_cases(self):
        for split in (False, True):
            s3 = ReadOnlyStore(split); before = deepcopy(s3.objects)
            with patch('socket.socket', side_effect=AssertionError('No football/network calls')):
                report = inspect_checkpoint(s3, 'test', NOW)
            self.assertEqual(s3.objects, before)
            self.assertEqual(report['checkpoint_writes'], 0)
            self.assertEqual(report['api_football_calls'], 0)
            self.assertEqual(report['counts']['2026-10-26']['published'], 1)
            case = report['requested_cases'][0]
            self.assertTrue(case['scope_allowed'])
            self.assertTrue(all('odd' not in r and 'statistical_pct' not in r for r in case['markets']))
            stale = inspect_checkpoint(s3, 'test', '2026-10-26T19:00:00Z')
            self.assertNotIn('published', stale['counts']['2026-10-26'])
            self.assertEqual(stale['counts']['2026-10-26']['last_saved_proposals'], 1)
            self.assertTrue(any(r['eligible'] for r in stale['requested_cases'][0]['as_published']['markets']))
            self.assertFalse(any(r['eligible'] for r in stale['requested_cases'][0]['markets']))

    def test_corrupt_or_missing_objects_fail_without_fallback_or_writes(self):
        s3 = ReadOnlyStore()
        key = next(k for k in s3.objects if k.startswith('state/objects/'))
        s3.objects[key] = b'{}'
        with self.assertRaisesRegex(ValueError, 'checksum'): inspect_checkpoint(s3, 'test', NOW)
        del s3.objects[key]
        with self.assertRaises(KeyError): inspect_checkpoint(s3, 'test', NOW)


if __name__ == '__main__': unittest.main()
