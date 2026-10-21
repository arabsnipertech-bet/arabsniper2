import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from audit_storage import write_store, read_store
from storage_cleanup import clean


class CleanupTests(unittest.TestCase):
    def test_only_orphans_removed_and_all_snapshots_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for folder in ('data', 'auditordata', 'auditarchive'):
                path=root/folder/'snapshot.json'
                write_store(path, {'old':'z'*200}, chunk_bytes=50)
                write_store(path, {'forecast':'è'*300,'result':None}, chunk_bytes=50)
                (path.parent/'legacy.json').write_text('{"historic":true}')
            before={p:p.read_bytes() for p in root.rglob('*.json')}
            report=clean(root)
            self.assertGreater(report['candidate_parts'],0)
            unknown=root/'data/_audit_parts/keep-me.json.gz';unknown.write_bytes(b'unknown')
            applied=clean(root,True)
            self.assertEqual(applied['removed_parts'],report['candidate_parts'])
            for p,b in before.items():self.assertEqual(p.read_bytes(),b)
            self.assertTrue(unknown.exists())
            self.assertEqual(read_store(root/'data/snapshot.json')['forecast'],'è'*300)
            self.assertEqual(clean(root)['candidate_parts'],0)

    def test_bad_manifest_aborts_before_any_deletion(self):
        for bad in ('missing', 'malformed', 'unknown'):
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as d:
                root=Path(d);path=root/'data/snapshot.json'
                write_store(path,{'old':'z'*200},chunk_bytes=50)
                write_store(path,{'current':'x'*300},chunk_bytes=50)
                if bad=='missing':
                    part=json.loads(path.read_text())['parts'][0]['sha256']
                    (path.parent/'_audit_parts'/f'{part}.json.gz').unlink()
                elif bad=='malformed':path.write_text('{broken')
                else:path.write_text('{"_storage":"future-format"}')
                before={p:p.read_bytes() for p in root.rglob('*') if p.is_file()}
                with self.assertRaises(Exception):clean(root,True)
                for p,b in before.items():self.assertEqual(p.read_bytes(),b)
