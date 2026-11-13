import sys
from pathlib import Path
from datetime import datetime
from tempfile import TemporaryDirectory
import unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from repair_calibration import refresh
from gold_storage import previous_manifest, sha, write_manifest
from test_gold_storage import Fake

class RepairTrainingTests(unittest.TestCase):
    def storage(self, complete=True):
        storage = Fake(); index = {}
        files = {'auditarchive/prematch_2026-11-05_daily.json': b'{}',
                 'auditarchive/prematch_2026-11-05_results.json': b'{}'}
        if complete: files['auditarchive/prematch_2026-11-05_calibration.json'] = b'[]\n'
        for path, raw in files.items():
            checksum = sha(raw); index[path] = dict(sha256=checksum, bytes=len(raw))
            storage.put_object(Bucket='test', Key='state/objects/'+checksum, Body=raw)
        write_manifest(storage, 'test', index)
        return storage

    def test_missing_evidence_never_changes_checkpoint(self):
        storage = self.storage(False); before = previous_manifest(storage, 'test')
        with TemporaryDirectory() as d, self.assertRaisesRegex(ValueError, 'Incomplete learning chain'):
            refresh(storage, 'test', Path(d), datetime.fromisoformat('2026-11-07T12:00:00+01:00'))
        self.assertEqual(previous_manifest(storage, 'test'), before)

    def test_explicit_empty_evidence_is_distinct_and_hashed(self):
        storage = self.storage(); before = previous_manifest(storage, 'test')
        with TemporaryDirectory() as d:
            state = refresh(storage, 'test', Path(d), datetime.fromisoformat('2026-11-07T12:00:00+01:00'))
        self.assertTrue(state['training_evidence']['complete'])
        self.assertEqual(state['training_evidence']['files'][0]['sha256'], sha(b'[]\n'))
        after = previous_manifest(storage, 'test')
        self.assertEqual({p:after[p] for p in before}, before)
        self.assertIn('data/arab_calibration_weights.json', after)

    def test_abandoned_archives_do_not_block_training_for_other_days(self):
        storage = self.storage()
        before = previous_manifest(storage, 'test')
        for day in ('2026-11-07', '2026-11-08'):
            path = f'auditordata/prematch_{day}.json'
            raw = b'{}'
            checksum = sha(raw)
            storage.put_object(Bucket='test', Key='state/objects/'+checksum, Body=raw)
            before[path] = dict(sha256=checksum, bytes=len(raw))
        write_manifest(storage, 'test', before)
        with TemporaryDirectory() as folder:
            state = refresh(storage, 'test', Path(folder), datetime.fromisoformat('2026-11-13T12:00:00+01:00'))
        self.assertTrue(state['training_evidence']['complete'])
        self.assertEqual(state['evidence_provenance']['excluded_recovery_dates'], ['2026-11-07', '2026-11-08'])
        self.assertEqual(state['evidence_provenance']['dates'], ['2026-11-05'])
        after = previous_manifest(storage, 'test')
        self.assertEqual({path: after[path] for path in before}, before)
