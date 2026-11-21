"""A fresh runner resumes durable prematch work without claiming certification."""
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from audit_daily_evidence import verified_evidence
from audit_journal import PREFIX
from audit_ledger import entries
from audit_storage import read_store, write_store
from auditor_daily_priority import build_yesterday, save_frozen_checkpoint, v7_ready
from auditor_schedule import MARKER
from gold_storage import previous_manifest, publish, restore_v7_priority
from test_audit_ledger import forecast
from test_gold_storage import Fake

NOW = datetime.fromisoformat('2026-11-21T00:40:00+01:00')
DAY = date(2026, 11, 20)
FROZEN = f'auditordata/prematch_{DAY}.json'


def journal(root, number, *, padding=0):
    captured = f'2026-11-20T10:{number//60:02d}:{number%60:02d}+01:00'
    row = forecast('1', captured)
    row['fixture']['kickoff'] = '2026-11-20T18:00:00+01:00'
    row['padding'] = 'x'*padding
    row['v7_original'] = {'p_cal': .24, 'ev_pct': -4.0, 'odd': 4.0}
    kind = 'forecasts' if number % 2 == 0 else 'policies'
    key = f'context-goals-v3.4::1::{captured}'
    collection, records = ('fixtures', {'1': row}) if kind == 'forecasts' else ('analysis_updates', {key: row})
    path = Path(root)/'data'/f'{PREFIX}20261120T09{number//60:02d}{number%60:02d}000000_{kind}.json'
    write_store(path, {'schema_version': 1, 'journal_type': kind, collection: records})
    return path


def results(*_):
    return {'days': [{'matches': [{'fixture': {'fixture_id': '1', 'result': {
        'status': 'FT', 'home_ht': 1, 'away_ht': 0, 'home_ft': 2, 'away_ft': 1}}}]}]}


class FreezeResume(unittest.TestCase):
    def setUp(self):
        self.storage = Fake()
        environment = patch.dict(os.environ, {'GOLD_PRIVATE': 'true', 'R2_BUCKET': 'test',
                                             'API_SPORTS_KEY': 'offline-test'})
        environment.start()
        self.addCleanup(environment.stop)
        client = patch('gold_storage.client', return_value=self.storage)
        client.start()
        self.addCleanup(client.stop)

    def freeze_then_timeout(self, root):
        with patch('current_audit.settle_day', side_effect=TimeoutError('protected deadline')):
            with self.assertRaisesRegex(TimeoutError, 'protected deadline'):
                build_yesterday(root, DAY, NOW, checkpoint_frozen=save_frozen_checkpoint)

    def test_109_journals_survive_timeout_and_fresh_runner_skips_replay(self):
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            root = Path(seed)
            sources = [journal(root, number) for number in range(109)]
            originals = {path: path.read_bytes() for path in sources}
            unrelated = 'data/arab_quote_unrelated.json'
            write_store(root/unrelated, {'preserved': True})
            publish(self.storage, 'test', root, checkpoint_only=True)
            before = previous_manifest(self.storage, 'test')
            self.freeze_then_timeout(root)
            after = previous_manifest(self.storage, 'test')
            self.assertEqual({key: after[key] for key in before}, before)
            self.assertFalse(v7_ready(root, NOW.date()))
            self.assertIsNone(verified_evidence(root, DAY))
            self.assertFalse((root/MARKER).exists())
            restore_v7_priority(self.storage, 'test', work, DAY)
            source_reads = []

            def counted(path, **kwargs):
                if Path(path).name.startswith(PREFIX):
                    source_reads.append(path)
                return entries(path, **kwargs)

            with patch('audit_ledger.entries', side_effect=counted), \
                    patch('audit_journal.ForecastHistory') as history:
                build_yesterday(work, DAY, NOW, results, max_result_ids=20,
                                checkpoint_frozen=save_frozen_checkpoint)
            self.assertEqual(source_reads, [])
            history.assert_not_called()
            frozen = read_store(Path(work)/FROZEN)
            self.assertEqual(len(frozen['priority_journals']['sha256']), 109)
            self.assertEqual(len(frozen['analysis_updates']), 54)
            self.assertEqual(frozen['fixtures']['1']['v7_original'],
                             {'p_cal': .24, 'ev_pct': -4.0, 'odd': 4.0})
            self.assertIsNotNone(verified_evidence(work, DAY))
            self.assertFalse(v7_ready(work, NOW.date()), 'Learning still needs its separate certificate')
            self.assertFalse((Path(work)/MARKER).exists())
            for path, raw in originals.items():
                self.assertEqual(path.read_bytes(), raw)
            self.assertTrue(all(key.startswith('state/') for key in self.storage.calls))

    def test_new_journal_is_merged_without_replaying_previous_inputs(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            journal(root, 0)
            self.freeze_then_timeout(root)
            new = journal(root, 1)
            reads = []

            def counted(path, **kwargs):
                if Path(path).name.startswith(PREFIX):
                    reads.append(Path(path))
                return entries(path, **kwargs)

            with patch('audit_ledger.entries', side_effect=counted):
                self.freeze_then_timeout(root)
            self.assertEqual(reads, [new])
            data = read_store(root/FROZEN)
            self.assertEqual(len(data['priority_journals']['sha256']), 2)
            self.assertEqual(len(data['fixtures']), 1)
            self.assertEqual(len(data['analysis_updates']), 1)

    def test_changed_processed_journal_fails_before_results_or_new_checkpoint(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            source = journal(root, 0)
            self.freeze_then_timeout(root)
            frozen = (root/FROZEN).read_bytes()
            previous = self.storage.objects['state/current.json']
            changed = read_store(source)
            changed['fixtures']['1']['v7_original']['ev_pct'] = 99
            write_store(source, changed)
            with patch('current_audit.settle_day') as settle:
                with self.assertRaisesRegex(ValueError, 'Previously frozen priority journal changed'):
                    build_yesterday(root, DAY, NOW, checkpoint_frozen=save_frozen_checkpoint)
            settle.assert_not_called()
            self.assertEqual((root/FROZEN).read_bytes(), frozen)
            self.assertEqual(self.storage.objects['state/current.json'], previous)

    def test_failed_checkpoint_stops_before_result_queries_and_preserves_remote_pointer(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            journal(root, 0)
            publish(self.storage, 'test', root, checkpoint_only=True)
            previous = self.storage.objects['state/current.json']
            with patch('gold_storage.publish', side_effect=RuntimeError('storage unavailable')), \
                    patch('current_audit.settle_day') as settle:
                with self.assertRaisesRegex(RuntimeError, 'storage unavailable'):
                    build_yesterday(root, DAY, NOW, checkpoint_frozen=save_frozen_checkpoint)
            settle.assert_not_called()
            self.assertEqual(self.storage.objects['state/current.json'], previous)
            self.assertFalse((root/MARKER).exists())

    def test_invalid_journal_cannot_leave_a_false_replay_receipt(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            source = journal(root, 0)
            data = read_store(source)
            data['journal_type'] = 'policies'
            write_store(source, data)
            with patch('current_audit.settle_day') as settle:
                with self.assertRaisesRegex(ValueError, 'Invalid forecast journal'):
                    build_yesterday(root, DAY, NOW, checkpoint_frozen=save_frozen_checkpoint)
            settle.assert_not_called()
            self.assertFalse((root/FROZEN).exists())
            self.assertFalse(self.storage.calls)

    def test_chunked_frozen_checkpoint_restores_on_a_fresh_runner(self):
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            root = Path(seed)
            journal(root, 0, padding=4*1024*1024)
            publish(self.storage, 'test', root, checkpoint_only=True)
            self.freeze_then_timeout(root)
            pointer = json.loads((root/FROZEN).read_bytes())
            self.assertIn('parts', pointer)
            restore_v7_priority(self.storage, 'test', work, DAY)
            self.assertEqual(read_store(Path(work)/FROZEN), read_store(root/FROZEN))
            self.assertTrue(any('/_audit_parts/' in key for key in previous_manifest(self.storage, 'test')))

    def test_malformed_replay_metadata_is_rejected(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            write_store(root/FROZEN, {'schema_version': 1, 'audit_date': str(DAY), 'fixtures': {},
                                     'priority_journals': {'version': 1, 'sha256': {'source': 'bad'}}})
            with patch('current_audit.settle_day') as settle:
                with self.assertRaisesRegex(ValueError, 'Invalid priority journal checkpoint'):
                    build_yesterday(root, DAY, NOW)
            settle.assert_not_called()

    def test_private_checkpoint_cannot_be_used_without_production_storage(self):
        with patch.dict(os.environ, {'GOLD_PRIVATE': 'false'}):
            with self.assertRaisesRegex(ValueError, 'requires private production storage'):
                save_frozen_checkpoint(Path('.'), DAY)
        self.assertFalse(self.storage.calls)


if __name__ == '__main__':
    unittest.main()
