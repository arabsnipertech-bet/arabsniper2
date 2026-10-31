from copy import deepcopy
from datetime import datetime
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from auditor_schedule import admission, preflight, already_completed, MARKER
from nightly_auditor import PROGRESS, run, run_command
from audit_journal import capture_forecasts, capture_policies, ingest, PREFIX
from audit_storage import read_store, write_store


class NightlySchedule(unittest.TestCase):
    def test_rome_window_boundaries_and_summer_time(self):
        for value, allowed in [
                ('2026-10-31T00:29:59+01:00', False),
                ('2026-10-30T23:30:00Z', True),
                ('2026-10-31T00:59:59+01:00', True),
                ('2026-10-31T01:00:00+01:00', False),
                ('2026-10-31T01:55:00+01:00', False),
                ('2026-10-31T16:30:00+01:00', False),
                ('2026-07-30T22:30:00Z', True)]:
            with self.subTest(value=value):
                result = admission(datetime.fromisoformat(value.replace('Z', '+00:00')))
                self.assertEqual(result['run'], allowed)
                self.assertIn('T01:50:00', result['deadline'])
        with self.assertRaises(ValueError):
            admission(datetime(2026, 10, 31, 0, 30))

    def test_daytime_trigger_never_enters_writer_queue_or_reads_storage(self):
        api = Mock()
        now = datetime.fromisoformat('2026-10-30T16:00:00+01:00')
        self.assertFalse(preflight(api, now, 1)['run'])
        api.active_runs.assert_not_called()
        runner = Mock()
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(run(folder, lambda: now, runner), 'outside_audit_window')
        runner.assert_not_called()

    def test_active_fast_blocks_nightly_admission_without_queueing(self):
        api = Mock()
        now = datetime.fromisoformat('2026-10-31T00:35:00+01:00')
        api.active_runs.return_value = [{'id': 9, 'path': '.github/workflows/fast_scan.yml'}]
        self.assertEqual(preflight(api, now, 1)['reason'], 'writer_busy')
        api.active_runs.return_value = [{'id': 1, 'path': '.github/workflows/run-auditor.yml'}]
        self.assertTrue(preflight(api, now, 1)['run'])

    def test_duplicate_completion_is_scoped_to_the_rome_date(self):
        now = datetime.fromisoformat('2026-10-31T00:45:00+01:00')
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/MARKER
            marker = {'schema_version': 1, 'status': 'complete', 'date': '2026-10-31',
                      'started_at': '2026-10-31T00:30:00+01:00', 'completed_at': now.isoformat()}
            write_store(path, marker)
            self.assertTrue(already_completed(folder, now))
            self.assertFalse(already_completed(folder, datetime.fromisoformat('2026-11-01T00:45:00+01:00')))
            marker['completed_at'] = '2026-10-31T01:30:00+01:00'
            write_store(path, marker)
            self.assertFalse(already_completed(folder, now))

    def test_runner_restores_then_skips_a_published_duplicate(self):
        now = datetime.fromisoformat('2026-10-31T00:45:00+01:00')
        runner = Mock()
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'GOLD_PRIVATE': 'true'}):
            write_store(Path(folder)/MARKER, {'schema_version': 1, 'status': 'complete',
                'date': '2026-10-31', 'started_at': '2026-10-31T00:30:00+01:00', 'completed_at': now.isoformat()})
            self.assertEqual(run(folder, lambda: now, runner), 'already_completed')
        self.assertEqual(len(runner.call_args_list), 1)
        self.assertEqual(runner.call_args.args[1][-1], 'restore')

    def test_runner_orders_evidence_before_settlement_and_publication(self):
        now = datetime.fromisoformat('2026-10-31T00:35:00+01:00')
        calls = []
        def runner(root, command, deadline, clock):
            self.assertEqual(deadline.isoformat(), '2026-10-31T01:50:00+01:00')
            calls.append(command[1:])
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'GOLD_PRIVATE': 'true'}):
            self.assertEqual(run(folder, lambda: now, runner), 'complete')
            self.assertTrue(already_completed(folder, now))
        self.assertEqual(calls, [
            ['src/gold_storage.py', 'restore'], ['auditor/current_audit.py', 'prepare'],
            ['auditor/current_audit.py', 'daily', '--date', '2026-10-30'],
            ['src/gold_storage.py', 'publish'], ['src/homepage_results.py'],
            ['src/private_publication.py', '--results-only'],
            ['auditor/current_audit.py', 'history', '--date', '2026-10-30'],
            ['src/gold_storage.py', 'publish'], ['src/gold_policy_audit.py'],
            ['src/gold_storage.py', 'publish']])

    def test_interrupted_history_resumes_the_committed_day_before_new_journals(self):
        now = datetime.fromisoformat('2026-10-31T00:40:00+01:00')
        remote = {}
        calls = []
        def runner(root, command, deadline, clock):
            args = command[1:]
            calls.append(args)
            if args == ['src/gold_storage.py', 'restore']:
                for path, raw in remote.items(): write_store(root/path, raw)
            elif args == ['src/gold_storage.py', 'publish']:
                remote.clear()
                for path in (PROGRESS, MARKER):
                    if (root/path).exists(): remote[path] = read_store(root/path)
            elif args[:2] == ['auditor/current_audit.py', 'history'] and not remote.get(MARKER) and fail[0]:
                fail[0] = False
                raise TimeoutError('protected cutoff')
        fail = [True]
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second, patch.dict(os.environ, {'GOLD_PRIVATE':'true'}):
            with self.assertRaises(TimeoutError): run(first, lambda: now, runner)
            self.assertEqual(remote[PROGRESS]['stage'], 'daily')
            self.assertNotIn(MARKER, remote)
            calls.clear()
            next_night = datetime.fromisoformat('2026-11-01T00:40:00+01:00')
            self.assertEqual(run(second, lambda: next_night, runner), 'complete')
            self.assertLess(calls.index(['auditor/current_audit.py','history','--date','2026-10-30']),
                            calls.index(['auditor/current_audit.py','prepare']))
            self.assertNotIn(['auditor/current_audit.py','daily','--date','2026-10-30'], calls)
            self.assertEqual(remote[MARKER]['date'],'2026-11-01')
            self.assertNotIn(PROGRESS, remote)

    def test_deadline_does_not_start_another_phase(self):
        now = datetime.fromisoformat('2026-10-31T01:50:00+01:00')
        with patch('nightly_auditor.subprocess.Popen') as process:
            with self.assertRaises(TimeoutError):
                run_command(ROOT, ['unused'], now, lambda: now)
            process.assert_not_called()

    def test_deadline_terminates_the_entire_phase(self):
        now = datetime.fromisoformat('2026-10-31T01:49:59+01:00')
        end = datetime.fromisoformat('2026-10-31T01:50:00+01:00')
        child = Mock(pid=1234)
        child.wait.side_effect = [subprocess.TimeoutExpired('unused', 1), 0]
        with patch('nightly_auditor.subprocess.Popen', return_value=child), patch('nightly_auditor.os.killpg') as kill:
            with self.assertRaises(TimeoutError):
                run_command(ROOT, ['unused'], end, lambda: now)
            kill.assert_called_once()
            self.assertEqual(kill.call_args.args[0], 1234)


class ScanJournal(unittest.TestCase):
    def namespace(self, directory, now):
        from test_policy_engine import engine
        return {'DATA_DIR': directory, 'PREMATCH_AUDIT_FILE': directory/'arab_quote_prematch_audit.json',
                'capture_prematch_predictions': engine.capture_prematch_predictions,
                'now_rome': lambda: now, 'DEFER_AUDIT': True}

    def test_fast_does_not_read_historical_ledger_or_query_results(self):
        from scan_audit import update_audit
        now = datetime.fromisoformat('2026-10-30T16:00:00+01:00')
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            old = directory/'arab_quote_prematch_audit.json'
            old.write_text('historical archive must not be read by this scan')
            client = Mock()
            report = update_audit(self.namespace(directory, now), {'days': []}, client)
            client.get.assert_not_called()
            self.assertEqual(report['status'], 'nightly_audit_pending')
            self.assertEqual(old.read_text(), 'historical archive must not be read by this scan')
            self.assertEqual(len(list(directory.glob(PREFIX+'*.json'))), 1)

    def test_ingestion_preserves_first_latest_and_is_retry_safe(self):
        from test_policy_engine import engine
        from test_audit_ledger import forecast
        first = datetime.fromisoformat('2026-10-20T10:00:00+02:00')
        second = datetime.fromisoformat('2026-10-20T11:00:00+02:00')
        row = forecast('1')
        match = {'fixture': row['fixture'], 'multimarket': row['multimarket']}
        output = {'days': [{'matches': [match]}]}
        expected = {'schema_version': 1, 'fixtures': {}}
        for now in (first, second):
            engine.capture_prematch_predictions(output, store=expected, persist=False, observed_at=now)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); directory = root/'data'
            for now in (first, second):
                capture_forecasts(self.namespace(directory, now), output)
            pending = {p: p.read_bytes() for p in directory.glob(PREFIX+'*.json')}
            self.assertEqual(ingest(root), 2)
            self.assertEqual(read_store(directory/'arab_quote_prematch_audit.json'), expected)
            for p, raw in pending.items(): p.write_bytes(raw)
            ingest(root)
            self.assertEqual(read_store(directory/'arab_quote_prematch_audit.json'), expected)
            self.assertFalse(list(directory.glob(PREFIX+'*.json')))

    def test_policy_choices_survive_deferred_ingestion_with_original_times(self):
        from test_gold_policy_audit import fixture, NOW
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_store(root/'data/arab_quote_day1.json', {'matches': [fixture()]})
            journal = capture_policies(root, NOW)
            frozen = read_store(journal)['gold']['fixtures']
            self.assertTrue(frozen)
            ingest(root)
            self.assertEqual(read_store(root/'data/arab_quote_gold_policy_audit.json')['fixtures'], frozen)

    def test_platinum_snapshot_integrity_and_failed_merge_keep_pending_evidence(self):
        from test_platinum_policy import fixture, NOW
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_store(root/'data/arab_quote_day1.json', {'matches': [fixture()]})
            journal = capture_policies(root, NOW)
            frozen = read_store(journal)
            snapshots = frozen['platinum']['fixtures']['100']['snapshots']
            self.assertTrue(snapshots)
            bad = deepcopy(frozen); bad['platinum']['fixtures']['100']['snapshots'][0]['choice']['odd'] = 999
            write_store(journal, bad)
            with self.assertRaises(ValueError): ingest(root)
            self.assertTrue(journal.exists())
            write_store(journal, frozen); ingest(root)
            self.assertEqual(read_store(root/'data/arab_quote_platinum_audit.json')['fixtures']['100']['snapshots'], snapshots)

    def test_journal_refuses_same_phase_timestamp_overwrite(self):
        now = datetime.fromisoformat('2026-10-30T16:00:00+01:00')
        with tempfile.TemporaryDirectory() as folder:
            ns = self.namespace(Path(folder), now)
            path = capture_forecasts(ns, {'days': []}); original = path.read_bytes()
            with self.assertRaises(ValueError): capture_forecasts(ns, {'days': []})
            self.assertEqual(path.read_bytes(), original)

    def test_unchanged_history_is_not_reparsed_but_corruption_still_blocks_publication(self):
        from gold_storage import publish, GOLD
        from test_gold_storage import Fake
        from audit_storage import validate_store
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); directory = root/'data'; directory.mkdir()
            for name in GOLD: write_store(directory/name, {'matches': []})
            history = directory/'arab_quote_prematch_audit.json'
            write_store(history, {'fixtures': {'1': {'text': 'x'*2000}}}, chunk_bytes=100)
            storage = Fake(); publish(storage, 'test', root)
            with patch('gold_storage.validate_store', wraps=validate_store) as validate:
                publish(storage, 'test', root)
                self.assertFalse(any(c.args[0] == history for c in validate.call_args_list))
            old_pointer = storage.objects['state/current.json']
            chunk = next((directory/'_audit_parts').iterdir()); chunk.write_bytes(b'corrupt')
            with self.assertRaises(Exception): publish(storage, 'test', root)
            self.assertEqual(storage.objects['state/current.json'], old_pointer)


if __name__ == '__main__':
    unittest.main()
