from datetime import date, datetime, timedelta
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from audit_daily_evidence import read_receipt, receipt_path, verified_evidence
from audit_storage import read_store, write_store
from auditor_daily_priority import record_v7_status, v7_ready
from auditor_light import PROGRESS, WORK_BUDGET, cycle, recovery_candidate, remote_receipt
from auditor_schedule import MARKER, preflight
from current_audit import official_scores, settle_day
from fast_backup_dispatch import IncompleteListing, TemporaryReadError
from gold_storage import previous_manifest, publish, write_manifest
from nightly_auditor import run
from test_daily_audit import record
from test_gold_storage import Fake
from v7_learning import run as learn_v7

NOW = datetime.fromisoformat('2026-11-20T00:40:00+01:00')
DAY = date(2026, 11, 19)
SCORE = {'status': 'FT', 'home_ht': 1, 'away_ht': 0, 'home_ft': 2, 'away_ft': 1,
         'source': 'API-Football', 'transport': 'audit_api'}


def seed_day(root, day, complete=True):
    row = record(f'{day}T10:00:00+01:00')
    row['fixture']['kickoff'] = f'{day}T18:00:00+01:00'
    row.pop('settlement')
    write_store(root/f'auditordata/prematch_{day}.json',
                {'schema_version': 1, 'audit_date': str(day),
                 'multimarket_evaluations': {'context-goals-v3.4::1': row}, 'fixtures': {}})
    matches = [{'fixture': {'fixture_id': '1', 'result': SCORE}}] if complete else []
    checked = NOW if day == DAY else NOW-timedelta(days=1)
    settle_day(root, day, checked, fetcher=lambda *_: {'days': [{'matches': matches}]},
               legacy_evidence=False, max_result_ids=20)


def seeded(storage, root, old=()):
    seed_day(root, DAY)
    for day in old:
        seed_day(root, day, complete=False)
    learn_v7(root, NOW)
    record_v7_status(root, NOW.date())
    publish(storage, 'test', root, checkpoint_only=True)


class LightweightAuditor(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {'API_SPORTS_KEY': 'test', 'GOLD_PRIVATE': 'true'})
        environment.start()
        self.addCleanup(environment.stop)

    def test_new_daily_certificate_uses_gold_then_saves_learning_without_legacy(self):
        from auditor_daily_priority import build_yesterday
        from gold_storage import checkpoint_day, checkpoint_results
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            root = Path(seed)
            row = record(f'{DAY}T10:00:00+01:00')
            row['fixture']['kickoff'] = f'{DAY}T18:00:00+01:00'
            row.pop('settlement')
            path = f'auditordata/prematch_{DAY}.json'
            write_store(root/path, {'schema_version': 1, 'audit_date': str(DAY), 'fixtures': {},
                                   'multimarket_evaluations': {'context-goals-v3.4::1': row}})
            publish(storage, 'test', root, checkpoint_only=True)
            original = previous_manifest(storage, 'test')[path]
            commands = []
            def runner(root, command, deadline, clock):
                args = command[1:]
                commands.append(args)
                if args[0] == 'src/auditor_daily_priority.py':
                    self.assertEqual(args[-2:], ['--max-result-ids', '20'])
                    build_yesterday(root, DAY, NOW, max_result_ids=20)
                elif args[:2] == ['auditor/current_audit.py', 'learn-v7']:
                    learn_v7(root, NOW)
                elif args[1] == 'checkpoint-results':
                    checkpoint_results(storage, 'test', root, DAY)
                elif args[1] == 'checkpoint-day':
                    checkpoint_day(storage, 'test', root, DAY)
                else:
                    self.fail('Unexpected global work: '+repr(args))
            with patch('audit_official_scores.read_live_scores', return_value={'1': SCORE}), \
                    patch('current_audit.fetch_results') as fetch:
                self.assertEqual(cycle(storage, 'test', work, NOW, NOW+WORK_BUDGET, runner, lambda: NOW), 'complete')
            fetch.assert_not_called()
            self.assertTrue(v7_ready(work, NOW.date()))
            self.assertIsNotNone(verified_evidence(work, DAY))
            self.assertEqual(previous_manifest(storage, 'test')[path], original)

    def test_only_selected_date_is_restored_and_global_history_survives(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            seeded(storage, Path(seed))
            index = previous_manifest(storage, 'test')
            giant = 'data/arab_quote_prematch_audit.json'
            index[giant] = {'sha256': 'a'*64, 'bytes': 25_125_379_119}
            write_manifest(storage, 'test', index)
            runner = Mock()
            self.assertEqual(cycle(storage, 'test', work, NOW, NOW+WORK_BUDGET, runner, lambda: NOW), 'complete')
            runner.assert_not_called()
            self.assertTrue(v7_ready(work, NOW.date()))
            self.assertFalse((Path(work)/giant).exists())
            after = previous_manifest(storage, 'test')
            self.assertEqual(after[giant], index[giant])
            self.assertEqual({p: after[p] for p in index}, index)
            marker = read_store(Path(work)/MARKER)
            self.assertEqual(marker['scope'], 'v7_daily')
            self.assertEqual(marker['legacy_status'], 'deferred')
            self.assertEqual(marker['pending_fixtures'], 0)

    def test_duplicate_restores_certificate_without_repeating_recovery(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as first, TemporaryDirectory() as second:
            seeded(storage, Path(seed))
            cycle(storage, 'test', first, NOW, NOW+WORK_BUDGET, Mock(), lambda: NOW)
            runner = Mock()
            self.assertEqual(cycle(storage, 'test', second, NOW, NOW+WORK_BUDGET, runner, lambda: NOW), 'already_completed')
            runner.assert_not_called()

    def test_one_old_day_saved_and_resumed_without_rewriting_frozen_forecasts(self):
        storage = Fake()
        days = (date(2026, 11, 17), date(2026, 11, 18))
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            seeded(storage, Path(seed), days)
            before = previous_manifest(storage, 'test')
            calls = []
            def runner(root, command, deadline, clock):
                args = command[1:]
                calls.append(args)
                if args[0] == 'auditor/current_audit.py':
                    day = date.fromisoformat(args[args.index('--date')+1])
                    settle_day(root, day, NOW, fetcher=lambda *_: {'days': [{'matches': [
                        {'fixture': {'fixture_id': '1', 'result': SCORE}}]}]}, legacy_evidence=False,
                        max_result_ids=20)
                else:
                    from gold_storage import checkpoint_results
                    checkpoint_results(storage, 'test', root, args[-1])
            self.assertEqual(cycle(storage, 'test', work, NOW, NOW+WORK_BUDGET, runner, lambda: NOW), 'complete')
            self.assertEqual(sum(c[0] == 'auditor/current_audit.py' for c in calls), 1)
            progress = read_store(Path(work)/PROGRESS)
            self.assertEqual(progress['recovery'], {'date': '2026-11-18', 'status': 'saved'})
            self.assertEqual(progress['days']['2026-11-18']['pending_fixtures'], 0)
            after = previous_manifest(storage, 'test')
            for day in days:
                path = f'auditordata/prematch_{day}.json'
                self.assertEqual(after[path], before[path])
            self.assertEqual(after[f'auditarchive/prematch_{days[0]}_results.json'],
                             before[f'auditarchive/prematch_{days[0]}_results.json'])
            self.assertEqual(recovery_candidate(Path(work), after, NOW.date()+timedelta(days=1), progress), days[0])

    def test_short_remaining_window_defers_backlog_and_still_saves_daily_proof(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            seeded(storage, Path(seed), (date(2026, 11, 18),))
            runner = Mock()
            self.assertEqual(cycle(storage, 'test', work, NOW, NOW+timedelta(minutes=3), runner, lambda: NOW), 'complete')
            runner.assert_not_called()
            self.assertEqual(read_store(Path(work)/MARKER)['recovery']['status'], 'deferred_budget')

    def test_timed_out_recovery_keeps_current_certificate_and_old_remote_results(self):
        storage = Fake()
        old = date(2026, 11, 18)
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            seeded(storage, Path(seed), (old,))
            before = previous_manifest(storage, 'test')
            def timeout(*_):
                raise TimeoutError('bounded old block')
            self.assertEqual(cycle(storage, 'test', work, NOW, NOW+WORK_BUDGET, timeout, lambda: NOW), 'complete')
            self.assertTrue(v7_ready(work, NOW.date()))
            after = previous_manifest(storage, 'test')
            for path in before:
                self.assertEqual(after[path], before[path])
            saved = read_store(Path(work)/PROGRESS)
            self.assertEqual(saved['recovery']['status'], 'deferred_budget')
            self.assertEqual(saved['days'][str(old)]['status'], 'timeout')

    def test_failed_publication_never_marks_a_daily_completion(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work:
            seeded(storage, Path(seed))
            before = storage.objects['state/current.json']
            with patch('auditor_light.publish', side_effect=RuntimeError('write unavailable')):
                with self.assertRaisesRegex(RuntimeError, 'write unavailable'):
                    cycle(storage, 'test', work, NOW, NOW+WORK_BUDGET, Mock(), lambda: NOW)
            self.assertFalse((Path(work)/MARKER).exists())
            self.assertEqual(storage.objects['state/current.json'], before)

    def test_receipt_is_reused_only_while_its_frozen_evidence_hashes_match(self):
        storage = Fake()
        with TemporaryDirectory() as seed:
            root = Path(seed)
            seeded(storage, root, (date(2026, 11, 18),))
            index = previous_manifest(storage, 'test')
            receipt = read_receipt(root, DAY)
            self.assertTrue(remote_receipt(index, receipt, DAY))
            index[f'auditordata/prematch_{DAY}.json']['sha256'] = 'b'*64
            self.assertFalse(remote_receipt(index, receipt, DAY))

    def test_parent_caps_runtime_and_does_not_claim_success_after_timeout(self):
        with TemporaryDirectory() as work, patch.dict(os.environ, {'GOLD_PRIVATE': 'true'}):
            runner = Mock(side_effect=TimeoutError)
            self.assertEqual(run(work, lambda: NOW, runner), 'paused')
            self.assertEqual(runner.call_args.args[2], NOW+WORK_BUDGET)
            self.assertFalse((Path(work)/MARKER).exists())

    def test_incomplete_github_listing_defers_without_admitting_a_writer(self):
        for error in (IncompleteListing('moving listing'), TemporaryReadError('HTTP 503')):
            api = Mock()
            api.active_runs.side_effect = error
            result = preflight(api, NOW, '1')
            self.assertFalse(result['run'])
            self.assertEqual(result['reason'], 'github_listing_unavailable')

    def test_invalid_listing_still_fails_visibly(self):
        api = Mock()
        api.active_runs.return_value = [{'id': 1}]
        with self.assertRaises(ValueError):
            preflight(api, NOW, '1')


class BoundedResults(unittest.TestCase):
    def test_complete_results_require_no_gold_or_provider_reads(self):
        with patch('audit_official_scores.read_live_scores') as gold, patch('current_audit.fetch_results') as provider:
            scores, disputes, queried = official_scores(DAY, NOW, {'1': f'{DAY}T18:00:00+01:00'},
                {'1': SCORE}, {}, False, set(), lambda _: None, max_result_ids=20)
        gold.assert_not_called()
        provider.assert_not_called()
        self.assertFalse(queried)
        self.assertEqual(scores['1'], SCORE)

    def test_gold_confirmed_result_avoids_a_new_football_call(self):
        with patch('audit_official_scores.read_live_scores', return_value={'1': SCORE}), \
                patch('current_audit.fetch_results') as provider:
            scores, disputes, queried = official_scores(DAY, NOW, {'1': f'{DAY}T18:00:00+01:00'},
                {}, {}, False, set(), lambda _: None, max_result_ids=20)
        provider.assert_not_called()
        self.assertFalse(queried)
        self.assertEqual(scores['1']['home_ft'], 2)

    def test_missing_ids_are_batched_and_rotate_past_unavailable_halftimes(self):
        fixtures = {str(i): f'{DAY}T18:00:00+01:00' for i in range(1, 46)}
        fetch = Mock(return_value={'days': [{'matches': []}]})
        with patch.dict(os.environ, {'API_SPORTS_KEY': 'test'}), \
                patch('audit_official_scores.read_live_scores', return_value={}), \
                patch('current_audit.fetch_results', fetch):
            attempted = []
            official_scores(DAY, NOW, fixtures, {}, {}, False, set(), lambda _: None,
                            max_result_ids=20, attempted=attempted)
            self.assertEqual(list(fetch.call_args.args[2]), list(range(1, 21)))
            official_scores(DAY, NOW, fixtures, {}, {}, False, set(), lambda _: None,
                            max_result_ids=20, after_fixture_id=attempted[-1])
            self.assertEqual(list(fetch.call_args.args[2]), list(range(21, 41)))
            self.assertEqual(fetch.call_count, 2)

    def test_disputed_gold_score_is_not_silently_accepted(self):
        wrong = {**SCORE, 'home_ft': 4}
        with patch('audit_official_scores.read_live_scores', return_value={'1': wrong}), \
                patch('current_audit.fetch_results') as provider, patch.dict(os.environ, {'API_SPORTS_KEY': ''}):
            scores, disputes, queried = official_scores(DAY, NOW, {'1': f'{DAY}T18:00:00+01:00'},
                {'1': SCORE}, {}, True, set(), lambda _: None, max_result_ids=20)
        provider.assert_not_called()
        self.assertEqual(scores['1']['home_ft'], 2)
        self.assertEqual(disputes, {'1'})


if __name__ == '__main__':
    unittest.main()
