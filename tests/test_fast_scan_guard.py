import contextlib
import io
import json
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import fast_scan_guard as guard
from gold_storage import FAST_DATA, GOLD, publish, restore, restore_fast
from fast_schedule import AFTERNOON, EXTENDED, FORCED, ON_HOUR, ORDINARY, latest_slot, slots

START = '2026-10-30T09:05:00+01:00'
FINISH = '2026-10-30T09:45:00+01:00'
NOW = guard.timestamp('2026-10-30T09:50:00+01:00')


def feeds(root, scope=ORDINARY, start=START, finish=FINISH):
    folder = Path(root) / 'data'; folder.mkdir(exist_ok=True)
    horizon = guard.window(guard.timestamp(start))
    dates = horizon[:guard.DAY_COUNT[scope]]
    for i, day in enumerate(horizon, 1):
        (folder / f'arab_quote_day{i}.json').write_text(json.dumps({
            'day': i, 'date': day, 'matches': [], 'updated_at': finish, 'refresh_status': 'ok',
            'universal_coverage': {'available_fixture_ids': 0, 'displayed_fixtures': 0,
                                   'coverage_basis': 'provider_schedule'}}))
    for name in GOLD[5:]: (folder / name).write_text('{}')
    (folder / 'arab_quote_run_state.json').write_text(json.dumps({
        'mode': 'fast', 'status': 'ok', 'returncode': 0, 'started_at': start, 'generated_at': finish,
        'fast_scope': scope, 'days': len(dates),
        'fast_slot': latest_slot(guard.timestamp(start), scope).isoformat() if scope != FORCED else None}))
    (folder / 'arab_quote_dashboard.json').write_text(json.dumps({'fast_scan': {
        'scope': scope, 'planned_at': start, 'dates': dates, 'collected_dates': dates, 'reused_dates': []}}))


class FakeStorage:
    def __init__(self):
        self.objects = {'state/current.json': b'{"version":1,"files":{}}',
                        'arab_quote_model_research.json': b'{}'}
        self.fail = False
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(self.objects[Key])}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail and Key == GOLD[1]: raise RuntimeError('failed Gold publication')
        self.objects[Key] = Body


class FastGuardTests(unittest.TestCase):
    def test_force_requests_five_days_and_requires_verified_refresh(self):
        with patch.dict('os.environ', {'ARAB_FORCE_FAST': 'true'}):
            self.assertEqual(guard.fast_day_count(), 5)
        with patch.dict('os.environ', {'ARAB_FORCE_FAST': 'false'}):
            self.assertEqual(guard.fast_day_count(), 1)
            self.assertEqual(guard.fast_day_count(EXTENDED), 3)
        with tempfile.TemporaryDirectory() as root:
            feeds(root, FORCED)
            (Path(root) / 'data/arab_quote_day5.json').unlink()
            with self.assertRaisesRegex(ValueError, 'all expected dates'):
                guard.record_completion(root, START, NOW)

    def test_no_marker_and_night_state_are_not_fast_completion(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            self.assertFalse(guard.completed_recently(root, NOW))
            state = Path(root) / 'data/arab_quote_run_state.json'
            data = json.loads(state.read_text()); data['mode'] = 'night'; state.write_text(json.dumps(data))
            with self.assertRaises(ValueError): guard.record_completion(root, START, NOW)

    def test_success_is_fresh_from_collection_start_not_finish(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root); guard.record_completion(root, START, NOW)
            self.assertTrue(guard.completed_recently(root, NOW))
            before = guard.timestamp('2026-10-30T10:29:59+01:00')
            after = guard.timestamp('2026-10-30T10:30:00+01:00')
            self.assertTrue(guard.completed_recently(root, before, slot=latest_slot(before, ORDINARY)))
            self.assertFalse(guard.completed_recently(root, after, slot=latest_slot(after, ORDINARY)))
            self.assertFalse(guard.completed_recently(root, guard.timestamp(START) + timedelta(minutes=90)))

    def test_cached_refresh_and_stale_failed_or_partial_feeds_cannot_record(self):
        for field, value in [('date', '2026-10-29'), ('updated_at', '2026-10-30T08:00:00+01:00'),
                             ('refresh_status', 'preserved_api_error'), ('matches', None)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                feeds(root); path = Path(root) / 'data/arab_quote_day1.json'
                data = json.loads(path.read_text()); data[field] = value; path.write_text(json.dumps(data))
                with self.assertRaises(ValueError): guard.record_completion(root, START, NOW)
                self.assertFalse((Path(root) / guard.MARKER).exists())
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            with self.assertRaises(ValueError): guard.record_completion(root, FINISH, NOW)

    def test_invalid_times_missing_dates_and_midnight_do_not_skip(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root); guard.record_completion(root, START, NOW)
            self.assertFalse(guard.completed_recently(root, guard.timestamp(START)))
            self.assertFalse(guard.completed_recently(root, guard.timestamp('2026-10-31T00:01:00+01:00')))
            (Path(root) / 'data/arab_quote_day1.json').unlink()
            self.assertFalse(guard.completed_recently(root, NOW))
            with self.assertRaises(ValueError): guard.record_completion(root, START, NOW)

    def test_failed_run_never_records_and_date_crossing_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root); state = Path(root) / 'data/arab_quote_run_state.json'
            original = json.loads(state.read_text())
            for change in ({'status': 'error'}, {'returncode': 1}, {'started_at': '2026-10-30T09:05:00'}):
                state.write_text(json.dumps({**original, **change}))
                with self.assertRaises(ValueError): guard.record_completion(root, START, NOW)
            state.write_text(json.dumps(original))
            with self.assertRaises(ValueError):
                guard.record_completion(root, START, NOW + timedelta(days=1))

    def test_failed_upload_leaves_old_marker_and_success_makes_it_durable(self):
        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as target:
            feeds(source); storage = FakeStorage()
            with contextlib.redirect_stdout(io.StringIO()): publish(storage, 'test', source)
            old_pointer = storage.objects['state/current.json']
            guard.record_completion(source, START, NOW); storage.fail = True
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError): publish(storage, 'test', source)
            self.assertEqual(storage.objects['state/current.json'], old_pointer)
            with contextlib.redirect_stdout(io.StringIO()): restore(storage, 'test', target)
            self.assertFalse(guard.completed_recently(target, NOW))
            storage.fail = False
            with contextlib.redirect_stdout(io.StringIO()):
                publish(storage, 'test', source); restore(storage, 'test', target)
            self.assertTrue(guard.completed_recently(target, NOW))

    def test_workflow_guards_all_provider_and_publication_steps(self):
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/fast_scan.yml').read_text()
        for name in ('Run Fast Scan', 'Refresh all five saved days to the active Gold policy (zero API calls)',
                     'Save verified state in private storage', 'Publish only public projections',
                     'Publish Fast Scan safely'):
            section = workflow.split('- name: ' + name + '\n', 1)[1].split('- name: ', 1)[0]
            self.assertIn("steps.fast_guard.outputs.run == 'true'", section)
        self.assertLess(workflow.index('- name: Record verified Fast completion'),
                        workflow.index('- name: Save verified state in private storage'))
        self.assertIn('cancel-in-progress: false', workflow)

    def test_late_schedule_skips_but_code_release_and_explicit_recovery_run(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            guard.record_completion(root, START, NOW)
            plan = lambda event, scope=ORDINARY: guard.plan(root, NOW, event, ON_HOUR, scope, None, created=NOW)
            self.assertFalse(plan('schedule')[0])
            self.assertFalse(plan('workflow_dispatch')[0])
            self.assertTrue(plan('push')[0])
            self.assertTrue(plan('workflow_dispatch', FORCED)[0])
            self.assertFalse(guard.plan(root, NOW+timedelta(days=1), 'schedule', ON_HOUR,
                                        ORDINARY, None, created=NOW)[0])

    def test_extended_certifies_reused_today_only_from_the_15_slot(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            planned = guard.timestamp('2026-10-30T15:35:00+01:00')
            ordinary = '2026-10-30T15:05:00+01:00'
            feeds(root, start=ordinary, finish='2026-10-30T15:25:00+01:00')
            guard.record_completion(root, ordinary, planned)
            first = root/'data/arab_quote_day1.json'
            original = first.read_bytes()
            self.assertTrue(guard.reuse_today(root, planned))
            finish = planned + timedelta(minutes=10)
            feeds(root, EXTENDED, planned.isoformat(), finish.isoformat())
            first.write_bytes(original)
            dashboard = root/'data/arab_quote_dashboard.json'
            result = json.loads(dashboard.read_text())
            result['fast_scan']['reused_dates'] = result['fast_scan']['collected_dates'][:1]
            result['fast_scan']['collected_dates'] = result['fast_scan']['collected_dates'][1:]
            dashboard.write_text(json.dumps(result))
            guard.record_completion(root, planned.isoformat(), finish)
            self.assertTrue(guard.completed_recently(root, finish, EXTENDED))
            self.assertEqual(first.read_bytes(), original)
            self.assertFalse(guard.reuse_today(root, guard.timestamp('2026-10-30T16:30:00+01:00')))

    def test_github_calendar_matches_the_rome_slots_across_dst(self):
        for raw, utc in [('2026-03-28T09:00:00+01:00', 8), ('2026-03-29T09:00:00+02:00', 7),
                         ('2026-10-24T09:00:00+02:00', 7), ('2026-10-25T09:00:00+01:00', 8)]:
            now = guard.timestamp(raw)
            self.assertEqual(len(slots(now.date(), ORDINARY)), 12)
            chosen = latest_slot(now, ORDINARY, ON_HOUR)
            self.assertEqual(chosen.strftime('%H:%M'), '09:00')
            self.assertEqual(chosen.astimezone(guard.timestamp('2026-01-01T00:00:00Z').tzinfo).hour, utc)
            later = now.replace(hour=15, minute=32)
            self.assertEqual(latest_slot(later, EXTENDED, AFTERNOON).strftime('%H:%M'), '15:30')

    def test_fast_checkpoint_preserves_journals_archives_and_explorer_inputs(self):
        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as target:
            source, target = Path(source), Path(target)
            feeds(source)
            names = ('data/arab_quote_scan_dated_20261029T080000000000_20261029_policies.json',
                     'auditordata/prematch_2026-10-29.json', 'auditarchive/prematch_2026-10-29_v7.json',
                     'auditarchive/prematch_2026-10-29_daily.json', 'auditarchive/prematch_2026-10-29_results.json')
            for name in names:
                path = source/name
                path.parent.mkdir(exist_ok=True)
                path.write_text('{}')
            storage = FakeStorage()
            with contextlib.redirect_stdout(io.StringIO()):
                publish(storage, 'test', source)
                before = json.loads(storage.objects['state/current.json'])['files']
                restore_fast(storage, 'test', target)
                self.assertEqual([name for name in names if (target/name).exists()], list(names[1:4]))
                guard.record_completion(target, START, NOW)
                publish(storage, 'test', target, selected_paths=FAST_DATA)
            after = json.loads(storage.objects['state/current.json'])['files']
            self.assertEqual({name: after[name] for name in names}, {name: before[name] for name in names})
            self.assertIn(guard.MARKER, after)

    def test_runner_collects_only_needed_dates_and_preserves_rolling_boards(self):
        from contextlib import ExitStack
        import subprocess
        from test_policy_engine import engine
        from test_night_scan_guard import runner

        with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
            root = Path(folder)
            data = root/'data'
            feeds(root, start='2026-10-29T09:00:00+01:00')
            # Tomorrow from the prior checkpoint must follow its date after midnight.
            old = data/'arab_quote_day3.json'
            saved = json.loads(old.read_text())
            saved['sentinel'] = 'unchanged future data'
            old.write_text(json.dumps(saved))
            (data/'arab_quote_selections_day3.json').write_text(json.dumps({'date': saved['date'], 'sentinel': True}))
            (data/'arab_bet_catalog.json').write_text(json.dumps({'updated_at': '2026-10-30T07:00:00Z'}))
            for name, value in vars(engine).copy().items():
                if isinstance(value, Path) and value.parent == engine.DATA_DIR and value != engine.DATA_DIR:
                    stack.enter_context(patch.object(engine, name, data/value.name))
            for module, attrs in ((engine, {'PROJECT_ROOT': root, 'DATA_DIR': data}),
                                  (runner, {'PROJECT_ROOT': root, 'RUN_STATE_FILE': data/'arab_quote_run_state.json'})):
                for name, value in attrs.items():
                    stack.enter_context(patch.object(module, name, value))
            current = guard.timestamp('2026-10-30T09:00:00+01:00')
            stack.enter_context(patch.object(engine, 'now_rome', side_effect=lambda: current))
            stack.enter_context(patch.object(runner, 'now_iso', side_effect=lambda: current.isoformat()))
            stack.enter_context(patch.dict('os.environ', {'API_SPORTS_KEY': 'mock', 'ARAB_FORCE_FAST': 'false',
                                                          'ARAB_DEEP_ODDS_ENABLED': '0'}))

            def child(command):
                with patch.object(sys, 'argv', command[1:]):
                    engine.main()
                return subprocess.CompletedProcess(command, 0, '', '')

            def provider(client, endpoint, params, use_cache=True):
                if endpoint == 'fixtures' and 'date' in params:
                    dates.append(params['date'])
                    client.request_count += 1
                    return {'response': []}
                raise AssertionError(f'Unexpected football query {endpoint} {params}')

            stack.enter_context(patch.object(runner, 'stream_process', side_effect=child))
            stack.enter_context(patch.object(engine.APIClient, '_get_page', provider))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            for hour, minute, scope, offsets in [(9, 0, ORDINARY, [0]), (15, 0, ORDINARY, [0]),
                                                  (15, 30, EXTENDED, [1, 2]), (16, 30, ORDINARY, [0])]:
                current = current.replace(hour=hour, minute=minute)
                days = guard.window(current)
                dates = []
                expected_slot = latest_slot(current, scope)
                self.assertEqual(runner.run('fast', fast_scope=scope, fast_slot=expected_slot.isoformat()), 0)
                checkpoint = guard.record_completion(root, current.isoformat(), current)
                self.assertEqual(dates, [days[offset] for offset in offsets])
                self.assertEqual(checkpoint['collected_dates'], dates)
                dashboard = guard.read_store(data/'arab_quote_dashboard.json')
                self.assertEqual([day['date'] for day in dashboard['days']], days)
                self.assertEqual(dashboard['api_health']['requests_this_run'], len(dates))
                if hour == 9:
                    self.assertEqual(dashboard['days'][1]['sentinel'], 'unchanged future data')
                    self.assertTrue(guard.read_store(data/'arab_quote_selections_day2.json')['sentinel'])
                if hour == 15 and minute == 0:
                    day1 = (data/'arab_quote_day1.json').read_bytes()
                if scope == EXTENDED:
                    self.assertEqual((data/'arab_quote_day1.json').read_bytes(), day1)
                    future = [(data/f'arab_quote_day{i}.json').read_bytes() for i in (2, 3)]
                if hour == 16:
                    self.assertEqual([(data/f'arab_quote_day{i}.json').read_bytes() for i in (2, 3)], future)


if __name__ == '__main__': unittest.main()
