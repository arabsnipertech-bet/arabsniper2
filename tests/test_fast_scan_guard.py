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
from gold_storage import GOLD, publish, restore

START = '2026-10-30T09:05:00+01:00'
FINISH = '2026-10-30T09:45:00+01:00'
NOW = guard.timestamp('2026-10-30T09:50:00+01:00')


def feeds(root):
    folder = Path(root) / 'data'; folder.mkdir(exist_ok=True)
    for i, day in enumerate(guard.window(NOW), 1):
        (folder / f'arab_quote_day{i}.json').write_text(json.dumps({
            'date': day, 'matches': [], 'updated_at': FINISH, 'refresh_status': 'ok'}))
    for name in GOLD[5:]: (folder / name).write_text('{}')
    (folder / 'arab_quote_run_state.json').write_text(json.dumps({
        'mode': 'fast', 'status': 'ok', 'returncode': 0, 'started_at': START, 'generated_at': FINISH}))


class FakeStorage:
    def __init__(self):
        self.objects = {'state/current.json': b'{"version":1,"files":{}}'}
        self.fail = False
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(self.objects[Key])}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail and Key == GOLD[1]: raise RuntimeError('failed Gold publication')
        self.objects[Key] = Body


class FastGuardTests(unittest.TestCase):
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
            self.assertTrue(guard.completed_recently(root, guard.timestamp('2026-10-30T10:04:59+01:00')))
            self.assertFalse(guard.completed_recently(root, guard.timestamp('2026-10-30T10:05:00+01:00')))

    def test_cached_refresh_and_stale_failed_or_partial_feeds_cannot_record(self):
        for field, value in [('date', '2026-10-29'), ('updated_at', '2026-10-30T08:00:00+01:00'),
                             ('refresh_status', 'preserved_api_error'), ('matches', None)]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as root:
                feeds(root); path = Path(root) / 'data/arab_quote_day2.json'
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
            (Path(root) / 'data/arab_quote_day2.json').unlink()
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
                     'Publish Fast Scan safely', 'Record verified Fast completion'):
            section = workflow.split('- name: ' + name + '\n', 1)[1].split('- name: ', 1)[0]
            self.assertIn("steps.fast_guard.outputs.run == 'true'", section)
        self.assertLess(workflow.index('- name: Record verified Fast completion'),
                        workflow.index('- name: Save verified state in private storage'))
        self.assertIn('cancel-in-progress: false', workflow)

    def test_late_schedule_skips_but_code_release_and_explicit_recovery_run(self):
        for event, force, expected in [('schedule', 'false', 'false'),
                                       ('workflow_dispatch', 'false', 'false'),
                                       ('push', 'false', 'true'),
                                       ('workflow_dispatch', 'true', 'true')]:
            with self.subTest(event=event, force=force), tempfile.TemporaryDirectory() as root:
                output = Path(root) / 'output'
                with patch.dict(guard.os.environ, {'EVENT_NAME': event, 'ARAB_FORCE_FAST': force,
                        'GITHUB_OUTPUT': str(output), 'GITHUB_STEP_SUMMARY': ''}), \
                        patch.object(guard, 'completed_recently', return_value=True), \
                        patch.object(sys, 'argv', ['fast_scan_guard.py']), \
                        contextlib.redirect_stdout(io.StringIO()):
                    guard.main()
                self.assertIn('run=' + expected + '\n', output.read_text())


if __name__ == '__main__': unittest.main()
