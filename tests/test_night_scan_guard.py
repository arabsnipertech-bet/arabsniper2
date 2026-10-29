import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import night_scan_guard as guard
from gold_storage import GOLD, publish, restore

spec = importlib.util.spec_from_file_location('night_runner', Path(__file__).resolve().parents[1] / 'src/3appdays_runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

START = '2026-10-29T01:10:00+01:00'
END = '2026-10-29T02:10:00+01:00'
NOW = guard.timestamp('2026-10-29T07:00:00+01:00')


def feeds(root, updated=END):
    folder = Path(root) / 'data'
    folder.mkdir(exist_ok=True)
    for i, date in enumerate(guard.window(NOW), 1):
        (folder / f'arab_quote_day{i}.json').write_text(json.dumps({
            'date': date, 'matches': [], 'updated_at': updated, 'refresh_status': 'ok'}))
    for name in GOLD[5:]:
        (folder / name).write_text('{}')


class FakeStorage:
    def __init__(self):
        self.objects = {}
        self.fail = False

    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail and Key == GOLD[1]:
            raise RuntimeError('simulated failed publication')
        self.objects[Key] = Body


class NightGuardTests(unittest.TestCase):
    def test_no_marker_does_not_confuse_fast_with_night(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            (Path(root) / 'data/arab_quote_run_state.json').write_text('{"mode":"fast","status":"ok"}')
            self.assertFalse(guard.completed_today(root, NOW))

    def test_complete_marker_survives_fast_and_expires_at_rome_midnight(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            guard.record_completion(root, START, END)
            (Path(root) / 'data/arab_quote_run_state.json').write_text('{"mode":"fast","status":"ok"}')
            self.assertTrue(guard.completed_today(root, NOW))
            self.assertTrue(guard.completed_today(root, guard.timestamp('2026-10-29T22:59:59Z')))
            self.assertFalse(guard.completed_today(root, guard.timestamp('2026-10-29T23:00:00Z')))

    def test_dst_windows_use_rome_date_not_utc_or_browser_timezone(self):
        self.assertEqual(guard.window(guard.timestamp('2026-10-24T22:30:00Z'))[0], '2026-10-25')
        self.assertEqual(guard.window(guard.timestamp('2026-10-25T22:30:00Z'))[0], '2026-10-25')
        self.assertEqual(guard.window(guard.timestamp('2026-10-25T23:30:00Z'))[0], '2026-10-26')

    def test_wrong_missing_or_failed_day_never_records_success(self):
        for invalid in ('date', 'missing', 'error', 'old', 'future', 'matches'):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as root:
                feeds(root)
                path = Path(root) / 'data/arab_quote_day4.json'
                data = json.loads(path.read_text())
                if invalid == 'date': data['date'] = '2026-10-31'
                if invalid == 'error': data['refresh_status'] = 'preserved_api_error'
                if invalid == 'old': data['updated_at'] = '2026-10-28T23:59:00+01:00'
                if invalid == 'future': data['updated_at'] = '2026-10-30T02:00:00+01:00'
                if invalid == 'matches': data['matches'] = None
                path.write_text(json.dumps(data))
                if invalid == 'missing': path.unlink()
                with self.assertRaises(ValueError): guard.record_completion(root, START, END)
                self.assertFalse((Path(root) / guard.MARKER).exists())

    def test_changed_calendar_or_future_marker_does_not_skip_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            feeds(root)
            guard.record_completion(root, START, END)
            self.assertFalse(guard.completed_today(root, guard.timestamp(START)))
            (Path(root) / 'data/arab_quote_day5.json').unlink()
            self.assertFalse(guard.completed_today(root, NOW))

    def test_midnight_crossing_and_naive_timestamps_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            for start, end in [(START, '2026-10-30T00:01:00+01:00'), (END, START),
                               ('2026-10-29T01:00:00', END)]:
                with self.assertRaises(ValueError): guard.record_completion(root, start, end)

    def test_publication_failure_does_not_publish_completion_marker(self):
        with tempfile.TemporaryDirectory() as source, tempfile.TemporaryDirectory() as target:
            feeds(source)
            store = FakeStorage()
            store.objects['state/current.json'] = b'{"version":1,"files":{}}'
            with contextlib.redirect_stdout(io.StringIO()): publish(store, 'test', source)
            old_pointer = store.objects['state/current.json']
            guard.record_completion(source, START, END)
            store.fail = True
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
                publish(store, 'test', source)
            self.assertEqual(store.objects['state/current.json'], old_pointer)
            with contextlib.redirect_stdout(io.StringIO()): restore(store, 'test', target)
            self.assertFalse(guard.completed_today(target, NOW))
            store.fail = False
            with contextlib.redirect_stdout(io.StringIO()):
                publish(store, 'test', source)
                restore(store, 'test', target)
            self.assertTrue(guard.completed_today(target, NOW))

    def test_runner_skips_before_provider_child_or_state_writes(self):
        with patch.object(guard, 'completed_today', return_value=True), \
             patch.object(runner, 'stream_process') as child, \
             patch.object(runner, 'atomic_write_json') as write, \
             patch.dict('os.environ', {'ARAB_FORCE_NIGHT': 'false'}), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.run('night'), 0)
            child.assert_not_called()
            write.assert_not_called()

    def test_success_failure_and_explicit_force_through_runner(self):
        for code, valid, force in [(0, True, False), (9, True, False), (0, False, False), (0, True, True)]:
            with self.subTest(code=code, valid=valid, force=force), tempfile.TemporaryDirectory() as root:
                feeds(root, END)
                if not valid: (Path(root) / 'data/arab_quote_day2.json').unlink()
                engine = Path(root) / '3appdays.py'; engine.write_text('# not executed')
                with patch.object(runner, 'PROJECT_ROOT', Path(root)), \
                     patch.object(runner, 'RUN_STATE_FILE', Path(root) / 'data/run.json'), \
                     patch.object(runner, 'ENGINE_CANDIDATES', (engine,)), \
                     patch.object(runner, 'now_iso', return_value=END), \
                     patch.object(guard, 'completed_today', return_value=force), \
                     patch.dict('os.environ', {'ARAB_FORCE_NIGHT': 'false'}), \
                     patch.object(runner, 'stream_process', return_value=subprocess.CompletedProcess([], code, '', '')) as child, \
                     contextlib.redirect_stdout(io.StringIO()):
                    actual = runner.run('night', force_night=force)
                self.assertEqual(actual, code if code else 0 if valid else 3)
                child.assert_called_once()
                self.assertEqual((Path(root) / guard.MARKER).exists(), code == 0 and valid)
                self.assertEqual(json.loads((Path(root) / 'data/run.json').read_text())['status'], 'ok' if actual == 0 else 'error')


if __name__ == '__main__':
    unittest.main()
