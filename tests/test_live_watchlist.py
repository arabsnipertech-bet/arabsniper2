import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import gold_storage
from live_watchlist import from_checkpoint


class LiveWatchlistRepairTests(TestCase):
    def test_checkpoint_repair_publishes_only_public_fixture_ids_and_skips_a_second_write(self):
        paths = [f'data/arab_quote_day{i}.json' for i in range(1, 6)]
        kickoff = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        def restore(storage, bucket, root, index, selected):
            self.assertEqual(selected, paths)
            for path in paths:
                target = root / path
                target.parent.mkdir(exist_ok=True)
                target.write_text(json.dumps({'matches': [{'fixture': {
                    'fixture_id': '123', 'kickoff': kickoff, 'private': 'do not publish'},
                    'odds': {'private': True}}]}))

        with (TemporaryDirectory() as folder,
              mock.patch.dict('os.environ', {'R2_BUCKET': 'test'}),
              mock.patch.object(gold_storage, 'client'),
              mock.patch.object(gold_storage, 'previous_manifest', return_value=dict.fromkeys(paths)),
              mock.patch.object(gold_storage, 'restore_subset', side_effect=restore) as read):
            root = Path(folder)
            payload = from_checkpoint(root)
            self.assertEqual(payload['fixtures'], [{'fixture_id': '123', 'kickoff': kickoff}])
            self.assertEqual(json.loads((root/'data/live_watchlist.json').read_bytes()), payload)
            self.assertIsNone(from_checkpoint(root))
            read.assert_called_once()
