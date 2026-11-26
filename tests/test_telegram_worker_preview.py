"""Check the local Worker preview against real Publisher text and Pillow output."""
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import unittest

from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from publisher_v71 import delivery, editorial
from publisher_v71.storage import Storage


class TelegramWorkerPreviewTests(unittest.TestCase):
    def test_real_editorial_caption_and_photo(self):
        now = datetime(2026, 11, 18, 10, tzinfo=timezone.utc)
        store = Storage(None, 'offline')
        view = editorial.academy(now.date())
        delivery.queue(store, 'academy', 'daily', view, ('telegram',), now, now + timedelta(hours=1))
        post = store.state['posts']['telegram/academy/daily']
        with Image.open(BytesIO(editorial.card('academy', view))) as card:
            self.assertEqual((card.format, card.size), ('JPEG', (1080, 1350)))
        script = """
            import fs from 'node:fs';
            import {preview} from './prototypes/telegram-preview/worker.mjs';
            const bytes = fs.readFileSync(0);
            const ledger = {size:bytes.length, json:async () => JSON.parse(bytes)};
            const result = await preview({get:async () => ledger}, Date.parse('2026-11-18T10:00:00Z'));
            process.stdout.write(JSON.stringify(result));
        """
        result = subprocess.run(['node', '--input-type=module', '-e', script], cwd=ROOT,
                                input=json.dumps({'schema':1, **store.state}).encode(),
                                capture_output=True, check=True, timeout=15)
        proposal, = json.loads(result.stdout)['proposals']
        self.assertEqual((proposal['caption'], proposal['marker']), (post['text'], post['marker']))
        self.assertEqual((proposal['method'], proposal['media']), ('sendPhoto', 'photo'))
