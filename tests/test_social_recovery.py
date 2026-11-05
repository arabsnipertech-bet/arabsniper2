import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from telegram_reconcile import confirmed_message
from telegram_watchdog import wake_facebook
from facebook_page import record_receipt


class SocialRecovery(unittest.TestCase):
    now = datetime.fromisoformat('2026-11-05T17:25:00+01:00')

    def page(self, message='Promo &amp; Gold', at='2026-11-05T16:16:00+00:00'):
        return ('<div data-post="arabsniper_signals/61">'
                f'<div class="tgme_widget_message_text js-message_text">{message}</div>'
                f'<time datetime="{at}"></time></div>')

    def test_reconcile_requires_one_exact_same_slot_message(self):
        post = {'day': '2026-11-05', 'slot': '17:15', 'text': 'Promo & Gold'}
        self.assertEqual(confirmed_message(self.page(), post, self.now), 61)
        for page in ['', self.page('Different promo'), self.page(at='2026-11-04T16:16:00Z'),
                     self.page()+self.page(), self.page(at='2026-11-05T16:30:00Z')]:
            with self.assertRaises(RuntimeError):
                confirmed_message(page, post, self.now)

    def test_facebook_recovery_is_independent_and_deduplicated(self):
        class API:
            sent = True
            facebook = False
            active = []
            calls = []
            def slot_recorded(self, day, slot): return self.sent
            def facebook_recorded(self, day, slot): return self.facebook
            def runs(self, workflow, due): return self.active
            def dispatch(self, workflow, slot): self.calls.append((workflow, slot))
        api = API()
        self.assertEqual(wake_facebook(api, self.now), [{'slot':'17:15', 'action':'dispatched'}])
        self.assertEqual(api.calls, [('facebook-recovery.yml', '17:15')])
        api.facebook = True
        self.assertEqual(wake_facebook(api, self.now), [])
        api.facebook = False
        api.active = [{'status': 'in_progress'}]
        self.assertEqual(wake_facebook(api, self.now), [])
        api.active = []
        api.sent = False
        self.assertEqual(wake_facebook(api, self.now), [])

    def test_facebook_receipt_retains_both_platform_identifiers(self):
        with tempfile.TemporaryDirectory() as folder:
            previous = os.getcwd()
            try:
                os.chdir(folder)
                with patch.dict(os.environ, {'GITHUB_OUTPUT': 'output'}):
                    record_receipt('604216843376166_123',
                                   {'day': '2026-11-05', 'slot': '17:15', 'message_id': 61}, self.now)
                result = json.loads(Path('facebook-receipt.json').read_text())
                self.assertEqual(result['telegram_message_id'], 61)
                self.assertIn('fb-2026-11-05-1715-sent', Path('output').read_text())
            finally:
                os.chdir(previous)

    def test_facebook_retry_recognizes_slot_after_expired_preview_changes(self):
        import facebook_page
        from unittest.mock import Mock
        heading = 'ArabSniper · Singola delle 17:30'
        post = {'day': '2026-11-05', 'slot': '17:30', 'message_id': 70,
                'text': heading+'\nOriginal preview', 'picks': []}
        graph = Mock(side_effect=[{'id': facebook_page.PAGE}, {'data': [
            {'id': facebook_page.PAGE+'_123', 'message': heading+'\nEarlier filtered preview'}]}])
        with tempfile.TemporaryDirectory() as folder:
            previous = os.getcwd()
            try:
                os.chdir(folder)
                Path('receipts').mkdir()
                Path('receipts/post.json').write_text(json.dumps(post))
                with patch('facebook_page.graph', graph), patch('facebook_page.selection',
                        return_value=('Slot title', heading+'\nUpdated preview')), patch.dict(
                            sys.modules, {'social_card': Mock()}):
                    facebook_page.main('selections')
                self.assertEqual(graph.call_count, 2)
                self.assertTrue(Path('facebook-receipt.json').is_file())
            finally:
                os.chdir(previous)
