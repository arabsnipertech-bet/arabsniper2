"""Exercise ownership and scoped storage across the existing delivery outbox."""
from base64 import b64decode, urlsafe_b64decode
from contextlib import nullcontext
from datetime import datetime, timezone
from hashlib import sha256
import hmac
import json
import os
from pathlib import Path
import sys
from unittest import TestCase, mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from publisher_v71 import bridge, cutover, monitor, runner
from publisher_v71.storage import Storage, PREFIX


class PublisherGrowthHandoffTests(TestCase):
    def test_switch_preserves_receipts_and_leaves_only_growth_eligible_to_send(self):
        store = Storage(mock.Mock(), 'test')
        store.state['legacy']['handoff'] = {'ids': ['123']}
        receipt = {'status': 'sent', 'receipt': {'id': '166'}}
        store.state['posts']['telegram/best/test'] = receipt
        store.etag = '"existing"'
        with (mock.patch.object(store, 'cycle', return_value=nullcontext(True)),
              mock.patch.object(store, 'load'),
              mock.patch.object(store, 'save') as save):
            self.assertEqual(cutover.owner(store), cutover.PRIMARY)
            failed = mock.Mock(side_effect=ValueError('invalid credentials'))
            with self.assertRaises(ValueError):
                runner.change_owner(store, cutover.GROWTH, verify=failed)
            self.assertEqual(cutover.owner(store), cutover.PRIMARY)
            save.assert_not_called()
            runner.change_owner(store, cutover.GROWTH, verify=mock.Mock())
            self.assertEqual(cutover.owner(store), cutover.GROWTH)
            self.assertEqual(store.state['posts']['telegram/best/test'], receipt)
            save.assert_called_once_with()
            self.assertEqual(monitor.poll(store, {}, '123', repository=cutover.PRIMARY),
                             'owned_by_other_repository')

    def test_bridge_signs_a_time_limited_editorial_session(self):
        source = mock.Mock()
        store = Storage(source, 'test')
        now = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)
        env = {'R2_ACCOUNT_ID': 'account', 'R2_ACCESS_KEY_ID': 'example-id',
               'R2_SECRET_ACCESS_KEY': 'example-key'}
        with mock.patch.object(store, 'read', return_value=(None, None)), mock.patch.dict(os.environ, env):
            bridge.refresh_session(store, now)
        call = source.put_object.call_args.kwargs
        self.assertEqual(call['Key'], bridge.SESSION_KEY)
        credentials = json.loads(call['Body'])
        jwt = b64decode(credentials['token']).removeprefix(b'jwt/')
        unsigned, signature = jwt.rsplit(b'.', 1)
        self.assertEqual(urlsafe_b64decode(signature + b'='), hmac.digest(b'example-key', unsigned, 'sha256'))
        body = unsigned.split(b'.')[1]
        claims = json.loads(urlsafe_b64decode(body + b'=' * (-len(body) % 4)))
        self.assertEqual(claims['paths']['prefixPaths'], [PREFIX])
        self.assertEqual(claims['exp'] - claims['iat'], 7200)
        self.assertEqual(credentials['secret_key'], sha256(jwt).hexdigest())
