"""Editorial-only state. Conditional writes prevent overlapping sends."""
from contextlib import closing, contextmanager
from datetime import timedelta
import json

import ijson

from multimarket_model import stamp

PREFIX = 'publisher/v7.1/'
LEDGER_SECTIONS = ('groups', 'posts', 'live', 'reports', 'legacy', 'runtime')


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True,
                       allow_nan=False) + '\n').encode()


def error_code(error):
    return str(getattr(error, 'response', {}).get('Error', {}).get('Code', ''))


class Storage:
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket
        self.etag = None
        self.state = {section: {} for section in LEDGER_SECTIONS}

    def read(self, key, default=None, *, root=False, limit=100_000_000):
        try:
            result = self.client.get_object(Bucket=self.bucket, Key=key if root else PREFIX + key)
        except Exception as exc:
            if error_code(exc) in ('NoSuchKey', '404', 'NotFound'):
                return default, None
            raise
        with result['Body'] as body:
            raw = body.read(limit + 1)
        if len(raw) > limit:
            raise ValueError('Publisher input exceeds bounded size')
        return json.loads(raw), result['ETag']

    def gold_board(self, number):
        """Stream the original Gold feed and retain only the Publisher inputs."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=f'arab_quote_day{number}.json')
        except Exception as exc:
            if error_code(exc) in ('NoSuchKey', '404', 'NotFound'):
                return {}
            raise
        board = {'source_bytes': response.get('ContentLength'), 'matches': []}

        def matches(body):
            for path, event, value in ijson.parse(body, use_float=True):
                if path in ('date', 'updated_at') and event == 'string':
                    board[path] = value
                if path == 'matches.item' or path.startswith('matches.item.'):
                    yield path, event, value

        with closing(response['Body']) as body:
            for item in ijson.items(matches(body), 'matches.item'):
                fixture = item.get('fixture') or {}
                if fixture.get('status_short') != 'NS' or fixture.get('result'):
                    continue
                matrix = (item.get('multimarket') or {}).get('matrix') or {}
                snapshot = matrix.get('v7') or {}
                sources = {pick['source_market_id'] for pick in (snapshot.get('triad') or {}).values()
                           if pick and pick.get('source_market_id') is not None}
                rows = [row for row in matrix.get('rows') or () if row.get('id') in sources]
                board['matches'].append({'fixture': fixture,
                                         'multimarket': {'matrix': {'v7': snapshot, 'rows': rows}}})
        if not board.get('date') or not stamp(board.get('updated_at')):
            raise ValueError('Invalid Gold board metadata')
        return board

    def load(self):
        value, etag = self.read('state.json', limit=5_000_000)
        if value is not None:
            if (not isinstance(value, dict) or value.get('schema') != 1 or
                    not all(isinstance(value.get(k), dict) for k in LEDGER_SECTIONS)):
                raise ValueError('Unrecognized editorial ledger')
            self.state = value
        self.etag = etag

    def save(self):
        self.state['schema'] = 1
        options = {'IfMatch': self.etag} if self.etag else {'IfNoneMatch': '*'}
        result = self.client.put_object(
            Bucket=self.bucket, Key=PREFIX + 'state.json', Body=encoded(self.state),
            ContentType='application/json', CacheControl='private, no-store', **options)
        self.etag = result['ETag']

    @contextmanager
    def cycle(self, now):
        """Serialize the short publishing cycles shared by both workflows."""
        record, tag = self.read('cycle.json', limit=512)
        if record and stamp(record['until']) > now:
            yield False
            return
        key = PREFIX + 'cycle.json'
        condition = {'IfMatch': tag} if tag else {'IfNoneMatch': '*'}
        try:
            result = self.client.put_object(
                Bucket=self.bucket, Key=key, **condition,
                Body=encoded({'until': (now + timedelta(minutes=8)).isoformat()}))
        except Exception as exc:
            if error_code(exc) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
                raise
            yield False
            return
        try:
            yield True
        finally:
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, IfMatch=result['ETag'],
                                       Body=encoded({'until': now.isoformat()}))
            except Exception as exc:
                if error_code(exc) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
                    raise

    def freeze(self, key, data):
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=PREFIX + 'evidence/' + key, Body=encoded(data),
                ContentType='application/json', CacheControl='private, no-store', IfNoneMatch='*')
        except Exception as exc:
            if error_code(exc) not in ('412', 'PreconditionFailed'):
                raise
            return False
        return True

    def remember(self, key, data):
        if self.freeze(key, data):
            return data
        saved, _ = self.read('evidence/' + key)
        if not isinstance(saved, dict):
            raise ValueError('Frozen publisher document missing')
        return saved

    def snapshots(self, key):
        prefix = 'evidence/' + key
        pages = self.client.get_paginator('list_objects_v2').paginate(
            Bucket=self.bucket, Prefix=PREFIX + prefix)
        for page in pages:
            for item in page.get('Contents', []):
                value, _ = self.read(item['Key'][len(PREFIX):])
                if value is None:
                    raise ValueError('Missing immutable publisher evidence')
                yield value

    def prune(self, now):
        cutoff = now - timedelta(days=9)
        for name in ('posts', 'groups', 'live', 'reports'):
            self.state[name] = {key: value for key, value in self.state[name].items()
                                if stamp(value['created_at']) >= cutoff}
