"""Editorial-only state. Conditional writes prevent overlapping sends."""
from contextlib import contextmanager
from datetime import timedelta
import json

from multimarket_model import stamp

PREFIX = 'publisher/v7.1/'


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
        self.state = {'groups': {}, 'posts': {}, 'live': {}, 'reports': {}, 'legacy': {}, 'runtime': {}}

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

    def load(self):
        value, self.etag = self.read('state.json', limit=5_000_000)
        if value is not None:
            if value.get('schema') != 1 or not all(isinstance(value.get(k), dict) for k in self.state):
                raise ValueError('Unrecognized editorial ledger')
            self.state = value

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
