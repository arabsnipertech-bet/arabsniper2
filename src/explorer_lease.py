"""Non-blocking, Explorer-only R2 writer lease; Gold never waits for this lock.
Atomic S3 preconditions protect acquisition/release. TTL exceeds every writer job
runtime; a killed runner leaves a recoverable lease, not a permanent lock.
"""
from contextlib import contextmanager
import json
import time
import uuid
KEY='explorer/writer-lease.json'
TTL=3*3600

def code(error):return getattr(error,'response',{}).get('Error',{}).get('Code')
class FencedStore:
    def __init__(self,storage,deadline,clock):self.storage=storage;self.deadline=deadline;self.clock=clock
    def get_object(self,**kwargs):return self.storage.get_object(**kwargs)
    def put_object(self,**kwargs):
        if self.clock()>=self.deadline-300:raise RuntimeError('Explorer lease nearing expiry')
        assert kwargs['Key'].startswith('explorer/')
        return self.storage.put_object(**kwargs)

@contextmanager
def writer(storage,bucket,clock=time.time):
    now=clock();condition={'IfNoneMatch':'*'}
    try:
        existing=storage.get_object(Bucket=bucket,Key=KEY)
        record=json.loads(existing['Body'].read())
        if record['expires_at']>now:
            yield None;return
        condition={'IfMatch':existing['ETag']}
    except Exception as error:
        if code(error) not in ('NoSuchKey','404','NotFound'):raise
    owner=uuid.uuid4().hex;expires=now+TTL
    raw=json.dumps({'owner':owner,'expires_at':expires}).encode()
    try:locked=storage.put_object(Bucket=bucket,Key=KEY,Body=raw,ContentType='application/json',**condition)
    except Exception as error:
        if code(error) in ('PreconditionFailed','412','ConditionalRequestConflict'):
            yield None;return
        raise
    try:yield FencedStore(storage,expires,clock)
    finally:
        try:storage.put_object(Bucket=bucket,Key=KEY,Body=json.dumps({'owner':owner,'expires_at':0}).encode(),ContentType='application/json',IfMatch=locked['ETag'])
        except Exception as error:
            if code(error) not in ('PreconditionFailed','412','ConditionalRequestConflict'):raise
