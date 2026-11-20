"""Bounded Fast reads must remain lossless, read-only and atomic."""
from collections import Counter
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor as RealPool
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import read_store, write_store
import gold_storage as gs


class Store:
    def __init__(self, source):
        index, self.objects = {}, {}
        for path, raw in gs.checkpoint_files(Path(source)):
            digest = gs.sha(raw)
            index[path] = dict(sha256=digest, bytes=len(raw))
            self.objects['state/objects/'+digest] = raw
        self.objects['state/current.json'] = json.dumps(dict(version=1, files=index)).encode()
        self.reads = Counter()
    def get_object(self, Bucket, Key):
        self.reads[Key] += 1
        return dict(Body=io.BytesIO(self.objects[Key]))
    def put_object(self, **kwargs):
        raise AssertionError('Restore must never write remote state')


def feeds(root):
    data = root/'data'; data.mkdir()
    for name in gs.GOLD:
        (data/name).write_text('{"matches":[]}')


class FastCheckpointReads(unittest.TestCase):
    def test_identical_files_are_downloaded_once_and_all_names_restored(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            source, target = Path(a), Path(b); feeds(source); store = Store(source)
            with redirect_stdout(io.StringIO()): gs.restore_fast(store,'test',target)
            self.assertEqual(sum(store.reads.values()),2) # manifest + one immutable object
            self.assertEqual({(target/'data'/name).read_bytes() for name in gs.GOLD},{b'{"matches":[]}'})

    def test_shared_chunk_objects_reach_every_historical_directory(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            source, target = Path(a), Path(b); feeds(source)
            names=['data/arab_quote_cache_sample.json','auditarchive/prematch_2026-11-19_daily.json']
            value={'records':['same historical facts'*10 for _ in range(12)]}
            for name in names: write_store(source/name,value,chunk_bytes=73)
            store=Store(source); index=json.loads(store.objects['state/current.json'])['files']
            snapshot=deepcopy(store.objects)
            with redirect_stdout(io.StringIO()): gs.restore_fast(store,'test',target)
            for name in names:self.assertEqual(read_store(target/name),value)
            self.assertTrue(all(n==1 for n in store.reads.values()))
            self.assertLess(len(store.reads),len(index)+1)
            self.assertEqual(store.objects,snapshot)

    def test_single_interrupted_object_is_retried_without_restarting_the_batch(self):
        class StreamFailure(Exception):pass
        class Broken(io.BytesIO):
            def read(self,*args):raise StreamFailure()
        exceptions=SimpleNamespace(IncompleteReadError=StreamFailure,ReadTimeoutError=StreamFailure,
                                   ResponseStreamingError=StreamFailure)
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source,target=Path(a),Path(b);feeds(source)
            (source/'data/arab_quote_other.json').write_text('{"extra":true}')
            store=Store(source); original=store.get_object
            interrupted=next(k for k in store.objects if k.startswith('state/objects/'))
            broken=Broken(store.objects[interrupted])
            def read(**kwargs):
                key=kwargs['Key']
                if key==interrupted and store.reads[key]==0:
                    store.reads[key]+=1;return dict(Body=broken)
                return original(**kwargs)
            with patch.dict(sys.modules,{'botocore.exceptions':exceptions}),patch.object(store,'get_object',side_effect=read),patch('gold_storage.sleep') as retry,redirect_stdout(io.StringIO()):
                gs.restore_fast(store,'test',target)
            self.assertTrue(broken.closed)
            self.assertEqual(store.reads[interrupted],2)
            self.assertTrue(all(n==1 for k,n in store.reads.items() if k!=interrupted))
            retry.assert_called_once_with(1)

    def test_corruption_never_replaces_any_existing_checkout_file(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source,target=Path(a),Path(b);feeds(source);feeds(target)
            for name in gs.GOLD:(target/'data'/name).write_text('{"previous":true}')
            original={p.relative_to(target):p.read_bytes() for p in target.rglob('*') if p.is_file()}
            store=Store(source);key=next(k for k in store.objects if k.startswith('state/objects/'))
            store.objects[key]=b'corrupt'
            with redirect_stdout(io.StringIO()),self.assertRaisesRegex(ValueError,'checksum'):
                gs.restore_fast(store,'test',target)
            self.assertEqual({p.relative_to(target):p.read_bytes() for p in target.rglob('*') if p.is_file()},original)
            self.assertFalse(list(target.glob('.gold-fast-*')))

    def test_first_failure_does_not_drain_a_hundred_queued_remote_reads(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source,target=Path(a),Path(b);feeds(source)
            for i in range(100):(source/f'data/arab_quote_a{i:03}.json').write_text(json.dumps({'row':i}))
            store=Store(source);original=store.get_object
            index=json.loads(store.objects['state/current.json'])['files']
            first='state/objects/'+index['data/arab_quote_a000.json']['sha256']
            def fail(**kwargs):
                if kwargs['Key']==first:
                    store.reads[first]+=1;raise RuntimeError('test read failure')
                return original(**kwargs)
            # A single physical worker makes queue cancellation deterministic;
            # the production scheduler is still allowed at most eight futures.
            with patch('gold_storage.ThreadPoolExecutor',side_effect=lambda **kw:RealPool(max_workers=1)),patch.object(store,'get_object',side_effect=fail),redirect_stdout(io.StringIO()),self.assertRaisesRegex(RuntimeError,'test read failure'):
                gs.restore_fast(store,'test',target)
            self.assertLessEqual(sum(n for k,n in store.reads.items() if k!='state/current.json'),8)
            self.assertFalse(list(target.rglob('*.json')))

    def test_conflicting_sizes_for_one_digest_are_rejected_before_download(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source,target=Path(a),Path(b);feeds(source);store=Store(source)
            manifest=json.loads(store.objects['state/current.json'])
            manifest['files']['data/'+gs.GOLD[0]]['bytes']+=1
            store.objects['state/current.json']=json.dumps(manifest).encode()
            with redirect_stdout(io.StringIO()),self.assertRaisesRegex(ValueError,'Conflicting'):
                gs.restore_fast(store,'test',target)
            self.assertEqual(store.reads,Counter({'state/current.json':1}))

    def test_legacy_subset_mode_keeps_its_previous_download_behavior(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            source,target=Path(a),Path(b);feeds(source);store=Store(source)
            index=gs.previous_manifest(store,'test')
            gs.restore_subset(store,'test',target,index,index.keys())
            self.assertEqual(sum(n for k,n in store.reads.items() if k!='state/current.json'),len(gs.GOLD))

    def test_only_fast_restore_uses_short_sdk_timeouts(self):
        factory=Mock(return_value=object())
        boto=SimpleNamespace(client=factory)
        config=SimpleNamespace(Config=lambda **kwargs:kwargs)
        env={'R2_ACCOUNT_ID':'a'*32,'R2_ACCESS_KEY_ID':'test','R2_SECRET_ACCESS_KEY':'test','R2_BUCKET':'test'}
        with patch.dict(sys.modules,{'boto3':boto,'botocore.config':config}),patch.dict('os.environ',env):
            gs.client(fast_restore=True)
            self.assertEqual(factory.call_args.kwargs['config'],dict(retries={'total_max_attempts':2},connect_timeout=5,read_timeout=20))
            gs.client()
            self.assertEqual(factory.call_args.kwargs['config'],dict(retries={'max_attempts':3},connect_timeout=15,read_timeout=60))


if __name__=='__main__':unittest.main()
