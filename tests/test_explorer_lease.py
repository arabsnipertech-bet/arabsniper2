import hashlib,io,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from explorer_lease import writer,KEY,TTL
class RemoteError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
    def __init__(self):self.objects={};self.race=False
    def tag(self,key):return hashlib.sha256(self.objects[key]).hexdigest()
    def get_object(self,Bucket,Key):
        if Key not in self.objects:raise RemoteError('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[Key]),'ETag':self.tag(Key)}
    def put_object(self,Bucket,Key,Body,**kw):
        if self.race:self.race=False;raise RemoteError('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and Key in self.objects:raise RemoteError('PreconditionFailed')
        if 'IfMatch' in kw and (Key not in self.objects or self.tag(Key)!=kw['IfMatch']):raise RemoteError('PreconditionFailed')
        self.objects[Key]=Body;return {'ETag':self.tag(Key)}
class LeaseTests(unittest.TestCase):
    def test_concurrent_writer_is_skipped_without_waiting(self):
        s=Store()
        with writer(s,'b',clock=lambda:100) as first:
            self.assertIsNotNone(first)
            with writer(s,'b',clock=lambda:101) as second:self.assertIsNone(second)
            first.put_object(Bucket='b',Key='explorer/current.json',Body=b'{}')
        with writer(s,'b',clock=lambda:102) as third:self.assertIsNotNone(third)
    def test_atomic_race_loser_is_skipped(self):
        s=Store();s.race=True
        with writer(s,'b',clock=lambda:0) as result:self.assertIsNone(result)
    def test_crashed_expired_writer_can_be_replaced(self):
        s=Store();s.objects[KEY]=b'{"owner":"crashed","expires_at":99}'
        with writer(s,'b',clock=lambda:100) as current:self.assertIsNotNone(current)
        self.assertEqual(json.loads(s.objects[KEY])['expires_at'],0)
    def test_old_writer_cannot_release_successor_or_write_after_deadline(self):
        s=Store();clock=[100]
        with writer(s,'b',clock=lambda:clock[0]) as current:
            s.objects[KEY]=b'{"owner":"successor","expires_at":99999}'
            clock[0]+=TTL
            with self.assertRaises(RuntimeError):current.put_object(Bucket='b',Key='explorer/current.json',Body=b'{}')
        self.assertEqual(json.loads(s.objects[KEY])['owner'],'successor')
    def test_failure_releases_only_own_lease(self):
        s=Store()
        with self.assertRaises(ValueError):
            with writer(s,'b',clock=lambda:100):raise ValueError('projection failed')
        self.assertEqual(json.loads(s.objects[KEY])['expires_at'],0)
if __name__=='__main__':unittest.main()
