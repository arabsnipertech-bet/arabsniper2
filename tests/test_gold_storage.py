import sys,unittest,tempfile,json,io
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from gold_storage import publish,restore,allowed,GOLD
class Missing(Exception):
 response={'Error':{'Code':'NoSuchKey'}}
class Fake:
 def __init__(self):self.objects={};self.calls=[]
 def put_object(self,**kw):self.objects[kw['Key']]=kw['Body'];self.calls.append(kw['Key'])
 def get_object(self,**kw):
  if kw['Key'] not in self.objects:raise Missing()
  return {'Body':io.BytesIO(self.objects[kw['Key']])}
class Storage(unittest.TestCase):
 def test_roundtrip_and_public_exclusion(self):
  with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
   p=Path(a)/'data';p.mkdir()
   for name in GOLD:(p/name).write_text('{"matches":[]}')
   (p/'arab_quote_homepage.json').write_text('{}')
   s=Fake();publish(s,'test',a);restore(s,'test',b)
   self.assertEqual(s.calls[-1],'state/current.json');self.assertFalse((Path(b)/'data/arab_quote_homepage.json').exists())
   self.assertTrue((Path(b)/'data/arab_quote_day1.json').exists())
 def test_tamper_fails_before_writing(self):
  with tempfile.TemporaryDirectory() as a:
   s=Fake();s.objects['state/current.json']=b'{"version":1,"files":{"../../evil":{"sha256":"a","bytes":1}}}'
   with self.assertRaises(ValueError):restore(s,'test',a)
 def test_no_partial_upload_on_missing_gold(self):
  with tempfile.TemporaryDirectory() as a:
   s=Fake()
   with self.assertRaises(ValueError):publish(s,'test',a)
   self.assertFalse(s.calls)
 def test_corrupt_object_does_not_replace_local_data(self):
  with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
   p=Path(a)/'data';p.mkdir()
   for name in GOLD:(p/name).write_text('{"matches":[]}')
   s=Fake();publish(s,'test',a)
   local=Path(b)/'data';local.mkdir();(local/GOLD[0]).write_text('{"original":true}')
   key=next(k for k in s.objects if k.startswith('state/objects/'));s.objects[key]=b'corrupt'
   with self.assertRaises(ValueError):restore(s,'test',b)
   self.assertEqual((local/GOLD[0]).read_text(),'{"original":true}')
 def test_reuses_unchanged_objects_and_pointer_survives_failed_upload(self):
  with tempfile.TemporaryDirectory() as a:
   p=Path(a)/'data';p.mkdir()
   for name in GOLD:(p/name).write_text('{"matches":[]}')
   s=Fake();publish(s,'test',a);s.calls=[];publish(s,'test',a)
   self.assertFalse(any(k.startswith('state/objects/') for k in s.calls))
   pointer=s.objects['state/current.json'];original=s.put_object
   def fail(**kw):
    if kw['Key']==GOLD[1]:raise RuntimeError('upload failed')
    return original(**kw)
   s.put_object=fail;(p/GOLD[0]).write_text('{"changed":true}')
   with self.assertRaises(RuntimeError):publish(s,'test',a)
   self.assertEqual(s.objects['state/current.json'],pointer)
 def test_missing_checkpoint_fails_closed(self):
  with tempfile.TemporaryDirectory() as a:
   with self.assertRaises(Missing):restore(Fake(),'test',a)
 def test_paths(self):
  self.assertFalse(allowed('data/../secret'));self.assertFalse(allowed('web/index.html'));self.assertFalse(allowed('data/arab_quote_results_public.json'))
if __name__=='__main__':unittest.main()
