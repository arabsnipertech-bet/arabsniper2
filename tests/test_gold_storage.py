import sys,unittest,tempfile,json,io
import gzip, hashlib
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from gold_storage import publish,restore,allowed,GOLD
class Missing(Exception):
 response={'Error':{'Code':'NoSuchKey'}}
class Fake:
 def __init__(self):self.objects={};self.calls=[]
 def put_object(self,**kw):self.objects[kw['Key']]=kw['Body'];self.calls.append(kw['Key'])
 def get_object(self,**kw):
  if kw['Key'] not in self.objects:raise Missing()
  return {'Body':io.BytesIO(self.objects[kw['Key']]),**self.head_object(**kw)}
 def head_object(self,**kw):
  return {'ETag':hashlib.sha256(self.objects[kw['Key']]).hexdigest()}
class Storage(unittest.TestCase):
 def test_roundtrip_and_public_exclusion(self):
  with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
   p=Path(a)/'data';p.mkdir()
   for name in GOLD:(p/name).write_text('{"matches":[]}')
   (p/'arab_quote_homepage.json').write_text('{}')
   s=Fake();publish(s,'test',a);restore(s,'test',b)
   self.assertEqual(s.calls[-1],'state/current.json');self.assertFalse((Path(b)/'data/arab_quote_homepage.json').exists())
   self.assertTrue((Path(b)/'data/arab_quote_day1.json').exists())
   self.assertEqual(json.loads(s.objects['arab_quote_day2.json']), {'matches':[]}, 'Social readers keep the original full feed')
   catalog=json.loads(s.objects['arab_quote_model_research.json']);self.assertIn('_gold_browser_days',catalog)
   self.assertEqual(catalog['_gold_browser_days']['arab_quote_day2.json']['match_count'],0)
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
 def test_restore_retries_only_the_interrupted_object(self):
  class StreamFailure(Exception):
   pass

  class InterruptedBody(io.BytesIO):
   def read(self):
    raise StreamFailure()

  with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
   source = Path(a) / 'data'
   source.mkdir()
   for name in GOLD:
    (source / name).write_text('{"matches": []}')
   storage = Fake()
   publish(storage, 'test', a)
   get = storage.get_object
   body = InterruptedBody()
   object_keys = [key for key in storage.objects if key.startswith('state/objects/')]
   interrupted_key = object_keys[0]
   calls = Counter()

   def interrupted(**request):
    key = request['Key']
    calls[key] += 1
    if key == interrupted_key and calls[key] == 1:
     return {'Body': body}
    return get(**request)

   exceptions = SimpleNamespace(IncompleteReadError=StreamFailure, ReadTimeoutError=StreamFailure,
                                ResponseStreamingError=StreamFailure)
   with (patch.dict(sys.modules, {'botocore.exceptions': exceptions}),
         patch.object(storage, 'get_object', side_effect=interrupted),
         patch('gold_storage.sleep') as retry):
    restore(storage, 'test', b)
   self.assertTrue(body.closed)
   expected = Counter(['state/current.json', *object_keys, interrupted_key])
   self.assertEqual(calls, expected)
   retry.assert_called_once_with(1)
   self.assertTrue((Path(b) / 'data' / GOLD[0]).is_file())

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

 def test_triad_history_survives_publication_and_only_tracks_market_changes(self):
  from gold_browser_feed import TRIAD_HISTORY, publish_catalog, read_triad_history
  from lateral_matrix import market_id, specification
  from v7_decision import fingerprint
  from v7_probability import VERSION

  start=datetime(2026,11,12,17,tzinfo=timezone.utc)
  first=market_id(specification('total','ft','over',2.5))
  next_market=market_id(specification('btts','sh','no'))
  alternative=market_id(specification('total','sh','under',1.5))
  day_name='arab_quote_day2.json'
  s=Fake()

  def board(at,market,label,odd):
   fixture={'fixture_id':42,'kickoff':start.isoformat(),'status_short':'NS'}
   v7={'version':VERSION,'status':'published','fixture_id':'42','kickoff':fixture['kickoff'],
       'captured_at':at.isoformat(),
       'triad':{'most_probable':{'market_id':market},
                'recommended_alternative':{'market_id':alternative}},
       'markets':{market:{'label':label,'odd':odd},
                  alternative:{'label':'Under 1,5 ST','odd':2.2}}}
   v7['sha256']=fingerprint(v7)
   return {'date':'2026-11-12','matches':[{'fixture':fixture,'multimarket':{'matrix':{'v7':v7}}}]}

  def browser_match():
   catalog=json.loads(s.objects['arab_quote_model_research.json'])
   page,=catalog['_gold_browser_days'][day_name]['pages']
   match,=json.loads(gzip.decompress(s.objects['_audit_parts/'+page['sha256']+'.json.gz']))
   return match

  with tempfile.TemporaryDirectory() as directory, patch('gold_storage.datetime',wraps=datetime) as clock:
   root=Path(directory);data=root/'data';data.mkdir()
   for name in GOLD:(data/name).write_text('{"matches":[]}')
   first_at=start-timedelta(hours=1)
   stages=((first_at,first,'Over 2,5 FT',1.53),
           (first_at+timedelta(minutes=15),first,'Over 2,5 FT',1.62),
           (first_at+timedelta(minutes=30),next_market,'No Goal ST',1.55))
   for at,market,label,odd in stages:
    payload=board(at,market,label,odd)
    (data/day_name).write_text(json.dumps(payload))
    clock.now.return_value=at+timedelta(minutes=1)
    publish(s,'test',root)
    self.assertEqual(json.loads(s.objects[day_name]),payload)
    if market==first:self.assertNotIn(TRIAD_HISTORY,browser_match())
   expected={'most_probable':[{'market_id':first,'label':'Over 2,5 FT',
                              'captured_at':first_at.isoformat(),'odd':1.53,'model_fair_odd':None}]}
   self.assertEqual(browser_match()[TRIAD_HISTORY],expected)
   with patch('gold_browser_feed.datetime',wraps=datetime) as browser_clock:
    browser_clock.now.return_value=clock.now.return_value
    publish_catalog(s,'test')
   self.assertEqual(browser_match()[TRIAD_HISTORY],expected)
   after_kickoff=board(start-timedelta(minutes=1),first,'Over 2,5 FT',1.65)
   (data/day_name).write_text(json.dumps(after_kickoff))
   before=read_triad_history(s,'test')
   clock.now.return_value=start+timedelta(minutes=1)
   publish(s,'test',root)
   self.assertEqual(read_triad_history(s,'test'),before)

if __name__=='__main__':unittest.main()
