import unittest,tempfile,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from audit_ledger import entries
from audit_storage import write_store
class DateStream(unittest.TestCase):
 def test_dates_keys_and_record_contents(self):
  records={'a.b':{'fixture':{'kickoff':'2026-11-05T23:30:00Z'},'p':.61,'values':[1,{'a':2}]},'old':{'fixture':{'kickoff':'2026-11-05T12:00:00Z'},'p':.3},'late':{'values':[1,2],'fixture':{'kickoff':'2026-11-06T10:00:00Z'},'p':.2}}
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'source.json';write_store(p,{'schema_version':1,'fixtures':records},chunk_bytes=64)
   result={k:r for c,k,r in entries(p,fixture_date='2026-11-06') if c=='fixtures' and k is not None}
   self.assertEqual(result,{k:records[k] for k in ('a.b','late')})
 def test_filter_cannot_hide_corrupt_tail(self):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'source.json';write_store(p,{'fixtures':{'old':{'fixture':{'kickoff':'2026-11-01T10:00:00Z'},'data':'x'*1000}}},chunk_bytes=64)
   manifest=json.loads(p.read_text());last=manifest['parts'][-1]['sha256']
   (p.parent/'_audit_parts'/f'{last}.json.gz').write_bytes(b'corrupt')
   with self.assertRaises(Exception):list(entries(p,fixture_date='2026-11-06'))
if __name__=='__main__':unittest.main()
