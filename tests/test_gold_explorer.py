"""Archive projection contract tests; outbound network is forbidden."""
import copy
import io
import json
import socket
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
import gold_explorer as e
from v7_decision import fingerprint, GOAL
from v7_probability import VERSION

AT=datetime(2026,11,17,10,tzinfo=timezone.utc)
KEY=GOAL['over25_ft']
def fixture(at=AT, start=AT+timedelta(hours=2), fid=1):
    source=dict(id=KEY,odd=2.,quote_status='recent',quote_observed_at=at.isoformat(),provider_updated_at=at.isoformat(),quote_executable=True,bookmaker_id=1)
    market=dict(source_market_id=KEY,calibrated=.6,odd=2.,ev=.2,ev_low=.05,supported=True,tier='GOLD',label='Over 2,5 FT',quote=source,features={'oci':55})
    snap=dict(version=VERSION,fixture_id=str(fid),kickoff=start.isoformat(),captured_at=at.isoformat(),markets={KEY:market},triad_commitments={'most_probable':{'market_id':KEY,'odd':2.,'captured_at':at.isoformat()}})
    snap['sha256']=fingerprint(snap)
    return dict(fixture=dict(fixture_id=fid,kickoff=start.isoformat(),status_short='NS',home='Casa',away='Ospite',league='Serie A'),multimarket={'matrix':{'rows':[source],'v7':snap}})
def resign(match):
    snap=match['multimarket']['matrix']['v7'];snap['sha256']=fingerprint({k:v for k,v in snap.items() if k!='sha256'})
class Missing(Exception):
    response={'Error':{'Code':'NoSuchKey'}}
class Store:
    def __init__(self):self.data={}
    def get_object(self,**kw):
        if kw['Key'] not in self.data:raise Missing()
        return {'Body':io.BytesIO(self.data[kw['Key']])}
    def put_object(self,**kw):
        assert kw['Key'].startswith('explorer/')
        if '/objects/' in kw['Key'] and kw['Key'] in self.data:assert self.data[kw['Key']]==kw['Body']
        self.data[kw['Key']]=kw['Body']
    def read(self,key):return e.read(self,'bucket','explorer/'+key)
class Projection(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.store=Store()
        self.deny=patch.object(socket.socket,'connect',side_effect=AssertionError('No network'));self.deny.start()
    def tearDown(self):self.deny.stop();self.tmp.cleanup()
    def publish(self,match=None,phase='night',at=AT,**kwargs):
        e.publish(self.store,'bucket',self.root,[{'matches':[match or fixture()]}],phase,now=at,**kwargs)
    def history(self):return self.store.read('days/2026-11-17.json')
    def test_authentic_fields_and_no_source_mutation(self):
        original=fixture();before=copy.deepcopy(original);row=e.project_match(original,AT)[0]
        self.assertEqual(original,before);self.assertEqual((row['probability'],row['odd'],row['ev']),(.6,2.,.2));self.assertTrue(row['validated']);self.assertEqual(row['category'],'official')
    def test_cutoff_and_started_postponed(self):
        for status in ('1H','FT','PST','CANC'):
            m=fixture();m['fixture']['status_short']=status;self.assertEqual(e.project_match(m,AT),[])
        self.assertEqual(e.project_match(fixture(),AT+timedelta(hours=2,minutes=-4)),[])
        self.assertTrue(e.project_match(fixture(),AT+timedelta(hours=2,minutes=-5)))
    def test_tamper_future_source_and_unqualified_are_not_recommended(self):
        m=fixture();m['multimarket']['matrix']['v7']['markets'][KEY]['calibrated']=.9
        self.assertEqual(e.project_match(m,AT),[])
        m=fixture();m['multimarket']['matrix']['rows'][0]['quote_observed_at']=(AT+timedelta(minutes=1)).isoformat();resign(m)
        self.assertFalse(e.project_match(m,AT)[0]['validated'])
        m=fixture();m['multimarket']['matrix']['v7']['markets'][KEY]['supported']=False;resign(m)
        self.assertEqual(e.rank(e.project_match(m,AT)),[])
    def test_idempotence_and_immutable_snapshot_freeze(self):
        self.publish();self.publish(at=AT+timedelta(minutes=1));self.assertEqual(len(self.history()['scans']),1)
        initial=copy.deepcopy(self.store.data);fast=AT+timedelta(minutes=50);self.publish(fixture(fast),phase='fast',at=fast)
        ref=next(iter(self.history()['references'].values()));self.assertNotEqual(ref['night'],ref['fast'])
        self.publish(phase='results',at=AT+timedelta(hours=2));ref=next(iter(self.history()['references'].values()));self.assertTrue(ref['frozen']);self.assertEqual(ref['final'],ref['fast'])
        for key,body in initial.items():
            if '/objects/' in key:self.assertEqual(self.store.data[key],body)
    def test_retained_is_not_a_fast_and_outage_freezes_night(self):
        self.publish();self.publish(phase='fast',at=AT+timedelta(minutes=5),scan_started=(AT+timedelta(minutes=2)).isoformat())
        self.assertEqual(len(self.history()['scans']),1)
        self.publish(phase='results',at=AT+timedelta(days=5));ref=next(iter(self.history()['references'].values()))
        self.assertTrue(ref['frozen']);self.assertTrue(ref['fallback_night']);self.assertEqual(ref['final'],ref['night'])
    def test_unusable_fast_does_not_replace_valid_reference(self):
        self.publish();m=fixture(AT+timedelta(minutes=5));m['multimarket']['matrix']['rows'][0]['quote_executable']=False
        self.publish(m,phase='fast',at=AT+timedelta(minutes=5));self.publish(phase='results',at=AT+timedelta(hours=2))
        self.assertTrue(next(iter(self.history()['references'].values()))['fallback_night'])
    def test_gold_publication_survives_explorer_failure(self):
        from gold_storage import publish, GOLD
        from test_gold_storage import Fake
        data=self.root/'data';data.mkdir()
        for name in GOLD:(data/name).write_text('{"matches":[]}')
        store=Fake()
        with patch.dict('os.environ', {'GOLD_EXPLORER_ENABLED':'true'}), patch('gold_explorer.publish', side_effect=RuntimeError('isolated')):
            publish(store,'test',self.root)
        self.assertIn('state/current.json',store.objects)
        for name in GOLD[:5]:self.assertEqual(json.loads(store.objects[name])['matches'],[])
    def test_reuses_gold_manifest_checksums(self):
        (self.root/'auditarchive').mkdir();path=self.root/'auditarchive/prematch_2026-11-17_v7.json';path.write_text('{"fixtures":{}}')
        with patch('gold_explorer.file_signature',side_effect=AssertionError('duplicate hashing')):
            self.publish(source_manifest={'auditarchive/'+path.name:{'sha256':'known'}})
        self.assertEqual(self.store.read('catalog.json')['days']['2026-11-17']['result_sha'],'known')
    def test_pages_scope_and_checksum(self):
        hashes=e.pages(self.store,'bucket',list(range(501)));self.assertEqual([len(self.store.read('objects/'+h+'.json')['rows']) for h in hashes],[250,250,1])
        self.store.data['explorer/objects/'+hashes[0]+'.json']=b'{}'
        with self.assertRaises(ValueError):self.store.read('objects/'+hashes[0]+'.json')
        with self.assertRaises(ValueError):e.put(self.store,'bucket','gold/forbidden',{})
    def test_results_only_joins_existing_results(self):
        self.publish();(self.root/'auditarchive').mkdir();data={'fixtures':{'1':{'kickoff':fixture()['fixture']['kickoff'],'status':'settled','outcomes':{KEY:{'win':1,'loss':0}}}}}
        (self.root/'auditarchive/prematch_2026-11-17_v7.json').write_text(json.dumps(data));self.publish(phase='results',at=AT+timedelta(hours=3))
        self.assertEqual(self.history()['results'],data['fixtures']);self.assertEqual(len(self.history()['scans']),1)
    def test_legacy_never_creates_explorer_classification(self):
        (self.root/'auditordata').mkdir();m=fixture();m.update(captured_at=AT.isoformat(),publication_snapshot=True,retrospective=False)
        path=self.root/'auditordata/prematch_2026-11-17.json';path.write_text(json.dumps({'analysis_updates':{'one':m,'bad':{**m,'retrospective':True}}}))
        self.publish(phase='results',at=AT+timedelta(days=1));h=self.history();self.assertEqual(h['scans'],[]);self.assertEqual(h['references'],{})
        rows=self.store.read('objects/'+h['legacy_pages'][0]+'.json')['rows'];self.assertEqual(len(rows),1);self.assertTrue(rows[0]['legacy_v7'])
if __name__=='__main__':unittest.main()
