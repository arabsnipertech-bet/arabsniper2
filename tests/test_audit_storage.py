import sys,json,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import read_store,write_store,FORMAT

class AuditStorageTests(unittest.TestCase):
    def test_legacy_and_small_json_are_compatible(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'audit.json';data={'fixtures':{'1':{'captured_at':'before','text':'è già'}}}
            p.write_text(json.dumps(data));self.assertEqual(read_store(p),data)
            write_store(p,data);self.assertEqual(json.loads(p.read_text()),data)

    def test_lossless_shards_and_immutable_previous_parts(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'audit.json';data={'fixtures':{str(i):{'text':'à'*80,'value':i} for i in range(30)}}
            write_store(p,data,chunk_bytes=257)
            manifest=json.loads(p.read_text());self.assertEqual(manifest['_storage'],FORMAT)
            self.assertGreater(len(manifest['parts']),1);self.assertEqual(read_store(p),data)
            files={f:f.read_bytes() for f in (Path(d)/'_audit_parts').iterdir()}
            data['fixtures']['new']={'snapshot':'new'};write_store(p,data,chunk_bytes=257)
            self.assertEqual(read_store(p),data)
            for f,b in files.items():self.assertEqual(f.read_bytes(),b)

    def test_missing_or_corrupt_parts_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'audit.json';write_store(p,{'records':['x'*300]},chunk_bytes=100)
            manifest=json.loads(p.read_text());part=Path(d)/'_audit_parts'/(manifest['parts'][0]['sha256']+'.json.gz')
            part.unlink()
            with self.assertRaises(FileNotFoundError):read_store(p,{})
            write_store(p,{'records':['x'*300]},chunk_bytes=100)
            part.write_bytes(b'corrupt')
            with self.assertRaises(Exception):read_store(p,{})

    def test_auditor_can_archive_and_read_split_source(self):
        import importlib.util
        from datetime import datetime,timezone
        spec=importlib.util.spec_from_file_location('storage_auditor',Path(__file__).resolve().parents[1]/'auditor/current_audit.py')
        a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source=root/'data/arab_quote_prematch_audit.json'
            record={'fixture':{'fixture_id':'1','kickoff':'2026-10-21T12:00:00+00:00'},'captured_at':'2026-10-20T08:00:00+00:00','padding':'è'*1000}
            write_store(source,{'fixtures':{'1':record}},chunk_bytes=100)
            result=a.archive(root,datetime(2026,10,20,10,tzinfo=timezone.utc))
            self.assertEqual(result['new_forecasts'],1)
            self.assertEqual(read_store(root/'auditordata/prematch_2026-10-21.json')['fixtures']['1'],record)
