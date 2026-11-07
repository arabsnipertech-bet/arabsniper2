import sys, unittest, json, gzip, hashlib
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from gold_browser_feed import project

class GoldBrowserFeed(unittest.TestCase):
    def test_exact_forecasts_and_lazy_history_without_mutating_input(self):
        matches=[{'fixture':{'fixture_id':i},'multimarket':{'captured_at':'2026-11-07T09:00:00Z','probability':i/13,'evidence':['日本','città']}} for i in range(25)]
        source={'date':'2026-11-07','matches':matches,'previous_day':{'date':'2026-11-06','history_version':'gold-history-v1','matches':matches[:3]}}
        before=json.dumps(source);parts={};result=project(source,lambda key,data:parts.__setitem__(key,data),500)
        self.assertEqual(json.dumps(source),before)
        self.assertEqual(result['matches'],[])
        def unpack(index):
            rows=[]
            for p in index['pages']:
                raw=gzip.decompress(parts['_audit_parts/'+p['sha256']+'.json.gz'])
                self.assertEqual(hashlib.sha256(raw).hexdigest(),p['sha256']);self.assertEqual(len(raw),p['bytes'])
                page=json.loads(raw);self.assertEqual(len(page),p['count']);rows.extend(page)
            self.assertEqual(len(rows),index['match_count']);return rows
        self.assertEqual(unpack(result),matches)
        self.assertEqual(unpack(result['previous_day']),matches[:3])
        self.assertGreater(len(result['pages']),1)
    def test_empty_and_non_day_payload(self):
        self.assertEqual(project({'summary':{}},lambda *_:self.fail('upload')),{'summary':{}})
        self.assertEqual(project({'matches':[]},lambda *_:self.fail('upload'))['match_count'],0)

    def test_catalog_publication_preserves_social_feeds_and_rejects_changed_sources(self):
        import io
        from gold_browser_feed import publish_catalog, encoded
        class S3:
            def __init__(self):
                self.files={'arab_quote_model_research.json':encoded({'summary':{'existing':True}}), 'arab_quote_prediction_audit.json':encoded({'summary':{'wins':4},'records':[1,2]})}
                self.files.update({f'arab_quote_day{i}.json':encoded({'date':'2026-11-07','matches':[{'fixture':{'id':i}}]}) for i in range(1,6)})
                self.changed=False
            def get_object(self, **kw):
                b=self.files[kw['Key']];return {'Body':io.BytesIO(b),'ETag':hashlib.sha256(b).hexdigest()}
            def head_object(self, **kw):
                return {'ETag':'changed' if self.changed else hashlib.sha256(self.files[kw['Key']]).hexdigest()}
            def put_object(self, **kw):self.files[kw['Key']]=kw['Body']
        s=S3();original=dict(s.files);publish_catalog(s,'test')
        for key in original:
            if key!='arab_quote_model_research.json':self.assertEqual(s.files[key],original[key])
        catalog=json.loads(s.files['arab_quote_model_research.json']);self.assertTrue(catalog['summary']['existing']);self.assertEqual(len(catalog['_gold_browser_days']),5)
        s.changed=True;before=s.files['arab_quote_model_research.json']
        with self.assertRaisesRegex(ValueError,'changed'):publish_catalog(s,'test')
        self.assertEqual(s.files['arab_quote_model_research.json'],before)
