import json,sys,tempfile,unittest
from pathlib import Path
from datetime import date
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from adaptive_calibration import nightly_state, market_of
from audit_storage import write_store,open_verified_json,read_store
from selection_studies import rank_study_ideas

class ChainTests(unittest.TestCase):
    def test_missing_is_distinct_from_empty_and_hashes_actual_input(self):
        from hashlib import sha256
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);base=root/'auditarchive';base.mkdir()
            (base/'prematch_2026-11-05_daily.json').write_text('{}')
            state=nightly_state(root,date(2026,11,7))
            self.assertFalse(state['training_evidence']['complete'])
            self.assertEqual(state['training_evidence']['missing_dates'],['2026-11-05'])
            p=base/'prematch_2026-11-05_calibration.json';p.write_text('[]\n')
            state=nightly_state(root,date(2026,11,7));e=state['training_evidence']
            self.assertFalse(e['complete']);self.assertEqual(e['missing_results_dates'],['2026-11-05'])
            (base/'prematch_2026-11-05_results.json').write_text('{}')
            e=nightly_state(root,date(2026,11,7))['training_evidence']
            self.assertTrue(e['complete']);self.assertEqual(e['empty_dates'],['2026-11-05'])
            self.assertEqual(e['files'][0]['sha256'],sha256(p.read_bytes()).hexdigest())
    def test_over25_ht_is_a_real_training_market(self):
        for kind in ('total','asian_total'):
            self.assertEqual(market_of(dict(kind=kind,period='ht',side='over',line=2.5)),'over25_ht')
    def test_star_fails_closed_and_keeps_rejections(self):
        good=dict(type='goal',market='over25_ft',odd=2,poisson_pct=60,quality='supported',quote_executable=True,quote_status='recent',ev_low_pct=1)
        for change in ({'odd':1.5},{'ev_low_pct':-0.01},{'ev_low_pct':None},{'quality':'limited'},{'quote_executable':False},{'quote_status':'stale'}):
            x=rank_study_ideas([dict(good,**change)])[0]
            self.assertFalse(x.get('best_study'));self.assertTrue(x['star_gate']['reasons'])
        self.assertTrue(rank_study_ideas([good])[0]['best_study'])
        self.assertTrue(rank_study_ideas([dict(good,ev_low_pct=0)])[0]['best_study'])
    def test_early_stream_exit_still_checks_tail(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'large.json';write_store(p,{'a':['a'*1000]*4},chunk_bytes=100)
            manifest=json.loads(p.read_bytes());last=manifest['parts'][-1]['sha256']
            (p.parent/'_audit_parts'/f'{last}.json.gz').write_bytes(b'corrupt')
            with self.assertRaises(Exception):
                with open_verified_json(p) as stream:stream.read(1)
    def test_large_manifest_cannot_materialize_via_read_store(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'arab_quote_prematch_audit.json'
            p.write_text(json.dumps(dict(_storage='arabsniper-audit-parts-v1',bytes=28_000_000_000,parts=[])))
            with self.assertRaisesRegex(ValueError,'AuditLedger'):read_store(p)
    def test_live_feeds_remain_readable_above_the_historical_master_limit(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as d, patch('audit_storage.MAX_MATERIALIZED_AUDIT_BYTES',64):
            root=Path(d);value={'matches':[{'fixture_id':1,'label':'a'*200}]}
            for chunk_bytes in (50,10000):
                for name in ('arab_quote_day1.json','arab_quote_state.json'):
                    p=root/name;write_store(p,value,chunk_bytes=chunk_bytes)
                    self.assertEqual(read_store(p),value)
                p=root/'arab_quote_prematch_audit.json';write_store(p,value,chunk_bytes=chunk_bytes)
                with self.assertRaisesRegex(ValueError,'AuditLedger'):read_store(p)

    def test_shadow_is_prematch_only_and_does_not_mutate(self):
        from test_opportunity_policy import row
        from opportunity_policy import annotate
        from decision_shadow import capture,PATHS
        r=row(win_weight=.6,loss_weight=.4,low_pct=54,high_pct=65,sensitivity_weights=[[.54,.46]],quote_executable=True)
        a={'matrix':{'rows':[r]}};at='2026-11-07T10:00:00Z';meta=dict(fixture_id='1',kickoff='2026-11-07T12:00:00Z',status_short='NS')
        annotate(a['matrix'],at);original=deepcopy(a)
        shadow=capture(a,meta,at,{})
        self.assertTrue(shadow['available']);self.assertEqual(set(shadow['paths']),set(PATHS));self.assertEqual(a,original)
        self.assertIsNone(capture(a,meta,meta['kickoff'],{}))
        self.assertNotIn('decision_shadow',a['matrix'])
