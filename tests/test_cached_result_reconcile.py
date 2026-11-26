import importlib.util
from datetime import datetime, timezone
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('cached_reconcile',Path(__file__).parents[1]/'scripts/reconcile-cached-v7-results.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class CachedResultGuard(unittest.TestCase):
    def setUp(self):
        self.now=datetime(2026,11,26,8,tzinfo=timezone.utc)
        self.row=dict(fixture_id='7',source='API-Football',status='FT',
            provider_kickoff='2026-11-25T19:00:00Z',observed_at='2026-11-25T21:00:00Z',
            fulltime=dict(home=2,away=1),halftime=dict(home=0,away=0))
        self.cache=dict(schema_version=1,enabled=True,fixtures={'7':self.row})
        self.result=dict(status='FT',home_ft=2,away_ft=1,home_ht=0,away_ht=0)
        self.prior=dict(fixtures={'7':dict(kickoff=self.row['provider_kickoff'],result=self.result)})
        self.archive=dict(fixtures={'7':dict(result=self.result)})

    def test_repeat_with_identical_scores_skips_even_with_new_capture_time(self):
        self.assertFalse(module.cached_progress(self.prior,self.archive,self.cache,self.now))
        self.row['observed_at']='2026-11-26T07:00:00Z'
        self.assertFalse(module.cached_progress(self.prior,self.archive,self.cache,self.now))

    def test_unavailable_cache_does_not_rebuild_unknown_result(self):
        self.prior['fixtures']['7']['result']=None
        self.cache['fixtures']={}
        self.assertFalse(module.cached_progress(self.prior,self.archive,self.cache,self.now))

    def test_confirmed_zero_halftime_adds_information(self):
        self.prior['fixtures']['7']['result']=dict(self.result,home_ht=None,away_ht=None)
        self.assertTrue(module.cached_progress(self.prior,self.archive,self.cache,self.now))

    def test_complete_receipt_does_not_hide_missing_compact_halftime(self):
        self.prior['pending_fixtures']=0
        self.archive['fixtures']['7']['result']=dict(self.result,home_ht=None,away_ht=None)
        self.assertTrue(module.cached_progress(self.prior,self.archive,self.cache,self.now))

    def test_future_different_fixture_and_conflicting_scores_never_trigger_rebuild(self):
        for changes in [dict(observed_at='2026-11-27T00:00:00Z'),dict(fixture_id='8'),
                        dict(fulltime=dict(home=3,away=1))]:
            with self.subTest(changes=changes):
                cache=dict(self.cache,fixtures={'7':dict(self.row,**changes)})
                self.assertFalse(module.cached_progress(self.prior,self.archive,cache,self.now))

    def test_missing_or_different_forecast_identity_rejects_before_publication(self):
        original={'7':dict(forecast_sha256='a',captured_at='prematch',kickoff='start')}
        self.assertTrue(module.same_forecasts(original,original))
        self.assertFalse(module.same_forecasts(original,{}))
        for key in ('forecast_sha256','captured_at','kickoff'):
            with self.subTest(key=key):
                self.assertFalse(module.same_forecasts(original,{'7':dict(original['7'],**{key:'changed'})}))

    def test_only_result_outputs_and_new_immutable_chunks_can_change(self):
        before={'auditordata/prematch_2026-11-25.json':{'sha256':'a'},
                'data/arab_quote_v7_weights.json':{'sha256':'b'},
                'data/arab_quote_day1.json':{'sha256':'c'},
                'auditarchive/prematch_2026-11-25_v7.json':{'sha256':'d'}}
        after={**before,'auditarchive/prematch_2026-11-25_v7.json':{'sha256':'e'},
               'auditarchive/_audit_parts/'+'f'*64+'.json.gz':{'sha256':'g'}}
        self.assertEqual(module.guarded_manifest(before,after),2)
        for path in ['auditordata/prematch_2026-11-25.json','data/arab_quote_v7_weights.json','data/arab_quote_day1.json']:
            with self.subTest(path=path),self.assertRaises(ValueError):
                module.guarded_manifest(before,{**after,path:{'sha256':'changed'}})

    def test_existing_chunks_and_unrelated_dates_cannot_be_rewritten(self):
        chunk='auditarchive/_audit_parts/'+'a'*64+'.json.gz'
        for before,after in [({chunk:{'sha256':'a'}},{chunk:{'sha256':'b'}}),
            ({},{'auditarchive/prematch_2026-11-09_v7.json':{'sha256':'b'}}),
            ({'auditordata/prematch_2026-11-25.json':{}},{})]:
            with self.subTest(before=before),self.assertRaises(ValueError):
                module.guarded_manifest(before,after)


if __name__=='__main__':unittest.main()
