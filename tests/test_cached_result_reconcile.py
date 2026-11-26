import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('cached_reconcile',Path(__file__).parents[1]/'scripts/reconcile-cached-v7-results.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


class CachedResultGuard(unittest.TestCase):
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
