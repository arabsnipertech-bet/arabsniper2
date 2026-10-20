import unittest, sys, importlib.util
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from match_lifecycle import observed_live_state

class LifecycleTests(unittest.TestCase):
    def test_scores_do_not_invent_zero_and_failure_retains_timestamp(self):
        live=observed_live_state({'fixture':{'status':{'short':'1H','elapsed':17}},'goals':{'home':0,'away':1}},'2026-10-20T12:00:00Z')
        self.assertEqual(live['goals']['home'],0)
        self.assertEqual(live['halftime'],{})
        old=deepcopy(live)
        failed=observed_live_state(None,'2026-10-20T13:00:00Z',live)
        self.assertEqual(failed['observed_at'],live['observed_at'])
        self.assertTrue(failed['refresh_failed'])
        self.assertEqual(live,old)
    def test_engine_preserves_forecast_for_live_and_finished(self):
        path=Path(__file__).resolve().parents[1]/'src/3appdays.py'
        spec=importlib.util.spec_from_file_location('lifecycle_engine',path)
        e=importlib.util.module_from_spec(spec);sys.modules[spec.name]=e;spec.loader.exec_module(e)
        old={'fixture':{'fixture_id':'123'},'multimarket':{'captured_at':'2026-10-20T10:00:00Z','markets':[{'statistical_pct':44.5,'odd':2.2}]}}
        saved=deepcopy(old)
        for status in ['1H','HT','2H','FT']:
            api={'fixture':{'status':{'short':status,'elapsed':45}},'goals':{'home':2,'away':1},
                 'score':{'halftime':{'home':1,'away':1},'fulltime':{'home':2,'away':1} if status=='FT' else {}}}
            out=e._preserve_existing_match(old,api,'test')
            self.assertEqual(out['multimarket'],old['multimarket'])
            self.assertEqual(out['fixture']['live']['status'],status)
            self.assertEqual(old,saved)
        self.assertIsNone(e._result_payload({'fixture':{'status':{'short':'AET'}},'goals':{'home':4,'away':3},'score':{}}))
