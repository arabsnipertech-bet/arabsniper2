import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from api_usage import sanitize_status

class UsageTests(unittest.TestCase):
    def test_status_exports_only_plan_and_counters(self):
        result=sanitize_status({'response':{'account':{'email':'private'},'subscription':{'plan':'Pro','active':True},'requests':{'current':700,'limit_day':7500}}})
        self.assertEqual(result['requests_remaining'],6800)
        self.assertNotIn('private',str(result))
    def test_missing_quota_fails_explicitly(self):
        with self.assertRaises(ValueError):sanitize_status({'response':{}})
