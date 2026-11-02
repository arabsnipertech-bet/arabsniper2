"""Regression tests for private Gold/Platinum Telegram reader."""
import json
import sys
import unittest
from copy import deepcopy
from datetime import datetime,timedelta
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from telegram_private_feed import from_private_day, ROME
from telegram_digest import picks

class PrivateTelegramReader(unittest.TestCase):
    def fixture(self):
        now=datetime(2026,11,3,19,0,tzinfo=ROME)
        case=json.loads((Path(__file__).parent/"platinum_cases.json").read_text())["base"]
        row=deepcopy(case)
        row.update(id="p1",label="Over 1,5 PT",odd=2.20,quote_status="recent",
                   provider_updated_at=(now-timedelta(minutes=4)).isoformat(),
                   quote_executable=True,movement_confirmed=True,
                   fair_direction_coherent=True,oci=91.3,
                   platinum={"eligible":True}, opportunity={"eligible":False})
        row["spec"]={"kind":"total","period":"ht","side":"over","line":1.5,"extra":None}
        game={"fixture":{"fixture_id":"100","status_short":"NS",
                         "kickoff":"2026-11-03T20:45:00+01:00",
                         "match":"Atalanta - Parma","league":"Serie A"},
              "gold_scope":{"allowed":True},
              "multimarket":{"captured_at":(now-timedelta(minutes=3)).isoformat(),
                             "matrix":{"rows":[row],"platinum_policy":{"id":"platinum-flow-v1.1","best_market_id":"p1"},
                                       "opportunity_policy":{"best_market_id":None}}}}
        day={"date":"2026-11-03","updated_at":now.isoformat(),"matches":[game]}
        return day,now
    def test_platinum_is_discovered_and_prioritized(self):
        day,now=self.fixture()
        out=from_private_day(day,now)
        self.assertEqual(len(out),1)
        self.assertEqual(out[0]["source_tier"],"platinum")
        self.assertEqual(out[0]["market"],"over15_ht")
        self.assertEqual(out[0]["odd"],2.2)
        self.assertEqual(picks({"date":day["date"],"selections":out},now,("20:30","22:00"),1)[0]["tier"],"platinum")
    def test_stale_expired_started_and_withdrawn_never_publish(self):
        day,now=self.fixture()
        self.assertEqual(from_private_day(day,now+timedelta(minutes=95)),[])
        day["matches"][0]["multimarket"]["matrix"]["rows"][0]["platinum"]["eligible"]=False
        self.assertEqual(from_private_day(day,now),[])
        day,now=self.fixture()
        day["matches"][0]["fixture"]["status_short"]="1H"
        self.assertEqual(from_private_day(day,now),[])
    def test_no_private_feed_leak(self):
        day,now=self.fixture()
        out=from_private_day(day,now)
        self.assertNotIn("matrix",out[0])
        self.assertNotIn("rows",out[0])

if __name__=="__main__":
    unittest.main()
