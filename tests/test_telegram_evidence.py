import importlib.util
import sys
sys.path.insert(0,'src')
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

spec=importlib.util.spec_from_file_location("evidence","src/telegram_evidence.py")
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ROME=ZoneInfo("Europe/Rome")


class EvidenceTests(unittest.TestCase):
    def test_only_valid_scheduled_slots(self):
        for cron, expected in module.SLOTS.items():
            h,m=map(int,cron.split()[:2])
            now=datetime(2026,11,4,h,m,tzinfo=ROME)
            self.assertEqual(module.choose_slot(now,"schedule",cron),expected)
            self.assertIsNone(module.choose_slot(now,"workflow_dispatch",cron))
            self.assertIsNone(module.choose_slot(now,"pull_request",cron))
            self.assertIsNone(module.choose_slot(now+timedelta(minutes=40),"schedule",cron))

    def test_audit_includes_all_outcomes(self):
        report={"date":"2026-11-03",
                "selections_total":{"total":8,"won":3,"lost":4,"pending":1,"theoretical_pnl":-1.4},
                "triple":{"status":"lost"}}
        text=module.audit_message(report)
        for bit in ("8","3","4","1","-1.40","❌","non garantiscono"):
            self.assertIn(bit,text)

    def test_invalid_audit_is_never_published(self):
        self.assertIsNone(module.audit_message({"date":"2026-11-03",
                        "selections_total":{"total":2,"won":1,"lost":0,"pending":0}}))
        self.assertIsNone(module.audit_message({"date":"2026-11-03",
                        "selections_total":{"total":0,"won":0,"lost":0,"pending":0}}))

    def test_market_requires_actual_quote_data(self):
        valid={"source_tier":"gold","delta_pct":-4.1,"oci":31.2,"odd":1.81,
               "match":"Team A - Team B","market":"over25_ft","bookmaker":"Example Book",
               "kickoff":"2026-11-04T19:00:00+01:00","stable":True,"market_confirmed":True}
        self.assertIsNone(module.market_message([]))
        self.assertIsNone(module.market_message([{**valid,"oci":None}]))
        msg=module.market_message([valid])
        self.assertIn("-4.1%",msg)
        self.assertIn("31.2",msg)
        self.assertIn("1.81",msg)
        self.assertIn("movimenti",msg.lower())


if __name__=="__main__":
    unittest.main()
