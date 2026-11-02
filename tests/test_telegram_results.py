import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from telegram_results import outcome, audit

class TelegramResultAuditTests(unittest.TestCase):
    def test_period_and_gg(self):
        self.assertEqual(outcome("over15_ht", {"status":"1H", "halftime":{"home":1,"away":1}}), "pending")
        self.assertEqual(outcome("over15_ht", {"status":"FT", "halftime":{"home":2,"away":0}}), "won")
        self.assertEqual(outcome("gg_ht", {"status":"FT", "halftime":{"home":2,"away":0}}), "lost")
        self.assertEqual(outcome("over25_ft", {"status":"HT", "fulltime":{"home":3,"away":0}}), "pending")
        self.assertEqual(outcome("over25_ft", {"status":"FT", "fulltime":{"home":2,"away":1}}), "won")
        self.assertEqual(outcome("over25_ft", {"status":"PST", "fulltime":{"home":2,"away":1}}), "pending")

    def test_only_sent_selections_and_separate_triple(self):
        def p(id, market="over15_ht", odd=2.0):
            return {"id":str(id),"market":market,"odd":odd,"match":f"Match {id}","label":"Over", "kickoff":"2026-11-02T19:00:00+01:00"}
        posts={"13:00":{"picks":[p(1),p(2),p(3)]},"19:00":{"picks":[p(4)]}}
        scores={"1":{"status":"FT","halftime":{"home":2,"away":0}},
                "2":{"status":"FT","halftime":{"home":2,"away":1}},
                "3":{"status":"FT","halftime":{"home":0,"away":0}},
                "4":{"status":"FT","halftime":{"home":1,"away":1}}}
        result=audit("2026-11-02",posts,scores)
        self.assertEqual(result["triple"]["status"], "lost")
        self.assertEqual(result["triple"]["pnl_one_unit"], -1)
        self.assertEqual(result["singles"]["won"], 1)
        self.assertEqual(result["selections_total"]["won"],3)
        self.assertEqual(result["selections_total"]["lost"],1)

    def test_no_receipt_is_not_a_loss(self):
        result=audit("2026-11-02",{}, {})
        self.assertEqual(result["posts_found"],0)
        self.assertEqual(result["triple"]["status"],"not_published")
        self.assertIsNone(result["singles"]["accuracy_pct"])

    def test_pending_unconfirmed_score(self):
        posts={"17:30":{"picks":[{"id":"5","market":"over35_ft","odd":3.2,"match":"A - B"}]}}
        result=audit("2026-11-02",posts,{"5":{"status":"FT","fulltime":{}}})
        self.assertEqual(result["singles"]["pending"],1)
        self.assertEqual(result["singles"]["theoretical_pnl"],0)

if __name__=="__main__":
    unittest.main()
