import unittest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from telegram_watchdog import wake, due_slots, ROME


class FakeApi:
    def __init__(self,runs=None):
        self.data=runs or []
        self.dispatched=[]
    def runs(self,workflow,due):
        return self.data
    def dispatch(self,workflow,slot):
        self.dispatched.append((workflow,slot))


class TelegramWatchdogTests(unittest.TestCase):
    def test_due_slots_backup_and_time_constraints(self):
        at=datetime(2026,11,3,17,44,tzinfo=ROME)
        self.assertEqual([(x,y) for x,y,_ in due_slots(at)],
                         [("telegram-digest.yml","17:30")])
        at=datetime(2026,11,3,21,25,tzinfo=ROME)
        self.assertEqual([(x,y) for x,y,_ in due_slots(at)],
                         [("telegram-gold-promo.yml","21:15")])
        at=datetime(2026,11,3,17,59,tzinfo=ROME)
        self.assertEqual(list(due_slots(at)),[])
        self.assertEqual(list(due_slots(datetime(2026,11,3,17,30,tzinfo=ROME))),[])

    def test_missing_workflow_gets_recovery_dispatch(self):
        now=datetime(2026,11,3,17,44,tzinfo=ROME)
        api=FakeApi()
        r=wake(api,now)
        self.assertEqual(api.dispatched,[("telegram-digest.yml","17:30")])
        self.assertEqual(r[0]["action"],"dispatched")

    def test_existing_successful_attempt_prevents_duplicates(self):
        now=datetime(2026,11,3,17,44,tzinfo=ROME)
        api=FakeApi([{"event":"schedule","status":"completed","conclusion":"success","head_branch":"main"}])
        self.assertEqual(wake(api,now)[0]["action"],"already_attempted")
        self.assertFalse(api.dispatched)

    def test_failed_attempt_recovered_and_dry_run_safe(self):
        now=datetime(2026,11,3,19,14,tzinfo=ROME)
        api=FakeApi([{"event":"schedule","status":"completed","conclusion":"failure","head_branch":"main"}])
        self.assertEqual(wake(api,now,dry_run=True)[0]["action"],"would_dispatch")
        self.assertFalse(api.dispatched)
        wake(api,now)
        self.assertEqual(api.dispatched,[("telegram-digest.yml","19:00")])

    def test_cest(self):
        now=datetime(2026,7,3,16,54,tzinfo=ROME)
        self.assertEqual([(x,y) for x,y,_ in due_slots(now)],
                         [("telegram-evidence.yml","market")])


if __name__=="__main__":
    unittest.main()
