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
    def slot_recorded(self,day,slot):
        return getattr(self,"receipt",False)
    def dispatch(self,workflow,slot):
        self.dispatched.append((workflow,slot))


class TelegramWatchdogTests(unittest.TestCase):
    def test_recovery_checks_actual_receipt_for_every_slot(self):
        from telegram_slots import CHANNEL_SLOTS
        for slot in CHANNEL_SLOTS:
            now = datetime.fromisoformat("2026-11-04T"+slot).replace(tzinfo=ROME)
            api = FakeApi([{"head_branch":"main","status":"completed","conclusion":"success"}])
            self.assertNotIn(slot,[s for _,s,_ in due_slots(now)])
            wake(api,now+timedelta(minutes=5))
            self.assertIn(("telegram-digest.yml",slot),api.dispatched)
            api.receipt=True
            self.assertEqual(wake(api,now+timedelta(minutes=15))[0]["action"],"already_attempted")
            self.assertNotIn(slot,[s for _,s,_ in due_slots(now+timedelta(minutes=25))])

    def test_no_old_slots_and_no_duplicate_while_active(self):
        now = datetime.fromisoformat("2026-11-04T17:35:00+01:00")
        api = FakeApi([{"head_branch":"main","status":"in_progress"}])
        self.assertEqual(wake(api,now)[0]["action"],"already_attempted")
        self.assertNotIn("13:00", [slot for _,slot,_ in due_slots(now.replace(hour=13,minute=5))])
