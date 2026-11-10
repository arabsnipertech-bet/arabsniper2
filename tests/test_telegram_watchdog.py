import unittest
import sys
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from telegram_watchdog import wake, wake_publisher, due_slots, ROME


class FakeApi:
    def __init__(self,runs=None):
        self.data=runs or []
        self.dispatched=[]
        self.state="active"
    def request(self, path, body=None):
        if body is not None:
            self.dispatched.append((path, body))
        return {"state": self.state}
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
        self.assertIn("13:00", [slot for _,slot,_ in due_slots(now.replace(hour=13,minute=5))])


    def test_publisher_watchdog_observes_activity_and_disable(self):
        now = datetime.fromisoformat("2026-11-10T17:35:00+01:00")
        api = FakeApi([{"status": "in_progress"}])
        self.assertEqual(wake_publisher(api, now), "already_running")
        api.data = [{"status": "completed", "created_at": (now - timedelta(minutes=10)).isoformat()}]
        self.assertEqual(wake_publisher(api, now), "recent_cycle")
        api.state = "disabled_manually"
        self.assertEqual(wake_publisher(api, now), "workflow_disabled")
        self.assertEqual(api.dispatched, [])

    def test_publisher_watchdog_wakes_the_same_production_workflow(self):
        now = datetime.fromisoformat("2026-11-10T17:35:00+01:00")
        api = FakeApi([{"status": "completed", "created_at": (now - timedelta(minutes=18)).isoformat()}])
        self.assertEqual(wake_publisher(api, now, dry_run=True), "would_dispatch")
        self.assertEqual(api.dispatched, [])
        self.assertEqual(wake_publisher(api, now), "dispatched")
        self.assertEqual(api.dispatched, [
            ("workflows/publisher-v71.yml/dispatches", {"ref": "main", "inputs": {"dry_run": "false"}})
        ])
