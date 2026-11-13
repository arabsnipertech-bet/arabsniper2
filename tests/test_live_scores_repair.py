import importlib.util
from pathlib import Path
import unittest
from datetime import datetime, timezone
spec = importlib.util.spec_from_file_location('repair', Path(__file__).resolve().parents[1]/'.github/ops/live-scores-repair.py')
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)
NOW = datetime(2026,11,13,12,tzinfo=timezone.utc)
class RepairBoundsTests(unittest.TestCase):
    def test_recent_closed_day(self):
        self.assertTrue(repair.permitted('2026-11-12', NOW, []))
    def test_open_future_and_old_days_rejected(self):
        for day in ('2026-11-13','2026-11-14','2026-11-09'):
            self.assertFalse(repair.permitted(day, NOW, []))
    def test_active_recovery_blocks_duplicate(self):
        self.assertFalse(repair.permitted('2026-11-12', NOW, [{'status':'in_progress'}]))
    def test_failed_recovery_also_consumes_cooldown(self):
        self.assertFalse(repair.permitted('2026-11-12', NOW, [{'status':'completed','conclusion':'failure','created_at':'2026-11-13T09:00:00Z'}]))
    def test_old_completed_recovery_allows_next_attempt(self):
        self.assertTrue(repair.permitted('2026-11-12', NOW, [{'status':'completed','created_at':'2026-11-12T23:59:00Z'}]))
    def test_large_old_history_does_not_permanently_block_recovery(self):
        self.assertTrue(repair.permitted('2026-11-12', NOW, [{'status':'completed','created_at':'2026-11-12T20:00:00Z'}]*100))
if __name__ == '__main__': unittest.main()
