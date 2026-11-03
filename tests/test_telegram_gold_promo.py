import importlib.util
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

spec = importlib.util.spec_from_file_location("promo", "src/telegram_gold_promo.py")
promo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(promo)
ROME = ZoneInfo("Europe/Rome")


class PromoTests(unittest.TestCase):
    def test_three_distinct_times(self):
        self.assertEqual(list(promo.SLOTS.values()), ["12:45", "17:15", "21:15"])

    def test_schedule_only_and_on_time(self):
        for cron, slot in promo.SLOTS.items():
            h, m = map(int, slot.split(":"))
            now = datetime(2026, 11, 3, h, m, tzinfo=ROME)
            self.assertEqual(promo.selected(now, "schedule", cron), slot)
            self.assertIsNone(promo.selected(now, "workflow_dispatch", cron))
            self.assertIsNone(promo.selected(now, "pull_request", cron))
            self.assertIsNone(promo.selected(now + timedelta(minutes=46), "schedule", cron))

    def test_one_day_slot_receipts(self):
        self.assertEqual(promo.name_for("2026-11-03", "17:15"),
                         "telegram-gold-promo-2026-11-03-1715-sent")
        self.assertNotEqual(promo.name_for("2026-11-03", "17:15"),
                            promo.name_for("2026-11-04", "17:15"))

    def test_three_messages_accurate_and_distinct(self):
        self.assertEqual(len(promo.MESSAGES), 3)
        self.assertEqual(len(set(promo.MESSAGES)), 3)
        for text in promo.MESSAGES:
            self.assertIn("https://t.me/ArabSniperBot", text)
            self.assertIn("/prova", text)
            self.assertIn("7 giorni", text.lower())
            self.assertIn("email", text.lower())
            self.assertIn("rinnovo", text.lower())
            self.assertIn("carta", text.lower())


if __name__ == "__main__":
    unittest.main()
