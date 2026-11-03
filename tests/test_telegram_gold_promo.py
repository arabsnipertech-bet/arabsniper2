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
        self.assertEqual(promo.SLOTS, ("12:45", "17:15", "21:15"))
        self.assertEqual(len(promo.UTC_CRONS), 12)

    def test_cet_and_cest_schedules_and_fallback(self):
        cases = [
            ("2026-11-03T12:45:00+01:00", "45 11 * * *", "12:45"),
            ("2026-11-03T12:58:00+01:00", "58 11 * * *", "12:45"),
            ("2026-07-03T12:45:00+02:00", "45 10 * * *", "12:45"),
            ("2026-07-03T12:58:00+02:00", "58 10 * * *", "12:45"),
            ("2026-11-03T17:15:00+01:00", "15 16 * * *", "17:15"),
            ("2026-11-03T17:28:00+01:00", "28 16 * * *", "17:15"),
            ("2026-07-03T17:15:00+02:00", "15 15 * * *", "17:15"),
            ("2026-11-03T21:15:00+01:00", "15 20 * * *", "21:15"),
            ("2026-07-03T21:15:00+02:00", "15 19 * * *", "21:15"),
        ]
        for timestamp, cron, slot in cases:
            now = datetime.fromisoformat(timestamp)
            self.assertEqual(promo.selected(now, "schedule", cron), slot)
            self.assertIsNone(promo.selected(now, "workflow_dispatch", cron))
            self.assertIsNone(promo.selected(now, "pull_request", cron))
            self.assertIsNone(promo.selected(now + timedelta(minutes=41), "schedule", cron))
        # The other season's UTC cron must never trigger a late/early promo.
        self.assertIsNone(promo.selected(datetime.fromisoformat(
            "2026-11-03T11:45:00+01:00"), "schedule", "45 10 * * *"))
        self.assertIsNone(promo.selected(datetime.fromisoformat(
            "2026-07-03T13:45:00+02:00"), "schedule", "45 11 * * *"))

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
