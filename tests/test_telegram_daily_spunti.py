import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, "src")
from telegram_daily_spunti import eligible, spunti, message_for, UTC_CRONS

ROME = ZoneInfo("Europe/Rome")


class PublicSpuntiTests(unittest.TestCase):
    def fixture(self, now):
        return {
            "date": now.date().isoformat(), "updated_at":now.isoformat(),
            "social_selections":[
                {"fixture_id": str(i), "match":f"A{i} - B{i}","kickoff":
                 (now+timedelta(hours=3+i)).isoformat(),"label":"Over 2,5 FT",
                 "odd":1.75+i*0.1,"captured_at":now.isoformat(),
                 "provider_updated_at":(now-timedelta(minutes=40)).isoformat()}
                for i in range(1,5)],
            "selections":[{"fixture_id":str(i),"statistical_pct":50+i,
                          "margin_pct":6.5,"market_confirmation":"unconfirmed"} for i in range(1,4)]
        }

    def test_three_independent_spunti_with_rationales(self):
        now=datetime(2026,11,3,13,2,tzinfo=ROME)
        items=spunti(self.fixture(now),now)
        self.assertEqual(len(items),3)
        msg=message_for(items,now)
        self.assertIn("NON una multipla",msg)
        self.assertIn("Statistica del modello",msg)
        self.assertIn("Segnale del flusso NON confermato",msg)
        self.assertIn("1️⃣",msg)
        self.assertIn("3️⃣",msg)
        self.assertIn("https://t.me/ArabSniperBot",msg)
        self.assertNotIn("Quota combinata",msg)

    def test_verified_movement_is_explained_but_not_guaranteed(self):
        now=datetime(2026,11,3,13,2,tzinfo=ROME)
        data=self.fixture(now)
        data["social_selections"][0].update({
            "statistical_pct":56.4, "first_odd":2.10,
            "odd":1.85, "delta_pct":100*(1.85/2.10-1),
            "oci":32.5, "bookmaker":"Book A", "bookmakers_count":4,
            "movement_confirmed":False, "margin_pct":5.5,
            "market_confirmation":"unconfirmed"})
        msg=message_for(spunti(data,now),now)
        for phrase in ("56.4%", "Probabilità implicita", "Differenza modello/implicita",
                       "2.10 → ora 1.85", "in discesa", "OCI osservato: +32.5",
                       "NON disponibile", "Book A"):
            self.assertIn(phrase,msg)

    def test_unverified_or_inconsistent_movement_is_not_claimed(self):
        now=datetime(2026,11,3,13,2,tzinfo=ROME)
        data=self.fixture(now)
        data["social_selections"][0].update({
            "first_odd":2.40, "delta_pct":-2.0, "oci":None, "movement_confirmed":True})
        msg=message_for(spunti(data,now),now)
        self.assertIn("Movimento apertura/attuale non verificabile",msg)
        self.assertIn("OCI non disponibile",msg)
        self.assertNotIn("Movimento verificato",msg)

    def test_no_started_or_stale_quotes_or_duplicates(self):
        now=datetime(2026,11,3,13,0,tzinfo=ROME)
        f=self.fixture(now)
        f["social_selections"][0]["kickoff"]=(now-timedelta(minutes=1)).isoformat()
        self.assertEqual(len(spunti(f,now)),3)
        f["social_selections"][1]["provider_updated_at"]=(now-timedelta(hours=9)).isoformat()
        self.assertEqual(len(spunti(f,now)),2)
        f=self.fixture(now)
        f["updated_at"]=(now-timedelta(hours=2)).isoformat()
        self.assertEqual(spunti(f,now),[])

    def test_cet_cest_and_manual_guard(self):
        self.assertEqual(len(UTC_CRONS),4)
        self.assertTrue(eligible(datetime.fromisoformat("2026-11-03T13:00:00+01:00"),
                                 "schedule","0 12 * * *"))
        self.assertTrue(eligible(datetime.fromisoformat("2026-07-03T13:13:00+02:00"),
                                 "schedule","13 11 * * *"))
        self.assertFalse(eligible(datetime.fromisoformat("2026-11-03T13:00:00+01:00"),
                                  "workflow_dispatch","0 12 * * *"))
        self.assertFalse(eligible(datetime.fromisoformat("2026-11-04T13:40:00+01:00"),
                                  "push","",oneoff=True))
        self.assertTrue(eligible(datetime.fromisoformat("2026-11-03T13:30:00+01:00"),
                                 "push","",oneoff=True))

if __name__ == "__main__":
    unittest.main()
