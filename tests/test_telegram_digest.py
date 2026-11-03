import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from telegram_digest import verdict, picks, ROME, WINDOWS, SLOTS, UTC_CRONS, slot_for_schedule

class TelegramTest(unittest.TestCase):
    now = datetime(2026,11,2,20,tzinfo=ROME)
    pick = dict(kickoff='2026-11-02T18:30:00+01:00',market='over15_ht')
    def test_live_then_closed_half(self):
        score = dict(status='1H',observed_at=self.now.isoformat(),goals=dict(home=1,away=1))
        self.assertIn('raggiunta live',verdict(self.pick,score,self.now))
        score.update(status='2H',halftime=dict(home=0,away=0))
        self.assertIn('Non centrato',verdict(self.pick,score,self.now))
    def test_extra_time_uses_regulation(self):
        score = dict(status='PEN',goals=dict(home=4,away=4),fulltime=dict(home=1,away=1))
        self.assertIn('Non centrato',verdict(dict(self.pick,market='over25_ft'),score,self.now))
    def test_stale(self):
        self.assertIn('non aggiornato',verdict(self.pick,dict(status='2H'),self.now))
    def test_picks(self):
        row=dict(kickoff='2026-11-02T21:00:00+01:00',captured_at='2026-11-02T19:55:00+01:00',fixture_id='1',match='A - B',label='Over 2,5 FT',odd=2,opportunity=dict(eligible=True,goal_market='over25_ft'))
        result=picks(dict(date='2026-11-02',selections=[row,row,dict(row,fixture_id='2',quote_executable=False)]),self.now)
        self.assertEqual([r['id'] for r in result],['1'])

    def test_evening_single_and_time_window(self):
        now = datetime(2026, 11, 2, 18, 55, tzinfo=ROME)
        def row(fid, start, score):
            return dict(fixture_id=fid, match='A - B', label='Over 2,5 FT',
                        kickoff=f'2026-11-02T{start}:00+01:00',
                        captured_at='2026-11-02T18:50:00+01:00', odd=1.9,
                        opportunity=dict(eligible=True, goal_market='over25_ft',
                                         playability_score=score))
        feed = dict(date='2026-11-02', selections=[row('1','19:20',200),
                     row('2','19:30',100), row('3','20:00',150), row('4','21:15',300)])
        self.assertEqual([p['id'] for p in picks(feed,now,WINDOWS['19:00'],limit=1)], ['3'])
        self.assertEqual([p['id'] for p in picks(feed,now,WINDOWS['19:00'],limit=1,exclude={'3'})], ['2'])

    def test_four_scheduled_posts(self):
        self.assertEqual(set(SLOTS.values()), {'13:00','17:30','19:00','20:00'})

    def test_utc_primary_and_backup_cet_cest(self):
        cases = [
            ("2026-11-03T13:00:00+01:00", "0 12 * * *", "13:00"),
            ("2026-11-03T13:13:00+01:00", "13 12 * * *", "13:00"),
            ("2026-07-03T13:00:00+02:00", "0 11 * * *", "13:00"),
            ("2026-07-03T13:13:00+02:00", "13 11 * * *", "13:00"),
            ("2026-11-03T17:30:00+01:00", "30 16 * * *", "17:30"),
            ("2026-07-03T17:30:00+02:00", "30 15 * * *", "17:30"),
            ("2026-11-03T19:00:00+01:00", "0 18 * * *", "19:00"),
            ("2026-07-03T19:00:00+02:00", "0 17 * * *", "19:00"),
            ("2026-11-03T20:00:00+01:00", "0 19 * * *", "20:00"),
            ("2026-07-03T20:00:00+02:00", "0 18 * * *", "20:00"),
        ]
        for instant, cron, expected in cases:
            now = datetime.fromisoformat(instant)
            self.assertEqual(slot_for_schedule(now, "schedule", cron), expected)
            self.assertIsNone(slot_for_schedule(now, "workflow_dispatch", cron))
            self.assertIsNone(slot_for_schedule(now, "push", cron))
        self.assertEqual(len(UTC_CRONS), 14)
        self.assertIsNone(slot_for_schedule(datetime.fromisoformat(
            "2026-11-03T12:00:00+01:00"), "schedule", "0 11 * * *"))

    def test_freshness_and_market_corrob(self):
        now=datetime(2026,11,2,19,0,tzinfo=ROME)
        def row(fid,score,confirmed,mins):
            from datetime import timedelta
            return dict(fixture_id=fid,match=f"Team {fid} - Opponent",label="Over 2,5 FT",
                kickoff="2026-11-02T20:00:00+01:00",
                captured_at=(now-timedelta(minutes=mins)).isoformat(),
                odd=1.9,market_confirmation="confirmed" if confirmed else "unconfirmed",
                opportunity=dict(eligible=True,strict_eligible=True,goal_market="over25_ft",
                    playability_score=score,stability={"status":"strict"}))
        feed=dict(date="2026-11-02",selections=[
             row("old",900,True,120),row("unconfirmed",200,False,10),row("confirmed",100,True,10)])
        result=picks(feed,now,WINDOWS["19:00"],limit=2)
        self.assertEqual([r["id"] for r in result],["confirmed","unconfirmed"])
