import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from telegram_digest import verdict, picks, ROME, WINDOWS, UTC_CRONS, slot_for_schedule, evening_rationale

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
        feed = dict(date='2026-11-02', selections=[row('1','18:59',200),
                     row('2','19:30',100), row('3','20:00',150), row('4','21:15',300)])
        self.assertEqual([p['id'] for p in picks(feed,now,WINDOWS['18:30'][:2],limit=1)], ['3'])
        self.assertEqual([p['id'] for p in picks(feed,now,WINDOWS['18:30'][:2],limit=1,exclude={'3'})], ['2'])

    def test_single_filters_before_ranking_in_recovery(self):
        from homepage_preview import social_board
        from unittest.mock import patch
        now = datetime.fromisoformat("2026-11-04T19:10:00+01:00")
        end = now.replace(hour=20, minute=0)
        def fixture(i, clock):
            return {"fixture":{"fixture_id":i, "kickoff":f"2026-11-04T{clock}:00+01:00",
                               "status_short":"NS"}}
        board = {"date":"2026-11-04", "updated_at":now.isoformat(),
                 "matches":[fixture("1","19:08"), fixture("2","20:01"),
                            fixture("3","19:25"), fixture("4","20:00")]}
        def candidate(fixture, assessment, now, **kwargs):
            fid = fixture["fixture_id"]
            return [(int(fid), {"fixture_id":fid})]
        with patch("homepage_preview.output_allowed", return_value=True), patch("homepage_preview.board_spunti", candidate):
            self.assertEqual(social_board(board, now, kickoff_until=end, exclude={"3"}, limit=1),
                             [{"fixture_id":"4"}])

    def test_all_slots_have_primary_cron_in_both_seasons(self):
        from datetime import timedelta, timezone
        for day in ("2026-11-04", "2026-07-04"):
            from telegram_slots import CHANNEL_SLOTS
            for slot in CHANNEL_SLOTS:
                now = datetime.fromisoformat(day+"T"+slot).replace(tzinfo=ROME)
                utc = now.astimezone(timezone.utc)
                cron = next(c for c in UTC_CRONS if int(c.split()[0]) == utc.minute
                            and str(utc.hour) in c.split()[1].split(","))
                self.assertEqual(slot_for_schedule(now, "schedule", cron), slot)
                self.assertEqual(slot_for_schedule(now+timedelta(minutes=15), "workflow_dispatch", "", slot), slot)
        self.assertEqual(len(WINDOWS),8)

    def test_editorial_rotation_and_no_pick_messages(self):
        from datetime import date, timedelta
        from telegram_evidence import EDUCATIONAL_THEMES, educational_message
        from telegram_digest import slot_message
        base = date(2026,11,4)
        self.assertEqual(len({educational_message(base+timedelta(days=i))
                              for i in range(len(EDUCATIONAL_THEMES))}),len(EDUCATIONAL_THEMES))
        self.assertIn("/prova",slot_message(dict(slot="22:00",picks=[])))
        self.assertIn("Nessuna nuova",slot_message(dict(slot="18:30",first="19:00",last="20:00",picks=[])))

    def test_morning_results_reject_wrong_date_or_invalid_totals(self):
        from telegram_digest import slot_message
        post = dict(slot='10:45', day='2026-11-07', picks=[])
        self.assertIn('in verifica', slot_message(post))
        report = dict(date='2026-11-06', selections_total=dict(total=3, won=1, lost=1, pending=1))
        self.assertIn('Vinte: 1', slot_message(dict(post, audit=report)))
        self.assertIn('Perse: 1', slot_message(dict(post, audit=report)))
        self.assertIn('in verifica', slot_message(dict(post, audit=dict(report, date='2026-11-05'))))
        report['selections_total']['total'] = 99
        self.assertIn('in verifica', slot_message(dict(post, audit=report)))

    def test_external_backup_dispatch_is_strictly_gated(self):
        now=datetime.fromisoformat("2026-11-03T17:44:00+01:00")
        self.assertEqual(slot_for_schedule(now,"workflow_dispatch","", "17:30"),"17:30")
        self.assertIsNone(slot_for_schedule(now,"workflow_dispatch","", "19:00"))
        self.assertIsNone(slot_for_schedule(now,"workflow_dispatch","", "13:00"))
        self.assertIsNone(slot_for_schedule(datetime.fromisoformat("2026-11-03T17:59:00+01:00"),
                                             "workflow_dispatch","", "17:30"))

    def test_evening_rationale_real_movement_and_statistics(self):
        pick=dict(model_probability=58.2,odd=1.80,first_odd=1.95,
                  delta_pct=100*(1.80/1.95-1),oci=36.2,margin_pct=5.6,
                  market_confirmed=True,movement_confirmed=True,
                  stable=True,bookmaker="Book A")
        msg="\\n".join(evening_rationale(pick))
        for piece in ("58.2%", "55.6%", "Scarto stimato", "1.95", "1.80",
                      "in calo", "flusso confermato", "OCI +36.2", "aggio 5.6%",
                      "Book A"):
            self.assertIn(piece,msg)

    def test_evening_rationale_missing_movement_never_invents(self):
        pick=dict(model_probability=49.3,odd=2.20,first_odd=3.2,
                  delta_pct=-1.0,oci=None,margin_pct=None,
                  market_confirmed=False,movement_confirmed=False)
        msg="\\n".join(evening_rationale(pick))
        self.assertIn("Apertura/variazione non verificata",msg)
        self.assertIn("conferma mercato assente",msg)
        self.assertNotIn("flusso confermato",msg)

    def test_evening_rationale_fits_photo_caption(self):
        pick=dict(model_probability=50.0,odd=2.1,first_odd=2.4,
                  delta_pct=100*(2.1/2.4-1),oci=31.0,margin_pct=6.0,
                  market_confirmed=True,movement_confirmed=True,
                  stable=True,bookmaker="B"*55)
        text="\\n".join(evening_rationale(pick))
        self.assertLess(len(text),700)

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
        result=picks(feed,now,WINDOWS["18:30"][:2],limit=2)
        self.assertEqual([r["id"] for r in result],["confirmed","unconfirmed"])
