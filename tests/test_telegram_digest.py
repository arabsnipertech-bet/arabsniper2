import sys
import unittest
from datetime import datetime
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from telegram_digest import verdict, picks, ROME, WINDOWS, SLOTS

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
