import sys
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from telegram_digest import refresh_reserved, slot_message
from homepage_preview import social_board
from facebook_page import selection

class ReservedTest(unittest.TestCase):
    now = datetime.fromisoformat('2026-11-05T17:35:00+01:00')
    def post(self):
        return dict(day='2026-11-05',slot='17:30',message_id=1,
            picks=[dict(id='1',match='Public A - Public B',label='Over 2,5 FT',tier='Gold',odd=1.8,
                        kickoff='2026-11-05T18:00:00+01:00',provider_updated_at=self.now.isoformat())],
            reserved=[dict(id='2',kickoff='2026-11-05T17:34:00+01:00'),
                      dict(id='3',kickoff='2026-11-05T18:30:00+01:00',match='SECRET',odd=9.0)])
    def test_priority_before_fixture_dedup_and_limit(self):
        board=dict(date='2026-11-05',updated_at=self.now.isoformat(),matches=[dict(fixture=dict(
            fixture_id='1',kickoff='2026-11-05T18:00:00+01:00',status_short='NS'))])
        candidates=[((0,-99),dict(fixture_id='1',tier='Platinum')),
                    ((1,-5),dict(fixture_id='1',tier='Gold')),
                    ((2,-50),dict(fixture_id='2',tier='Blu')),
                    ((0,-100),dict(fixture_id='3',tier='Platinum'))]
        with patch('homepage_preview.output_allowed',return_value=True),patch('homepage_preview.board_spunti',return_value=candidates):
            new=social_board(board,self.now,limit=3,tier_order=('Gold','Blu','Platinum'))
            self.assertEqual([p['tier'] for p in new],['Gold','Blu','Platinum'])
            old=social_board(board,self.now,limit=1)
            self.assertEqual(old[0]['tier'],'Platinum')
    def test_private_fields_removed_and_expiry(self):
        post=refresh_reserved(self.post(),self.now)
        self.assertEqual(post['reserved'],[dict(id='3',kickoff='2026-11-05T18:30:00+01:00')])
        text=slot_message(post)
        self.assertIn('/prova',text)
        self.assertIn('selezioni gratuite',text)
        self.assertNotIn('SECRET',text)
        self.assertLessEqual(len(text.encode('utf-16-le'))//2,1024)
        post['reserved']=[]
        self.assertNotIn('Altre partite selezionabili',slot_message(post))
    def test_facebook_rechecks_reserved(self):
        post=self.post();post['reserved']=post['reserved'][:1];post['text']=slot_message(post)
        _,text=selection(post,self.now)
        self.assertEqual(post['reserved'],[])
        self.assertNotIn('Altre partite selezionabili',text)

if __name__=='__main__': unittest.main()
