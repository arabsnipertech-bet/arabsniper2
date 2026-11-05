import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from telegram_live import eligible,candidates,advance,observation,deliver

class LiveTest(unittest.TestCase):
    now=datetime.fromisoformat('2026-11-05T18:20:00+01:00')
    def pick(self,**changes):
        return dict(dict(id='123',kickoff='2026-11-05T18:30:00+01:00',odd=2.1,
                         market='over25_ft',model_probability=60,match='A - B',label='Over 2,5 FT',tier='Gold',source_message_id=55),**changes)
    def obs(self,h=0,a=0,status='1H'):
        return dict(home=h,away=a,status=status,minute=20,goal_minute=None,scorer_team=None)
    def state(self):return dict(pick=self.pick())
    def confirmed(self,s,o,t):
        first=advance(s,o,t);return first+advance(s,o,t+60)
    def test_limits_and_exclusions(self):
        self.assertTrue(eligible(self.pick(),self.now))
        for change in ({'market':'gg_ht'},{'market':'over25_ht'},{'odd':3.01},{'odd':1.69},
                       {'model_probability':None},{'model_probability':float('nan')},
                       {'kickoff':'2026-11-05T18:00:00+01:00'}):
            self.assertFalse(eligible(self.pick(**change),self.now))
    def test_only_published_and_best_probability(self):
        r=[dict(day='2026-11-05',message_id=55,picks=[self.pick(),self.pick(id='124',model_probability=65)])]
        self.assertEqual(candidates(r,self.now)[0]['id'],'124')
        r[0].pop('message_id');self.assertEqual(candidates(r,self.now),[])
    def test_goal_confirmation_and_no_duplicate(self):
        s=self.state();advance(s,self.obs(),0)
        self.assertEqual(advance(s,self.obs(1),60),[])
        self.assertEqual([k for k,_ in advance(s,self.obs(1),120)],['goal'])
        self.assertEqual(advance(s,self.obs(1),180),[])
    def test_win_and_var_correction(self):
        s=self.state();self.confirmed(s,self.obs(),0)
        self.assertEqual([k for k,_ in self.confirmed(s,self.obs(2,1),120)],['goal','win'])
        changes=self.confirmed(s,self.obs(1,1),240)
        self.assertTrue(any(k=='notice' for k,_ in changes));self.assertFalse(s['won'])
        self.assertEqual([k for k,_ in self.confirmed(s,self.obs(2,1),360)],['goal','win'])
    def test_no_loss_before_fulltime_and_final_grace(self):
        s=self.state();self.confirmed(s,self.obs(1,1,'HT'),0)
        self.assertNotIn('lose',[k for k,_ in self.confirmed(s,self.obs(1,1,'2H'),120)])
        self.assertEqual([k for k,_ in self.confirmed(s,self.obs(1,1,'FT'),240)],['lose'])
        self.assertFalse(s.get('done'));advance(s,self.obs(1,1,'FT'),1000);self.assertTrue(s['done'])
    def test_extra_time_uses_regulation_score(self):
        row=dict(fixture=dict(status=dict(short='AET',elapsed=120)),goals=dict(home=3,away=2),
                 score=dict(fulltime=dict(home=1,away=1)))
        o=observation(row);self.assertEqual((o['home'],o['away']),(1,1))
    def test_missed_penalty_is_not_goal_event(self):
        row=dict(fixture=dict(status=dict(short='1H',elapsed=20)),goals=dict(home=1,away=0),
                 events=[dict(type='Goal',detail='Missed Penalty',time=dict(elapsed=20),team=dict(name='A'))])
        self.assertIsNone(observation(row)['goal_minute'])
    def test_penalty_scored_is_one_goal_notification(self):
        s=self.state();self.confirmed(s,self.obs(),0)
        o=dict(self.obs(1),penalty=True,scorer_team='A',goal_minute=20)
        self.assertEqual([k for k,_ in self.confirmed(s,o,120)],['penalty'])
        self.assertEqual(advance(s,o,240),[])
    def test_missed_penalty_confirmation_and_dedup(self):
        s=self.state();self.confirmed(s,self.obs(),0)
        o=dict(self.obs(),missed=[dict(key='m1',team='A',minute=20)])
        self.assertEqual(advance(s,o,120),[])
        self.assertEqual([k for k,_ in advance(s,o,180)],['missed'])
        self.assertEqual(advance(s,o,240),[])
    def test_missed_penalty_removed_before_confirmation(self):
        s=self.state();self.confirmed(s,self.obs(),0)
        advance(s,dict(self.obs(),missed=[dict(key='m1',team='A',minute=20)]),120)
        self.assertEqual(advance(s,self.obs(),180),[])
    def test_pending_delivery_committed_with_receipt(self):
        from tempfile import TemporaryDirectory
        import os
        s=self.state();s['pending_events']=[['goal','Goal']]
        class Store:
            def write(self,state):pass
        with TemporaryDirectory() as temp:
            p=Path(temp)/'assets/telegram-live';p.mkdir(parents=True);(p/'goal.gif').write_bytes(b'GIF89a')
            original=os.getcwd()
            try:
                os.chdir(temp)
                with patch('telegram_live.telegram',return_value=dict(chat=dict(username='arabsniper_signals'),message_id=77)):
                    deliver(s,Store(),[s['pending_events'][0]],self.obs(1))
            finally:os.chdir(original)
        self.assertEqual(s['pending_events'],[]);self.assertNotIn('outbox',s)
        self.assertEqual(s['receipts'][0]['message_id'],77)

if __name__=='__main__':unittest.main()
