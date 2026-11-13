import json
import sys
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from gold_history import project, FILE, merge_day, frozen_display, refresh_scores, project_browser_scores
from audit_storage import read_store
ROOT = Path(__file__).resolve().parents[1]

def time(s): return datetime.fromisoformat(s)
def match(fid=1, kickoff='2026-11-05T23:30:00+01:00', captured='2026-11-05T22:00:00+01:00', odd=2):
    return {'fixture': {'fixture_id':fid, 'home':'A', 'away':'B', 'match':'A - B',
                        'kickoff':kickoff, 'status_short':'NS'},
            'multimarket':{'model_id':'lateral-matrix-v5.0', 'captured_at':captured,
                          'matrix':{'rows':[{'id':'x','odd':odd,'statistical_pct':55}]}}}
def view(matches):
    return [{'name':'GOLD','tone':'gold','category':'gold','proposal_id':'x','platinum_ids':[],'radar_ids':[]} for m in matches]
def board(date, matches):return {'date':date,'matches':matches,'updated_at':date+'T22:10:00+01:00'}

class History(unittest.TestCase):
    def test_rollover_keeps_forecast_and_all_matches_without_touching_today(self):
        with tempfile.TemporaryDirectory() as d:
            b=board('2026-11-05',[match()]); original=deepcopy(b)
            p=project(d,b,now=time('2026-11-05T22:20:00+01:00'),scores={},display=view)
            self.assertEqual(b,original);self.assertEqual(p['matches'],b['matches']);self.assertIsNone(p['previous_day'])
            self.assertEqual(p['current_day_archive']['date'],'2026-11-05')
            today=board('2026-11-06',[match(2,'2026-11-06T20:00:00+01:00','2026-11-06T02:00:00+01:00')])
            p=project(d,today,now=time('2026-11-06T03:00:00+01:00'),scores={},display=view)
            archived=p['previous_day']['matches'][0]
            self.assertEqual(archived['multimarket'],b['matches'][0]['multimarket'])
            self.assertEqual(archived['_gold_history']['category'],'gold')
            self.assertEqual(p['matches'],today['matches'])
            # Archive survives reruns, remains separate, expires from the UI after a day.
            p=project(d,today,now=time('2026-11-06T10:00:00+01:00'),scores={},display=view)
            self.assertEqual(len(p['previous_day']['matches']),1)
            project(d,board('2026-11-07',[]),now=time('2026-11-07T02:00:00+01:00'),scores={},display=view)
            self.assertNotIn('2026-11-05',read_store(Path(d)/FILE)['days'])

    def test_freezes_after_kickoff_but_scores_can_be_corrected(self):
        b=board('2026-11-05',[match()])
        archived=merge_day(None,b,time('2026-11-05T22:30:00+01:00'),view)
        new=deepcopy(b);new['matches'][0]['multimarket']['matrix']['rows'][0]['odd']=9
        new['matches'][0]['fixture']['result']={'status':'FT','home_ft':2,'away_ft':1,'home_ht':1,'away_ht':0}
        updated=merge_day(archived,new,time('2026-11-06T02:00:00+01:00'),view)
        self.assertEqual(updated['matches'][0]['multimarket']['matrix']['rows'][0]['odd'],2)
        self.assertEqual(updated['matches'][0]['fixture']['result']['home_ft'],2)
        days={'2026-11-05':updated}
        row={'fixture_id':'1','status':'FT','observed_at':'2026-11-06T03:00:00+01:00','halftime':{'home':1,'away':0},'fulltime':{'home':3,'away':1},'goals':{'home':3,'away':1}}
        refresh_scores(days,{'schema_version':1,'enabled':True,'fixtures':{'1':row}})
        self.assertEqual(updated['matches'][0]['fixture']['live']['fulltime']['home'],3)
        self.assertEqual(updated['matches'][0]['multimarket'],b['matches'][0]['multimarket'])
        rerun=merge_day(updated,new,time('2026-11-06T04:00:00+01:00'),view)
        self.assertEqual(rerun['matches'][0]['fixture']['status_short'],'FT')
        self.assertEqual(rerun['matches'][0]['fixture']['live']['fulltime']['home'],3)

    def test_latest_prematch_and_no_fabricated_analysis(self):
        b=board('2026-11-05',[match()]); a=merge_day(None,b,time('2026-11-05T22:30:00+01:00'),view)
        newer=board('2026-11-05',[match(captured='2026-11-05T23:00:00+01:00',odd=2.2)])
        a=merge_day(a,newer,time('2026-11-05T23:10:00+01:00'),view)
        self.assertEqual(a['matches'][0]['multimarket']['matrix']['rows'][0]['odd'],2.2)
        missing=match(2,captured='2026-11-06T00:00:00+01:00')
        a=merge_day(a,board('2026-11-05',[missing]),time('2026-11-06T01:00:00+01:00'),view)
        self.assertFalse(a['matches'][1]['_gold_history']['verified_prematch'])
        self.assertEqual(len(a['matches']),2)

    def test_first_install_can_seed_yesterday_and_dst_uses_rome(self):
        with tempfile.TemporaryDirectory() as d:
            yesterday=board('2026-10-25',[match(kickoff='2026-10-25T20:00:00+01:00',captured='2026-10-25T18:00:00+01:00')])
            p=project(d,board('2026-10-26',[]),seed=yesterday,now=time('2026-10-25T23:30:00+00:00'),scores={},display=view)
            self.assertEqual(p['previous_day']['date'],'2026-10-25')
            self.assertEqual(len(p['previous_day']['matches']),1)

    def test_real_display_helper_does_not_mutate_forecast(self):
        m=match();m['_gold_history']={'captured_at':'2026-11-05T22:00:00+01:00'}
        before=deepcopy(m);v=frozen_display([m],ROOT)
        self.assertEqual(m,before);self.assertEqual(v[0]['category'],'studio')

    def test_current_gold_feed_persists_final_and_preserves_frozen_selection(self):
        today = board('2026-11-05', [match()])
        original = deepcopy(today)
        with tempfile.TemporaryDirectory() as d:
            row = {'fixture_id':'1','status':'FT','observed_at':'2026-11-05T23:55:00+01:00',
                   'fulltime':{'home':1,'away':0}, 'halftime':{'home':0,'away':0}}
            feed = {'schema_version':1,'enabled':True,'fixtures':{'1':row}}
            archive = project(d,today,now=time('2026-11-05T23:56:00+01:00'),scores=feed,display=view)
            self.assertEqual(archive['matches'], original['matches'])
            projected = project_browser_scores(archive)
            fixture = projected['matches'][0]['fixture']
            self.assertEqual(fixture['result']['home_ft'],1)
            self.assertEqual(fixture['result']['home_ht'],0)
            self.assertEqual(projected['matches'][0]['multimarket'],original['matches'][0]['multimarket'])
            self.assertEqual(today,original)

if __name__=='__main__':unittest.main()
