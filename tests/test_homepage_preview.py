import unittest
import random
from datetime import datetime, timezone
from copy import deepcopy
from homepage_preview import build_preview

NOW = datetime(2026, 10, 22, 10, tzinfo=timezone.utc)

def fixture(i):
    return {'fixture': {'fixture_id': str(i), 'kickoff':'2026-10-22T20:00:00+02:00', 'status_short':'NS', 'match':'Home - Away'},
            'multimarket': {'best_candidate': {'market':'over25_ft', 'primary_eligible':True, 'coverage':1, 'odd':1.8, 'statistical_pct':65, 'book_probability_pct':60, 'secret_detail':'never public'}}}

def payload():
    return {'date':'2026-10-22', 'updated_at':NOW.isoformat(), 'matches':[fixture(i) for i in range(6)]}

class PreviewTests(unittest.TestCase):
    def test_limit_unique_projection(self):
        data=payload();data['matches'].insert(0,fixture(0))
        rows=build_preview(data,now=NOW)['selections']
        self.assertEqual(len(rows),3)
        self.assertEqual(len({r['fixture_id'] for r in rows}),3)
        self.assertNotIn('secret_detail',str(rows))
    def test_rotation_changes_trio_when_alternatives_exist(self):
        first=build_preview(payload(),now=NOW,rng=random.Random(7))
        second=build_preview(payload(),first,NOW,rng=random.Random(7))
        self.assertNotEqual({r['fixture_id'] for r in first['selections']}, {r['fixture_id'] for r in second['selections']})
    def test_only_three_candidates_and_new_snapshot(self):
        data=payload();data['matches']=data['matches'][:3]
        first=build_preview(data,now=NOW)
        data['matches'][0]['multimarket']['best_candidate']['odd']=2.0
        second=build_preview(data,first,NOW)
        self.assertEqual(len(second['selections']),3)
        self.assertEqual(next(r for r in second['selections'] if r['fixture_id']=='0')['odd'],2.0)
    def test_rotation_excludes_previously_selected_started_match(self):
        first=build_preview(payload(),now=NOW,rng=random.Random(7))
        data=payload();fid=first['selections'][0]['fixture_id']
        next(m for m in data['matches'] if m['fixture']['fixture_id']==fid)['fixture']['status_short']='1H'
        second=build_preview(data,first,NOW)
        self.assertNotIn(fid,{r['fixture_id'] for r in second['selections']})
    def test_stale_and_wrong_day(self):
        for key,value in [('date','2026-10-21'),('updated_at','2026-10-20T10:00:00Z')]:
            data=payload();data[key]=value
            self.assertEqual(build_preview(data,now=NOW)['selections'],[])
    def test_exclude_live_missing_odds_conflicts_and_weak_sources(self):
        for changes in [{'odd':None},{'statistical_reliability':'context_conflict'},{'statistical_reliability':'fragile_halftime'},{'coverage':.2},{'book_probability_pct':20},{'primary_eligible':False},{'odd':float('nan')}]:
            data=payload();data['matches']=[fixture(0)]
            data['matches'][0]['multimarket']['best_candidate'].update(changes)
            self.assertEqual(build_preview(data,now=NOW)['selections'],[])
        for key,value in [('status_short','PST'),('status_short','1H'),('kickoff','2026-10-22T08:00:00Z')]:
            data=payload();data['matches']=[fixture(0)];data['matches'][0]['fixture'][key]=value
            self.assertEqual(build_preview(data,now=NOW)['selections'],[])
    def test_rollover_and_empty_can_fill(self):
        data=payload();old=build_preview(data,now=NOW);old['date']='2026-10-21'
        self.assertEqual(len(build_preview(data,old,NOW)['selections']),3)
        self.assertEqual(len(build_preview(data,{'date':'2026-10-22','selections':[]},NOW)['selections']),3)

if __name__ == '__main__': unittest.main()

