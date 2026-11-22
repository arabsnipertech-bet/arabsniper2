"""A small preference must never manufacture probability, price or eligibility."""
from copy import deepcopy
from datetime import timedelta
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]/'src')]
from multimarket_model import stamp
from v7_decision import capture, route, GOAL, GG, GG_FT, OVER_CARRY
from v7_over15_deficit import MARKET, POLICY, MAX_PRIORITY_BONUS, single_deficit, priority_bonus
from test_v7 import AT, START, META, PATTERNS, assessment


def histories(home=(0, 0, 1, 1, 1, 1, 1, 1), away=(1, 1, 1, 1, 1, 1, 1, 1)):
    return {side: [dict(fixture_id=1000+100*n+i,
        kickoff=(stamp(AT)-timedelta(days=3+i*3)).isoformat(), halftime_available=True,
        team_goals_ht=value, team_goals_ft=value+1)
        for i,value in enumerate(values)] for n,(side,values) in enumerate((('home',home),('away',away)))}


def context(history=None):
    return single_deficit(dict(statistics_as_of=AT,inputs=dict(histories=history or histories())),
                          dict(META,league='Serie A'), AT)


def record(key=MARKET, ev=.025):
    p,odd=.41,(1+ev)/.41
    return dict(source_market_id=key,label='Over 1,5 PT' if key==MARKET else 'Over 3,5 FT',
        calibrated=p,odd=odd,ev=ev,ev_low=.01,fair=.35,supported=True,tier='BLU',
        quote_state='observed',quote=dict(bookmaker_id='1',first_odd=odd),
        niche_id=None,horizon='PT' if key==MARKET else 'FT',features={},spec_kind='total',
        model_fair_odd=1/p)


class SingleDeficit(unittest.TestCase):
    def test_exact_two_and_separate_baseline_with_deduplication(self):
        raw=histories();raw['home']=[*reversed(raw['home']),deepcopy(raw['home'][0])]
        c=context(raw)
        self.assertEqual(c['team'],'home')
        self.assertEqual(c['teams']['home']['recent_mean'],0)
        self.assertEqual(c['teams']['home']['baseline_mean'],1)
        self.assertEqual(c['teams']['home']['baseline_n'],6)
        self.assertEqual(len(c['teams']['home']['recent_fixture_ids']),2)

    def test_no_bonus_for_both_neither_or_ft_only_deficit(self):
        self.assertIsNone(context(histories(away=(0,0,1,1,1,1,1,1))))
        raw=histories(home=(1,1,1,1,1,1,1,1));self.assertIsNone(context(raw))
        raw['home'][0]['team_goals_ft']=1;raw['home'][1]['team_goals_ft']=1
        self.assertIsNone(context(raw))

    def test_incomplete_future_target_and_incoherent_inputs_fail_closed(self):
        for change in ('short','missing_recent','future','target','incoherent','malformed'):
            with self.subTest(change=change):
                raw=histories()
                if change=='short':raw['home']=raw['home'][:5]
                elif change=='missing_recent':raw['home'][0]['team_goals_ht']=None
                elif change=='future':raw['home'][0]['kickoff']=START
                elif change=='target':raw['home'][0]['fixture_id']=META['fixture_id']
                elif change=='incoherent':raw['home'][0].update(team_goals_ht=3,team_goals_ft=1)
                else:raw['home']=[None]
                self.assertIsNone(context(raw))
        a=dict(statistics_as_of=AT,inputs=dict(histories=histories()))
        self.assertIsNone(single_deficit(a,dict(META,status_short='FT'),AT))
        self.assertIsNone(single_deficit(a,META,START))
        self.assertIsNone(single_deficit(a,dict(META,league='World Cup'),AT))

    def test_small_close_ranking_change_without_numeric_mutation(self):
        rival=GOAL['over35_ft'];target=record();target['selection_context']=context()
        records={MARKET:target,rival:record(rival,ev=.028)};saved=deepcopy(records)
        with patch.dict(POLICY,enabled=False):
            baseline=route(records,PATTERNS)[0]['recommended_alternative']
        selected=route(records,PATTERNS)[0]['recommended_alternative']
        self.assertEqual(baseline['market_id'],rival)
        self.assertEqual(selected['market_id'],MARKET)
        self.assertEqual(records,saved)
        self.assertEqual((selected['probability'],selected['ev'],selected['ev_low']),(.41,.025,.01))
        records[rival]=record(rival,ev=.04)
        self.assertEqual(route(records,PATTERNS)[0]['recommended_alternative']['market_id'],rival)
        records[rival]['niche_id']='established_pattern'
        self.assertEqual(route(records,PATTERNS)[0]['recommended_alternative']['market_id'],rival)

    def test_only_simple_market_positive_value_and_recommended_role(self):
        r=record();r['selection_context']=context()
        self.assertEqual(priority_bonus(MARKET,'recommended_alternative',r),MAX_PRIORITY_BONUS)
        for key in (GG,GG_FT,GOAL['over25_ht'],OVER_CARRY):
            self.assertEqual(priority_bonus(key,'recommended_alternative',r),0)
        for role in ('most_probable','high_odds_shot'):
            self.assertEqual(priority_bonus(MARKET,role,r),0)
        for changes in (dict(ev=-.001),dict(ev_low=-.001),dict(odd=None),
                        dict(supported=False),dict(quote_state='model_estimate')):
            self.assertEqual(priority_bonus(MARKET,'recommended_alternative',{**r,**changes}),0)

    def test_real_capture_keeps_all_probabilities_ev_and_other_market_records(self):
        a=assessment();a['rates'].update(home_ft=1.8,away_ft=1.7,home_ht=.9,away_ht=.85)
        a['inputs']={'histories':histories()}
        before_inputs=deepcopy(a)
        with patch.dict(POLICY,enabled=False):baseline=capture(deepcopy(a),dict(META,league='Serie A'),AT)
        changed=capture(deepcopy(a),dict(META,league='Serie A'),AT)
        self.assertEqual(a,before_inputs)
        self.assertIn('selection_context',changed['markets'][MARKET])
        for key,old in baseline['markets'].items():
            if key!=MARKET:self.assertEqual(old,changed['markets'][key])
            for field in ('raw','statistics','corrected','calibrated','odd','ev','ev_low','model_fair_odd','tier','quote'):
                self.assertEqual(old.get(field),changed['markets'][key].get(field))
        self.assertEqual(changed['rates'],baseline['rates'])
        self.assertIn('Plus PT',changed['markets'][MARKET]['origin_badge'])
        self.assertIn('probabilità ed EV originali',changed['markets'][MARKET]['origin_tooltip'])


if __name__=='__main__':unittest.main()
