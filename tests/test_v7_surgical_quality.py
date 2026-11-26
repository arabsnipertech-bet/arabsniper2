"""Execution identity and explicit role rules never rewrite V7 probabilities."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'src'),str(Path(__file__).parent)]
from lateral_matrix import market_id,specification
from matrix_quotes import upgrade_equivalent_prices
from opportunity_policy import operational_role_allowed,margin_friction
from statistics_first import market_lambda_profile
from v7_decision import mark_opposite_conflicts,route,GOAL,GG_FT,RESULTS
from v7_probability import BASE_WEIGHT,price_identity

AT='2026-11-26T09:00:00Z';START='2026-11-26T20:00:00Z'

def record(spec,p,odd,book='1',margin=5.):
    key=market_id(spec)
    source=dict(id=key,spec=spec,odd=odd,bookmaker_id=book,selected_margin_pct=margin,
        selected_market_fair_pct=100/odd/(1+margin/100),quote_observed_at=AT,provider_updated_at=AT,
        quote_executable=True,quote_status='recent',quote_adapter_version=2,
        bet_id=4 if spec['kind']=='handicap' else 1 if spec['kind']=='result' else 5)
    row=dict(source_market_id=key,label='quoted market',calibrated=p,raw=p,statistics=p,corrected=p,
        odd=odd,ev=p*odd-1,ev_low=p*odd-1-.005,quote=dict(bookmaker_id=book,quote_status='recent'),
        spec_kind=spec['kind'],side=spec['side'],horizon='PT' if spec['period']=='ht' else 'FT',
        features=dict(margin_pct=margin),tier='BLU',supported=True,study_supported=True,
        niche_id=None,model_fair_odd=1/p,quote_state='observed',edge_exclusions=[])
    return key,row,source

class Surgery(unittest.TestCase):
    def test_opposite_conflict_retains_original_ev_and_demotes_ranking(self):
        over,r,a=record(specification('total','ht','over',1.5),.4,2.4)
        under,s,b=record(specification('total','ht','under',1.5),.6,1.75)
        a.update(opportunity=dict(eligible=True,reasons=[]),platinum=dict(eligible=True,reasons=[]))
        records={over:r,under:s};sources={over:a,under:b};prob=deepcopy([r['calibrated'],s['calibrated'],r['ev']])
        mark_opposite_conflicts(records,sources,AT,START)
        self.assertTrue(r['opposite_conflict_flag']);self.assertTrue(r['opposite_same_operator'])
        self.assertEqual(r['opposite_market'],under);self.assertEqual(r['opposite_odd'],1.75)
        self.assertFalse(a['opportunity']['eligible']);self.assertFalse(a['platinum']['eligible'])
        self.assertEqual(prob,[r['calibrated'],s['calibrated'],r['ev']])
        other,t,c=record(specification('total','ft','over',3.5),.4,2.6)
        t['ev_low']=-.001 # still a priced observation, not a robust positive offer
        records[other]=t
        triad,_=route(records,dict(conversion=False,rebound_gg=False),study=True)
        self.assertEqual(triad['recommended_alternative']['market_id'],other)

    def test_cross_operator_is_explicit_and_unusable_opposite_cannot_trigger(self):
        a,r,s=record(specification('total','ht','over',1.5),.4,2.4)
        b,o,t=record(specification('total','ht','under',1.5),.6,1.75,book='2')
        mark_opposite_conflicts({a:r,b:o},{a:s,b:t},AT,START)
        self.assertTrue(r['opposite_conflict_flag']);self.assertIs(r['cross_same_operator'],False)
        t['quote_executable']=False
        mark_opposite_conflicts({a:r,b:o},{a:s,b:t},AT,START)
        self.assertFalse(r['opposite_conflict_flag']);self.assertIsNone(r['opposite_ev'])

    def test_same_operator_positive_opposite_precedes_a_cross_operator_price(self):
        a,r,s=record(specification('total','ht','over',1.5),.4,2.4)
        b,o,t=record(specification('total','ht','under',1.5),.6,1.75)
        other=dict(t,id=b+'-cross',bookmaker_id='2',odd=1.8,selected_market_fair_pct=100/1.8/1.05)
        mark_opposite_conflicts({a:r,b:o},{a:s,b:t,'cross':other},AT,START)
        self.assertTrue(r['opposite_same_operator']);self.assertEqual(r['opposite_odd'],1.75)

    def test_under_half_goal_st_excluded_from_every_standard_role(self):
        spec=specification('total','sh','under',.5)
        for role in ('most_probable','recommended_alternative','high_odds_shot'):
            self.assertFalse(operational_role_allowed(role,spec,2.1,.1,4.))

    def test_under_primary_floor_edge_and_margin_are_joint_requirements(self):
        spec=specification('total','sh','under',1.5)
        self.assertTrue(operational_role_allowed('most_probable',spec,1.72,.01,6.5))
        for odd,ev,margin in [(1.71,.1,5),(1.72,0,5),(1.8,.1,6.51),(1.8,.1,None)]:
            self.assertFalse(operational_role_allowed('most_probable',spec,odd,ev,margin))
        self.assertFalse(operational_role_allowed('most_probable',specification('result','ft','1'),1.49,.1,3))

    def test_lambda_uses_period_minimum_team_difference_and_combo(self):
        rates=dict(home_ht=.8,away_ht=.4,home_st=1.2,away_st=.6,home_ft=2.,away_ft=1.)
        for spec,expected in [(specification('total','ht','over',1.5),1.2),
             (specification('total','sh','under',1.5),1.8),(specification('total','ft','over',2.5),3.),
             (specification('btts','ht','yes'),.4),(specification('btts','ft','yes'),1.),
             (specification('result','ft','2'),-1.),(specification('halves_total','ft','over',.5,extra=.5),1.2)]:
            result=market_lambda_profile(spec,rates)
            self.assertAlmostEqual(result['value'],expected);self.assertEqual(result['probability_bonus'],0)
        del rates['home_st']
        missing=market_lambda_profile(specification('total','sh','over',1.5),rates)
        self.assertIsNone(missing['value']);self.assertIsNone(missing['without_peak'])
        failed=market_lambda_profile(specification('total','sh','over',1.5),rates,without_peak=False)
        self.assertIs(failed['without_peak'],False)

    def test_weak_gg_ft_is_ranked_below_supported_base_without_changing_p(self):
        key,r,_=record(specification('btts','ft','yes'),.62,1.8)
        r['lambda_profile']=market_lambda_profile(specification('btts','ft','yes'),dict(home_ft=2.,away_ft=.8))
        other,s,_=record(specification('total','ft','over',2.5),.58,1.9)
        triad,_=route({key:r,other:s},dict(conversion=False,rebound_gg=False))
        self.assertEqual(triad['most_probable']['market_id'],other)
        only,_=route({key:r},dict(conversion=False,rebound_gg=False))
        self.assertEqual(only['most_probable']['probability'],.62)

    def test_equivalent_upgrade_preserves_p_and_uses_genuine_market_name(self):
        key,r,a=record(specification('result','ft','1'),.55,1.85)
        alt,s,b=record(specification('handicap','ft','home',-.5),.55,1.95)
        before=[r['calibrated'],s['calibrated']]
        records={key:r,alt:s};sources={key:a,alt:b}
        upgrades=upgrade_equivalent_prices(records,sources,AT,START)
        self.assertEqual(len(upgrades),1);self.assertEqual(before,[r['calibrated'],s['calibrated']])
        triad,_=route(records,dict(conversion=False,rebound_gg=False),study=True)
        pick=triad['most_probable'];self.assertEqual((pick['market_id'],pick['source_market_id']),(alt,alt))
        self.assertEqual(pick['odd'],1.95);self.assertIn('AH -0,5',pick['label'])
        self.assertEqual(pick['price_upgrade_source'],'AH-0.5');self.assertEqual(BASE_WEIGHT,.8)

    def test_no_upgrade_for_other_operator_unaligned_or_nonidentical_payout(self):
        for change in [dict(bookmaker_id='2'),dict(provider_updated_at='2026-11-26T08:55:00Z'),
                       dict(spec=specification('handicap','ft','home',-.25)),dict(quote_executable=False)]:
            key,r,a=record(specification('result','ft','1'),.55,1.85)
            alt,s,b=record(specification('handicap','ft','home',-.5),.55,1.95)
            b.update(change)
            self.assertEqual(upgrade_equivalent_prices({key:r,alt:s},{key:a,alt:b},AT,START),[])

    def test_same_operator_fair_identity_and_margin_boundary(self):
        _,_,source=record(specification('btts','ft','yes'),.55,1.95,margin=0)
        self.assertTrue(price_identity(source));self.assertEqual(margin_friction(6.5),0)
        self.assertEqual(margin_friction(6.51),.015)
        source['selected_market_fair_pct']=55.
        self.assertFalse(price_identity(source))

if __name__=='__main__':unittest.main()
