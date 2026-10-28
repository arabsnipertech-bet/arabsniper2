"""Product requirements, adversarial boundaries and end-to-end cache behavior."""
from copy import deepcopy
from math import exp
import json
import subprocess
import unittest
from goal_evidence import evidence
from gold_competitions import fixture_profile
from lateral_matrix import ScoreModel, build_matrix, specification as S, settle_spec
from matrix_quotes import attach, expire
from opportunity_policy import annotate, evaluate
from quote_cache import remember, restore
from statistics_first import statistical_analysis
from test_opportunity_policy import row
from test_lateral_matrix import payload
from test_statistics_first import match, NOW


class GoldV61(unittest.TestCase):
    def test_oci_rise_and_divergence_are_independent_hard_stops(self):
        cases = [({'oci':-20.0001},'oci_negative'),({'delta_pct':2.0001},'odds_rising'),
                 ({'statistical_pct':70.,'selected_market_fair_pct':54.99,'delta_pct':0.},'suspicious_divergence'),
                 ({'statistical_pct':70.,'selected_market_fair_pct':54.99,'delta_pct':-1.,'oci':0.},'suspicious_divergence')]
        for changes,reason in cases:
            r=row(**changes);self.assertIn(reason,evaluate(r)['reasons'])
            annotate({'rows':[r]},NOW);self.assertFalse(r['opportunity']['eligible'])
        self.assertTrue(evaluate(row(oci=-20,delta_pct=2))['eligible'])
        self.assertTrue(evaluate(row(statistical_pct=70,selected_market_fair_pct=55,delta_pct=0))['eligible'])
        self.assertTrue(evaluate(row(statistical_pct=70,selected_market_fair_pct=54,delta_pct=-.01,oci=.01))['eligible'])

    def test_gol_peak_ratio_and_symmetry_boundaries(self):
        r=row()
        for t in r['goal_evidence']['teams'].values(): t['ft'].update(mean=2.,without_last=1.6)
        self.assertTrue(evaluate(r)['eligible'])
        r['goal_evidence']['teams']['home']['ft']['without_last']-=.0001
        self.assertIn('isolated_peak',evaluate(r)['reasons'])
        r=row(spec=S('btts','ht','yes'),odd=4.,statistical_pct=26.,ev_pct=6.,ev_low_pct=-1.)
        for t in r['goal_evidence']['teams'].values():t['ht'].update(expected_goals=.55,mean=.5,without_last=.5,conceded_mean=.5,sample=4)
        self.assertTrue(evaluate(r)['eligible'])
        for field in ('expected_goals','mean','conceded_mean','sample'):
            trial=deepcopy(r);trial['goal_evidence']['teams']['away']['ht'][field]-=.0001
            self.assertFalse(evaluate(trial)['eligible'],field)
        self.assertIn('sensitivity',evaluate({**r,'selected_margin_pct':6.5})['reasons'])
        self.assertTrue(evaluate({**r,'selected_margin_pct':6.5,'ev_low_pct':.5})['eligible'])

    def test_ht_over_requires_rate_frequency_samples_and_oci_for_25(self):
        for line,total,odd,p,low in [(1.5,1.35,2.5,40,2),(2.5,1.70,5.5,18,0)]:
            r=row(spec=S('total','ht','over',line),odd=odd,statistical_pct=p,ev_low_pct=low,oci=15)
            for t in r['goal_evidence']['teams'].values():t['ht'].update(expected_goals=total/2,frequency=.75)
            self.assertTrue(evaluate(r)['eligible'])
            for t in r['goal_evidence']['teams'].values():t['ht']['frequency']=.7499
            self.assertIn('ht_frequency',evaluate(r)['reasons'])
            r['goal_evidence']['teams']['home']['ht']['frequency']=.75
            r['goal_evidence']['teams']['away']['ht']['expected_goals']-=.0001
            self.assertIn('ht_expected',evaluate(r)['reasons'])
        r=row(spec=S('total','ht','over',2.5),odd=5.5,statistical_pct=25,oci=14.999)
        self.assertIn('ht_oci',evaluate(r)['reasons'])

    def test_low_price_upgrade_prefers_35_then_15_without_forcing_bad_pick(self):
        ft=row(id='low',odd=1.84)
        over35=row(id='upgrade',spec=S('total','ft','over',3.5),odd=2.1,statistical_pct=40)
        ht=row(id='ht',spec=S('total','ht','over',1.5),odd=2.1,statistical_pct=50,ev_low_pct=50)
        matrix={'rows':[ft,over35,ht]}
        self.assertEqual(annotate(matrix,NOW)['id'],'upgrade')
        over35['oci']=-21
        self.assertEqual(annotate(matrix,NOW)['id'],'ht')
        ht['delta_pct']=2.01
        self.assertIsNone(annotate(matrix,NOW))

    def test_professional_roman_name_does_not_unblock_actual_reserves(self):
        def check(name,country='Netherlands',league='KNVB Beker'):
            return fixture_profile({'league':{'country':country,'name':league},'teams':{'home':{'name':name},'away':{'name':'Helmond Sport'}}})
        self.assertTrue(check('Willem II')['allowed'])
        for name in ('Willem II U19','Jong Willem II','Ajax II','Bayern III','Barcelona B'):
            self.assertFalse(check(name)['allowed'])
        self.assertFalse(check('Willem II',league='Unknown Cup')['allowed'])

    def test_cache_window_is_inclusive_and_never_renewed_or_future_dated(self):
        r=row(odd=2.2,first_odd=2.4,bookmaker_id='A',quote_observed_at=NOW,
              win_weight=.6,loss_weight=.4,sensitivity_weights=[[.55,.45]])
        remember(r);kickoff='2026-10-27T20:00:00Z'
        r.update(odd=None,quote_status='stale',selected_margin_pct=29,bookmaker_id='B')
        self.assertTrue(restore(r,'2026-10-26T20:00:00Z',kickoff,True))
        self.assertEqual(r['bookmaker_id'],'A');self.assertEqual(r['selected_margin_pct'],3)
        self.assertEqual(r['quote_status'],'cached_stable');self.assertAlmostEqual(r['ev_low_pct'],21.)
        self.assertEqual(r['provider_updated_at'],NOW)
        self.assertFalse(restore(r,'2026-10-26T20:00:01Z',kickoff,True))
        self.assertFalse(restore(r,'2026-10-26T07:59:59Z',kickoff,True))
        self.assertFalse(restore(r,'2026-10-26T09:00:00Z',kickoff,False))
        self.assertFalse(restore(r,kickoff,kickoff,True))

    def test_empty_feed_recovers_same_operator_prices_in_serie_b(self):
        m=match();m['fixture']['league']='Serie B';m['fixture']['kickoff']='2026-10-27T20:00:00Z'
        a=build_matrix(statistical_analysis(m,NOW),NOW);state={}
        attach(a,payload(NOW,2.2,1.8),state,m['fixture'],NOW)
        before={r['id']:r['statistical_pct'] for r in a['matrix']['rows']}
        attach(a,{'response':[]},state,m['fixture'],'2026-10-26T15:00:00Z')
        quoted=[r for r in a['matrix']['rows'] if r.get('odd')]
        self.assertTrue(quoted);self.assertTrue(all(r['quote_status']=='cached_stable' for r in quoted))
        self.assertEqual(before,{r['id']:r['statistical_pct'] for r in a['matrix']['rows']})
        expire(a,m['fixture'],'2026-10-26T20:00:01Z')
        self.assertTrue(all(r.get('odd') is None for r in a['matrix']['rows']))
        remembered=[r.get('last_odd') for r in a['matrix']['rows']]
        expire(a,m['fixture'],'2026-10-26T20:01:00Z')
        self.assertEqual(remembered,[r.get('last_odd') for r in a['matrix']['rows']])

    def test_repeated_feed_without_provider_timestamp_cannot_renew_cache(self):
        m=match();m['fixture']['kickoff']='2026-10-28T20:00:00Z'
        a=build_matrix(statistical_analysis(m,NOW),NOW);state={};feed=payload(NOW,2.2,1.8)
        feed['response'][0]['update']=None
        attach(a,feed,state,m['fixture'],NOW)
        self.assertTrue(any(r.get('odd') for r in a['matrix']['rows']))
        attach(a,feed,state,m['fixture'],'2026-10-26T21:00:00Z')
        self.assertTrue(all(r.get('odd') is None for r in a['matrix']['rows']))

    def test_combo_formula_floor_settlement_and_all_activation_conditions(self):
        a=build_matrix(statistical_analysis(match(),NOW),NOW)
        a['rates']={'home_ht':.8,'away_ht':.7,'home_ft':1.7,'away_ft':1.7}
        a=build_matrix(a,NOW)
        rows=a['matrix']['rows'];ft=next(r for r in rows if r['spec']==S('total','ft','over',2.5));ft['oci']=-10
        annotate(a['matrix'],NOW,a)
        combo=next(r for r in rows if r['spec']['kind']=='halves_total')
        self.assertAlmostEqual(combo['statistical_pct'],100*(1-exp(-1.5))*(1-exp(-1.9)),places=3)
        self.assertTrue(combo['combo']['active']);self.assertFalse(combo['opportunity']['eligible'])
        for result,expected in [({'home_ft':2,'away_ft':0,'home_ht':1,'away_ht':0},(1.,0.)),
                                ({'home_ft':2,'away_ft':0,'home_ht':2,'away_ht':0},(0.,1.))]:
            self.assertEqual(settle_spec(combo['spec'],{'status':'FT',**result}),expected)
        ft['oci']=-10.001;annotate(a['matrix'],NOW,a);self.assertFalse(combo['combo']['active'])
        low=ScoreModel({'home_ht':.8,'away_ht':.7,'home_ft':.9,'away_ft':.8}).estimate(combo['spec'])
        self.assertAlmostEqual(low['p'],(1-exp(-1.5))*(1-exp(-.3)))
        # Real observations with two scoreless games in the window block the combo.
        ft['oci']=0
        for h in a['inputs']['histories']['home'][:2]:h['team_goals_ft']=0;h['team_goals_ht']=0
        annotate(a['matrix'],NOW,a);self.assertIn('combo_goalless',combo['combo']['reasons'])

    def test_combo_and_cached_label_use_existing_ui_sections(self):
        r=row();annotate({'rows':[r]},NOW)
        combo={'id':'combo','spec':S('halves_total','ft','over',.5,.5),'label':'Over 0,5 PT + Over 0,5 ST',
               'statistical_pct':60,'fair_odd':1/0.6,'combo':{'active':True,'status':'Combo Attiva','policy_id':'gold-goals-v6.1'}}
        script="const a=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const r=JSON.parse(s);console.log(JSON.stringify({panel:a.opportunityPanel(r),targets:a.sectionRows(r,'targets').length,row:a.marketRow(r[1])}));});"
        output=json.loads(subprocess.check_output(['node','-e',script],input=json.dumps([r,combo]).encode()))
        self.assertIn('★ Combo Consigliata',output['panel']);self.assertIn('Combo Attiva',output['row'])
        self.assertEqual(output['targets'],2)


if __name__ == '__main__': unittest.main()
