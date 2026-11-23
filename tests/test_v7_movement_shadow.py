"""Prospective variants share inputs and never feed probabilities or ROI back."""
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import json,sys,unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from decision_shadow import capture_v7,settle_v7
from v7_decision import GG,GG_FT,GOAL,RESULTS,TRIAD_MARKETS,route
from v7_movement_shadow import capture,CONTROL,PROTOCOL,VARIANTS,GG_FT_SAME_RULES
from v7_shadow_metrics import report

AT='2026-11-22T09:00:00+00:00';START='2026-11-22T11:30:00+00:00'
FIRST='2026-11-22T07:00:00+00:00';LAST='2026-11-22T08:59:00+00:00'
FLAGS=dict(conversion=False,rebound_gg=False)
PROFILES={s:{'for_'+p:dict(mean=1.5,without_peak=1.4,sample=8) for p in ('ht','ft')} for s in ('home','away')}

def candidate(key,odd,p,*,first=None,oci=35.,margin=5.,leg=-2.,low=.01,tier='BLU'):
    spec=json.loads(key);first=first or odd/1.05
    source=dict(id=key,spec=spec,odd=odd,first_odd=first,quote_status='recent',bookmaker_id='8',bookmaker='Book',
        quote_executable=True,quote_observed_at=LAST,provider_updated_at=LAST,first_observed_at=FIRST,
        selected_margin_pct=margin,selected_market_fair_pct=100/(odd*(1+margin/100)),oci=oci,
        selected_movement=dict(delta_pct=leg,observed_from=FIRST,observed_to=LAST))
    row=dict(source_market_id=key,label=key,spec_kind=spec['kind'],calibrated=p,statistics=p,raw=p,
        odd=odd,ev=p*odd-1,ev_low=low,supported=True,study_supported=True,tier=tier,niche_id=None,
        fair=1/(odd*(1+margin/100)),model_fair_odd=1/p,quote_state='moving',horizon='PT' if spec['period']=='ht' else 'FT',
        features=dict(oci=oci,margin_pct=margin,delta_pct=leg,opening_delta_pct=100*(odd/first-1)),
        quote={k:source[k] for k in ['odd','first_odd','bookmaker_id','quote_status','first_observed_at','quote_observed_at','provider_updated_at']},
        pricing_evidence={'basis':'binary'})
    return row,source

def fixture():
    rows={};sources={}
    for key,odd,p in [(GOAL['over25_ft'],1.9,.6),(GOAL['over15_ht'],2.3,.48),(GG,4.5,.24)]:
        rows[key],sources[key]=candidate(key,odd,p)
    return rows,sources

def freeze(rows,sources,**kwargs):
    published=route(rows,FLAGS,study=True)[0]
    return capture_v7(rows,sources,FLAGS,published,locked=False,allow_model_estimates=False,
                      as_of=AT,kickoff=START,profiles=PROFILES,**kwargs)

class Variants(unittest.TestCase):
    def test_gg_same_rules_changes_only_recommended_market_admission(self):
        rows,sources=fixture()
        rows[GG_FT],sources[GG_FT]=candidate(GG_FT,2.3,.52)
        before=deepcopy((rows,sources));live_roles=deepcopy(TRIAD_MARKETS);qualified=('recommended_alternative',)
        proof=dict(policy_id='EV_RECOVERY_AGGIO_V1',role='recommended_alternative',
                   original_ev=rows[GOAL['over15_ht']]['ev'],probability=rows[GOAL['over15_ht']]['calibrated'])
        with patch('v7_decision.route',wraps=route) as routed:
            frozen=freeze(rows,sources,ev_recoveries={'recommended_alternative':{GOAL['over15_ht']:proof}},
                          qualified_alternatives=qualified)
        baseline=next(call for call in routed.call_args_list if call.kwargs.get('allow_unpriced') is False
                      and 'role_markets' not in call.kwargs and 'ranker' not in call.kwargs)
        expected={**TRIAD_MARKETS,'recommended_alternative':TRIAD_MARKETS['recommended_alternative']|{GG_FT}}
        trial=next(call for call in routed.call_args_list if call.kwargs.get('role_markets')==expected
                   and call.kwargs.get('allow_unpriced') is False)
        self.assertEqual(trial.args,baseline.args)
        self.assertEqual({k:v for k,v in trial.kwargs.items() if k!='role_markets'},baseline.kwargs)
        self.assertEqual(frozen['variants'][GG_FT_SAME_RULES]['recommended_alternative']['market_id'],GG_FT)
        self.assertEqual(frozen['variants'][CONTROL]['recommended_alternative']['market_id'],GOAL['over15_ht'])
        self.assertEqual((rows,sources),before)
        self.assertEqual(TRIAD_MARKETS,live_roles)
        self.assertNotIn(GG_FT,TRIAD_MARKETS['recommended_alternative'])

    def test_gg_same_rules_does_not_force_a_worse_or_unquoted_candidate(self):
        rows,sources=fixture();rows[GG_FT],sources[GG_FT]=candidate(GG_FT,2.3,.4,tier=None,low=-.1)
        frozen=freeze(rows,sources)
        self.assertEqual(frozen['variants'][GG_FT_SAME_RULES],frozen['variants'][CONTROL])
        rows[GG_FT],sources[GG_FT]=candidate(GG_FT,2.3,.52)
        sources[GG_FT]['quote_executable']=False
        frozen=freeze(rows,sources)
        self.assertEqual(frozen['variants'][GG_FT_SAME_RULES],frozen['variants'][CONTROL])

    def test_native_statistical_lambdas_are_copied_without_changing_choices(self):
        rows,sources=fixture();rates=dict(home_ht=.8,away_ht=.5,home_ft=1.7,away_ft=1.2)
        before=deepcopy(rates);baseline=freeze(rows,sources)
        frozen=freeze(rows,sources,statistical_rates=rates,statistics_at=FIRST)
        context=frozen['movement_study']['statistical_context']
        self.assertTrue(context['available']);self.assertEqual(context['rates'],rates)
        self.assertAlmostEqual(context['lambda_ht_total'],1.3)
        self.assertEqual(frozen['variants'],baseline['variants']);self.assertEqual(rates,before)
        rates['home_ft']=9
        self.assertEqual(context['rates']['home_ft'],1.7)

    def test_missing_or_future_native_lambdas_are_not_fabricated(self):
        rows,sources=fixture()
        missing=freeze(rows,sources)['movement_study']['statistical_context']
        self.assertFalse(missing['available']);self.assertNotIn('rates',missing)
        rates=dict(home_ht=.8,away_ht=.5,home_ft=1.7,away_ft=1.2)
        for at in (None,'2026-11-22T10:00:00Z'):
            context=freeze(rows,sources,statistical_rates=rates,statistics_at=at)['movement_study']['statistical_context']
            self.assertFalse(context['available']);self.assertNotIn('rates',context)

    def test_gg_context_distinguishes_a_different_1x2_operator_without_changing_old_cross_arm(self):
        rows,sources=fixture();key=RESULTS['1'];initial={'1':1.5,'x':4.2,'2':6.5};current={'1':1.6,'x':4.,'2':6.}
        margin=100*(sum(1/x for x in current.values())-1)
        rows[key],sources[key]=candidate(key,1.6,.6,first=1.5,margin=margin)
        sources[key].update(opening_market_odds=initial,selected_market_odds=current)
        rows[GG_FT],sources[GG_FT]=candidate(GG_FT,2.3,.52)
        sources[GG_FT]['bookmaker_id']='different'
        cohort=next(r for r in freeze(rows,sources)['movement_study']['cohorts'] if r['market_id']==GG_FT)
        context=cohort['over_gg_1x2_context']
        self.assertTrue(context['available']);self.assertFalse(context['same_operator'])
        self.assertEqual(context['current_favorite_odd'],1.6)
        self.assertFalse(cohort['cross_1x2_available'])

    def test_missing_live_context_does_not_reconstruct_a_historical_protocol(self):
        rows,sources=fixture()
        ledger,variants=capture(rows,sources,FLAGS,{},profiles=PROFILES)
        self.assertFalse(ledger['available']);self.assertEqual(variants,{})

    def test_four_hypotheses_are_frozen_without_mutation_or_network(self):
        rows,sources=fixture();before=deepcopy((rows,sources))
        with patch('urllib.request.urlopen',side_effect=AssertionError('network forbidden')):
            frozen=freeze(rows,sources)
        self.assertEqual((rows,sources),before)
        self.assertTrue(set(VARIANTS)<=frozen['variants'].keys())
        ledger=frozen['movement_study']
        self.assertEqual(ledger['protocol'],PROTOCOL);self.assertFalse(ledger['automatic_promotion'])
        self.assertEqual(frozen['variants'][CONTROL],frozen['variants']['nov22_margin_friction'])
        self.assertTrue(all(r['excess_margin_ev_cost']==0 for r in ledger['cohorts']))
        for variant in VARIANTS:
            for pick in frozen['variants'][variant].values():
                if pick:
                    original=rows[pick['market_id']]
                    for name in ['odd','ev','ev_low']:
                        self.assertEqual(pick[name],original[name])
                    self.assertEqual(pick['probability'],original['calibrated'])

    def test_roi_and_future_outcome_never_change_a_decision(self):
        rows,sources=fixture();before=freeze(rows,sources)
        for row in rows.values():row.update(roi=999999,outcome=1)
        for source in sources.values():source.update(result={'status':'FT','home_ft':10,'away_ft':0})
        after=freeze(rows,sources)
        self.assertEqual(before,after)

    def test_oci_zero_missing_saturated_and_steep_last_leg_are_separate(self):
        key=GOAL['over25_ft']
        for oci,leg,expected in [(0,-2,False),(None,-2,False),(49.99,-2,True),(50,-2,False),(35,-5,False),(35,-4.99,True)]:
            with self.subTest(oci=oci,leg=leg):
                r,s=candidate(key,1.9,.6,oci=oci,leg=leg)
                ledger=freeze({key:r},{key:s})['movement_study'];row=ledger['cohorts'][0]
                self.assertEqual(row['oci_moderate'],expected)
                self.assertEqual(row['oci_state'],'missing' if oci is None else 'neutral' if oci==0 else 'current')

    def test_cross_anchor_requires_one_complete_operator_partition(self):
        rows,sources=fixture();key=RESULTS['1']
        initial={'1':1.5,'x':4.2,'2':6.5};current={'1':3.8,'x':3.6,'2':1.6}
        margin=100*(sum(1/x for x in current.values())-1)
        rows[key],sources[key]=candidate(key,3.8,.3,first=1.5,margin=margin)
        sources[key].update(opening_market_odds=initial,selected_market_odds=current)
        ledger=freeze(rows,sources)['movement_study'];anchor=ledger['anchor_1x2']
        self.assertTrue(anchor['favorite_135_170']);self.assertTrue(anchor['favorite_inversion'])
        self.assertEqual(anchor['bookmaker_id'],'8')
        cross=next(r for r in ledger['cohorts'] if r['market_id']==GOAL['over25_ft'])
        self.assertEqual(cross['cross_direction'],'up|up')
        self.assertTrue(cross['cross_same_operator'])
        sources[key]['selected_market_odds']={**current,'1':3.7}
        self.assertFalse(freeze(rows,sources)['movement_study']['anchor_1x2']['available'])
        current['1']=9.9
        self.assertEqual(anchor['current']['1'],3.8)

    def test_margin_cost_is_a_separate_score_and_reference_counterfactual(self):
        key=GOAL['over25_ft'];odd=1/(.5*1.10)
        r,s=candidate(key,odd,.54,margin=10.,low=-.03,tier=None)
        before=r['ev'];ledger=freeze({key:r},{key:s})['movement_study'];row=ledger['cohorts'][0]
        self.assertLess(before,0);self.assertGreater(row['ev_at_reference_margin'],0)
        self.assertTrue(row['negative_ev_becomes_nonnegative_at_reference_margin'])
        self.assertLess(row['shadow_ranking_score'],before)
        self.assertEqual(r['ev'],before);self.assertEqual(r['calibrated'],.54)

    def test_experiments_retain_existing_recovery_certificates(self):
        key=GOAL['over15_ht'];r,s=candidate(key,2.5,.39,low=-.03,tier=None)
        proof=dict(policy_id='EV_RECOVERY_AGGIO_V1',role='recommended_alternative',
                   original_ev=r['ev'],probability=r['calibrated'])
        frozen=freeze({key:r},{key:s},ev_recoveries={'recommended_alternative':{key:proof}})
        for name in (CONTROL,'nov22_margin_friction'):
            pick=frozen['variants'][name]['recommended_alternative']
            self.assertEqual(pick['ev_recovery'],proof)
            self.assertLess(pick['ev'],0);self.assertFalse(pick.get('study_only',False))

    def test_gg_21_and_25_are_distinct_and_never_accept_negative_ev(self):
        r,s=candidate(GG,4.5,.24)
        frozen=freeze({GG:r},{GG:s})
        self.assertEqual(frozen['variants']['nov22_triad_rank_21']['high_odds_shot']['market_id'],GG)
        self.assertIsNone(frozen['variants']['nov22_triad_rank_25']['high_odds_shot'])
        r,s=candidate(GG,4.2,.21,low=-.13,tier=None)
        frozen=freeze({GG:r},{GG:s})
        self.assertIsNone(frozen['variants']['nov22_triad_rank_21']['high_odds_shot'])

    def test_positive_shot_prevents_a_higher_probability_negative_study(self):
        other=GOAL['over25_sh'];rows={};sources={}
        rows[GG],sources[GG]=candidate(GG,5.,.21,low=-.01,tier=None)
        rows[other],sources[other]=candidate(other,4.,.24,low=-.06,tier=None)
        self.assertLess(rows[other]['ev'],0)
        proof=dict(original_ev=rows[other]['ev'],probability=rows[other]['calibrated'])
        shot=freeze(rows,sources,ev_recoveries={'high_odds_shot':{other:proof}})['variants']['nov22_triad_rank_21']['high_odds_shot']
        self.assertEqual(shot['market_id'],GG);self.assertGreater(shot['ev'],0)

    def test_settlement_preserves_frozen_evidence_and_scores_the_actual_price(self):
        rows,sources=fixture();frozen=freeze(rows,sources);before=deepcopy(frozen)
        snapshot=dict(decision_shadow=frozen,captured_at=AT,sha256='frozen-sha')
        settled=settle_v7(snapshot,dict(status='FT',home_ht=1,away_ht=1,home_ft=2,away_ft=1))
        self.assertEqual(frozen,before)
        for row in settled['movement_study']['cohorts']:
            self.assertEqual(row['win'],1);self.assertAlmostEqual(row['roi'],row['odd']-1)


class ProspectiveMetrics(unittest.TestCase):
    def test_absent_new_arm_is_unavailable_not_a_historical_empty_selection(self):
        rows,sources=fixture();frozen=freeze(rows,sources)
        del frozen['variants'][GG_FT_SAME_RULES]
        old=deepcopy(frozen)
        settled=settle_v7(dict(decision_shadow=frozen,captured_at=AT,sha256='sha'),
                         dict(status='FT',home_ht=1,away_ht=1,home_ft=2,away_ft=1))
        archive={'fixtures':{'1':{'decision_shadow':{'latest':settled}}}}
        metrics=next(iter(report([archive],datetime.fromisoformat('2026-11-23T00:00:00+00:00'),draws=5)['reports'].values()))
        arm=metrics['variants'][GG_FT_SAME_RULES]
        self.assertFalse(arm['available']);self.assertEqual(arm['available_fixtures'],0)
        self.assertIsNone(arm['coverage']);self.assertEqual(arm['missing_reason'],'variant_not_frozen')
        self.assertEqual(old,frozen)

    def test_recommended_role_is_reported_separately_from_the_other_slots(self):
        rows,sources=fixture();frozen=freeze(rows,sources)
        settled=settle_v7(dict(decision_shadow=frozen,captured_at=AT,sha256='sha'),
                         dict(status='FT',home_ht=1,away_ht=1,home_ft=2,away_ft=1))
        archive={'fixtures':{'1':{'decision_shadow':{'latest':settled}}}}
        metrics=next(iter(report([archive],datetime.fromisoformat('2026-11-23T00:00:00+00:00'),draws=5)['reports'].values()))
        arm=metrics['role_variants']['recommended_alternative'][GG_FT_SAME_RULES]
        self.assertTrue(arm['available']);self.assertEqual(arm['selected'],1)
        self.assertGreater(metrics['variants'][GG_FT_SAME_RULES]['selected'],arm['selected'])

    def test_metrics_cluster_roles_by_fixture_and_ignore_absent_historical_protocol(self):
        rows,sources=fixture();frozen=freeze(rows,sources)
        s=settle_v7(dict(decision_shadow=frozen,captured_at=AT,sha256='sha'),
                    dict(status='FT',home_ht=1,away_ht=1,home_ft=2,away_ft=1))
        archive={'fixtures':{str(i):{'observed_at':'2026-11-22T14:00:00Z',
                           'decision_shadow':{'latest':deepcopy(s)}} for i in range(3)}}
        archive['fixtures']['old']={'decision_shadow':{'latest':{'variants':{}}}}
        result=report([archive,archive],datetime.fromisoformat('2026-11-23T00:00:00+00:00'),draws=80)
        self.assertEqual(result['frozen_fixture_snapshots'],3)
        self.assertEqual(result['unavailable_fixtures'],1)
        metrics=next(iter(result['reports'].values()))
        base=metrics['variants']['published'];self.assertEqual(base['distinct_fixtures'],3)
        self.assertIsNotNone(base['brier']);self.assertIsNotNone(base['log_loss'])
        self.assertEqual(base['bootstrap']['distinct_fixtures'],3)
        self.assertEqual(set(base['bootstrap']['intervals']),{'brier','log_loss','hit_rate','roi'})
        self.assertFalse(result['automatic_promotion'])
        self.assertEqual(result,report([archive,archive],datetime.fromisoformat('2026-11-23T00:00:00+00:00'),draws=80))

if __name__=='__main__':unittest.main()
