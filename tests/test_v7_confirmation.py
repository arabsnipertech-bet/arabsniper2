"""Qualifications must explain a real premise, not the sign of an arrow."""
from copy import deepcopy
from pathlib import Path
import json,sys,unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from v7_confirmation import confirmation,oci_context
from v7_decision import market_origin
from opportunity_policy import opening_change,confirmed_flow
from matrix_quotes import preserve_confirmed_oci

AT='2026-11-22T09:00:00+00:00';START='2026-11-22T11:30:00+00:00'
FIRST='2026-11-22T07:00:00+00:00';LAST='2026-11-22T08:59:00+00:00'

def case(first=2.3,odd=2.5,p=.43,oci=0,low=.02):
    source=dict(odd=odd,first_odd=first,bookmaker_id='8',bookmaker='Book',quote_status='recent',
        quote_executable=True,first_observed_at=FIRST,quote_observed_at=LAST,provider_updated_at=LAST,
        selected_margin_pct=5.,selected_market_fair_pct=100/(odd*1.05),quality='supported',oci=oci)
    record=dict(odd=odd,calibrated=p,statistics=.45,ev=p*odd-1,ev_low=low,supported=True,
        tier='BLU',niche_id=None,model_fair_odd=1/p,fair=1/(odd*1.05),
        quote={k:source[k] for k in ['first_odd','bookmaker_id','first_observed_at','quote_observed_at','quote_status']},
        features=dict(margin_pct=5.,oci=oci,opening_delta_pct=100*(odd/first-1)),pricing_evidence={'basis':'binary'})
    return record,source

SPEC={'kind':'total','period':'ht','side':'over','line':1.5}

class Qualifications(unittest.TestCase):
    def test_value_can_survive_rising_price_without_inventing_flow(self):
        r,s=case();before=deepcopy((r,s))
        q=confirmation(SPEC,r,s,AT,START)
        self.assertEqual(q['label'],'Rialzo con supporto documentato')
        self.assertFalse(q['observation_only']);self.assertFalse(q['flow_confirmed'])
        self.assertEqual(q['oci_state'],'neutral');self.assertIn('OCI attuale 0',q['detail'])
        self.assertEqual((r,s),before)

    def test_study_is_not_promoted_by_a_rising_arrow(self):
        r,s=case();q=confirmation(SPEC,r,s,AT,START,pick={'study_only':True})
        self.assertEqual(q['label'],'Rialzo senza conferma');self.assertTrue(q['observation_only'])

    def test_user_union_example_does_not_turn_negative_ev_into_value(self):
        r,s=case(first=2.25,p=.377,low=-.064)
        q=confirmation(SPEC,r,s,AT,START)
        self.assertEqual(q['label'],'Rialzo senza conferma')
        self.assertFalse(q['value_qualified']);self.assertAlmostEqual(r['ev'],-.0575)

    def test_user_scafatese_example_retains_negative_prudent_ev(self):
        r,s=case(first=2.36,odd=2.49,p=.412,low=-.009)
        spec={'kind':'result','period':'ft','side':'1'}
        self.assertEqual(confirmation(spec,r,s,AT,START)['label'],'Rialzo senza conferma')
        self.assertEqual(r['ev_low'],-.009)

    def test_falling_price_requires_qualification(self):
        r,s=case(first=2.7)
        self.assertEqual(confirmation(SPEC,r,s,AT,START)['label'],'Calo con conferma statistica')
        r['supported']=False
        q=confirmation(SPEC,r,s,AT,START)
        self.assertEqual(q['label'],'Quota in calo, conferma insufficiente')
        self.assertTrue(q['observation_only'])

    def test_oci_states_and_historical_reading_are_distinct(self):
        historical={'value':35.,'observed_at':FIRST}
        for value,state in [(None,'missing'),(0.,'neutral'),(35.,'current')]:
            self.assertEqual(oci_context({'oci':value,'last_confirmed_oci':historical})['oci_state'],state)
        q=oci_context({'oci':35.,'historical_oci':True,'last_confirmed_oci':historical})
        self.assertIsNone(q['oci']);self.assertEqual(q['last_confirmed_oci'],historical)
        for value in [0,None]:
            self.assertFalse(confirmed_flow({'oci':value},minimum_oci=0))

    def test_neutral_current_oci_does_not_overwrite_the_last_confirmed_flow(self):
        previous={'value':35.,'observed_at':FIRST};history={'market':previous}
        current=dict(id='market',quote_status='recent',movement_confirmed=True,oci=0.,quote_observed_at=LAST)
        preserve_confirmed_oci(current,history,AT)
        self.assertEqual(current['last_confirmed_oci'],previous)
        self.assertEqual(history['market'],previous);self.assertEqual(current['oci'],0.)

    def test_specific_gg_study_requires_current_negative_oci_and_named_pattern(self):
        r,s=case(first=5.,odd=5.5,p=.23,oci=-60.)
        spec={'kind':'btts','period':'ht','side':'yes'}
        q=confirmation(spec,r,s,AT,START,pick={'study_only':True},patterns={'rebound_gg':True})
        self.assertEqual(q['label'],'Studio GG PT contrarian');self.assertTrue(q['observation_only'])
        value=confirmation(spec,r,s,AT,START,patterns={'rebound_gg':True})
        self.assertEqual(value['label'],'Rialzo con supporto documentato')
        self.assertFalse(value['contrarian_study'])
        s['historical_oci']=True
        self.assertEqual(confirmation(spec,r,s,AT,START,pick={'study_only':True},patterns={'rebound_gg':True})['label'],'Rialzo senza conferma')

    def test_documented_recovery_is_never_renamed_positive_ev(self):
        r,s=case(p=.39,low=-.035)
        r['ev_recovery']=dict(policy_id='EV_RECOVERY_AGGIO_V1',original_ev=r['ev'],original_ev_low=r['ev_low'],
            probability=.39,statistical_probability=.43,prudent_probability=.386,
            bookmaker_id='8',statistics_at=FIRST)
        q=confirmation(SPEC,r,s,AT,START)
        self.assertTrue(q['statistical_recovery']);self.assertFalse(q['value_qualified'])
        self.assertEqual(q['label'],'Rialzo · recupero statistico');self.assertLess(r['ev'],0)
        self.assertEqual(q['qualification_type'],'statistical_recovery')
        r['ev_recovery']['original_ev']=-.5
        self.assertTrue(confirmation(SPEC,r,s,AT,START)['observation_only'])

    def test_reasons_explain_failed_qualification_without_turning_missing_flow_into_a_gate(self):
        r,s=case(low=-.01)
        q=confirmation(SPEC,r,s,AT,START)
        self.assertEqual(q['qualification_type'],'observation')
        self.assertIn('ev_low_negative',q['qualification_reasons'])
        r,s=case()
        q=confirmation(SPEC,r,s,AT,START)
        self.assertEqual(q['qualification_type'],'documented_value')
        self.assertEqual(q['qualification_reasons'],[])
        self.assertFalse(q['flow_confirmed'])
        study=confirmation(SPEC,r,s,AT,START,pick={'study_only':True})
        self.assertEqual(study['qualification_type'],'study')
        self.assertEqual(study['qualification_reasons'],['study_only'])

    def test_temporal_and_operator_mismatch_do_not_invent_an_opening(self):
        r,s=case();r['quote']['bookmaker_id']='other'
        q=confirmation(SPEC,r,s,AT,START)
        self.assertIsNone(q['opening_delta_pct']);self.assertFalse(q['value_qualified'])
        r,s=case();r['quote']['first_observed_at']=LAST
        self.assertIsNone(confirmation(SPEC,r,s,AT,START)['opening_delta_pct'])

    def test_opening_guard_never_silently_uses_a_reversed_last_leg(self):
        row=dict(delta_basis='last_leg',opening_delta_pct=-8.,delta_pct=4.)
        self.assertEqual(opening_change(row),-8.)
        row['opening_delta_pct']=None
        self.assertIsNone(opening_change(row))
        self.assertEqual(opening_change({'delta_pct':-3.}),-3.)

    def test_legacy_origin_fallback_does_not_claim_trend_confirmation(self):
        r,s=case(p=.377,low=-.064);r['features']['opening_delta_pct']=11.1
        rates=dict(home_ht=.6,away_ht=.5,home_st=.8,away_st=.7,home_ft=1.4,away_ft=1.2)
        r['confirmation']=confirmation(SPEC,r,s,AT,START)
        origin=market_origin(SPEC,rates,r)
        self.assertEqual(origin['origin_badge'],'Rialzo senza conferma')
        self.assertNotIn('Trend Quota',origin['origin_tooltip'])

    def test_published_observations_and_radar_share_the_record_qualification(self):
        from test_v7 import assessment,row,AT as at,META
        from v7_decision import AWAY,capture
        a=assessment()
        away=row(json.loads(AWAY),4.5,.1,1/(4.5*1.05))
        away.update(first_odd=4.,first_observed_at='2026-11-09T21:30:00+01:00',oci=0)
        a['matrix']['rows'].append(away)
        frozen=capture(a,META,at,commitments={})
        observations=[p for p in frozen['triad'].values() if p and p.get('observation_only')]
        self.assertTrue(observations)
        for pick in observations:
            self.assertTrue(pick['study_only'])
            self.assertTrue(pick['motivation'].startswith('Osservazione · qualifica non confermata'))
            self.assertNotIn('Valore +EV',pick['origin_badge'])
        radar=frozen['signals']['radar_sorprese']
        self.assertIsNotNone(radar)
        original=frozen['markets'][radar['market_id']]
        for field in ('confirmation','origin_badge','origin_tooltip'):
            self.assertEqual(radar[field],original[field])

if __name__=='__main__':unittest.main()
