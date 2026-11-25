"""Prospective instrumentation must not influence a published V7 decision."""
from copy import deepcopy
from datetime import date, datetime
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]/'src'), str(Path(__file__).resolve().parent)]
from test_v7 import assessment, row
from lateral_matrix import market_id, payout_weights, specification
from matrix_quotes import equivalent_event, equivalent_prices
from official_selections import archived_choice, trace_snapshot
from v7_archive import checked_score, results_for_day, score_periods, valid_snapshot, WEIGHTS, WEIGHT_BENCHMARKS
from v7_decision import capture, fingerprint, GG_FT, RESULTS
from v7_movement_shadow import evidence_state
from v7_probability import BASE_WEIGHT, MEASUREMENT_PROTOCOL
from v7_shadow_metrics import paired_metrics, research_report

AT='2026-11-25T09:00:00+00:00'
START='2026-11-25T21:45:00+00:00'


def inputs():
    a = assessment()
    a['statistics_as_of'] = AT
    a['matrix']['rows'] += [row(json.loads(GG_FT), 2.1, .5),
                            row(json.loads(RESULTS['2']), 4., .25),
                            row(specification('handicap', 'ft', 'away', -.5), 4.1, .25)]
    for source in a['matrix']['rows']:
        source.update(quote_observed_at=AT, provider_updated_at=AT,
                      first_observed_at='2026-11-25T08:00:00+00:00',
                      quote_adapter_version=2,
                      bet_id=4 if source['spec']['kind']=='handicap' else 1 if source['spec']['kind']=='result' else 8,
                      paired_books=0, agreeing_books=0, movement_confirmed=False,
                      fair_direction_coherent=False, persistence=0., selected_movement={'reversal':False})
    return a, dict(fixture_id=321, kickoff=START, status_short='NS')


def archive(snapshot, meta):
    store = {'analysis_updates': {'new': dict(publication_snapshot=True, fixture=meta,
        captured_at=AT, multimarket={'matrix': {'v7':snapshot}},
        settlement={'result': dict(status='PEN', home_ft=1, away_ft=1, home_ht=1, away_ht=0,
                                  extra_time={'home':2,'away':2}, penalties={'home':5,'away':4})})}}
    return results_for_day(store, date(2026,11,25), datetime.fromisoformat('2026-11-26T03:00:00+00:00'))


class ResearchTrace(unittest.TestCase):
    def test_sealing_preserves_all_original_probabilities_prices_gates_and_roles(self):
        a, meta = inputs()
        with patch('official_selections.trace_snapshot'):
            control = capture(deepcopy(a), meta, AT, commitments={})
        before = deepcopy(a)
        with patch('urllib.request.urlopen', side_effect=AssertionError('no network')):
            measured = capture(a, meta, AT, commitments={})
        for field in ('rates','raw_rates','statistics','market_projection','weight','markets','triad','signals','tier','badge'):
            self.assertEqual(control[field], measured[field], field)
        self.assertEqual(BASE_WEIGHT,.8)
        self.assertTrue(valid_snapshot(measured,meta,AT))
        trace = measured['measurement']
        self.assertEqual(trace['protocol'],MEASUREMENT_PROTOCOL)
        self.assertEqual(trace['config_sha256'],fingerprint(trace['config']))
        self.assertEqual(trace['config']['control']['weight'],measured['weight'])
        self.assertEqual(trace['prematch_evidence_sha256'],fingerprint({k:measured.get(k) for k in trace['evidence_fields']}))
        self.assertEqual(control['decision_shadow']['variants'],measured['decision_shadow']['variants'])
        # Pricing is the pre-existing mutation; measurement adds no change to sources.
        self.assertEqual(before['rates'],a['rates'])
        for slot in measured['triad_commitments'].values():
            self.assertEqual(slot['probability'],measured['markets'][slot['market_id']]['calibrated'])
            self.assertEqual((slot['fixture_id'],slot['kickoff'],slot['captured_at']),('321',START,AT))
            self.assertEqual(archived_choice(slot,measured)['forecast_sha256'],measured['sha256'])

    def test_old_fixed_probability_is_never_filled_using_new_forecast(self):
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={})
        old=dict(market_id=GG_FT,odd=2.1,captured_at='2026-11-24T12:00:00Z')
        snapshot['triad_commitments']={'recommended_alternative':deepcopy(old)}
        trace_snapshot(snapshot,{r['id']:r for r in a['matrix']['rows']},a['profiles'])
        self.assertEqual(snapshot['triad_commitments']['recommended_alternative'],old)
        self.assertNotIn('forecast_sha256',archived_choice(old,snapshot))

    def test_source_snapshot_hash_carries_forward_without_mutating_frozen_input(self):
        from v7_decision import prior_triad_commitments
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={});frozen=deepcopy(snapshot)
        a['matrix']['v7']=snapshot
        slots=prior_triad_commitments(a,meta,AT)
        self.assertEqual(snapshot,frozen)
        self.assertTrue(slots)
        self.assertTrue(all(slot['forecast_sha256']==snapshot['sha256'] for slot in slots.values()))
        info=snapshot['measurement']['markets'][GG_FT]
        self.assertEqual(info['flow_states']['reversal'],'no_reversal')

    def test_raw_native_final_origins_and_weak_side_do_not_mix(self):
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={})
        snapshot['rates'].update(home_ft=.9,away_ft=2.)
        snapshot['statistics'].update(home_ft=1.8,away_ft=1.5)
        snapshot['markets'][RESULTS['2']].update(raw=.3,calibrated=.2)
        sources={r['id']:r for r in a['matrix']['rows']}
        before=deepcopy(snapshot['triad'])
        trace_snapshot(snapshot,sources,a['profiles'])
        trace=snapshot['measurement'];markets=trace['markets']
        self.assertTrue(markets[GG_FT]['observers']['gg_ft_weak_side']['matched'])
        self.assertEqual(markets[GG_FT]['observers']['gg_ft_weak_side']['native_lambda_ft'],[1.8,1.5])
        self.assertTrue(markets[RESULTS['2']]['observers']['away_raw_market_gap']['matched'])
        self.assertEqual(trace['probability_origins']['raw_poisson']['rates'],snapshot['raw_rates'])
        self.assertEqual(snapshot['triad'],before)
        snapshot['statistics_at']='2026-11-25T10:00:00Z'
        trace_snapshot(snapshot,sources,a['profiles'])
        self.assertIsNone(snapshot['measurement']['markets'][GG_FT]['observers']['gg_ft_weak_side']['matched'])

    def test_missing_zero_failed_and_st_sample_are_distinct(self):
        self.assertEqual([evidence_state(v) for v in (None,0,False,True)],['missing','neutral','failed','passed'])
        a,meta=inputs();source=a['matrix']['rows'][0]
        a['observed_goal_means']={'captured_at':AT,'teams':{'home':{
            'ht':dict(sample=0,scored=0,conceded=0),
            'st':dict(sample=4,scored=1.2,conceded=.8)}}}
        source.pop('movement_confirmed');source['selected_movement'].pop('reversal')
        sh=row(specification('total','sh','over',1.5));sh.update(quote_observed_at=AT,provider_updated_at=AT)
        a['matrix']['rows'].append(sh)
        snap=capture(a,meta,AT,commitments={});info=snap['measurement']['markets'][source['id']]
        self.assertEqual(info['flow_states']['paired_books'],'neutral')
        self.assertEqual(info['flow_states']['fair_direction_coherent'],'failed')
        self.assertIn('movement_confirmed',info['missing_flow_fields'])
        self.assertIn('reversal',info['missing_flow_fields'])
        self.assertEqual(info['observed_goal_samples']['home'],{'ht':0,'st':4})
        self.assertEqual(snap['measurement']['markets'][sh['id']]['without_peak_state'],'missing')

    def test_gg_ft_weight_grid_is_prospective_and_cannot_enter_learning(self):
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={})
        self.assertIn(GG_FT,WEIGHT_BENCHMARKS)
        data=archive(snapshot,meta)
        gg=next(r for r in data['samples'] if r['market']==GG_FT)
        self.assertEqual(set(gg['variants']),{str(w) for w in WEIGHTS})
        self.assertFalse(gg['benchmark']);self.assertTrue(gg['audit_weight_benchmark'])
        self.assertEqual(gg['statistics'],snapshot['markets'][GG_FT]['statistics'])
        original=deepcopy(snapshot);original.pop('measurement');original['sha256']=fingerprint({k:v for k,v in original.items() if k!='sha256'})
        legacy=next(r for r in archive(original,meta)['samples'] if r['market']==GG_FT)
        self.assertEqual(legacy['variants'],{});self.assertNotIn('audit',legacy)
        self.assertEqual(data['fixtures']['321']['score_periods']['regulation_90']['home'],1)

    def test_report_reads_future_ledgers_and_missing_history_is_not_backfilled(self):
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={});data=archive(snapshot,meta)
        before=deepcopy(data);cutoff=datetime.fromisoformat('2026-11-26T04:00:00+00:00')
        report=research_report([data],cutoff,draws=30)
        self.assertEqual(len(report['reports']),1);self.assertEqual(data,before)
        self.assertEqual(len(next(iter(report['reports'].values()))['audit_weight_grid'][GG_FT]),len(WEIGHTS))
        reduced={'fixtures':{'321':{k:v for k,v in data['fixtures']['321'].items() if k in ('decision_shadow','observed_at')}}}
        self.assertEqual(len(research_report([reduced],cutoff,draws=30)['reports']),1)
        data['fixtures']['321'].pop('measurement');data['fixtures']['321'].pop('decision_shadow')
        self.assertEqual(research_report([data],cutoff,draws=30)['reports'],{})

    def test_control_configurations_and_conflicting_kickoffs_are_not_pooled(self):
        a,meta=inputs();snapshot=capture(a,meta,AT,commitments={});first=archive(snapshot,meta)
        a,meta=inputs();other=capture(a,meta,AT,commitments={})
        other['calibration']='separate_audit_config'
        trace_snapshot(other,{r['id']:r for r in a['matrix']['rows']},a['profiles'])
        self.assertNotEqual(other['measurement']['config_sha256'],snapshot['measurement']['config_sha256'])
        second=deepcopy(first);second['fixtures']['321']['measurement']=other['measurement']
        cutoff=datetime.fromisoformat('2026-11-26T04:00:00+00:00')
        self.assertEqual(len(research_report([first,second],cutoff,draws=20)['reports']),2)
        second['fixtures']['321']['kickoff']='2026-11-25T22:00:00Z'
        report=research_report([first,second],cutoff,draws=20)
        self.assertEqual(report['reports'],{});self.assertEqual(report['kickoff_conflict_fixtures'],['321'])


class SettlementAndEquivalence(unittest.TestCase):
    def test_regulation_score_never_uses_aet_penalties_or_live_goals(self):
        from lateral_matrix import settle_spec
        spec=specification('result','ft','1')
        for status in ('FT','AET','PEN'):
            result=dict(status=status,home_ft=1,away_ft=1,home_ht=0,away_ht=0,
                        extra_time={'home':2,'away':1},penalties={'home':5,'away':3})
            self.assertEqual(settle_spec(spec,checked_score(result)),(0.,1.))
            self.assertEqual(score_periods(result)['regulation_90']['home'],1)
        missing=dict(status='PEN',goals={'home':2,'away':1},extra_time={'home':2,'away':1},penalties={'home':5,'away':3})
        self.assertIsNone(settle_spec(spec,checked_score(missing)))
        self.assertFalse(score_periods(missing)['regulation_90']['available'])
        self.assertIsNone(checked_score(dict(missing,home_ft=2,away_ft=1,ft_period='extra_time'))['home_ft'])

    def test_full_payout_identity_including_draws_and_large_scores(self):
        for side,team in [('1','home'),('2','away')]:
            result=specification('result','ft',side);ah=specification('handicap','ft',team,-.5)
            self.assertEqual(equivalent_event(result),equivalent_event(ah))
            for home in (*range(8),20,100):
                for away in (*range(8),20,100):
                    self.assertEqual(payout_weights(result,home,away),payout_weights(ah,home,away))
        for line in (0,-1,1,.5,-.25,-.75):
            self.assertIsNone(equivalent_event(specification('handicap','ft','home',line)))
        self.assertIsNone(equivalent_event(specification('dnb','ft','1')))
        self.assertIsNone(equivalent_event(specification('handicap','ht','home',-.5)))

    def test_price_comparison_requires_same_book_time_probability_and_real_market(self):
        a,meta=inputs();snap=capture(a,meta,AT,commitments={})
        sources={r['id']:r for r in a['matrix']['rows']};before=deepcopy(snap['triad'])
        comparisons=equivalent_prices(snap['markets'],sources,AT,START)
        self.assertEqual(len(comparisons),1);proof=comparisons[0]
        self.assertEqual(proof['probability_difference'],0)
        self.assertEqual(proof['alternative']['provider_market'],'Asian Handicap')
        self.assertIn('-0.5',proof['alternative']['label'])
        self.assertGreater(proof['ev_gain'],0);self.assertFalse(proof['applied_to_official'])
        self.assertEqual(snap['triad'],before)
        ah=next(k for k,v in sources.items() if v['spec']['kind']=='handicap')
        for field,value in [('bookmaker_id','different'),('provider_updated_at','2026-11-25T08:58:59Z'),('bet_id',2)]:
            bad=deepcopy(sources);bad[ah][field]=value
            self.assertEqual(equivalent_prices(snap['markets'],bad,AT,START),[])
        rows=deepcopy(snap['markets']);rows[ah]['calibrated']+=1e-8
        self.assertEqual(equivalent_prices(rows,sources,AT,START),[])

    def test_paired_metrics_cluster_by_fixture_and_day(self):
        rows=[dict(fixture_id=str(i),date='2026-11-'+str(25+i//2),probability=.6,fair=.5,outcome=i%2)
              for i in range(4)]
        metrics=paired_metrics(rows,draws=40)
        self.assertEqual(metrics['n'],4)
        self.assertAlmostEqual(metrics['brier_delta'],.01)
        self.assertEqual(metrics['bootstrap']['clusters']['fixture_id']['clusters'],4)
        self.assertEqual(metrics['bootstrap']['clusters']['date']['clusters'],2)
        rows[0]['fair']=None
        self.assertEqual(paired_metrics(rows,draws=20)['n'],3)

    def test_goal_partition_keeps_both_sides_at_both_times(self):
        from test_quote_sources import feed, bet, NOW, START as QUOTE_START
        from matrix_quotes import collect, ledger_offers, analyse_prices
        state={};spec=specification('total','ht','over',1.5)
        source=row(spec)
        source.update(win_weight=.45,loss_weight=.55,sensitivity_weights=[[.45,.55]])
        for at,q in [(NOW,2.2),('2026-10-26T18:20:00+00:00',2.3)]:
            groups,_=collect(feed([bet(6,1.5,q,1.65)],at=at),100,at)
            offers=ledger_offers(state,100,groups,at,QUOTE_START)
            analyse_prices(source,offers[market_id(spec)],at,QUOTE_START)
        partition=source['audit_partitions']
        self.assertEqual(partition['opening']['odds'],{'over':2.2,'under':1.65})
        self.assertEqual(partition['current']['odds'],{'over':2.3,'under':1.65})
        self.assertEqual(partition['opening']['provider_updated_at'],NOW)
        for point in (partition['opening'],partition['current']):
            self.assertAlmostEqual(sum(point['fair'].values()),1.)
            self.assertAlmostEqual(point['margin_pct'],100*(sum(1/q for q in point['odds'].values())-1))


if __name__=='__main__': unittest.main()
