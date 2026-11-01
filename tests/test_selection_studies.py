import copy
import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from selection_studies import classify, build_studies


def row():
    return dict(statistical_probability=.60,book_probability_pct=55,odd=1.7,
                price_assessment={'reference_probability_pct':55},market='over25_ft',selection='over',
                primary=True,period='prospective',snapshot='first',model_id='context-goals-v3.4',
                fixture_id='1',fixture={'country':'Japan','league_id':98},outcome=True)


class StudiesTests(unittest.TestCase):
    def test_membership_ignores_result(self):
        r=row();a=classify(r);r['outcome']=False
        self.assertEqual(a,classify(r))
        self.assertTrue(a['agreement_below_break_even'])
        self.assertFalse(a['agreement_above_break_even'])

    def test_thresholds_and_missing(self):
        r=row();r['statistical_probability']=.5
        self.assertFalse(classify(r)['both_over_50'])
        r['statistical_probability']=.65
        self.assertTrue(classify(r)['agreement_10pp'])
        r['statistical_probability']=.651
        self.assertFalse(classify(r)['agreement_10pp'])
        r['odd']=None
        self.assertIsNone(classify(r)['agreement_below_break_even'])
        r['book_probability_pct']=None
        self.assertIsNone(classify(r))

    def test_phase_pending_matched_and_no_mutation(self):
        r=row();pending=copy.deepcopy(r);pending.update(fixture_id='2',outcome=None)
        report={'audit_date':'2026-10-21','markets':[r,pending]};original=copy.deepcopy(report)
        a=build_studies(report)
        self.assertEqual(report,original)
        g=next(g for g in a['groups'] if g['scope']=='primary_only' and g['country']=='*' and g['rule']=='agreement_10pp')
        self.assertEqual((g['fixtures'],g['pending'],g['statistics']['n'],g['book']['n']),(2,1,1,1))
        self.assertEqual(g['phase'],'exploratory')
        report['audit_date']='2026-10-22'
        self.assertTrue(all(g['phase']=='validation' for g in build_studies(report)['groups']))

    def test_missing_data_not_a_rejection(self):
        r=row();r['book_probability_pct']=None
        a=build_studies({'audit_date':'2026-10-22','markets':[r]})
        self.assertTrue(all(g['membership']=='missing_data' and g['statistics']['n']==0 for g in a['groups']))


def perth_fixture():
    """Real fixture/kickoff; controlled prices, OCI and scores, not a replay.
    Fast 41283380497 / job 124779681842 logged Perth–Adelaide at 07:08:59Z.
    """
    from lateral_matrix import specification, label, market_id
    from selection_studies import generate_ideas
    at, kickoff = '2026-11-01T07:08:59Z', '2026-11-01T08:00:00Z'
    def observed(spec, probability, price, blockers):
        return dict(id=market_id(spec), spec=spec, label=label(spec), statistical_pct=probability,
                    odd=price, market_fair_pct=42, oci=40, delta_pct=-3, bookmaker_id='9', quote_status='recent',
                    quote_executable=True, provider_updated_at=at, quote_observed_at=at,
                    opportunity=dict(eligible=False, reasons=blockers, odds_min=1.70, odds_max=2.6))
    rows = [observed(specification('result','ft','1'), 45, 2.4, ['market']),
            observed(specification('total','ft','over',2.5), 100/1.84, 1.69, ['yield_floor']),
            observed(specification('total','ht','over',1.5), 31, 2.5, ['history','edge'])]
    m = dict(fixture=dict(home='Perth Glory', away='Adelaide United', fixture_id='perth-test',
                          kickoff=kickoff, status_short='NS'),
             multimarket=dict(rates=dict(home_ft=1.6,away_ft=1.2,home_ht=.6,away_ht=.5),
                              matrix=dict(rows=rows)))
    # Direct controlled bookmaker offers, never products of marginal odds.
    m['multimarket']['matrix']['study_combo_quotes'] = {
        market_id(specification(kind,'ft',side,2.5,'over')): dict(odd=3.2, bookmaker_id='9', updated=at)
        for kind,side in [('result_total','1'),('dc_total','1x'),('result_total','2'),('dc_total','x2')]}
    m['multimarket']['matrix']['quote_monitoring'] = dict(as_of=at, studio_ideas=generate_ideas(m['multimarket'], at, kickoff))
    return m


class PerthStudyTests(unittest.TestCase):
    def test_freeze_journal_and_settle_without_official_picks(self):
        from tempfile import TemporaryDirectory
        from audit_journal import capture_policies, ingest
        from audit_storage import read_store, write_store
        from prematch_audit import settle_predictions, build_report
        from selection_studies import freeze_study
        m = perth_fixture()
        at = '2026-11-01T07:18:00Z'
        with TemporaryDirectory() as temp:
            root = Path(temp)
            write_store(root/'data/arab_quote_day1.json', {'matches':[m]})
            for when in (at, '2026-11-01T07:20:00Z'):
                journal = read_store(capture_policies(root, when))
                for category in ('gold', 'platinum'):
                    self.assertFalse(journal[category]['fixtures'])
            self.assertIsNone(freeze_study(m, m['fixture']['kickoff']))
            ingest(root)
            store = read_store(root/'data/arab_quote_prematch_audit.json')
        before = build_report(store)['studio_ideas_audit']
        self.assertTrue(all(r['outcome']=='pending' for r in before['selections']))
        self.assertEqual(len(before['selections']), 6)
        for home, away, expected in [(2,1,(1,1)),(2,2,(0,1)),(1,2,(0,0)),(1,0,(0,0))]:
            result=dict(status='PEN',home_ht=None,away_ht=None,home_ft=home,away_ft=away)
            settle_predictions(store, {'days':[{'matches':[{'fixture':dict(m['fixture'],result=result)}]}]})
            report = build_report(store)
            self.assertFalse(report['groups'])
            study = report['studio_ideas_audit']
            combos = {r['market']:r for r in study['selections'] if r['type']=='studio_combo_1x2_over25'}
            self.assertEqual(tuple(combos[k]['y'] for k in ('1_over25_ft','1x_over25_ft')), expected)
            self.assertAlmostEqual(study['by_type']['studio_combo_1x2_over25']['roi'], 3.2*sum(expected)/2-1)
            self.assertEqual(study['by_type']['studio_ht_flow_forcing']['pending'],1)
            self.assertNotIn('history',study['by_blocking_reason'])
            self.assertAlmostEqual(study['by_blocking_reason']['yield_floor']['roi'], .69 if home+away>2 else -1.)

    def test_pure_poisson_last_flow_and_flat_or_official_suppression(self):
        from selection_studies import generate_ideas
        m=perth_fixture()
        a=m['multimarket']; matrix=a['matrix']; at=matrix['quote_monitoring']['as_of']
        def ideas():
            return generate_ideas(a, at, m['fixture']['kickoff'])
        for official in ('opportunity_policy','platinum_policy'):
            matrix[official]={'best_market_id':'official'}
            self.assertEqual(ideas(),[])
            del matrix[official]
        ht=matrix['rows'][2]
        ht['study_flow']=dict(oci=35,quote_observed_at=at,provider_updated_at=at)
        ht.update(oci=None,statistical_pct=65,calibration_base=dict(statistical_pct=31))
        forcing=next(i for i in ideas() if i['type']=='studio_ht_flow_forcing')
        self.assertTrue(forcing['historical_oci'])
        self.assertEqual(forcing['poisson_pct'],31)
        for row in matrix['rows']:
            row.update(oci=0,odd=1.1, delta_pct=0, fair_delta_pp=0)
        self.assertEqual(ideas(),[])

    def test_direct_combo_price_is_the_exact_market_only(self):
        from matrix_quotes import study_combo_quotes
        from lateral_matrix import market_id, specification
        m=perth_fixture(); meta=m['fixture']; at=m['multimarket']['matrix']['quote_monitoring']['as_of']
        data={'response':[{'update':at,'fixture':{'id':meta['fixture_id']},'bookmakers':[{'id':9,'bets':[
            {'id':25,'name':'Result/Total Goals','values':[{'value':'Home/Over 2.5','odd':'3.2'}]},
            {'id':183,'name':'Double Chance/Total','values':[{'value':'1X/Over 2.5','odd':'2.4'}]},
            {'id':25,'name':'Corners','values':[{'value':'Home/Over 2.5','odd':'55'}]}]}]}]}
        offers=study_combo_quotes(data, meta, at)
        self.assertEqual(offers[market_id(specification('result_total','ft','1',2.5,'over'))]['odd'],3.2)
        self.assertEqual(offers[market_id(specification('dc_total','ft','1x',2.5,'over'))]['odd'],2.4)

    def test_away_combo_probabilities_and_settlement(self):
        from lateral_matrix import settle_spec, poisson
        from selection_studies import generate_ideas
        m=perth_fixture()
        a=m['multimarket']; at=a['matrix']['quote_monitoring']['as_of']
        a['matrix']['rows'][0]['spec']['side']='2'
        ideas=generate_ideas(a,at,m['fixture']['kickoff'])
        combos={i['market']:i for i in ideas if i['type']=='studio_combo_1x2_over25'}
        for key,select in [('2_over25_ft', lambda h,g:g>h),('x2_over25_ft',lambda h,g:g>=h)]:
            idea=combos[key]
            p=sum(x*y for h,x in enumerate(poisson(1.6)) for g,y in enumerate(poisson(1.2))
                  if h+g>=3 and select(h,g))
            self.assertAlmostEqual(idea['poisson_pct'],100*p)
            self.assertAlmostEqual(idea['fair_odd'],1/p)
            self.assertEqual(settle_spec(idea['spec'],dict(status='AET',home_ft=2,away_ft=2))[0],
                             int(select(2,2)))

    def test_highest_convergence_wins_independently_of_row_order(self):
        from selection_studies import rank_study_ideas
        base = dict(type='goal', market='over25_ft', odd=2., poisson_pct=58, oci=40,
                    paired_books=5, agreeing_books=4, poisson_low_pct=52)
        higher_price = dict(base, market='over35_ft', odd=2.5, historical_oci=True,
                            agreeing_books=1, poisson_low_pct=35)
        unpriced = dict(base, market='combo', odd=None)
        for items in ([base,higher_price,unpriced],[unpriced,higher_price,base]):
            ranked = rank_study_ideas(copy.deepcopy(items))
            self.assertEqual([i['market'] for i in ranked if i.get('best_study')], ['over25_ft'])
            self.assertEqual(len(ranked[0]['study_signals']), 4)
        self.assertFalse(rank_study_ideas([unpriced])[0].get('best_study'))
        self.assertEqual(rank_study_ideas([]), [])

    def test_como_venezia_outsider_cannot_take_star_even_with_unanimous_flow(self):
        from selection_studies import rank_study_ideas
        outsider = dict(type='studio_1x2_value_flow', market='1x2_2', odd=9.50, poisson_pct=16,
                        oci=95, delta_pct=-8, fair_delta_pp=4, paired_books=8, agreeing_books=8, best_study=True)
        favorite = dict(type='studio_1x2_value_flow', market='1x2_1', odd=1.35, poisson_pct=75)
        over = dict(type='studio_goal_flow_near_miss', market='over25_ft', odd=1.9, poisson_pct=58)
        for solid in (favorite, over):
            ranked = rank_study_ideas([outsider, solid])
            self.assertEqual([i['market'] for i in ranked if i.get('best_study')], [solid['market']])
            self.assertIn(outsider, ranked)
        for p in (None, float('nan'), 31.999, 32, 39.99, 40):
            item = dict(outsider, poisson_pct=p, best_study=True)
            rank_study_ideas([item])
            self.assertEqual(bool(item.get('best_study')), p in (32, 39.99, 40))
            self.assertEqual('strong_oci' in item['study_signals'], p == 40)
            self.assertEqual('falling_price' in item['study_signals'], p == 40)

    def test_price_penalty_only_waived_for_unanimous_current_flow(self):
        from selection_studies import rank_study_ideas
        long = dict(type='goal', market='long', odd=4.01, poisson_pct=55, oci=60,
                    paired_books=5, agreeing_books=5, delta_pct=-2, fair_delta_pp=1.5)
        short = dict(type='goal', market='short', odd=2, poisson_pct=50)
        for change, winner in [({}, 'long'), ({'agreeing_books':4}, 'short'),
                              ({'historical_oci':True}, 'short'), ({'delta_pct':0}, 'short'),
                              ({'paired_books':2,'agreeing_books':2}, 'short'),
                              ({'fair_delta_pp':0}, 'short'), ({'oci':59}, 'short'),
                              ({'odd':4,'oci':0}, 'long')]:
            ranked = rank_study_ideas([dict(long, **change), short.copy()])
            self.assertEqual(ranked[0]['market'], winner, change)


def distortion_fixture(name):
    """User-reported prices/probabilities; controlled supporting rates, not a historical replay."""
    from lateral_matrix import specification, label, market_id
    from selection_studies import generate_ideas
    m = perth_fixture()
    a=m['multimarket']; matrix=a['matrix']; at=matrix['quote_monitoring']['as_of']
    matrix['study_combo_quotes'] = {}
    if name == 'santander':
        m['fixture'].update(home='Racing Santander', away='Real Madrid')
        prices, probabilities, changes = [12.,6.,1.26], [33.3,22.3,44.4], [-3.,0.,-2.]
        o25, ht, o35 = 1.22, 1.74, 2.1
        a['rates'].update(home_ft=1.52,away_ft=2.2,home_ht=.65,away_ht=1.)
    else:
        m['fixture'].update(home='IF Elfsborg', away='Orgryte IS')
        prices, probabilities, changes = [1.4,5.2,7.5], [52.2,23.4,24.4], [100*(1.4/1.36-1),6.1,0.]
        o25, ht, o35 = 1.57, 2.2, 2.49
        a['rates'].update(home_ft=2.1,away_ft=1.2,home_ht=.9,away_ht=.5)
    def row(spec, p, odd, delta=0):
        return dict(spec=spec,id=market_id(spec),label=label(spec),statistical_pct=p,
                    odd=odd,oci=60,delta_pct=delta,quote_status='recent',quote_executable=True,
                    provider_updated_at=at,quote_observed_at=at,bookmaker_id='9',
                    opportunity=dict(eligible=False,reasons=['market']))
    matrix['rows'] = [row(specification('result','ft',side),p,odd,d)
                      for side,p,odd,d in zip(('1','x','2'),probabilities,prices,changes)]
    matrix['rows'] += [row(specification('total','ft','over',2.5),75,o25),
                       row(specification('total','ht','over',1.5),40,ht),
                       row(specification('total','ft','over',3.5),45,o35)]
    suppressed=[]
    ideas=generate_ideas(a,at,m['fixture']['kickoff'],suppressed)
    matrix['quote_monitoring']=dict(as_of=at,studio_ideas=ideas,suppressed_study_candidates=suppressed)
    return m


class DistortionTests(unittest.TestCase):
    def test_santander_and_elfsborg(self):
        for name, forbidden in [('santander', {'1x2_1','1_over25_ft','1x_over25_ft'}),
                                 ('elfsborg', {'1x2_x','1x2_2','2_over25_ft','x2_over25_ft'})]:
            m=distortion_fixture(name)
            monitor=m['multimarket']['matrix']['quote_monitoring']
            visible={i['market'] for i in monitor['studio_ideas']}
            self.assertFalse(visible & forbidden)
            self.assertTrue({'gg_ft','over15_ht','over35_ft'} <= visible)
            self.assertFalse(any(i['type']=='studio_combo_1x2_over25' for i in monitor['studio_ideas']))
            hidden={i['market'] for i in monitor['suppressed_study_candidates']}
            self.assertTrue(forbidden <= hidden)
            for i in monitor['studio_ideas']:
                if i['market'].startswith('1x2_') and i.get('real_flow'):
                    self.assertTrue(i['delta_pct'] <= -2 or (i.get('fair_delta_pp') or 0) >= 1.5)

    def test_gate_boundaries_and_rebound(self):
        from selection_studies import result_gate
        base=dict(market='1x2_1',odd=3.6,poisson_pct=32,market_fair_pct=30,delta_pct=-3)
        self.assertEqual(result_gate(base), [])
        for changes, reason in [({'poisson_pct':31.99},'probability_floor'),
                                ({'odd':3.61},'probability_floor'),
                                ({'market':'1x2_x'},'probability_floor'),
                                ({'odd':4.51,'poisson_pct':40},'outsider_price_cap'),
                                ({'market_fair_pct':17.99},'model_market_divergence'),
                                ({'poisson_pct':49,'delta_pct':-2.99},'model_market_divergence'),
                                ({'market':'1x2_x','poisson_pct':35,'delta_pct':0},'oci_rebound')]:
            self.assertIn(reason,result_gate(dict(base,**changes)))
        self.assertEqual(result_gate(dict(base,odd=4.5,poisson_pct=35)), [])
        self.assertEqual(result_gate(dict(base,poisson_pct=49)), [])
        self.assertEqual(result_gate(dict(base,poisson_pct=48,delta_pct=0)), [])

    def test_suppression_survives_freeze_settlement_and_calibration(self):
        from selection_studies import freeze_study, StudyEvidence, study_audit
        from adaptive_calibration import Evidence, fit
        from datetime import date
        m=distortion_fixture('santander')
        record=freeze_study(m,'2026-11-01T07:12:00Z')
        self.assertTrue(record['suppressed_study_candidates'])
        record['settlement']={'result':dict(status='FT',home_ft=1,away_ft=3,home_ht=0,away_ht=2)}
        report=study_audit({'study_observations':{'1':record,'duplicate':record}})
        self.assertEqual(len(report['suppressed_study_candidates']),len(record['suppressed_study_candidates']))
        trap=next(i for i in report['suppressed_study_candidates'] if i['market']=='1x2_1')
        self.assertEqual(trap['outcome'],'lost')
        self.assertEqual(trap['poisson_pct'],33.3)
        e=Evidence(); e.observe(record)
        state=fit(e.samples(),date(2026,11,2))
        self.assertFalse(state['cohorts'])
        self.assertTrue(state['contrarian_poisson_trap'])
        self.assertTrue(all(not r['promotion_candidate'] for r in state['contrarian_poisson_trap'].values()))
        self.assertNotIn('studio_1x2_value_flow', state['studio_cohorts'])

    def test_only_favorite_quoted_combos_and_blended_bounds(self):
        from selection_studies import generate_ideas
        from lateral_matrix import market_id,specification
        m=distortion_fixture('santander'); a=m['multimarket']; at=a['matrix']['quote_monitoring']['as_of']
        for side in ('1','2'):
            spec=specification('result_total','ft',side,2.5,'over')
            a['matrix']['study_combo_quotes'][market_id(spec)]=dict(odd=2.,bookmaker_id='9',updated=at)
        ideas=generate_ideas(a,at,m['fixture']['kickoff'])
        combos=[i for i in ideas if i['type']=='studio_combo_1x2_over25']
        self.assertEqual([i['market'] for i in combos],['2_over25_ft'])
        self.assertTrue(0 < combos[0]['blended_pct'] < 100)
        self.assertNotEqual(combos[0]['blended_pct'],combos[0]['poisson_pct'])


    def test_unpriced_fallback_requires_blended_probability_and_no_alternatives(self):
        from selection_studies import generate_ideas
        m=distortion_fixture('santander'); a=m['multimarket']; matrix=a['matrix']
        a['rates'].update(home_ft=.9,away_ft=3.,home_ht=.3,away_ht=1.1)
        matrix['rows']=matrix['rows'][:4]
        for row in matrix['rows']:
            row.update(oci=30,delta_pct=0)
        matrix['rows'][3]['odd']=None
        def generate():
            return generate_ideas(a,matrix['quote_monitoring']['as_of'],m['fixture']['kickoff'])
        ideas=generate()
        self.assertEqual(len(ideas),1)
        self.assertEqual(ideas[0]['market'],'x2_over25_ft')
        self.assertGreater(ideas[0]['blended_pct'],48)
        matrix['rows'][3]['oci']=35
        self.assertFalse(any(i['type']=='studio_combo_1x2_over25' for i in generate()))

    def test_xg_can_support_goal_context_but_shots_cannot(self):
        from selection_studies import generate_ideas
        m=distortion_fixture('santander'); a=m['multimarket']; matrix=a['matrix']
        a['rates']['home_ft']=.8
        for row in matrix['rows']:
            row['oci']=0
        a['profiles']={'home':{'for_ft':{'xg_mean':1.15}}}
        ideas=generate_ideas(a,matrix['quote_monitoring']['as_of'],m['fixture']['kickoff'])
        self.assertIn('over15_ht',{i['market'] for i in ideas})
        a['profiles']['home']['for_ft']={'shots_mean':5}
        ideas=generate_ideas(a,matrix['quote_monitoring']['as_of'],m['fixture']['kickoff'])
        self.assertNotIn('over15_ht',{i['market'] for i in ideas})

    def test_fair_delta_cannot_label_flat_or_rising_price_as_flow(self):
        from selection_studies import generate_ideas
        m=perth_fixture();a=m['multimarket'];matrix=a['matrix'];at=matrix['quote_monitoring']['as_of']
        result=matrix['rows'][0]
        for delta,expected in [(0,False),(6.1,False),(-.5,True),(-2,True)]:
            result.update(delta_pct=delta,fair_delta_pp=2,oci=95)
            ideas=generate_ideas(a,at,m['fixture']['kickoff'])
            one=next(i for i in ideas if i['market']=='1x2_1')
            self.assertEqual(one['real_flow'],expected)

    def test_missing_market_fair_and_mixed_books_cannot_pass_sanity(self):
        from selection_studies import generate_ideas,result_gate
        for p in (None,float('nan'),40):
            self.assertIn('market_fair_missing',result_gate(dict(market='1x2_1',odd=2.5,poisson_pct=p,delta_pct=-3)))
        m=distortion_fixture('santander');a=m['multimarket'];matrix=a['matrix'];at=matrix['quote_monitoring']['as_of']
        for index,r in enumerate(matrix['rows'][:3]):
            r.update(bookmaker_id=str(index),market_fair_pct=None)
        hidden=[]
        ideas=generate_ideas(a,at,m['fixture']['kickoff'],hidden)
        self.assertTrue(any('market_fair_missing' in i['suppression_reasons'] for i in hidden))
        self.assertFalse(any(i['market'].startswith('1x2_') for i in ideas))

    def test_goal_channel_precedes_cheap_over_without_breaking_star_floor(self):
        m=distortion_fixture('elfsborg')
        ideas=m['multimarket']['matrix']['quote_monitoring']['studio_ideas']
        best=next(i for i in ideas if i.get('best_study'))
        self.assertIn(best['market'],('gg_ft','gg_ht','over15_ht','over35_ft','combo_ht_sh'))
        self.assertEqual(best.get('study_note'),'goal_context')
        self.assertGreaterEqual(best['poisson_pct'],32)
