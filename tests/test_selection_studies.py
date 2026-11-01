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
                    odd=price, oci=40, delta_pct=-3, bookmaker_id='9', quote_status='recent',
                    quote_executable=True, provider_updated_at=at, quote_observed_at=at,
                    opportunity=dict(eligible=False, reasons=blockers, odds_min=1.70, odds_max=2.6))
    rows = [observed(specification('result','ft','1'), 45, 2.4, ['market']),
            observed(specification('total','ft','over',2.5), 100/1.84, 1.69, ['yield_floor']),
            observed(specification('total','ht','over',1.5), 31, 2.5, ['history','edge'])]
    m = dict(fixture=dict(home='Perth Glory', away='Adelaide United', fixture_id='perth-test',
                          kickoff=kickoff, status_short='NS'),
             multimarket=dict(rates=dict(home_ft=1.6,away_ft=1.2,home_ht=.6,away_ht=.5),
                              matrix=dict(rows=rows)))
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
            self.assertIsNone(study['by_type']['studio_combo_1x2_over25']['roi'])
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
