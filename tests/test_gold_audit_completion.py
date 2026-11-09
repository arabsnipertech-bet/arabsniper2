import json
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path[:0] = [str(Path(__file__).resolve().parents[1]/p) for p in ('src', 'auditor')]
from audit_pending import summarize
from auditor_history_chunks import digest
from decision_shadow import capture, FAIR_PROTOCOL
from opportunity_policy import annotate
from repair_certificate import build
from statistics_first import statistical_analysis
from test_opportunity_policy import row
import test_repair_certificate as certificate_fixture
from test_statistics_first import match, NOW, KICKOFF


class GoldAuditCompletion(unittest.TestCase):
    def assessment(self):
        a = statistical_analysis(match(), NOW)
        r = row(win_weight=.6, loss_weight=.4, low_pct=54, high_pct=65,
                sensitivity_weights=[[.54,.46]], quote_executable=True)
        a['matrix'] = {'rows':[r]}
        annotate(a['matrix'], NOW, assessment=a)
        return a

    def test_new_assessment_freezes_peak_and_next_audit_has_experiment(self):
        m = match()
        m['statistics']['home']['last_matches'][2]['team_goals_ft'] = 12
        m['statistics']['home']['last_matches'][2]['research_ft']['scored'] = 12
        a = statistical_analysis(m, NOW)
        profile = a['profiles']['home']['for_ft']
        self.assertEqual(profile['without_peak'], 3.)
        self.assertLess(profile['without_peak'], profile['mean'])
        self.assertEqual(a['anti_peak_most_prolific']['captured_at'], NOW)
        r = row(win_weight=.6,loss_weight=.4,low_pct=54,high_pct=65,sensitivity_weights=[[.54,.46]])
        a['matrix']={'rows':[r]};annotate(a['matrix'], NOW, assessment=a)
        before=deepcopy(a)
        frozen=capture(a,m['fixture'],NOW,{})
        self.assertIn('anti_peak_most_prolific',frozen['experiments'])
        self.assertEqual(frozen['anti_peak_most_prolific'],a['anti_peak_most_prolific'])
        self.assertEqual(a,before)
        a['anti_peak_most_prolific']['teams']['home']['ft']['without_peak'] += .1
        self.assertNotEqual(frozen['evidence_sha256'],capture(a,m['fixture'],NOW,{})['evidence_sha256'])

    def test_missing_history_is_explicit_not_zero_peak(self):
        m=match();m['statistics']['home']['last_matches']=[]
        a=statistical_analysis(m,NOW)
        self.assertIsNone(a['profiles']['home']['for_ht']['without_peak'])
        self.assertFalse(a['anti_peak_most_prolific']['periods']['ht']['available'])
        self.assertIsNone(a['anti_peak_most_prolific']['periods']['ht']['passes_80pct'])

    def test_st_means_use_paired_games_not_difference_of_window_means(self):
        m=match()
        for i,r in enumerate(m['statistics']['home']['last_matches']):
            r['team_goals_ft']=r['research_ft']['scored']=5 if i<4 else 0
        a=statistical_analysis(m,NOW)
        st=a['observed_goal_means']['teams']['home']['st']
        self.assertEqual(st['sample'],4)
        self.assertEqual(st['scored'],4.)
        self.assertNotEqual(st['scored'],a['profiles']['home']['for_ft']['mean']-a['profiles']['home']['for_ht']['mean'])

    def test_market_fair_admission_is_not_vetoed_by_book_margin(self):
        a=self.assessment()
        a['matrix']['rows'][0]['selected_market_fair_pct'] = None
        saved=deepcopy(a)
        result=capture(a,match()['fixture'],NOW,{})
        fair=result['paths']['market_fair']
        self.assertEqual(result['market_fair_protocol'],FAIR_PROTOCOL)
        self.assertEqual(fair['first'],'primary')
        chosen=fair['rows'][0]
        self.assertAlmostEqual(chosen['eligibility_odd'],2*1.03)
        self.assertEqual(chosen['odd'],2.)
        self.assertLess(chosen['gross_quote_ev_pct'],0)
        self.assertNotIn('edge',chosen['reasons']);self.assertNotIn('sensitivity',chosen['reasons'])
        self.assertEqual(a,saved)
        # Real price, probability and flow gates still apply in this comparison.
        a['matrix']['rows'][0]['oci']=-30
        self.assertIsNone(capture(a,match()['fixture'],NOW,{})['paths']['market_fair']['first'])
        self.assertIn('oci_negative',capture(a,match()['fixture'],NOW,{})['paths']['market_fair']['rows'][0]['reasons'])

    def test_daily_pending_deduplicates_and_separates_official_gold(self):
        def record(fid,closed=False,gold=False):
            return dict(fixture={'fixture_id':fid,'kickoff':KICKOFF},captured_at=NOW,
                settlement={'outcomes':{'target':False}} if closed else {},
                multimarket={'matrix':{'rows':[{'opportunity':{'eligible':True,'rank':1}}] if gold else []}})
        store={'fixtures':{'g':record(7),'n':record(8),'closed':record(9,True),'unknown':{'fixture':{}}},
               'multimarket_evaluations':{'g':record(7,True,True),'n':record(8)},
               'analysis_updates':{'repeat':record(8)}}
        result=summarize(store)
        self.assertTrue(result['pending'])
        self.assertEqual(result['pending_fixtures_count'],2)
        self.assertEqual(result['non_gold_pending_fixtures_count'],1)
        self.assertEqual(result['non_gold_pending_fixture_ids'],['8'])
        self.assertEqual(result['gold_pending_fixture_ids'],['7'])
        self.assertEqual(result['pending_records_without_fixture_id'],1)
        self.assertEqual(result['pending_records_count'],5)

    def test_certificate_exposes_exact_sealed_pending_counts(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);index,state,day=certificate_fixture.RepairCertificate().fixture(root)
            prefix=f'auditarchive/prematch_{day}_'
            report=json.loads((root/(prefix+'daily.json')).read_text())
            counts=summarize({'fixtures':{'n':{'fixture':{'fixture_id':123}}}})
            report['pending_summary']=counts
            (root/(prefix+'daily.json')).write_text(json.dumps(report))
            checksum=digest(root/(prefix+'daily.json'));index[prefix+'daily.json']['sha256']=checksum
            receipt=json.loads((root/(prefix+'checkpoint.json')).read_text())
            receipt.update(pending=True,pending_summary=counts);receipt['outputs'][prefix+'daily.json']=checksum
            (root/(prefix+'checkpoint.json')).write_text(json.dumps(receipt))
            certified=build(root,index,state,[day],'b'*64)['dates'][day]
            self.assertEqual(certified['pending_summary']['non_gold_pending_fixtures_count'],1)
            receipt['pending_summary']=dict(counts,non_gold_pending_fixtures_count=2)
            (root/(prefix+'checkpoint.json')).write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError,'Pending counts'):
                build(root,index,state,[day],'b'*64)
