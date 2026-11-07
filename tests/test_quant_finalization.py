import unittest
from copy import deepcopy
from datetime import date
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from adaptive_calibration import fit, nightly_state
from daily_audit import fingerprint, same_frozen_forecast
from decision_metrics import uncertainty
from decision_shadow import capture, same_book_fair, stage
from lambda_shadow import expanded_rates
from official_selections import OfficialSelections
from opportunity_policy import annotate
from statistics_first import market_anchored_rates
from test_opportunity_policy import row


class QuantFinalization(unittest.TestCase):
    def test_frozen_identity_ignores_only_ingestion_label_and_settlement(self):
        original = dict(fixture={'fixture_id':7},captured_at='2026-11-05T10:00:00Z',odd=2.,probability=.6)
        labelled = dict(original, update_reason='journal recovery',settlement={'outcome':True})
        self.assertTrue(same_frozen_forecast(original,labelled))
        self.assertNotEqual(fingerprint(original),fingerprint(labelled))
        for key,value in [('odd',2.1),('probability',.61),('captured_at','2026-11-05T10:01:00Z'),('fixture',{'fixture_id':8})]:
            self.assertFalse(same_frozen_forecast(original,dict(labelled,**{key:value})))

    def test_provenance_empty_is_valid_missing_is_blocked(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);archive=root/'auditarchive';archive.mkdir()
            (archive/'prematch_2026-11-05_daily.json').write_text('{}')
            p=nightly_state(root,date(2026,11,7))['evidence_provenance']
            self.assertIn('missing_calibration_evidence_for_settled_day',[b['code'] for b in p['blockers']])
            raw=b'[]\n';(archive/'prematch_2026-11-05_calibration.json').write_bytes(raw)
            (archive/'prematch_2026-11-05_results.json').write_text('{}')
            p=nightly_state(root,date(2026,11,7))['evidence_provenance']
            self.assertTrue(p['complete']);self.assertEqual(p['dates'],['2026-11-05'])
            self.assertEqual(p['samples_count'],0);self.assertEqual(p['files'][0]['sha256'],sha256(raw).hexdigest())
            self.assertEqual((p['window_days'],p['half_life_days']),(28,14))

    def test_uncertainty_uses_fixtures_not_contracts_and_is_repeatable(self):
        rows=[dict(fid='a',odd=2,y=1)]*10
        self.assertIsNone(uncertainty(rows)['roi_pct_interval'])
        rows += [dict(fid='b',odd=2,y=0)]*10
        a=uncertainty(rows);self.assertEqual(a,uncertainty(rows))
        self.assertEqual(a['distinct_fixtures_count'],2)
        self.assertEqual(a['roi_pct_interval'],[-100.,100.])

    def test_missing_same_operator_partition_never_uses_median(self):
        self.assertIsNone(same_book_fair(dict(odd=2,market_fair_pct=50,margin_pct=3)))
        self.assertIsNone(same_book_fair(dict(odd=2,selected_margin_pct=10)))
        self.assertAlmostEqual(same_book_fair(dict(odd=2,selected_margin_pct=4)),50/1.04)
        self.assertIsNone(stage(dict(_anchor_applied=True,win_weight=.6),'poisson_base'))

    def test_lambda_shadow_preserves_input_and_caps_total(self):
        base=dict(home_ft=.8,away_ft=1.5,home_ht=.35,away_ht=.6)
        saved=deepcopy(base);prices={'1':1.3,'x':5.,'2':9.}
        anchored,_=market_anchored_rates(base,prices,.35)
        expanded,info=expanded_rates(base,prices,.35)
        self.assertTrue(info['applied']);self.assertEqual(base,saved)
        self.assertAlmostEqual(anchored['home_ft']+anchored['away_ft'],2.3)
        self.assertGreater(expanded['home_ft']+expanded['away_ft'],2.3)
        self.assertLessEqual(expanded['home_ft']+expanded['away_ft'],2.3*1.1+1e-12)
        for team in ('home','away'):
            self.assertAlmostEqual(expanded[team+'_ht']/expanded[team+'_ft'],base[team+'_ht']/base[team+'_ft'])

    def test_alpha_waits_for_25_new_fixtures_then_cools_without_retry_ratchet(self):
        rows=[dict(fid=str(i),type='studio_value',market='1x2_1',suppressed=True,
            suppression_reasons=['model_market_divergence'],date='2026-11-05',at='2026-11-05T10:00:00Z',
            p=.8,y=1,odd=2.,market_fair_pct=60.) for i in range(25)]
        old={'mismatch_shrinkage_factor':.25}
        small=fit(rows[:24],date(2026,11,7),old)
        self.assertEqual(small['mismatch_symmetric_shadow']['action'],'hold')
        full=fit(rows,date(2026,11,7),small)
        shadow=full['mismatch_symmetric_shadow']
        self.assertEqual(shadow['action'],'return_toward_base');self.assertLess(shadow['factor'],.25)
        self.assertEqual(len(shadow['consumed_fixtures']),25)
        self.assertEqual(full['mismatch_shrinkage_factor'],.25)
        self.assertEqual(fit(rows,date(2026,11,7),full),full)

    def test_experiments_are_separate_and_do_not_mutate_published_rows(self):
        at='2026-11-07T10:00:00Z';meta=dict(fixture_id=1,kickoff='2026-11-07T12:00:00Z',status_short='NS')
        r=row(win_weight=.6,loss_weight=.4,low_pct=54,high_pct=65,sensitivity_weights=[[.54,.46]],quote_executable=True)
        a={'matrix':{'rows':[r]}};annotate(a['matrix'],at);saved=deepcopy(a)
        result=capture(a,meta,at,{})
        self.assertTrue(result['available']);self.assertEqual(a,saved)
        self.assertEqual(set(result['experiments']),{'gold_no_automatic_upgrade','divergence_10ft_8ht','market_prior_blend','over25_floor_185','anti_peak_most_prolific'})

    def test_official_first_is_retained_when_latest_has_no_proposal(self):
        report=OfficialSelections()
        first=dict(fixture={'fixture_id':1,'kickoff':'2026-11-07T12:00:00Z'},captured_at='2026-11-07T10:00:00Z',
            multimarket={'matrix':{'rows':[dict(id='a',odd=2.,opportunity={'eligible':True,'rank':1})]}})
        report.observe(first)
        latest=deepcopy(first);latest['captured_at']='2026-11-07T11:00:00Z';latest['multimarket']['matrix']['rows'][0]['opportunity']['eligible']=False
        report.observe(latest);r=report.report()
        self.assertEqual(r['gold_certificate']['contracts_count'],1)
        self.assertEqual(r['latest_prematch_gold']['contracts_count'],0)
        self.assertEqual(r['all_eligible_markets']['distinct_fixtures_count'],1)

    def test_lambda_capture_hash_covers_original_rates_and_preserves_source(self):
        from lateral_matrix import build_matrix
        from adaptive_pricing import apply_matrix
        at='2026-11-07T08:00:00Z';meta=dict(fixture_id=1,kickoff='2026-11-07T20:00:00Z',status_short='NS')
        a=build_matrix(dict(rates=dict(home_ft=.8,away_ft=1.5,home_ht=.35,away_ht=.6)),at)
        for r in a['matrix']['rows']:
            if r['spec']['kind']=='result' and r['period']=='ft':
                r.update(selected_market_odds={'1':1.3,'x':5.,'2':9.},provider_updated_at=at,quote_status='recent')
        apply_matrix(a,meta,at);annotate(a['matrix'],at)
        saved=deepcopy(a);x=capture(a,meta,at,{})
        self.assertTrue(x['lambda_expansion_study']['available'])
        self.assertTrue(x['lambda_expansion_study']['applied'])
        self.assertIn('lambda_total_expansion',x['experiments']);self.assertEqual(a,saved)
        a['pure_poisson_rates']['home_ft'] += .01
        self.assertNotEqual(x['evidence_sha256'],capture(a,meta,at,{})['evidence_sha256'])
