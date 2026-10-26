import json
import subprocess
import unittest
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import patch

from opportunity_policy import annotate, POLICY_ID
from proposal_stability import seed_stability, LIMITS
from lateral_matrix import MODEL_ID, specification as S
from matrix_quotes import expire
from refresh_opportunities import refresh_day
from homepage_preview import build_preview
from gold_policy_audit import policy_choices
from test_opportunity_policy import row
from test_statistics_first import NOW, match


def later(minutes):
    return (datetime.fromisoformat(NOW)+timedelta(minutes=minutes)).isoformat()


def saved(**changes):
    r = row(bookmaker_id='1', ev_pct=2.2, ev_low_pct=.2, odd=1.7, **changes)
    a = {'model_id': MODEL_ID, 'captured_at': NOW, 'matrix': {'rows': [r]}}
    annotate(a['matrix'], NOW)
    return a, r


def wobble(r):
    r.update(odd=1.69, ev_pct=1.4, ev_low_pct=-.2, oci=50., selected_margin_pct=3.1)


class ProposalStability(unittest.TestCase):
    def test_full_scan_transfers_saved_proposal_across_day_file_rollover(self):
        import tempfile
        from pathlib import Path
        from test_policy_engine import engine
        from policy_integration import install
        old, _ = saved(); fresh, r = saved(); wobble(r)
        m = match(); m['multimarket'] = fresh
        previous = match(); previous['multimarket'] = old
        output = {'days': [{'day': 1, 'date': '2026-10-26', 'matches': [m]}]}
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            (folder/'arab_quote_day2.json').write_text(json.dumps({'date': '2026-10-26', 'matches': [previous]}))
            ns = dict(engine.__dict__)
            ns.update(DATA_DIR=folder, OUTPUT_FILE=folder/'dashboard.json', iso_now=lambda: later(15),
                      _scan_dates_before_prematch_audit=lambda *args: output)
            install(ns)
            with patch('scan_audit.update_audit', return_value={'cohorts': [], 'status': 'experimental'}):
                ns['scan_dates'](None, ['2026-10-26'], [])
            published = json.loads((folder/'arab_quote_day1.json').read_text())['matches'][0]['multimarket']['matrix']
            self.assertEqual(published['proposal_stability']['last_strict_at'], NOW)
            self.assertEqual(published['proposal_stability']['status'], 'retained')
            self.assertFalse(published['rows'][0]['opportunity']['strict_eligible'])

    def test_entry_is_strict_and_only_published_choice_can_be_retained(self):
        a, r = saved(); wobble(r)
        fresh = {'rows': [deepcopy(r)]}
        self.assertIsNone(annotate(fresh, later(15)))
        self.assertEqual(annotate(a['matrix'], later(15))['id'], r['id'])
        self.assertFalse(r['opportunity']['strict_eligible'])
        self.assertEqual(r['opportunity']['stability']['status'], 'retained')
        self.assertEqual(policy_choices([r], {})['published'], r['id'])
        self.assertIsNone(policy_choices([r], {})['without_hysteresis'])

    def test_retention_does_not_reset_anchor_or_mutate_probabilities(self):
        a, r = saved(); wobble(r)
        for minutes in (15, 60, 90):
            p = r['statistical_pct']
            self.assertIs(annotate(a['matrix'], later(minutes)), r)
            self.assertEqual(a['matrix']['proposal_stability']['last_strict_at'], NOW)
            self.assertEqual(r['statistical_pct'], p)
            frozen = deepcopy(a)
            annotate(a['matrix'], later(minutes))
            self.assertEqual(a, frozen)
        self.assertIsNone(annotate(a['matrix'], later(90.01)))

    def test_tolerance_boundaries_and_hard_stops(self):
        allowed = [{'odd': 1.7*.97}, {'statistical_pct': 59.}, {'selected_margin_pct': 3.75},
                   {'oci': 40.}, {'ev_pct': 1.}, {'ev_low_pct': -.5}]
        rejected = [{'odd': 1.7*.9699}, {'statistical_pct': 58.999}, {'selected_margin_pct': 3.751},
                    {'oci': 39.99}, {'oci': None}, {'ev_pct': .999}, {'ev_low_pct': -.501},
                    {'odd': 1.399}, {'odd': 3.501}, {'statistical_pct': 34.99},
                    {'quality': 'limited'}, {'quote_status': 'stale'}, {'selected_margin_pct': -1},
                    {'bookmaker_id': '2'}, {'selected_movement': {'reversal': True}}]
        for changes in allowed+rejected:
            with self.subTest(changes=changes):
                a, r = saved(); wobble(r); r.update(changes)
                self.assertEqual(annotate(a['matrix'], later(15)) is not None, changes in allowed)

    def test_cumulative_drift_is_measured_from_strict_anchor(self):
        a, r = saved(); wobble(r); r['odd'] = 1.7*.98
        self.assertIsNotNone(annotate(a['matrix'], later(10)))
        r['odd'] *= .98
        self.assertIsNone(annotate(a['matrix'], later(20)))

    def test_renewal_requires_strict_readmission_and_unconfirmed_flow_is_not_frozen(self):
        a, r = saved(); wobble(r)
        r['movement_confirmed'] = False
        annotate(a['matrix'], later(20))
        self.assertEqual(r['opportunity']['signal'], 'unconfirmed')
        r.update(ev_pct=2.1, ev_low_pct=.1)
        annotate(a['matrix'], later(30))
        self.assertEqual(a['matrix']['proposal_stability']['last_strict_at'], later(30))
        self.assertTrue(r['opportunity']['strict_eligible'])

    def test_small_rank_noise_does_not_replace_incumbent_but_large_gain_does(self):
        a = {'rows': [row(bookmaker_id='1'), row(id='other', spec=S('total', 'ft', 'over', 3.5), ev_low_pct=7.9)]}
        self.assertEqual(annotate(a, NOW)['id'], 'primary')
        a['rows'][1]['ev_low_pct'] = 8.1
        self.assertEqual(annotate(a, later(5))['id'], 'primary')
        a['rows'][1]['ev_low_pct'] = 10.
        self.assertEqual(annotate(a, later(10))['id'], 'other')

    def test_pt_priority_and_coherence_remain_hard(self):
        a, r = saved(); wobble(r)
        a['matrix']['rows'].append(row(id='pt', spec=S('total', 'ht', 'over', 1.5), ev_low_pct=.1))
        a['matrix']['goal_context'] = {'pt_priority_active': True, 'preferred_pt_market': 'over15_ht'}
        self.assertEqual(annotate(a['matrix'], later(5))['id'], 'pt')
        self.assertFalse(r['opportunity']['eligible'])
        a, r = saved(); wobble(r)
        a['matrix']['rows'].append(row(id='incoherent', spec=S('total', 'ft', 'over', 3.5), statistical_pct=70.))
        self.assertIsNone(annotate(a['matrix'], later(5)))
        self.assertIn('coherence', r['opportunity']['reasons'])

    def test_seed_json_roundtrip_cache_refresh_and_homepage_agree(self):
        old, _ = saved()
        fresh, r = saved(); wobble(r)
        seed_stability(json.loads(json.dumps(old)), fresh)
        m = match(); m['multimarket'] = fresh
        day = {'date': '2026-10-26', 'matches': [m]}
        self.assertEqual(refresh_day(day, {}, later(15)), 1)
        self.assertFalse(r['opportunity']['strict_eligible'])
        frozen = deepcopy(day)
        preview = build_preview(day, now=datetime.fromisoformat(later(15)))
        self.assertEqual(preview['selections'][0]['odd'], r['odd'])
        self.assertEqual(day, frozen)
        seed_stability({}, fresh)
        self.assertIsNone(annotate(fresh['matrix'], later(20)))

    def test_migration_seeds_only_valid_saved_v2_choice(self):
        old, r = saved(); old['matrix'].pop('proposal_stability')
        old['matrix']['opportunity_policy']['id'] = 'gold-goals-v2'
        current, _ = saved(); seed_stability(old, current)
        self.assertEqual(current['matrix']['proposal_stability']['last_strict_at'], NOW)
        r['quote_status'] = 'stale'; seed_stability(old, current)
        self.assertNotIn('proposal_stability', current['matrix'])

    def test_stale_quotes_release_immediately_and_started_forecasts_are_immutable(self):
        a, r = saved(); wobble(r)
        meta = match()['fixture']; meta['kickoff'] = later(120)
        expire(a, meta, later(61))
        self.assertIsNone(a['matrix']['opportunity_policy']['best_market_id'])
        self.assertEqual(r['quote_status'], 'stale')
        frozen = deepcopy(a); expire(a, meta, later(121)); self.assertEqual(a, frozen)

    def test_frontend_accepts_only_explicit_unexpired_bounded_hold(self):
        a, r = saved(); wobble(r); annotate(a['matrix'], later(15))
        script = "const a=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let r=JSON.parse(s);const o={now:Date.parse('"+later(15)+"')};let good=a.bestOpportunity([r],o)?.id;r.opportunity.stability.expires_at='2026-10-26T08:01:00Z';let expired=a.bestOpportunity([r],o);delete r.opportunity.stability;let forged=a.bestOpportunity([r],o);console.log(JSON.stringify({good,expired,forged}));});"
        result = json.loads(subprocess.check_output(['node', '-e', script], input=json.dumps(r).encode()))
        self.assertEqual(result, {'good': r['id'], 'expired': None, 'forged': None})


if __name__ == '__main__': unittest.main()
