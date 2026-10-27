"""PT/FT admission and honest evidence; all inputs synthetic, zero API calls."""
import json
import subprocess
import unittest
from copy import deepcopy

from opportunity_policy import annotate, evaluate, first_half_evidence, min_probability, POLICY_ID
from lateral_matrix import specification as S, build_matrix
from statistics_first import statistical_analysis
from refresh_opportunities import refresh_day
from test_statistics_first import match, NOW, team_history
from test_opportunity_policy import row


class FirstHalfAdmission(unittest.TestCase):
    def test_dedicated_inclusive_thresholds_and_exact_asian_equivalents(self):
        for spec, floor in [(S('total', 'ht', 'over', 1.5), 35),
                (S('asian_total', 'ht', 'over', 1.5), 35), (S('btts', 'ht', 'yes'), 30),
                (S('total', 'ft', 'over', 2.5), 30), (S('total', 'ft', 'over', 3.5), 30),
                (S('asian_total', 'ft', 'over', 2.5), 30), (S('asian_total', 'ft', 'over', 3.5), 30)]:
            for p, ok in [(floor-.0001, False), (floor, True), (floor+.0001, True), (101, False)]:
                with self.subTest(spec=spec, p=p):
                    d = evaluate(row(spec=spec, statistical_pct=p))
                    self.assertEqual(d['min_probability_pct'], floor)
                    self.assertEqual(d['eligible'], ok)
                    self.assertEqual(d['threshold_status'], 'prudential_unvalidated')

    def test_real_ht_history_required_even_if_ft_history_and_price_are_strong(self):
        a = statistical_analysis(match(), NOW)
        for side in ('home', 'away'):
            for r in a['inputs']['histories'][side][3:]: r['halftime_available'] = False
        rs = [row(spec=S('total', 'ht', 'over', 1.5), statistical_pct=90),
              row(id='ft', statistical_pct=30)]
        matrix = {'rows': rs}; best = annotate(matrix, NOW, a)
        self.assertEqual(best['id'], 'ft')
        self.assertIn('ht_history', rs[0]['opportunity']['reasons'])
        self.assertEqual(len(matrix['rows']), 2)  # Observations remain visible.
        rs[0].pop('ht_evidence')
        self.assertIn('ht_history', evaluate(rs[0])['reasons'])

    def test_lower_floors_still_require_real_price_value_and_controlled_margin(self):
        for spec, p, odd in [(S('total', 'ht', 'over', 1.5), 35., 3.5),
                (S('btts', 'ht', 'yes'), 30., 5.5),
                (S('total', 'ft', 'over', 2.5), 30., 3.5),
                (S('total', 'ft', 'over', 3.5), 30., 5.5)]:
            r = row(spec=spec, statistical_pct=p, low_pct=p-.5, odd=odd,
                    ev_pct=p*odd-100, ev_low_pct=(p-.5)*odd-100)
            before = (r['statistical_pct'], r['low_pct'])
            self.assertIs(annotate({'rows': [r]}, NOW), r)
            self.assertEqual((r['statistical_pct'], r['low_pct']), before)
            self.assertIn('margin', evaluate(dict(r, selected_margin_pct=31.))['reasons'])
            # An accessible probability is insufficient at a non-value price.
            r.update(odd=1.4, ev_pct=p*1.4-100, ev_low_pct=(p-.5)*1.4-100)
            self.assertIn('edge', evaluate(r)['reasons'])
        r = row(statistical_pct=30., odd=3.5, ev_pct=5., ev_low_pct=3.25,
                selected_margin_pct=10.)
        self.assertIn('edge', evaluate(r)['reasons'])

    def test_ht_evidence_uses_observations_not_ft_xg_or_fabricated_zeros(self):
        histories = {s: team_history(s) for s in ('home', 'away')}
        for records in histories.values():
            for r in records:
                r.update(team_xg_for=12., team_xg_against=12., team_shots_on_target_for=40)
        a = {'inputs': {'histories': histories}}
        e = first_half_evidence(a)
        self.assertFalse(e['xg_supported'])
        self.assertEqual(e['teams']['home']['xg_for_sample'], 0)
        self.assertIsNone(e['teams']['home']['xg_for_mean'])
        self.assertIsNone(e['teams']['home']['shots_for_mean'])
        for records in histories.values():
            for r in records[:3]: r.update(team_xg_for_ht=0., team_xg_against_ht=.8, team_shots_on_target_for_ht=0)
        e = first_half_evidence(a)
        self.assertTrue(e['xg_supported'])
        self.assertEqual(e['teams']['home']['xg_for_sample'], 3)
        self.assertEqual(e['teams']['home']['xg_for_mean'], 0.)
        self.assertEqual(e['teams']['home']['shots_for_mean'], 0.)
        self.assertEqual(e['validation_status'], 'prospective_pending')
        # Duplicates and scores inconsistent with FT cannot manufacture coverage.
        histories['home'] = [histories['home'][0]]*4
        histories['away'][0]['team_goals_ht'] = 99
        e = first_half_evidence(a)
        self.assertEqual(e['teams']['home']['sample'], 1)
        self.assertEqual(e['teams']['away']['sample'], 4)

    def test_cached_migration_changes_admission_not_probabilities_or_started_matches(self):
        m = match(); a = build_matrix(statistical_analysis(m, NOW), NOW); m['multimarket'] = a
        before = {r['id']: (r['statistical_pct'], r['low_pct'], r['high_pct']) for r in a['matrix']['rows']}
        rates = deepcopy(a['rates'])
        annotate(a['matrix'], NOW, a)
        a['matrix']['opportunity_policy']['id'] = 'gold-goals-v5'
        day = {'date': '2026-10-26', 'matches': [m]}
        self.assertEqual(refresh_day(day, {}, NOW), 1)
        self.assertEqual(a['rates'], rates)
        self.assertEqual(before, {r['id']: (r['statistical_pct'], r['low_pct'], r['high_pct']) for r in a['matrix']['rows']})
        policy = a['matrix']['opportunity_policy']
        self.assertEqual(policy['id'], POLICY_ID)
        self.assertEqual(policy['min_probability_pct'], 30.)
        self.assertEqual(policy['min_probability_pct_by_market'], {'over15_ht': 35., 'gg_ht': 30., 'over25_ft': 30., 'over35_ft': 30.})
        json.dumps(day, allow_nan=False)
        frozen = deepcopy(day)
        self.assertEqual(refresh_day(day, {}, '2026-10-27T08:00:00Z'), 0)
        self.assertEqual(day, frozen)

    def test_hysteresis_preserves_soft_tolerance_but_cannot_relax_pt_floor(self):
        r = row(spec=S('total', 'ht', 'over', 1.5), statistical_pct=35.1, bookmaker_id='10')
        matrix = {'rows': [r]}; annotate(matrix, NOW)
        r.update(ev_pct=1.5, statistical_pct=35.)
        self.assertIs(annotate(matrix, '2026-10-26T08:10:00Z'), r)
        self.assertEqual(r['opportunity']['stability']['status'], 'retained')
        r['statistical_pct'] = 34.99
        self.assertIsNone(annotate(matrix, '2026-10-26T08:11:00Z'))
        self.assertIn('probability', r['opportunity']['reasons'])

    def test_browser_enforces_dedicated_floor_and_evidence_even_with_bad_metadata(self):
        cases = []
        for spec in [S('total', 'ht', 'over', 1.5), S('btts', 'ht', 'yes'),
                     S('asian_total', 'ht', 'over', 1.5), S('total', 'ft', 'over', 2.5),
                     S('total', 'ft', 'over', 3.5), S('asian_total', 'ft', 'over', 3.5)]:
            for p in (29.999, 30, 34.999, 35, 40, 45, 60):
                r = row(spec=spec, statistical_pct=p); annotate({'rows': [r]}, NOW); cases.append(r)
        script = "const a=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>console.log(JSON.stringify(JSON.parse(s).map(r=>!!a.bestOpportunity([r])))));"
        def browser(rows): return json.loads(subprocess.check_output(['node','-e',script], input=json.dumps(rows).encode()))
        self.assertEqual(browser(cases), [r['opportunity']['eligible'] for r in cases])
        invalid = []
        for r in cases:
            if r['statistical_pct'] < min_probability(r):
                r['opportunity'].update(eligible=True, rank=1, min_probability_pct=min_probability(r)-1)
                invalid.append(r)
        r = deepcopy(cases[6]); r['opportunity']['ht_history_supported'] = False; invalid.append(r)
        self.assertFalse(any(browser(invalid)))


if __name__ == '__main__': unittest.main()
