import json
import subprocess
import unittest
from copy import deepcopy
from datetime import datetime
from opportunity_policy import evaluate, annotate, best_opportunity, POLICY_ID
from refresh_opportunities import refresh_day, cached_refresh_safe
from homepage_preview import build_preview
from matrix_quotes import attach, expire
from lateral_matrix import build_matrix, specification as S
from statistics_first import statistical_analysis
from test_statistics_first import match, NOW
from test_lateral_matrix import payload


def row(**changes):
    result = {'id': 'primary', 'label': 'Over 2,5 FT', 'spec': S('total', 'ft', 'over', 2.5),
        'family': 'totals', 'period': 'ft', 'quality': 'supported', 'quote_status': 'recent',
        'odd': 2., 'statistical_pct': 60., 'ev_pct': 20., 'ev_low_pct': 8.,
        'selected_margin_pct': 3., 'selected_margin_delta_pp': 0., 'oci': 60.,
        'paired_books': 3, 'agreeing_books': 3, 'movement_confirmed': True,
        'fair_direction_coherent': True, 'category': 'mature',
        'provider_updated_at': NOW, 'selected_movement': {'delta_pct': -8.,
            'fair_delta_pp': 3., 'persistence': 1., 'reversal': False}}
    result.update(changes)
    return result


class PlayableValue(unittest.TestCase):
    def test_inclusive_probability_and_price_boundaries(self):
        for p, q in [(35, 3.5), (80, 1.4)]:
            self.assertTrue(evaluate(row(statistical_pct=p, odd=q))['eligible'])
        for p, q, reason in [(34.999, 3.5, 'probability'), (80, 1.3999, 'price'), (35, 3.5001, 'price')]:
            self.assertIn(reason, evaluate(row(statistical_pct=p, odd=q))['reasons'])
        for p, eligible in [(34.999, False), (35, True)]:
            self.assertEqual(evaluate(row(spec=S('total', 'ht', 'over', 1.5), statistical_pct=p))['eligible'], eligible)

    def test_nonfinite_missing_or_limited_data_cannot_be_promoted(self):
        for key in ('statistical_pct', 'odd', 'ev_pct', 'ev_low_pct'):
            for value in (None, True, '50', float('nan'), float('inf')):
                self.assertFalse(evaluate(row(**{key: value}))['eligible'], (key, value))
        for fields in ({'quality': 'limited'}, {'quote_status': 'stale'}, {'ev_low_pct': 0}):
            self.assertFalse(evaluate(row(**fields))['eligible'])

    def test_longshots_and_exact_scores_never_enter_box(self):
        for r in [row(spec=S('score', 'ft', '0:0'), odd=2., statistical_pct=60),
                  row(spec=S('htft', 'ft', '1', extra='1'), odd=2., statistical_pct=60),
                  row(odd=150., statistical_pct=1., ev_pct=50., ev_low_pct=50.)]:
            self.assertIsNone(best_opportunity([r]))

    def test_only_four_goal_markets_can_be_proposed(self):
        primary = row(ev_pct=5., ev_low_pct=2.)
        secondary = row(id='asian', spec=S('handicap', 'ft', 'home', -.25),
                        quote_adapter_version=2, ev_pct=35., ev_low_pct=30.)
        combo = row(id='combo', spec=S('result_total', 'ft', '1', 1.5, 'over'), ev_pct=50., ev_low_pct=40.)
        self.assertEqual(best_opportunity([combo, secondary, primary])['id'], 'primary')
        self.assertIsNone(best_opportunity([combo, secondary]))
        self.assertIsNone(best_opportunity([dict(secondary, quote_adapter_version=1)]))

    def test_margin_tiers_expansion_and_selected_operator_anomaly(self):
        self.assertTrue(evaluate(row(ev_pct=3., ev_low_pct=.1))['eligible'])
        self.assertFalse(evaluate(row(ev_pct=3., selected_margin_pct=6.))['eligible'])
        self.assertFalse(evaluate(row(ev_pct=8.9, selected_margin_pct=10.))['eligible'])
        self.assertTrue(evaluate(row(ev_pct=9., ev_low_pct=2., selected_margin_pct=10.))['eligible'])
        d = evaluate(row(selected_margin_pct=10., selected_margin_delta_pp=1.))
        self.assertEqual((d['min_ev_pct'], d['min_ev_low_pct']), (11., 3.))
        for margin in (-.01, 30.01):
            self.assertIn('margin', evaluate(row(selected_margin_pct=margin, margin_pct=3.))['reasons'])
        unknown = evaluate(row(selected_margin_pct=None, ev_pct=10., ev_low_pct=3.))
        self.assertTrue(unknown['eligible']); self.assertEqual(unknown['signal'], 'unconfirmed')
        self.assertFalse(evaluate(row(selected_margin_pct=None, ev_pct=9.9))['eligible'])

    def test_convergence_and_contrarian_require_credible_same_book_flow(self):
        self.assertEqual(evaluate(row())['signal'], 'convergence')
        contrary = row(oci=-60., selected_movement={'delta_pct': 8., 'fair_delta_pp': -3., 'persistence': 1., 'reversal': False})
        self.assertEqual(evaluate(contrary)['signal'], 'contrarian')
        for fields in ({'oci': 39.9}, {'paired_books': 1, 'agreeing_books': 1},
                       {'agreeing_books': 1}, {'fair_direction_coherent': False},
                       {'selected_movement': None}, {'movement_confirmed': False},
                       {'selected_movement': {**contrary['selected_movement'], 'reversal': True}},
                       {'selected_movement': {**contrary['selected_movement'], 'persistence': 0.}},
                       {'selected_movement': row()['selected_movement']}):
            self.assertEqual(evaluate({**contrary, **fields})['signal'], 'unconfirmed')

    def test_frontend_consumes_same_rank_and_fails_closed(self):
        matrix = {'rows': [row(), row(id='distant', odd=150, statistical_pct=1),
                           row(id='secondary', spec=S('total', 'ht', 'over', 1.5), ev_pct=15., ev_low_pct=5.)]}
        best = annotate(matrix, NOW)
        raw = subprocess.check_output(['node', '-e', "const api=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const rows=JSON.parse(s);console.log(JSON.stringify({id:api.bestOpportunity(rows)?.id,old:api.bestOpportunity(rows.map(({opportunity,...r})=>r))}));});"], input=json.dumps(matrix['rows']).encode())
        result = json.loads(raw)
        self.assertEqual(result['id'], best['id']); self.assertIsNone(result['old'])
        self.assertEqual(matrix['opportunity_policy']['eligible_markets'], 2)
        self.assertEqual(len(matrix['rows']), 3)

    def test_public_preview_uses_same_policy_and_does_not_mutate_snapshot(self):
        m = match(); a = {'model_id': 'lateral-matrix-v5.0', 'captured_at': NOW,
            'matrix': {'rows': [row(), row(id='remote', odd=150, statistical_pct=1, ev_low_pct=99)]}}
        m['multimarket'] = a
        p = {'date': '2026-10-26', 'updated_at': NOW, 'multimarket_method': {'model_id': a['model_id']}, 'matches': [m]}
        frozen = deepcopy(p)
        result = build_preview(p, now=datetime.fromisoformat(NOW))
        self.assertEqual(result['opportunity_policy'], POLICY_ID)
        self.assertEqual(result['selections'][0]['odd'], 2.)
        self.assertEqual(p, frozen)
        self.assertEqual(build_preview(p, now=datetime.fromisoformat('2026-10-26T18:00:00+00:00'))['selections'], [])

    def test_real_adapter_refresh_and_expiry_preserve_statistics_and_started(self):
        for first, middle, last, signal in [(2.5, 2.3, 2.1, 'convergence'), (2.1, 2.35, 2.6, 'contrarian')]:
            m = match(); a = build_matrix(statistical_analysis(m, NOW), NOW); state = {}
            for when, price in [('2026-10-26T06:00:00Z', first), ('2026-10-26T07:00:00Z', middle), (NOW, last)]:
                attach(a, payload(when, price, 1.6), state, m['fixture'], when)
            r = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
            self.assertEqual(r['opportunity']['signal'], signal)
            self.assertAlmostEqual(r['selected_margin_pct'], (1/last+1/1.6-1)*100)
            m['multimarket'] = a; day = {'matches': [m]}
            before = [(r['id'], r['statistical_pct'], r.get('provider_updated_at')) for r in a['matrix']['rows']]
            ledger = deepcopy(state)
            self.assertEqual(refresh_day(day, state, NOW), 1)
            self.assertEqual(r['opportunity']['signal'], signal)
            self.assertEqual(before, [(r['id'], r['statistical_pct'], r.get('provider_updated_at')) for r in a['matrix']['rows']])
            self.assertEqual(state, ledger)
            frozen = deepcopy(day)
            self.assertEqual(refresh_day(day, state, '2026-10-27T08:00:00Z'), 0)
            self.assertEqual(day, frozen)
            expire(a, m['fixture'], '2026-10-26T18:00:00Z')
            self.assertFalse(r['opportunity']['eligible'])
            self.assertIsNone(a['matrix']['opportunity_policy']['best_market_id'])

    def test_price_only_release_avoids_collection_but_engine_changes_do_not(self):
        self.assertTrue(cached_refresh_safe(['src/opportunity_policy.py', 'web/lateral-matrix.js', 'tests/test_opportunity_policy.py']))
        for paths in ([], ['web/lateral-matrix.js'], ['src/statistics_first.py', 'src/opportunity_policy.py'],
                      ['src/3appdays.py', 'src/matrix_quotes.py'], ['new_unknown.py']):
            self.assertFalse(cached_refresh_safe(paths))

    def test_one_book_margin_expansion_still_requires_extra_value(self):
        m = match(); a = build_matrix(statistical_analysis(m, NOW), NOW); state = {}
        attach(a, payload('2026-10-26T06:00:00Z', 2.5, 1.6, books=1), state, m['fixture'], '2026-10-26T06:00:00Z')
        attach(a, payload(NOW, 2.1, 1.6, books=1), state, m['fixture'], NOW)
        r = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
        self.assertGreater(r['selected_margin_delta_pp'], 1)
        self.assertIsNone(r['oci'])
        self.assertEqual(r['opportunity']['signal'], 'unconfirmed')
        self.assertGreater(r['opportunity']['min_ev_pct'], 10)


if __name__ == '__main__': unittest.main()
