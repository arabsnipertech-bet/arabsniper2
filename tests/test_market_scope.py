from copy import deepcopy
import json
from math import exp
import subprocess
import unittest
from unittest.mock import patch

from lateral_matrix import build_matrix, ScoreModel, specification as S, market_id
from market_scope import core_market, MARKET_SCOPE_ID
from matrix_quotes import attach, collect, analyse_prices
from refresh_opportunities import refresh_day, cached_refresh_safe
from statistics_first import statistical_analysis
from test_statistics_first import match, NOW
from test_lateral_matrix import payload


class CoreMarketScope(unittest.TestCase):
    def test_only_core_markets_are_estimated_including_missing_history(self):
        original = ScoreModel.estimate
        seen = []
        def checked(model, spec):
            self.assertTrue(core_market(spec), spec); seen.append(spec)
            return original(model, spec)
        for m in (match(), {'fixture': match()['fixture'], 'statistics': {}}):
            with patch.object(ScoreModel, 'estimate', checked):
                a = build_matrix(statistical_analysis(m, NOW), NOW,
                    [S('score', side='6:5'), S('total', side='over', line=5.5), S('dc', side='x2')])
            rows = a['matrix']['rows']
            self.assertEqual(len(rows), 58)
            self.assertNotIn('top_scores', a['matrix'])
            self.assertEqual(a['matrix']['market_scope']['id'], MARKET_SCOPE_ID)
            self.assertEqual(len({r['id'] for r in rows}), 58)
            self.assertEqual(sum(r['family'] == 'asian' for r in rows), 40)
        self.assertTrue(seen)

    def test_core_probabilities_remain_the_same_poisson_events(self):
        a = build_matrix(statistical_analysis(match(), NOW), NOW)
        rows = {r['id']: r for r in a['matrix']['rows']}; rates = a['rates']
        for period, lines in [('ft', (1.5, 2.5, 3.5)), ('ht', (1.5,))]:
            total = rates['home_'+period]+rates['away_'+period]
            for line in lines:
                term = exp(-total); cumulative = term
                for k in range(1, int(line)+1): term *= total/k; cumulative += term
                over = rows[market_id(S('total', period, 'over', line))]['statistical_pct']
                under = rows[market_id(S('total', period, 'under', line))]['statistical_pct']
                self.assertAlmostEqual(over, (1-cumulative)*100, places=3)
                self.assertAlmostEqual(over+under, 100, places=3)
            gg = (1-exp(-rates['home_'+period]))*(1-exp(-rates['away_'+period]))*100
            self.assertAlmostEqual(rows[market_id(S('btts', period, 'yes'))]['statistical_pct'], gg, places=3)
        self.assertAlmostEqual(sum(r['statistical_pct'] for r in rows.values() if r['family'] == '1x2'), 100, places=3)

    def test_irrelevant_provider_groups_never_reach_margin_or_movement(self):
        a = build_matrix(statistical_analysis(match(), NOW), NOW); state = {}
        for p in [payload(bid=10, name='Exact Score', values=[{'value': '6:5', 'odd': '150'}]),
                  payload(values=[{'value': side+' 5.5', 'odd': '2'} for side in ('Over', 'Under')])]:
            with patch('matrix_quotes.movement', side_effect=AssertionError('unused OCI')), patch('matrix_quotes.fair', side_effect=AssertionError('unused margin')):
                attach(a, p, state, match()['fixture'], NOW)
        self.assertTrue(all(r['quote_status'] == 'missing' for r in a['matrix']['rows']))
        self.assertEqual(state['matrix_quotes']['100']['groups'], {})
        with self.assertRaises(ValueError):
            analyse_prices({'spec': S('score', side='6:5')}, [], NOW, match()['fixture']['kickoff'])

    def test_stale_core_offer_never_recomputes_oci_or_margin(self):
        r = {'spec': S('total', side='over', line=2.5)}
        with patch('matrix_quotes.movement', side_effect=AssertionError('stale OCI')), patch('matrix_quotes.fair', side_effect=AssertionError('stale margin')):
            analyse_prices(r, [{'odd': 2., 'provider_updated_at': '2026-10-25T08:00:00Z'}], NOW, match()['fixture']['kickoff'])
        self.assertEqual(r['quote_status'], 'stale')

    def test_refresh_prunes_future_matrix_and_preserves_started_and_ledger(self):
        m = match(); m['multimarket'] = build_matrix(statistical_analysis(m, NOW), NOW)
        a = m['multimarket']; before = {r['id']: r['statistical_pct'] for r in a['matrix']['rows']}
        a['matrix']['rows'].append({'id': 'unused', 'spec': S('score', side='6:5')})
        a['matrix']['top_scores'] = [{'score': '6:5', 'p': .001}]
        state = {}; day = {'matches': [m]}
        frozen = deepcopy(day)
        self.assertEqual(refresh_day(day, state, '2026-10-27T08:00:00Z'), 0)
        self.assertEqual(m['multimarket'], frozen['matches'][0]['multimarket'])
        self.assertEqual(refresh_day(day, state, NOW), 1)
        self.assertEqual({r['id']: r['statistical_pct'] for r in a['matrix']['rows']}, before)
        self.assertNotIn('top_scores', a['matrix'])
        self.assertEqual(state, {})

    def test_browser_and_backend_scope_agree_even_on_old_payloads(self):
        specs = [S(k, p, side, line, extra) for k in ('score','dc','total','asian_total','handicap','btts','result','dnb')
                 for p in ('ft','ht','sh') for side in ('over','under','1','x','2','yes','no','home','away')
                 for line in (None,-1.5,-.75,0,1.25,1.5,2.5,3.5,5.5) for extra in (None,'over')]
        script = "const a=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>console.log(JSON.stringify(JSON.parse(s).map(a.coreMarket))));"
        actual = json.loads(subprocess.check_output(['node','-e',script], input=json.dumps(specs).encode()))
        self.assertEqual(actual, [core_market(s) for s in specs])

    def test_scope_migration_is_cache_safe_but_general_model_changes_are_not(self):
        self.assertTrue(cached_refresh_safe(['src/market_scope.py','src/lateral_matrix.py','src/proposal_stability.py','src/statistics_integration.py','web/lateral-matrix.js']))
        self.assertFalse(cached_refresh_safe(['src/lateral_matrix.py','src/proposal_stability.py']))
        self.assertFalse(cached_refresh_safe(['src/market_scope.py','src/statistics_first.py']))


if __name__ == '__main__': unittest.main()

