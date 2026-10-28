"""Market-specific admission, cached migration and consumer parity (no APIs)."""
import json
import subprocess
import unittest
from copy import deepcopy
from datetime import datetime

from opportunity_policy import annotate, evaluate, goal_market, POLICY_ID, ODDS_MIN_BY_MARKET, ODDS_MAX_BY_MARKET, MIN_PROBABILITY_BY_MARKET
from lateral_matrix import specification as S, build_matrix
from statistics_first import statistical_analysis
from matrix_quotes import attach
from matrix_pipeline import build_board
from refresh_opportunities import refresh_day
from homepage_preview import build_preview
from gold_policy_audit import capture, build_report, requested_case
from test_opportunity_policy import row
from test_statistics_first import match, NOW
from test_gold_policy_audit import fixture


class MarketOddsCaps(unittest.TestCase):
    def test_boundaries_on_every_target_and_exact_asian_equivalent(self):
        for kind, period, line in [('total','ht',1.5),('total','ht',2.5),('total','ft',2.5),
                                  ('total','ft',3.5),('btts','ht',None),('asian_total','ht',1.5),
                                  ('asian_total','ft',2.5),('asian_total','ft',3.5)]:
            spec = S(kind, period, 'yes' if kind == 'btts' else 'over', line)
            key = goal_market({'spec': spec}); minimum, maximum = ODDS_MIN_BY_MARKET[key], ODDS_MAX_BY_MARKET[key]
            for price, eligible in [(minimum-.0001, False),(minimum, True),(maximum, True),(maximum+.0001,False)]:
                r = row(spec=spec, odd=price, statistical_pct=60)
                for t in r['goal_evidence']['teams'].values(): t['ht']['expected_goals'] = 1.
                with self.subTest(key=key,price=price):
                    d = evaluate(r)
                    self.assertEqual(d['eligible'],eligible,d)
                    self.assertEqual((d['odds_min'],d['odds_max']),(minimum,maximum))
            for fields, reason in [({'statistical_pct':MIN_PROBABILITY_BY_MARKET[key]-.01},'probability'),
                    ({'selected_margin_pct':8.001},'margin'),({'quality':'limited'},'history'),({'quote_status':'stale'},'stale')]:
                self.assertIn(reason,evaluate(row(spec=spec,odd=maximum,**fields))['reasons'])

    def test_unrelated_and_partial_settlement_markets_stay_excluded(self):
        for spec in [S('btts', 'ft', 'yes'), S('btts', 'ht', 'no'),
                     S('total', 'ft', 'under', 3.5), S('total', 'ft', 'over', 5.5),
                     S('asian_total', 'ft', 'over', 3), S('asian_total', 'ft', 'over', 3.25),
                     S('asian_total', 'ht', 'over', 1.75), S('result', 'ft', '1'),
                     S('total', 'ft', 'over', 3.5, extra='combo')]:
            d = evaluate(row(spec=spec, odd=4.2))
            self.assertIn('market', d['reasons'])
            self.assertEqual(d['odds_max'], 7.5)

    def test_backend_and_gold_browser_agree_on_all_boundaries_and_legacy(self):
        cases = []
        for spec in [S('total', 'ht', 'over', 1.5), S('total', 'ft', 'over', 2.5),
                     S('total', 'ft', 'over', 3.5), S('btts', 'ht', 'yes'),
                     S('asian_total', 'ft', 'over', 3.5), S('asian_total', 'ft', 'over', 3.25)]:
            for price in (1.3999, 1.4, 3.5, 3.5001, 4.2, 5.5, 5.5001):
                r = row(spec=spec, odd=price, statistical_pct=60)
                annotate({'rows': [r]}, NOW)
                cases.append(r)
        script = "const a=require('./web/lateral-matrix.js');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>console.log(JSON.stringify(JSON.parse(s).map(r=>!!a.bestOpportunity([r])))));"
        actual = json.loads(subprocess.check_output(['node', '-e', script], input=json.dumps(cases).encode()))
        self.assertEqual(actual, [r['opportunity']['eligible'] for r in cases])
        for r in cases:
            if r['odd'] > 3.5:
                r['opportunity'].update(policy_id='gold-goals-v3', eligible=True, rank=1)
        actual = json.loads(subprocess.check_output(['node', '-e', script], input=json.dumps(cases).encode()))
        self.assertFalse(any(ok for r, ok in zip(cases, actual) if r['odd'] > 3.5))

    def test_real_quote_adapter_cache_refresh_public_export_and_started_freeze(self):
        m = match(); a = build_matrix(statistical_analysis(m, NOW), NOW)
        m['multimarket'] = a
        state = {}
        payload = {'response': [{'fixture': {'id': m['fixture']['fixture_id']}, 'update': NOW,
            'bookmakers': [{'id': 10, 'name': 'Synthetic', 'bets': [
                {'id': 50, 'name': 'Goal Line', 'values': [
                    {'value': 'Over 3.5', 'odd': '4.20'}, {'value': 'Under 3.5', 'odd': '1.25'}]}]}]}]}
        attach(a, payload, state, m['fixture'], NOW)
        r = next(r for r in a['matrix']['rows'] if r['spec'] == S('asian_total', 'ft', 'over', 3.5))
        # The model is unchanged; use its genuine probability/EV in both paths.
        self.assertEqual(r['opportunity']['odds_max'], 4.5)
        self.assertNotIn('price', r['opportunity']['reasons'])
        self.assertAlmostEqual(r['selected_margin_pct'], (1/4.2+1/1.25-1)*100)
        old_probabilities = {r['id']: r['statistical_pct'] for r in a['matrix']['rows']}
        old_ledger = deepcopy(state)
        a['matrix']['opportunity_policy']['id'] = 'gold-goals-v3'
        day = {'date': '2026-10-26', 'updated_at': NOW, 'matches': [m]}
        self.assertEqual(refresh_day(day, state, NOW), 1)
        self.assertEqual(old_probabilities, {r['id']: r['statistical_pct'] for r in a['matrix']['rows']})
        self.assertEqual(state, old_ledger)
        self.assertEqual(a['matrix']['opportunity_policy']['id'], POLICY_ID)
        self.assertEqual(r['opportunity']['odds_max'], 4.5)
        # A supported synthetic admission verifies legacy/public consumers too.
        r.update(statistical_pct=35., low_pct=30., quality='supported', ev_pct=47., ev_low_pct=26.)
        annotate(a['matrix'], NOW, a); build_board(day, NOW)
        self.assertEqual(a['best_candidate']['matrix_market_id'], r['id'])
        self.assertEqual(a['best_candidate']['odd'], 4.2)
        preview = build_preview(day, now=datetime.fromisoformat(NOW))
        self.assertEqual(preview['selections'][0]['spec'], r['spec'])
        self.assertEqual(preview['selections'][0]['opportunity']['odds_max'], 4.5)
        frozen = deepcopy(day)
        self.assertEqual(refresh_day(day, state, '2026-10-27T08:00:00Z'), 0)
        self.assertEqual(day, frozen)

    def test_coherence_and_whitelist_cannot_be_bypassed_by_alias(self):
        rows = [row(spec=S('total', 'ft', 'over', 2.5), statistical_pct=40),
                row(id='asian', spec=S('asian_total', 'ft', 'over', 3.5), odd=4.2, statistical_pct=50)]
        self.assertIsNone(annotate({'rows': rows}, NOW))
        self.assertTrue(all('coherence' in r['opportunity']['reasons'] for r in rows))
        m = match(); m['fixture'].update(country='Cyprus', league='1. Division')
        m['multimarket'] = {'model_id': 'lateral-matrix-v5.0', 'matrix': {'rows': [rows[1]]}}
        day = {'date': '2026-10-26', 'updated_at': NOW, 'matches': [m]}
        refresh_day(day, {}, NOW)
        self.assertEqual(day['matches'], [])

    def test_audit_accepts_extended_price_and_keeps_alias_samples_separate(self):
        m = fixture(); m['fixture']['home'] = 'Odense'
        a = m['multimarket']; ordinary = a['matrix']['rows'][1]
        self.assertEqual(goal_market(ordinary), 'over35_ft')
        ordinary.update(odd=4.2)
        alias = deepcopy(ordinary); alias.update(id='asian-over35', spec=S('asian_total', 'ft', 'over', 3.5))
        a['matrix']['rows'].append(alias)
        annotate(a['matrix'], NOW, a)
        case = requested_case(m, NOW)
        self.assertTrue(all(r['price_range_met'] for r in case['markets'] if r['market'].startswith('over35')))
        store = {}; self.assertTrue(capture(store, m, NOW))
        report = build_report(store, NOW)
        groups = [r for r in report['groups'] if r['dimension'] == 'all_observed_market' and r['snapshot'] == 'first']
        self.assertEqual({r['value']: r['observations'] for r in groups if r['value'].startswith('over35')},
                         {'over35_ft': 1, 'over35_ft_asian': 1})


if __name__ == '__main__': unittest.main()

