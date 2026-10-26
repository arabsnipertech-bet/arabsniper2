import unittest
from copy import deepcopy
from datetime import datetime, timezone

from lateral_matrix import (ScoreModel, specification as S, market_id, build_matrix,
                            payout_weights, settle_spec, MODEL_ID)
from matrix_quotes import collect, attach, expire, parse_value, migrate_handicap_history
from repair_matrix_quotes import repair_day
from matrix_pipeline import build_board, complete_day, fixture_allowed
from matrix_audit import MatrixAccumulator
from statistics_first import statistical_analysis
from test_statistics_first import match, NOW, KICKOFF


def payload(when=NOW, over=2.2, under=1.9, books=2, values=None, bid=5, name='Goals Over/Under'):
    return {'response': [{'fixture': {'id': 100, 'date': KICKOFF}, 'update': when,
        'bookmakers': [{'id': i+1, 'name': f'Book {i+1}', 'bets': [{'id': bid, 'name': name,
            'values': values or [{'value': 'Over 2.5', 'odd': str(over)}, {'value': 'Under 2.5', 'odd': str(under)}]}]}
            for i in range(books)]}]}


class MatrixMath(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = build_matrix(statistical_analysis(match(), NOW), NOW)
        cls.model = ScoreModel({'home_ft': 1.7, 'away_ft': 1.2, 'home_ht': .7, 'away_ht': .5})

    def test_full_coverage_and_binary_complements(self):
        self.assertGreater(len(self.a['matrix']['rows']), 250)
        for period in ('ft', 'ht', 'sh'):
            self.assertAlmostEqual(sum(self.model.estimate(S('result', period, side))['p'] for side in ('1', 'x', '2')), 1, places=8)
            for line in (.5, 1.5, 2.5):
                self.assertAlmostEqual(sum(self.model.estimate(S('total', period, s, line))['p'] for s in ('over', 'under')), 1, places=8)

    def test_joint_combo_not_product_of_overlapping_marginals(self):
        a = self.model.estimate(S('dc', 'ft', '1x'))['p']
        b = self.model.estimate(S('total', 'ft', 'over', 1.5))['p']
        joint = self.model.estimate(S('dc_total', 'ft', '1x', 1.5, 'over'))['p']
        self.assertLessEqual(joint, min(a, b))
        self.assertGreater(abs(joint-a*b), .001)
        outcomes = [self.model.estimate(S('htft', 'ft', ht, extra=ft))['p'] for ht in ('1', 'x', '2') for ft in ('1', 'x', '2')]
        self.assertAlmostEqual(sum(outcomes), 1, places=8)
        self.assertAlmostEqual(sum(outcomes[:3]), self.model.estimate(S('result', 'ht', '1'))['p'], places=8)

    def test_asian_refunds_and_fair_price_zero_return(self):
        self.assertEqual(payout_weights(S('asian_total', 'ft', 'over', 2.25), 1, 1), (0, .5))
        self.assertEqual(payout_weights(S('asian_total', 'ft', 'under', 2.25), 1, 1), (.5, 0))
        self.assertEqual(payout_weights(S('handicap', 'ft', 'home', -.75), 2, 1), (.5, 0))
        self.assertEqual(payout_weights(S('dnb', 'ft', '1'), 1, 1), (0, 0))
        for spec in [S('asian_total', 'ft', 'over', 2.25), S('handicap', 'ft', 'away', .75), S('dnb', 'ft', '1')]:
            p = self.model.estimate(spec)
            self.assertAlmostEqual(p['win_weight']*(p['fair_odd']-1)-p['loss_weight'], 0, places=10)

    def test_missing_history_still_calculated_with_explicit_prior(self):
        a = build_matrix(statistical_analysis({'fixture': match()['fixture'], 'statistics': {}}, NOW), NOW)
        self.assertGreater(len(a['matrix']['rows']), 250)
        self.assertTrue(all(r['quality'] == 'prior_only' for r in a['matrix']['rows']))
        attach(a, payload(over=100, under=1.01), {}, match()['fixture'], NOW)
        self.assertFalse(any(r.get('category') in ('mature', 'contrarian', 'lateral') for r in a['matrix']['rows']))

    def test_quotes_do_not_change_probabilities_or_rates(self):
        a = deepcopy(self.a)
        before = {r['id']: (r['statistical_pct'], r['low_pct'], r['high_pct']) for r in a['matrix']['rows']}
        attach(a, payload(), {}, match()['fixture'], NOW)
        self.assertEqual(a['rates'], self.a['rates'])
        for row in a['matrix']['rows']:
            if row['id'] in before: self.assertEqual(before[row['id']], (row['statistical_pct'], row['low_pct'], row['high_pct']))

    def test_every_fixture_is_retained_without_daily_cap(self):
        sample = {'fixture': match()['fixture'], 'multimarket': self.a}
        day = {'matches': []}
        for i in range(12):
            m = deepcopy(sample); m['fixture']['fixture_id'] = str(i)
            day['matches'].append(m)
        build_board(day, NOW)
        self.assertEqual(len(day['matches']), 12)
        self.assertIsNone(day['selection_board']['max_per_market'])
        self.assertEqual(day['matrix_summary']['analyzed_fixtures'], 12)

    def test_schedule_completion_keeps_sparse_future_and_started_fixtures(self):
        raw = [
            {'fixture': {'id': 1, 'date': KICKOFF, 'status': {'short': 'TBD'}},
             'league': {'country': 'Italy', 'name': 'Serie A'}},
            {'fixture': {'id': 2, 'date': '2026-10-26T04:00:00Z', 'status': {'short': 'FT'}},
             'league': {'country': 'Romania', 'name': 'Liga I'}},
            {'fixture': {'id': 3, 'date': KICKOFF, 'status': {'short': 'NS'}},
             'league': {'country': 'Italy', 'name': 'Primavera U19'}}]
        class Client:
            def get(self, path, params):
                return {'response': raw}
        ns = {'fixture_meta': lambda r: {'fixture_id': str(r['fixture']['id']),
                'league': r['league']['name'], 'country': r['league']['country'],
                'kickoff': r['fixture']['date'], 'status_short': r['fixture']['status']['short']},
              '_result_payload': lambda r: {'status': 'FT', 'home_ft': 1, 'away_ft': 0}}
        day = {'date': '2026-10-26', 'matches': []}
        self.assertEqual([fixture_allowed(r) for r in raw], [True, True, False])
        complete_day(ns, Client(), day, NOW)
        self.assertEqual(len(day['matches']), 2)
        indexed = {m['fixture']['fixture_id']: m for m in day['matches']}
        future, started = (indexed[k]['multimarket'] for k in ('1', '2'))
        self.assertGreater(len(future['matrix']['rows']), 250)
        self.assertTrue(all(r['quality'] == 'prior_only' for r in future['matrix']['rows']))
        self.assertEqual(started['matrix']['rows'], [])
        self.assertEqual(started['status'], 'no_prematch_snapshot')
        self.assertEqual(day['universal_coverage']['excluded_by_competition'], 1)
        build_board(day, NOW)

    def test_started_snapshot_is_immutable(self):
        a = deepcopy(self.a); m = {'fixture': match()['fixture'], 'multimarket': a}
        build_board({'matches': [m]}, NOW)
        frozen = deepcopy(a)
        build_board({'matches': [m]}, '2026-10-27T08:00:00Z')
        self.assertEqual(a, frozen)

    def test_settlement_never_invents_missing_ht_or_accepts_incoherence(self):
        result = {'status': 'FT', 'home_ft': 2, 'away_ft': 1, 'home_ht': None, 'away_ht': None}
        self.assertEqual(settle_spec(S('total', 'ft', 'over', 2.5), result), (1, 0))
        self.assertIsNone(settle_spec(S('total', 'ht', 'over', .5), result))
        self.assertIsNone(settle_spec(S('htft', 'ft', '1', extra='1'), result))
        result.update(home_ht=3, away_ht=0)
        self.assertIsNone(settle_spec(S('total', 'sh', 'over', .5), result))

    def test_price_movement_coverage_and_exact_partition(self):
        partial = payload(values=[{'value': 'Over 2.5', 'odd': '2.2'}])
        a = deepcopy(self.a); attach(a, partial, {}, match()['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
        self.assertEqual(row['odd'], 2.2)
        self.assertIsNone(row['margin_pct'])
        self.assertIsNone(row['oci'])
        mismatched = payload(name='Total - Home')
        groups, diagnostics = collect(mismatched, 100, NOW)
        self.assertEqual(groups, {})
        self.assertEqual(diagnostics['catalog_identity_mismatch'], 2)

    def test_convergence_and_contrarian_use_same_market_and_books(self):
        for first, last, expected in [(2.5, 2.1, 'convergence'), (2.1, 2.6, 'divergence')]:
            state = {}; a = deepcopy(self.a)
            attach(a, payload('2026-10-26T06:00:00Z', first, 2), state, match()['fixture'], '2026-10-26T06:00:00Z')
            attach(a, payload(NOW, last, 2), state, match()['fixture'], NOW)
            row = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
            self.assertEqual(row['market_behavior'], expected)
            self.assertEqual(row['paired_books'], 2)
            self.assertGreater(row['ev_pct'], 0)
            gg = next(r for r in a['matrix']['rows'] if r['spec'] == S('btts', 'ht', 'yes'))
            self.assertEqual(gg['quote_status'], 'missing')
            self.assertFalse(gg.get('oci'))

    def test_margin_only_change_is_not_directional_confirmation(self):
        a, state = deepcopy(self.a), {}
        attach(a, payload('2026-10-26T06:00:00Z', 1.9, 1.9), state, match()['fixture'], '2026-10-26T06:00:00Z')
        attach(a, payload(NOW, 2, 2), state, match()['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
        self.assertEqual(row['oci'], 0)
        self.assertEqual(row['market_behavior'], 'neutral')
        self.assertEqual(row['margin_trend'], 'compressing')

    def test_stale_prices_and_only_one_book_do_not_create_confirmation(self):
        a = deepcopy(self.a)
        attach(a, payload('2026-10-25T21:00:00Z'), {}, match()['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
        self.assertEqual(row['quote_status'], 'stale')
        self.assertIsNone(row['odd'])
        state = {}
        attach(a, payload('2026-10-26T06:00:00Z', books=1), state, match()['fixture'], '2026-10-26T06:00:00Z')
        attach(a, payload(NOW, over=1.7, books=1), state, match()['fixture'], NOW)
        self.assertIsNone(row.get('oci'))
        expire(a, match()['fixture'], '2026-10-26T18:00:00Z')
        self.assertEqual(row['quote_status'], 'stale')
        self.assertIsNone(row['ev_pct'])
        self.assertIsNone(row['margin_pct'])
        self.assertIsNone(row['oci_previous'])
        self.assertFalse(row['theoretical_value'])

    def test_compatible_old_quote_history_is_preserved(self):
        from multimarket_model import collect_quotes, update_ledger
        state = {}
        old = '2026-10-26T06:00:00Z'
        quotes = collect_quotes(payload(old, over=2.5), old)['100']
        update_ledger(state, 100, quotes, old, KICKOFF)
        a = deepcopy(self.a)
        attach(a, payload(NOW, over=2.1), state, match()['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('total', 'ft', 'over', 2.5))
        self.assertEqual(row['first_odd'], 2.5)
        self.assertEqual(row['paired_books'], 2)

    def test_parser_keeps_actual_combo_and_overlapping_dc_has_no_fake_margin(self):
        self.assertEqual(parse_value(183, 'Home/Draw / Over 1.5'), S('dc_total', 'ft', '1x', 1.5, 'over'))
        self.assertEqual(parse_value(49, 'Over 2.5/Yes'), S('btts_total', 'ft', 'yes', 2.5, 'over'))
        a = deepcopy(self.a)
        attach(a, payload(bid=12, name='Double Chance', values=[{'value': s, 'odd': '1.5'} for s in ('Home/Draw', 'Draw/Away', 'Home/Away')]), {}, match()['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('dc', 'ft', '1x'))
        self.assertEqual(row['odd'], 1.5)
        self.assertIsNone(row['margin_pct'])

    def test_provider_handicap_is_home_anchored_and_partitions_are_complements(self):
        for bid, period in ((4, 'ft'), (19, 'ht'), (104, 'sh')):
            self.assertEqual(parse_value(bid, 'Away -1.5'), S('handicap', period, 'away', 1.5))
            self.assertEqual(parse_value(bid, 'Away +3.5'), S('handicap', period, 'away', -3.5))
        values = [{'value': 'Home +3.5', 'odd': '1.02'}, {'value': 'Away +3.5', 'odd': '12'},
                  {'value': 'Home -3.5', 'odd': '13'}, {'value': 'Away -3.5', 'odd': '1.01'},
                  {'value': 'Home +0', 'odd': '1.8'}, {'value': 'Away +0', 'odd': '2.1'}]
        a = deepcopy(self.a)
        attach(a, payload(bid=4, name='Asian Handicap', values=values), {}, match()['fixture'], NOW)
        rows = {r['id']: r for r in a['matrix']['rows']}
        easy = rows[market_id(S('handicap', 'ft', 'away', 3.5))]
        hard = rows[market_id(S('handicap', 'ft', 'away', -3.5))]
        self.assertEqual(easy['odd'], 1.01)
        self.assertEqual(hard['odd'], 12)
        self.assertGreater(easy['statistical_pct'], hard['statistical_pct'])
        self.assertAlmostEqual(hard['margin_pct'], 100*(1/1.02+1/12-1))
        self.assertIsNotNone(rows[market_id(S('handicap', 'ft', 'away', 0))]['margin_pct'])
        self.assertEqual(payout_weights(hard['spec'], 0, 2), (0, 1))
        self.assertEqual(payout_weights(easy['spec'], 2, 0), (1, 0))

    def test_legacy_handicap_repair_preserves_observations_and_started_forecasts(self):
        early = '2026-10-26T06:00:00Z'
        fixture = {'kickoff': KICKOFF, 'groups': {
            '4|handicap|ft|3.5|1': [
                {'odds': {'home': 1.02, 'away': 1.01}, 'complete': True, 'provider_updated_at': early, 'observed_at': early}],
            '4|handicap|ft|-3.5|1': [
                {'odds': {'home': 13., 'away': 12.}, 'complete': True, 'provider_updated_at': early, 'observed_at': early}]}}
        original = deepcopy(fixture['groups'])
        migrate_handicap_history(fixture)
        self.assertEqual(fixture['groups']['ah2|4|handicap|ft|3.5|1'][0]['odds'], {'home': 1.02, 'away': 12.})
        self.assertEqual(fixture['groups']['ah2|4|handicap|ft|-3.5|1'][0]['odds'], {'away': 1.01, 'home': 13.})
        for key in original: self.assertEqual(fixture['groups'][key], original[key])
        once = deepcopy(fixture); migrate_handicap_history(fixture); self.assertEqual(fixture, once)
        state = {'matrix_quotes': {'100': fixture}}
        m = {'fixture': deepcopy(match()['fixture']), 'multimarket': deepcopy(self.a)}
        m['fixture']['status_short'] = 'NS'
        day = {'date': '2026-10-26', 'matches': [m]}
        before_rates = deepcopy(m['multimarket']['rates'])
        self.assertEqual(repair_day(day, state, NOW)[0], 1)
        easy = next(r for r in m['multimarket']['matrix']['rows'] if r['spec'] == S('handicap', 'ft', 'away', 3.5))
        self.assertEqual(easy['odd'], 1.01)
        self.assertEqual(easy['quote_observed_at'], early)
        self.assertEqual(m['multimarket']['rates'], before_rates)
        frozen = deepcopy(day)
        self.assertEqual(repair_day(day, state, '2026-10-27T08:00:00Z'), (0, 0))
        self.assertEqual(day, frozen)

    def test_legacy_handicap_prices_cannot_inflate_audit_returns(self):
        a = deepcopy(self.a)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('handicap', 'ft', 'away', .75))
        row.update(odd=12., quote_status='recent', category='value_watch')
        a['matrix']['rows'] = [row]
        record = {'fixture': match()['fixture'], 'captured_at': NOW, 'multimarket': a,
            'settlement': {'result': {'status': 'FT', 'home_ft': 1, 'away_ft': 1}}}
        audit = MatrixAccumulator(); audit.add(record)
        g = next(g for g in audit.report()['groups'] if g['dimension'] == 'market')
        self.assertEqual(g['priced_observations'], 0)
        self.assertIsNone(g['paper_roi_pct'])
        self.assertEqual(row['odd'], 12.)

    def test_audit_asian_half_loss_and_no_postkickoff_leak(self):
        a = deepcopy(self.a)
        row = next(r for r in a['matrix']['rows'] if r['spec'] == S('asian_total', 'ft', 'over', 2.25))
        row.update(odd=2., quote_status='recent', category='contrarian')
        a['matrix']['rows'] = [row]
        record = {'fixture': match()['fixture'], 'captured_at': NOW, 'multimarket': a,
                  'settlement': {'result': {'status': 'FT', 'home_ft': 1, 'away_ft': 1, 'home_ht': 0, 'away_ht': 0}}}
        audit = MatrixAccumulator(); audit.add(record)
        g = next(g for g in audit.report()['groups'] if g['dimension'] == 'market')
        self.assertEqual(g['paper_roi_pct'], -50)
        self.assertEqual(g['binary_observations'], 0)
        late = deepcopy(record); late['captured_at'] = '2026-10-27T08:00:00Z'
        audit = MatrixAccumulator(); audit.add(late)
        self.assertEqual(audit.report()['frozen_fixtures'], 0)


if __name__ == '__main__': unittest.main()
