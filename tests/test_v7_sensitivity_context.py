"""A prudential probe must omit a game from every statistical input it uses."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))

from lateral_matrix import build_matrix, specification, market_id
from statistics_first import statistical_analysis, frozen_goal_observations
from test_statistics_first import match, NOW
from v7_decision import capture, scenario_means
from v7_probability import raw_rates, statistical_rates, blend_rates, score_model


GG_ST = market_id(specification('btts', 'sh', 'yes'))


def fixture():
    # Fixed synthetic stress case. These are not the reported match's results.
    item = match()
    scores = {
        'home': [(0,0,0,0), (0,2,1,6), (2,2,5,3), (1,2,4,2),
                 (2,2,5,2), (2,2,3,3), (2,2,2,3), (0,2,2,2)],
        'away': [(2,2,3,2), (0,1,2,4), (0,2,1,3), (2,2,2,2),
                 (0,1,0,2), (1,2,2,3), (0,2,0,3), (1,1,1,3)]}
    for side, games in scores.items():
        for row, (gf_ht, ga_ht, gf_ft, ga_ft) in zip(item['statistics'][side]['last_matches'], games):
            row.update(team_goals_ht=gf_ht, team_conceded_ht=ga_ht,
                       team_goals_ft=gf_ft, team_conceded_ft=ga_ft)
    return item


def priced(item, gg_odd=2.7):
    assessment = build_matrix(statistical_analysis(item, NOW), NOW)
    for row in assessment['matrix']['rows']:
        spec = row['spec']
        fair = None
        if spec['kind'] == 'result' and spec['period'] == 'ft':
            fair = {'1': .2, 'x': .25, '2': .55}[spec['side']]
            row['selected_market_odds'] = {s: 1/(p*1.05) for s,p in
                                           {'1':.2, 'x':.25, '2':.55}.items()}
        elif spec['kind'] == 'total' and spec['side'] == 'over':
            fair = {('ft',2.5):.625, ('ht',1.5):.424, ('sh',1.5):.515}.get(
                (spec['period'], spec['line']))
        elif row['id'] == GG_ST:
            fair = 1/(gg_odd*1.05)
        if fair is not None:
            odd = 1/(fair*1.05)
            row.update(odd=odd, first_odd=odd, quote_status='recent', quote_source='bookmaker',
                       quote_executable=True, bookmaker_id='test', bookmaker='Synthetic operator',
                       quote_observed_at=NOW, provider_updated_at=NOW,
                       selected_margin_pct=5., selected_market_fair_pct=100*fair)
    return assessment


class ScenarioInputs(unittest.TestCase):
    def test_removed_observation_does_not_remain_in_second_half_means(self):
        a = statistical_analysis(fixture(), NOW)
        before = deepcopy(a)
        source = next((i, s) for i, s in enumerate(a['sensitivity_sources'])
                      if s['omitted'] == {'side':'home', 'fixture_id':'home-3'})
        i, s = source
        actual = scenario_means(a, s['rates'], i, NOW)
        histories = deepcopy(a['inputs']['histories'])
        histories['home'] = [r for r in histories['home'] if r['fixture_id'] != 'home-3']
        expected = frozen_goal_observations(histories, {}, NOW)[0]['teams']
        self.assertEqual(actual, expected)
        self.assertNotEqual(actual['home']['st'], a['observed_goal_means']['teams']['home']['st'])
        self.assertEqual(a, before)

    def test_probe_order_survives_json_and_binds_rates_to_omitted_game(self):
        import json
        a = json.loads(json.dumps(statistical_analysis(fixture(), NOW)))
        self.assertEqual(len(a['sensitivity_sources']), len(a['sensitivity_rates']))
        for i, rates in enumerate(a['sensitivity_rates']):
            self.assertIsNotNone(scenario_means(a, rates, i, NOW))
        a['sensitivity_sources'][1]['rates']['home_ft'] += .01
        self.assertIsNone(scenario_means(a, a['sensitivity_rates'][1], 1, NOW))

    def test_missing_or_ambiguous_evidence_is_not_claimed_as_verified(self):
        a = statistical_analysis(fixture(), NOW)
        rates = a['sensitivity_rates'][1]
        for change in ('missing_sources', 'missing_history', 'missing_id', 'duplicate_id'):
            b = deepcopy(a)
            if change == 'missing_sources': b.pop('sensitivity_sources')
            elif change == 'missing_history': b['inputs']['histories'].pop('away')
            elif change == 'missing_id': b['sensitivity_sources'][1]['omitted']['fixture_id'] = 'absent'
            else: b['inputs']['histories']['home'].append(deepcopy(b['inputs']['histories']['home'][0]))
            self.assertIsNone(scenario_means(b, rates, 1, NOW), change)

    def test_fixed_projection_can_falsely_pass_the_existing_gold_ev_low_floor(self):
        a = statistical_analysis(fixture(), NOW)
        means = a['observed_goal_means']['teams']
        projection = dict(home_ht=.6, away_ht=.7, home_st=.8, away_st=.9,
                          home_ft=1.4, away_ft=1.6)
        def chance(rates, observed):
            stats = statistical_rates(raw_rates({'rates':rates}), observed)
            return score_model(blend_rates(stats, projection, .8)).estimate(
                specification('btts','sh','yes'))['p']
        central = chance(a['rates'], means)
        old = min([central, *(chance(r, means) for r in a['sensitivity_rates'])])
        consistent = min([central, *(chance(r, scenario_means(a,r,i,NOW))
                                    for i,r in enumerate(a['sensitivity_rates']))])
        # An explicit common price demonstrates the existing +2% admission boundary.
        odd = 1.02/((old+consistent)/2)
        self.assertGreater(old*odd-1, .02)
        self.assertLess(consistent*odd-1, .02)
        self.assertGreater(central*odd-1, 0)


class CompleteEstimator(unittest.TestCase):
    def test_each_bound_probe_matches_reanalysis_after_the_same_game_is_removed(self):
        item = fixture()
        a = priced(item)
        snapshot = capture(a, item['fixture'], NOW)
        row = next(r for r in a['matrix']['rows'] if r['id'] == GG_ST)
        actual = row['sensitivity_weights']
        expected = [snapshot['markets'][GG_ST]['calibrated']]
        for source in a['sensitivity_sources']:
            trial = deepcopy(item)
            omitted = source['omitted']
            if omitted:
                side, fid = omitted['side'], omitted['fixture_id']
                trial['statistics'][side]['last_matches'] = [r for r in
                    trial['statistics'][side]['last_matches'] if r['fixture_id'] != fid]
            reference = capture(priced(trial), trial['fixture'], NOW)
            expected.append(reference['markets'][GG_ST]['calibrated'])
        for p in expected:
            self.assertTrue(any(abs(w-p) < 6e-8 for w, loss in actual), p)
        self.assertAlmostEqual(row['low_pct']/100, min(expected), places=6)
        self.assertTrue(snapshot['sensitivity']['complete'])
        self.assertEqual(snapshot['sensitivity']['verified_contexts'], 17)

    def test_central_probability_price_and_ev_are_unchanged(self):
        item = fixture()
        old_a, new_a = priced(item, 2.75), priced(item, 2.75)
        with patch('v7_decision.scenario_means', return_value=None):
            old = capture(old_a, item['fixture'], NOW)
        new = capture(new_a, item['fixture'], NOW)
        for key, market in new['markets'].items():
            for field in ('calibrated', 'raw', 'statistics', 'corrected', 'odd', 'ev', 'fair'):
                self.assertEqual(market[field], old['markets'][key][field], (key, field))
        self.assertTrue(any(new['markets'][key]['ev_low'] != old['markets'][key]['ev_low']
                            for key in new['markets']))
        old_row = next(r for r in old_a['matrix']['rows'] if r['id'] == GG_ST)
        new_row = next(r for r in new_a['matrix']['rows'] if r['id'] == GG_ST)
        self.assertTrue(old_row['opportunity']['eligible'])
        self.assertFalse(new_row['opportunity']['eligible'])
        self.assertEqual(new_row['opportunity']['reasons'], ['sensitivity'])
        self.assertEqual(old['sensitivity']['legacy_fallback_contexts'], 17)

    def test_legacy_snapshot_keeps_its_bounds_and_reports_missing_context(self):
        item = fixture()
        a = priced(item)
        a.pop('sensitivity_sources')
        snapshot = capture(a, item['fixture'], NOW)
        self.assertFalse(snapshot['sensitivity']['complete'])
        self.assertEqual(snapshot['sensitivity']['verified_contexts'], 0)
        self.assertEqual(snapshot['sensitivity']['legacy_fallback_contexts'], 17)


if __name__ == '__main__':
    unittest.main()
