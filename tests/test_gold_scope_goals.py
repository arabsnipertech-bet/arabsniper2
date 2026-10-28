import json
import unittest
from copy import deepcopy
from gold_competitions import (TOP, CUPS, SCOPE_ID, profile, fixture_allowed,
                               output_allowed, enforce_day)
from opportunity_policy import (annotate, scoring_gap, goal_context, evaluate,
                                 best_opportunity, POLICY_ID)
from refresh_opportunities import refresh_day
from test_opportunity_policy import row
from test_statistics_first import match, NOW
from lateral_matrix import specification as S
from lateral_matrix import build_matrix, market_id
from statistics_first import statistical_analysis
from matrix_pipeline import build_board, selection_export


def history_for(side, gap=False):
    goals = [1, 1, 1, 3, 3, 3, 3, 3] if gap else [2]*8
    return [{'fixture_id': f'{side}-{i}', 'side': side if i % 2 == 0 else ('away' if side == 'home' else 'home'),
        'team_goals_ft': g, 'team_conceded_ft': 2, 'team_conceded_ht': 1, 'team_goals_ht': min(1, g), 'halftime_available': True}
        for i, g in enumerate(goals)]


def flow(favorite='1', balanced_gap=None):
    odds = {'1': 1.6, 'x': 4.3, '2': 5.5} if favorite == '1' else {'1': 5.5, 'x': 4.3, '2': 1.6}
    if balanced_gap is not None: odds = {'1': 2.5, 'x': 3.5, '2': 2.5+balanced_gap}
    return row(id='flow', spec=S('result', 'ft', favorite), family='1x2',
        odd=odds[favorite], selected_market_odds=odds,
        selected_margin_pct=100*(sum(1/q for q in odds.values())-1), selected_margin_delta_pp=0.)


def goal_rows():
    return [row(id='ft25', odd=2.0, statistical_pct=70., ev_pct=26., ev_low_pct=12.),
        row(id='ft35', spec=S('total', 'ft', 'over', 3.5), odd=2.7, statistical_pct=45., ev_pct=21.5, ev_low_pct=8.),
        row(id='ht15', spec=S('total', 'ht', 'over', 1.5), period='ht', odd=2.1, statistical_pct=55., ev_pct=10., ev_low_pct=5.),
        row(id='gght', spec=S('btts', 'ht', 'yes'), period='ht', odd=3.5, statistical_pct=40., ev_pct=20., ev_low_pct=6.)]


def example(gap_side='home', favorite='1', balanced_gap=None):
    a = {'rates': {'home_ft': 2., 'away_ft': 2., 'home_ht': 1., 'away_ht': 1.}, 'inputs': {'histories': {s: history_for(s, s == gap_side) for s in ('home', 'away')}},
         'matrix': {'rows': goal_rows()+[flow(favorite, balanced_gap)]}}
    return a


class GoldScope(unittest.TestCase):
    def test_each_country_and_explicit_domestic_cup_is_allowed(self):
        for country, aliases in TOP.items():
            for name in aliases: self.assertTrue(profile({'country': country, 'name': name})['allowed'], (country, name))
        for country, aliases in CUPS.items():
            for name in aliases: self.assertTrue(profile({'country': country, 'name': name})['allowed'], (country, name))
        self.assertTrue(profile({'country': 'Romania', 'name': 'Cupa României'})['allowed'])
        self.assertTrue(profile({'country': 'Czech-Republic', 'name': 'Czech Liga'})['allowed'])
        self.assertTrue(profile({'country': 'South-Korea', 'name': 'K League 1'})['allowed'])

    def test_minor_youth_unknown_and_unlisted_cups_are_blocked(self):
        for country, name in [('Romania', 'Liga IV'), ('Cyprus', '1. Division'), ('Latvia', 'Virsliga'),
            ('Lithuania', 'A Lyga'), ('Italy', 'Serie D - Girone A'), ('Italy', 'Primavera U19'),
            ('England', 'Premier League 2'), ('Japan', 'Japan Football League'), ('USA', 'USL League Two'),
            ('Belgium', 'Jupiler Pro League'), ('Brazil', 'Copa Rio'), ('Brazil', 'Copa do Brasil'),
            ('Australia', 'Australia Cup'), ('World', 'UEFA Youth League'), ('World', 'AFC Cup'),
            ('World', 'Friendlies Clubs'), ('Italy', 'Serie A Women'), ('Unknown', 'Premier League')]:
            self.assertFalse(profile({'country': country, 'name': name, 'type': 'Cup'})['allowed'], (country, name))
        self.assertTrue(profile({'country': 'World', 'name': 'UEFA Champions League'})['allowed'])
        raw = {'fixture': {'id': 1}, 'league': {'country': 'Italy', 'name': 'Serie A'},
            'teams': {'home': {'name': 'Milan U19'}, 'away': {'name': 'Juventus'}}}
        self.assertFalse(fixture_allowed(raw))
        for name in ('Jong Ajax', 'Bayern II', 'Barcelona B'):
            raw['teams']['home']['name'] = name
            self.assertFalse(fixture_allowed(raw))

    def test_saved_output_filtered_without_destroying_archive_or_reappearing(self):
        allowed, excluded = match('1'), match('2')
        allowed['fixture'].update(country='Romania', league='Liga I')
        excluded['fixture'].update(country='Cyprus', league='1. Division')
        original = deepcopy(excluded)
        day = {'matches': [allowed, excluded]}
        enforce_day(day); once = deepcopy(day); enforce_day(day)
        self.assertEqual(day, once)
        self.assertEqual(excluded, original)
        self.assertEqual([m['fixture']['fixture_id'] for m in day['matches']], ['1'])
        self.assertEqual(day['gold_scope']['excluded_fixtures'], 1)
        self.assertEqual(day['gold_scope']['policy_id'], SCOPE_ID)
        self.assertTrue(output_allowed(allowed))

    def test_all_excluded_refresh_is_an_explicit_empty_schedule(self):
        m = match(); m['fixture'].update(country='Latvia', league='Virsliga')
        day = {'date': '2026-10-26', 'matches': [m]}
        self.assertEqual(refresh_day(day, {}, NOW), 0)
        self.assertEqual(day['matches'], [])
        self.assertEqual(day['matrix_summary']['displayed_fixtures'], 0)
        self.assertTrue(day['gold_scope']['romania_allowed'])

    def test_same_policy_new_exclusions_rebuild_empty_exports(self):
        day = {'date': '2026-10-26', 'matches': []}
        refresh_day(day, {}, NOW)
        m = match(); m['fixture'].update(country='Cyprus', league='1. Division')
        day['matches'].append(m)
        day['market_rankings'] = {'over25_ft': [{'fixture_id': 'obsolete'}]}
        refresh_day(day, {}, NOW)
        self.assertFalse(day['matches'])
        self.assertTrue(all(not rows for rows in selection_export(day, NOW)['rankings'].values()))


class GoalPriority(unittest.TestCase):
    def test_legacy_exports_and_gold_share_pt_choice(self):
        m = match(); a = build_matrix(statistical_analysis(m, NOW), NOW)
        context = example('away', '1')
        a['inputs'] = context['inputs']
        current = {market_id(r['spec']): r for r in a['matrix']['rows']}
        for source in context['matrix']['rows']:
            target = current[market_id(source['spec'])]
            target.update({k: v for k, v in source.items() if k != 'id'})
        m['multimarket'] = a
        day = {'date': '2026-10-26', 'matches': [m]}
        build_board(day, NOW)
        self.assertEqual(a['best_candidate']['market'], 'gg_ht')
        self.assertEqual(m['display']['primary_choice']['market'], 'gg_ht')
        self.assertEqual(m['display']['ranked_choices'][0]['market'], 'gg_ht')
        export = selection_export(day, NOW)
        self.assertEqual(export['opportunity_policy'], POLICY_ID)
        self.assertEqual(export['rankings']['gg_ht'][0]['published_rank'], 1)

    def test_favorite_gap_home_and_away_prefers_over_pt(self):
        for side, favorite in [('home', '1'), ('away', '2')]:
            a = example(side, favorite)
            before = [(r['id'], r['statistical_pct']) for r in a['matrix']['rows']]
            best = annotate(a['matrix'], NOW, a)
            self.assertEqual(best['id'], 'ht15')
            self.assertEqual(a['matrix']['goal_context']['drought_role'], 'favorite_'+side)
            self.assertEqual(a['matrix']['goal_context']['imbalance']['favorite_venue'], side)
            self.assertEqual(a['matrix']['goal_context']['probability_bonus_pp'], 0)
            self.assertEqual(before, [(r['id'], r['statistical_pct']) for r in a['matrix']['rows']])

    def test_underdog_is_not_synonymous_with_away_and_prefers_goal_pt(self):
        for side, favorite in [('away', '1'), ('home', '2')]:
            a = example(side, favorite)
            self.assertEqual(annotate(a['matrix'], NOW, a)['id'], 'gght')
            self.assertEqual(a['matrix']['goal_context']['drought_role'], 'underdog_'+side)

    def test_balanced_boundary_and_no_forced_winner(self):
        for gap in (0., .3, .4):
            a = example('home', '1', gap)
            best = annotate(a['matrix'], NOW, a)
            context = a['matrix']['goal_context']
            self.assertIsNone(context['imbalance']['favorite'])
            self.assertIsNone(context['preferred_pt_market'])
            self.assertIn(best['id'], ('ht15', 'gght'))
        a = example('home', '1', .40001)
        annotate(a['matrix'], NOW, a)
        self.assertFalse(a['matrix']['goal_context']['pt_priority_active'])

    def test_venue_conflict_or_insufficient_data_does_not_trigger_priority(self):
        a = example('home')
        history = a['inputs']['histories']['home']
        for i, r in enumerate(history): r['side'] = 'away' if i < 3 else 'home'
        annotate(a['matrix'], NOW, a)
        c = a['matrix']['goal_context']
        self.assertTrue(c['trends']['home']['ft']['active'])
        self.assertFalse(c['pt_priority_active'])
        a['inputs']['histories']['home'] = history[:3]
        annotate(a['matrix'], NOW, a)
        self.assertFalse(a['matrix']['goal_context']['pt_priority_active'])

    def test_preferred_pt_falls_back_only_to_eligible_goal_markets(self):
        a = example('away', '1')
        gg = a['matrix']['rows'][3]; gg['statistical_pct'] = 25.99
        self.assertEqual(annotate(a['matrix'], NOW, a)['id'], 'ht15')
        a['matrix']['rows'][2]['odd'] = 3.5001
        self.assertEqual(annotate(a['matrix'], NOW, a)['id'], 'ft25')
        for r in a['matrix']['rows']: r['odd'] = 150
        self.assertIsNone(annotate(a['matrix'], NOW, a))

    def test_missing_anomalous_or_nonpersistent_flow_does_not_trigger_pt(self):
        for update in ({'oci': None}, {'paired_books': 1}, {'selected_margin_pct': -1},
                       {'selected_margin_pct': 12}, {'selected_margin_delta_pp': 2},
                       {'selected_market_odds': {'1': 1.6}}, {'spec': S('result', 'ft', 'x')},
                       {'selected_movement': None}, {'quote_status': 'stale'}):
            a = example(); a['matrix']['rows'][-1].update(update)
            annotate(a['matrix'], NOW, a)
            self.assertFalse(a['matrix']['goal_context']['pt_priority_active'], update)

    def test_1x2_flow_never_becomes_a_goal_market_confirmation(self):
        a = example()
        for r in a['matrix']['rows'][:-1]: r['oci'] = None
        best = annotate(a['matrix'], NOW, a)
        self.assertEqual(best['id'], 'ht15')
        self.assertEqual(best['opportunity']['signal'], 'unconfirmed')

    def test_goal_subset_incoherence_blocks_both_proposals(self):
        rows = goal_rows()
        rows[1]['statistical_pct'] = 80
        rows[3]['statistical_pct'] = 60
        self.assertIsNone(annotate({'rows': rows}))
        self.assertTrue(all('coherence' in r['opportunity']['reasons'] for r in rows))

    def test_contrarian_requires_controlled_margin_not_expansion(self):
        r = goal_rows()[0]
        r.update(oci=-60, selected_movement={'delta_pct': 5, 'fair_delta_pp': -2, 'persistence': 1, 'reversal': False})
        self.assertIn('oci_negative', evaluate(r)['reasons'])
        r['selected_margin_pct'] = 10
        self.assertIn('margin', evaluate(r)['reasons'])


if __name__ == '__main__': unittest.main()

