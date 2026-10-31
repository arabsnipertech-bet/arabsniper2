import unittest
from copy import deepcopy

from gold_competitions import TOP, SECOND, THIRD, EUROPEAN, SCOPE_ID, profile, fixture_allowed, enforce_day
from matrix_pipeline import complete_day, build_board
from refresh_opportunities import cached_refresh_safe
from test_statistics_first import NOW, KICKOFF


class ThreeDivisionScope(unittest.TestCase):
    def test_country_scope_is_unchanged_and_aliases_have_exact_levels(self):
        self.assertEqual(set(TOP), set(SECOND))
        self.assertEqual(set(TOP), set(THIRD))
        for level, mapping in ((2, SECOND), (3, THIRD)):
            for country, names in mapping.items():
                self.assertTrue(names, country)
                for name in names:
                    with self.subTest(country=country, name=name):
                        result = profile({'country': country, 'name': name})
                        excluded = (level == 2 and country in ('ukraine', 'slovakia') or
                                    level == 3 and country in EUROPEAN and country not in ('italy', 'england'))
                        self.assertEqual(result['allowed'], not excluded)
                        self.assertEqual(result['tier'], 'blocked' if excluded else f'senior_division_{level}')
                        self.assertEqual(result['policy_id'], SCOPE_ID)

    def test_numeric_names_do_not_admit_fourth_tiers_or_other_countries(self):
        blocked = [('Italy', 'Serie D - Girone A'), ('Italy', 'Serie C - Girone D'),
                   ('Italy', 'Coppa Italia Serie C'), ('Italy', 'Serie C - Supercoppa Lega Finals'),
                   ('England', 'National League'), ('Germany', 'Regionalliga - West'),
                   ('France', 'National 2 - Group A'), ('Finland', 'Kakkonen - Lohko A'),
                   ('Croatia', 'Third NL - Istok'), ('Poland', 'III Liga - Group 1'),
                   ('Norway', '3. Division - Girone 1'), ('Turkey', '3. Lig - Group 1'),
                   ('Spain', 'Segunda Division RFEF - Group 1'), ('Netherlands', 'Derde Divisie'),
                   ('Brazil', 'Serie D'), ('Japan', 'Japan Football League'), ('South-Korea', 'K4 League'),
                   ('USA', 'USL League Two'), ('Argentina', 'Primera C'), ('Belgium', 'Challenger Pro League'),
                   ('Egypt', 'Second League'), ('Russia', 'First League'), ('Romania', 'Liga III - Serie 11'),
                   ('Italy', 'Serie B Women'), ('Italy', 'Serie B U19')]
        for country, name in blocked:
            with self.subTest(country=country, name=name):
                self.assertFalse(profile({'country': country, 'name': name})['allowed'])

    def test_new_divisions_still_reject_youth_and_reserve_teams(self):
        for team in ('Milan U23', 'Juventus U19', 'Barcelona B', 'Bayern II', 'Jong Ajax'):
            raw = {'fixture': {'id': 1}, 'league': {'country': 'Italy', 'name': 'Serie C - Girone A'},
                   'teams': {'home': {'name': team}, 'away': {'name': 'Senior Club'}}}
            self.assertFalse(fixture_allowed(raw), team)

    def test_schedule_recovers_serie_b_and_all_serie_c_groups_without_forcing_gold(self):
        names = ['Serie B', 'Serie C - Girone A', 'Serie C - Girone B', 'Serie C - Girone C', 'Serie D - Girone A']
        raw = [{'fixture': {'id': i, 'date': KICKOFF, 'status': {'short': 'NS'}},
                'league': {'country': 'Italy', 'name': name}}
               for i, name in enumerate(names, 1)]
        class Client:
            def get(self, path, params):
                return {'response': raw}
        ns = {'fixture_meta': lambda r: {'fixture_id': str(r['fixture']['id']),
                    'league': r['league']['name'], 'country': r['league']['country'],
                    'kickoff': r['fixture']['date'], 'status_short': 'NS'},
              '_result_payload': lambda r: None}
        day = {'date': '2026-10-26', 'matches': [],
               'gold_scope': {'policy_id': 'gold-senior-whitelist-v1', 'excluded_fixtures': 10,
                              'exclusion_reasons': {'competition_not_whitelisted': 10}}}
        complete_day(ns, Client(), day, NOW)
        build_board(day, NOW)
        self.assertEqual([m['fixture']['league'] for m in day['matches']], names[:4])
        self.assertEqual(day['gold_scope']['policy_id'], SCOPE_ID)
        self.assertEqual(day['universal_coverage']['excluded_by_competition'], 1)
        self.assertTrue(all(not m['multimarket'].get('best_candidate') for m in day['matches']))
        before = deepcopy(day)
        enforce_day(day)
        self.assertEqual(day, before)

    def test_expanding_whitelist_cannot_take_cached_price_only_shortcut(self):
        self.assertFalse(cached_refresh_safe(['src/gold_competitions.py']))
        self.assertFalse(cached_refresh_safe(['src/gold_competitions.py', 'src/statistics_integration.py',
                                            'src/refresh_opportunities.py', 'web/lateral-matrix.js']))
        self.assertTrue(cached_refresh_safe(['src/opportunity_policy.py', 'tests/test_opportunity_policy.py']))


if __name__ == '__main__':
    unittest.main()
