from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from market_discovery import identify, VERSION
from odds_collection import OddsEnricher, coverage, merge_payload
from test_quote_sources import bet, feed, NOW
from test_policy_engine import engine


class SupplementalDiscoveryTests(unittest.TestCase):
    def test_asian_or_alternative_id_recovers_and_clears_no_gain_streak(self):
        for bid, line, name, market in [(50, 2.5, 'Goal Line', 'over25_ft'),
                (72, 1.5, 'Goal Line (1st Half)', 'over15_ht'),
                (336, 3.5, 'Goal Line', 'over35_ft'),
                (9001, 2.5, 'Goals Over/Under Main', 'over25_ft'),
                (9002, 1.5, 'Alternative Total Goals 1st Half', 'over15_ht')]:
            with self.subTest(bid=bid):
                extra = feed([bet(bid, line, name=name)], book=998)
                base = feed(book=999)
                client = Mock(); client.request_count = 0
                def get(path, params, **kwargs):
                    client.request_count += 1
                    return {'response': {'requests': {'current': 100, 'limit_day': 75000}}} if path == 'status' else deepcopy(extra)
                client.get.side_effect = get
                state = {'discovery_version': VERSION, 'route_health': {'bet': {'no_gain_streak': 23}}}
                enricher = OddsEnricher(client, state, NOW, max_calls=1,
                                       market_catalog=[{'id': bid, 'name': name}])
                result = enricher.enrich(base, 100)
                self.assertIn((market, '998'), coverage(result, 100, NOW))
                self.assertEqual(enricher.stats['odds_targeted_markets_recovered'], 1)
                self.assertEqual(enricher.stats['odds_targeted_no_gain'], 0)
                self.assertFalse(state['route_health']['bet'])

    def test_neutral_qualifiers_preserve_identity_and_numeric_line(self):
        for qualifier in ('Main', 'Alternative', 'Alt', 'Regular', 'Regulation Time',
                          '2 way', '2way', '2-way', 'Standard', 'Markets', 'Market', 'Full'):
            with self.subTest(qualifier=qualifier):
                name = f'Goals Over/Under {qualifier}'
                identity = identify(name)
                self.assertEqual((identity['kind'], identity['period']), ('total', 'ft'))
                self.assertIn(('over25_ft','999'), coverage(feed([bet(900,2.5,name=name)]),100,NOW))
        for name, kind in [('BTTS Main','btts'), ('DNB Regular Market','dnb'),
                           ('Asian Handicap 2way','handicap'), ('Home/Away Main','dnb')]:
            self.assertEqual(identify(name)['kind'], kind)
        raw = {'id': 999, 'name': 'Goals Over/Under 2-way 2.5 Main',
               'values': [{'value':'Over','odd':1.9},{'value':'Under','odd':1.9}]}
        self.assertIn(('over25_ft','999'), coverage(feed([raw]),100,NOW))

    def test_neutral_words_cannot_hide_wrong_sport_statistic_team_or_combo(self):
        names = ['Goals Over/Under Main Corners', 'Goals Main Cards', 'Goals Over/Under Team Total',
                 'Goals Over/Under Main Home', 'Goals Over/Under Main 15 minutes',
                 'Goals Over/Under Main Extra Time', 'Goals Main Multigoal', 'Goals Main Scorer',
                 'BTTS Main and Over 2.5', 'DNB Regular and Over 2.5',
                 'Asian Handicap Main and Goals', 'Goals Main 1st Half 2nd Half',
                 'Goals Main First', 'Goals Main Half', 'Goals Main 2nd', 'Goals Main 1st Quarter']
        for name in names:
            with self.subTest(name=name): self.assertIsNone(identify(name))

    def test_merge_is_detached_and_keeps_whole_markets(self):
        first, second = feed(), feed([bet(72,1.5)], book=998)
        originals = deepcopy((first, second)); result = merge_payload(first,second,100,NOW)
        result['response'][0]['fixture']['id'] = 9999
        result['response'][0]['bookmakers'][0]['bets'][0]['values'][0]['odd'] = 99
        self.assertEqual((first, second), originals)

    def test_parser_upgrade_reassesses_paused_routes_without_erasing_prices(self):
        state = {'discovery_version': 'football-market-discovery-v1', 'bookmakers': [],
                 'route_health': {'bet': {'paused_at': NOW}}, 'queries': {'100:bet:900': NOW},
                 'quote_sentinel': {'first': 1.9}}
        OddsEnricher(Mock(), state, NOW)
        self.assertNotIn('route_health', state); self.assertNotIn('queries', state)
        self.assertEqual(state['quote_sentinel'], {'first':1.9})

    def test_base_extractors_normalize_without_lowering_signal_minimum(self):
        native, alternative = feed([bet(5,2.5)]), feed([bet(900,2.5,name='Goals Over/Under Main')])
        asian = feed([bet(50,2.5)])
        with patch.object(engine,'iso_now',return_value=NOW):
            baseline = engine.extract_bookmaker_markets(native)
            for raw in (alternative, asian):
                self.assertEqual(engine.extract_bookmaker_markets(raw), baseline)
                _, audit = engine.extract_bookmaker_markets_with_audit(raw)
                _, original = engine.extract_bookmaker_markets_with_audit(native)
                self.assertEqual(audit[engine.MARKET_O25]['usable_for_signals'],
                                 original[engine.MARKET_O25]['usable_for_signals'])
                self.assertFalse(audit[engine.MARKET_O25]['usable_for_signals'])
            wrong = feed([bet(5,2.5,name='Goals Over/Under Main Cards')])
            for extractor in (engine.extract_bookmaker_markets,
                              lambda p: engine.extract_bookmaker_markets_with_audit(p)[0]):
                self.assertFalse(extractor(wrong)['999']['markets'])


if __name__ == '__main__':
    unittest.main()
