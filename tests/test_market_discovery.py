"""Semantic discovery, provenance and retention regression cases."""
import sys, unittest
from pathlib import Path
from copy import deepcopy
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from market_discovery import identify, discover_payload
from quote_sources import normalize_payload, synthetic_margin
from matrix_quotes import collect, ledger_offers, attach
from matrix_pipeline import complete_day, build_board, quote_monitoring
from odds_collection import discovery_routes, TARGETS
from near_kickoff_quotes import NearKickoffRefresher
from lateral_matrix import build_matrix, base_specs
from refresh_opportunities import captured_prices
from statistics_first import statistical_analysis
from test_quote_sources import feed, bet, NOW, START, META
from test_statistics_first import match

class DiscoveryTests(unittest.TestCase):
    def test_period_and_punctuation_aliases_without_raw_id_gate(self):
        for label in ('Over/Under 1st Half','Totale Gol, primo tempo','GOALS — 1H',
                      'TotalGoals(FirstHalf)','O/U PT','Goals 1.5 (1st Half)','Goals Over/Under Half Time'):
            with self.subTest(label=label):
                identity=identify(label)
                self.assertEqual((identity['kind'],identity['period']),('total','ht'))
                groups,_=collect(feed([bet(908,1.5,name=label)]),100,NOW)
                self.assertEqual(len(groups),1)
                group=next(iter(groups.values()))
                self.assertTrue(group['complete'])
                self.assertEqual(group['quote_provenance']['source_bet_id'],908)
    def test_second_half_is_distinct_and_first_does_not_match_second(self):
        for label in ('2nd Half Over/Under','Over/Under secondo tempo','Total Goals 2h'):
            self.assertEqual(identify(label)['period'],'sh')
        self.assertIsNone(identify('Over/Under 1st Half 2nd Half'))
    def test_wrong_settlement_is_rejected_even_with_known_id(self):
        names=['Total Corners 1h','Goal Line Cards','Total goals Home 1h','Liverpool Total Goals',
               'Over/Under first 15 minutes','Total Goals including extra time','Goals odd/even',
               'Over/Under 1h and match winner','Two goals in both halves','Both teams score in both halves',
               'Home team to score in both halves','Away to score in each half','Total Shots',
               'Exact Goals','Next Goal','Asian Goal Line (extra time)','Total goals 1st quarter']
        for name in names:
            with self.subTest(name=name):
                self.assertIsNone(identify(name))
                self.assertEqual(collect(feed([bet(5,2.5,name=name)]),100,NOW)[0],{})
    def test_value_decimals_and_line_in_name(self):
        for values in ([{'value':'O 1,5','odd':'1.9'},{'value':'1,5 U','odd':'1.9'}],
                       [{'value':'Over','odd':1.9},{'value':'Under','odd':1.9}],
                       [{'value':'Più di 1,5 gol','odd':1.9},{'value':'Meno di 1,5 gol','odd':1.9}]):
            raw=feed([{'id':987,'name':'Totale gol primo tempo 1,5','values':values}])
            groups,_=collect(raw,100,NOW)
            self.assertEqual(next(iter(groups.values()))['odds'],{'over':1.9,'under':1.9})
    def test_conflicting_name_line_is_not_silently_accepted(self):
        self.assertEqual(collect(feed([bet(9999,2.5,name='Over/Under FT 3.5')]),100,NOW)[0],{})
    def test_unknown_ids_exact_asian_fallback_and_no_quarter_alias(self):
        for label,period,line in [('Asian Goal Line primo tempo','ht',1.5),('Total Goals Asian FT','ft',3.5)]:
            raw=feed([bet(8282,line,name=label)])
            groups,_=collect(raw,100,NOW)
            aliases=[g for g in groups.values() if g['quote_source']=='asian_halfline']
            self.assertEqual(len(aliases),1)
            self.assertEqual(aliases[0]['quote_provenance']['source_bet_id'],8282)
            self.assertEqual(next(iter(aliases[0]['specs'].values()))['period'],period)
            quarter=collect(feed([bet(8282,2.25,name=label)]),100,NOW)[0]
            self.assertFalse(any(g['quote_source']=='asian_halfline' for g in quarter.values()))
    def test_normalization_is_idempotent_and_does_not_mutate_feed(self):
        raw=feed([bet(989,2.5,name='Goal Line FT')]);before=deepcopy(raw)
        normalized=normalize_payload(raw,NOW)
        self.assertEqual(normalized,normalize_payload(normalized,NOW))
        self.assertEqual(raw,before)
    def test_source_change_never_inherits_first_price(self):
        state={}
        initial=feed([bet(5,2.5,over=2.2,under=1.8)])
        ledger_offers(state,100,collect(initial,100,NOW)[0],NOW,START)
        newer='2026-10-26T18:10:00+00:00'
        groups,_=collect(feed([bet(900,2.5,name='Totale Gol FT')],at=newer),100,newer)
        offers=ledger_offers(state,100,groups,newer,START)
        for offer in (o for rows in offers.values() for o in rows):
            self.assertEqual(offer['first_odd'],offer['odd'])
            self.assertEqual(len(offer['trace']),1)
        rebuilt=captured_prices(state['matrix_quotes']['100'],[{'spec':s} for s in base_specs()])
        self.assertTrue(any(k.startswith('discovery1_') for k in rebuilt))
    def test_two_secondary_operators_can_confirm_and_one_cannot_count_twice(self):
        state={};a=build_matrix(statistical_analysis(match(),NOW),NOW)
        def payload(at,over,under,duplicate=False):
            raw=feed([bet(900,2.5,over,under,name='Over/Under FT')],book=876,at=at)
            second=deepcopy(raw['response'][0]['bookmakers'][0]);second['id']=877
            raw['response'][0]['bookmakers'].append(second)
            if duplicate:
                raw['response'][0]['bookmakers'].pop()
                item=deepcopy(raw['response'][0]['bookmakers'][0]['bets'][0]);item['id']=901
                raw['response'][0]['bookmakers'][0]['bets'].append(item)
            return raw
        later='2026-10-26T18:30:00+00:00'
        for duplicate in (False,True):
            state={}
            attach(a,payload(NOW,2.2,1.8,duplicate),state,META,NOW)
            attach(a,payload(later,1.9,1.9,duplicate),state,META,later)
            row=next(r for r in a['matrix']['rows'] if r['spec']=={'kind':'total','period':'ft','side':'over','line':2.5})
            self.assertEqual(row['paired_books'],1 if duplicate else 2)
            self.assertEqual(row['agreeing_books'],1 if duplicate else 2)
    def test_synthetic_combo_with_discovered_legs_stays_estimated(self):
        raw=feed([bet(970,.5,name='Over/Under primo tempo'),bet(971,.5,name='Over/Under secondo tempo')])
        groups,_=collect(raw,100,NOW)
        derived=next(g for g in groups.values() if g['quote_source']=='synthetic_combo')
        self.assertFalse(derived['quote_executable']);self.assertAlmostEqual(derived['odds']['over'],3.61)
        self.assertLessEqual(synthetic_margin(derived),9)
    def test_direct_combo_new_id_preserves_actual_complement_and_history(self):
        raw=feed([{'id':912,'name':'Goals in each half','values':[{'value':'Si','odd':1.9},{'value':'No','odd':1.9}]}])
        groups,_=collect(raw,100,NOW);state={}
        group=next(iter(groups.values()));self.assertEqual(group['odds'],{'over':1.9,'not_over':1.9})
        ledger_offers(state,100,groups,NOW,START)
        self.assertTrue(any(k.startswith('direct_combo_discovery_v1|') for k in captured_prices(state['matrix_quotes']['100'],[{'spec':s} for s in base_specs()])))
    def test_catalog_routes_are_semantic_and_include_new_ids(self):
        catalog=[{'id':987,'name':'Total Goals Asian primo tempo'},{'id':999,'name':'Goals 2h'},
                 {'id':5,'name':'Total Corners'},{'id':33,'name':'BTTS 1H'}]
        self.assertEqual(set(discovery_routes(catalog,[],dict.fromkeys(TARGETS,0))),{987,999,33})
    def test_refresh_cap_cannot_exceed_forty(self):
        self.assertEqual(NearKickoffRefresher(None,{},400).max_calls,40)
    def test_conflicting_duplicate_snapshots_do_not_create_a_pair(self):
        first=bet(900,2.5,name='Totale Gol FT');other=deepcopy(first);other['values'][0]['odd']=2.3
        self.assertEqual(collect(feed([first,other,first]),100,NOW)[0],{})
    def test_synthetic_source_change_resets_history_and_restores(self):
        state={}
        for bid,at in ((900,NOW),(901,'2026-10-26T18:10:00+00:00')):
            groups,_=collect(feed([bet(bid,.5,name='Over/Under 1h'),bet(26,.5)],at=at),100,at)
            offers=ledger_offers(state,100,groups,at,START)
        derived=[o for rows in offers.values() for o in rows if o['quote_source']=='synthetic_combo']
        self.assertEqual(len(derived),1);self.assertEqual(len(derived[0]['raw_trace']),1)
        self.assertEqual(derived[0]['trace'],[])  # Estimated Combo never invents a complete OCI partition.
        restored=captured_prices(state['matrix_quotes']['100'],[{'spec':s} for s in base_specs()])
        self.assertEqual(sum(k.startswith('synthetic_combo_discovery_v1|') for k in restored),2)
    def test_compounds_are_not_reinterpreted_as_simple_markets(self):
        for label in ('Draw No Bet and Over 2.5','Asian Handicap Corners','Both Teams Score or over 2.5',
                      'Asian Handicap Home Team Goals','BTTS 1st quarter','DNB 15 min','Total Goals 2.5/3.5'):
            self.assertIsNone(identify(label),label)

class RetentionTests(unittest.TestCase):
    def test_statistics_failure_still_reads_quotes_and_retains_whitelist(self):
        raw={'fixture':{'id':100,'date':START,'status':{'short':'NS'}},'league':{'country':'Italy','name':'Serie A'}}
        calls=[]
        class Client:
            def get(self,path,params):
                calls.append(path)
                return {'response':[raw]} if path=='fixtures' else feed()
        ns={'fixture_meta':lambda _:META,'_result_payload':lambda _:None}
        day={'date':'2026-10-26','matches':[]}
        complete_day(ns,Client(),day,NOW);build_board(day,NOW)
        self.assertEqual(calls,['fixtures','odds'])
        self.assertEqual(len(day['matches']),1)
        matrix=day['matches'][0]['multimarket']['matrix']
        self.assertTrue(any(r.get('odd') for r in matrix['rows']))
        self.assertTrue(matrix['quote_monitoring']['study_only'])
        self.assertEqual(matrix['quote_monitoring']['state'],'waiting_quotes')
        self.assertFalse(matrix['opportunity_policy']['best_market_id'])
    def test_quote_failure_keeps_fixture_in_study_with_no_fake_flow(self):
        raw={'fixture':{'id':100,'date':START,'status':{'short':'NS'}},'league':{'country':'Italy','name':'Serie A'}}
        class Client:
            def get(self,path,params):
                if path=='fixtures': return {'response':[raw]}
                raise TimeoutError()
        day={'date':'2026-10-26','matches':[]}
        complete_day({'fixture_meta':lambda _:META},Client(),day,NOW);build_board(day,NOW)
        m=day['matches'][0]['multimarket']['matrix']
        self.assertEqual(len(m['quote_monitoring']['waiting_markets']),6)
        self.assertFalse(any(r.get('oci') for r in m['rows']))
        self.assertFalse(any(r.get('odd') for r in m['rows']))

if __name__=='__main__': unittest.main()
