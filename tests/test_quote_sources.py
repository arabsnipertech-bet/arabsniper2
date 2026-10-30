import importlib.util
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from quote_sources import normalize_payload, synthetic_combo_groups, synthetic_margin
from matrix_quotes import collect, ledger_offers, analyse_prices, attach
from refresh_opportunities import captured_prices
from near_kickoff_quotes import NearKickoffRefresher, needs_refresh
from lateral_matrix import build_matrix, base_specs
from statistics_first import statistical_analysis
from test_statistics_first import match

NOW = '2026-10-26T18:00:00+00:00'
START = '2026-10-26T20:00:00+00:00'
META = {**match()['fixture'], 'kickoff': START}


def bet(bid, line=.5, over=1.9, under=1.9, name=None):
    names = {5:'Goals Over/Under', 6:'Goals Over/Under First Half',
             26:'Goals Over/Under - Second Half', 50:'Goal Line', 72:'Goal Line (1st Half)', 336:'Goal Line'}
    return {'id':bid, 'name':name or names[bid], 'values':[
        {'value':f'Over {line}', 'odd':over}, {'value':f'Under {line}', 'odd':under}]}


def feed(bets=None, book=999, at=NOW):
    return {'collection':{'complete':True}, 'response':[{'fixture':{'id':100,'date':START},
        'update':at, 'bookmakers':[{'id':book,'name':None,'bets':bets or [bet(5, 2.5)]}]}]}


def saved():
    state = {}
    groups, _ = collect(feed(), 100, NOW)
    ledger_offers(state, 100, groups, NOW, START)
    return state


class ExactSources(unittest.TestCase):
    def test_all_requested_exact_lines_and_all_books(self):
        for bid, line in ((72,1.5),(72,2.5),(50,2.5),(336,3.5)):
            with self.subTest(bid=bid, line=line):
                raw=feed([bet(bid,line)])
                normalized=normalize_payload(raw,NOW)
                self.assertEqual(normalize_payload(normalized,NOW),normalized)
                self.assertEqual(len(raw['response']),1)
                groups,_=collect(raw,100,NOW)
                aliases=[g for g in groups.values() if g['quote_source']=='asian_halfline']
                self.assertEqual(len(aliases),1)
                self.assertTrue(aliases[0]['complete'])
                self.assertEqual(aliases[0]['bookmaker_id'],'999')
                self.assertEqual(aliases[0]['quote_provenance']['source_bet_id'],bid)
                self.assertEqual(aliases[0]['provider_updated_at'],NOW)

    def test_native_preferred_and_partial_native_replaced_by_whole_pair(self):
        raw=feed([bet(5,2.5),bet(50,2.5,2.1,1.8)])
        self.assertEqual(len(normalize_payload(raw,NOW)['response']),1)
        raw['response'][0]['bookmakers'][0]['bets'][0]['values'].pop()
        aliases=[g for g in collect(raw,100,NOW)[0].values() if g['quote_source']=='asian_halfline']
        self.assertEqual(aliases[0]['odds'],{'over':2.1,'under':1.8})
        a=build_matrix(statistical_analysis(match(),NOW),NOW)
        attach(a,raw,{},META,NOW)
        row=next(r for r in a['matrix']['rows'] if r['spec']=={'kind':'total','period':'ft','side':'over','line':2.5})
        self.assertEqual(row['quote_source'],'asian_halfline')
        self.assertEqual(row['odd'],2.1)

    def test_no_quarter_integer_partial_ambiguous_wrong_identity_or_stale_alias(self):
        inputs=[feed([bet(50,2.25)]),feed([bet(50,2)]),feed([bet(50,2.5,name='Total Corners')]),
                feed([bet(50,2.5)],at='2026-10-26T16:00:00+00:00'),
                feed([bet(50,2.5)],at='2026-10-26T19:00:00+00:00')]
        partial=feed([bet(50,2.5)]);partial['response'][0]['bookmakers'][0]['bets'][0]['values'].pop();inputs.append(partial)
        conflict=feed([bet(50,2.5)]);conflict['response'][0]['bookmakers'][0]['bets'][0]['values'].append({'value':'Over 2.5','odd':3});inputs.append(conflict)
        for raw in inputs:
            self.assertFalse(any(g.get('quote_source')=='asian_halfline' for g in collect(raw,100,NOW)[0].values()))

    def test_asian_history_never_imports_native_opening_or_invents_oci(self):
        state=saved()
        groups,_=collect(feed([bet(50,2.5,2.1,1.8)]),100,NOW)
        offers=ledger_offers(state,100,groups,NOW,START)
        alias=next(o for values in offers.values() for o in values if o['quote_source']=='asian_halfline')
        self.assertEqual(alias['first_odd'],alias['odd'])
        self.assertEqual(len(alias['trace']),1)
        rebuilt=captured_prices(state['matrix_quotes']['100'],[{'spec':s} for s in base_specs()])
        self.assertTrue(any(g.get('quote_source')=='asian_halfline' for g in rebuilt.values()))


class ComboSources(unittest.TestCase):
    def combo(self, raw=None):
        groups=synthetic_combo_groups(normalize_payload(raw or feed([bet(6),bet(26)]),NOW),100,NOW)
        return next(iter(groups.values())) if groups else None

    def test_product_uses_each_leg_margin_not_compounded_margin(self):
        combo=self.combo()
        self.assertAlmostEqual(combo['odds']['over'],3.61)
        self.assertAlmostEqual(synthetic_margin(combo),5.263157894736836)
        self.assertGreater((1.0526315789473684**2-1)*100,9)
        self.assertFalse(combo['complete']);self.assertFalse(combo['quote_executable'])
        self.assertEqual(combo['quote_provenance']['margin_basis'],'each_component')

    def test_rejects_bad_leg_missing_pair_wrong_book_snapshot_or_conflict(self):
        bad=feed([bet(6),bet(26,over=1.7,under=1.7)])
        different=feed([bet(6)]);different['response']+=feed([bet(26)],book=2)['response']
        timestamp=feed([bet(6)]);timestamp['response']+=feed([bet(26)],at='2026-10-26T17:59:00+00:00')['response']
        partial=feed([bet(6),bet(26)]);partial['response'][0]['bookmakers'][0]['bets'][1]['values'].pop()
        conflict=feed([bet(6),bet(26),bet(26,over=2,under=1.9)])
        for raw in (bad,different,timestamp,partial,conflict,feed([bet(6,1.5),bet(26)])):
            self.assertIsNone(self.combo(raw))
        forged=self.combo();forged['quote_provenance']['components'][0]['odds']['under']=1.1
        self.assertIsNone(synthetic_margin(forged))

    def test_survives_ledger_cache_without_direct_movement_and_keeps_policy(self):
        a=build_matrix(statistical_analysis(match(),NOW),NOW)
        before={r['id']:r['statistical_pct'] for r in a['matrix']['rows']}
        state={};attach(a,feed([bet(6),bet(26)]),state,META,NOW)
        row=next(r for r in a['matrix']['rows'] if r['spec']['kind']=='halves_total')
        self.assertAlmostEqual(row['odd'],3.61)
        self.assertEqual(row['quote_source'],'synthetic_combo')
        self.assertIsNone(row['oci']);self.assertFalse(row['movement_confirmed'])
        self.assertFalse(row.get('opportunity',{}).get('eligible'))
        self.assertTrue(row['combo']['price_available'])
        self.assertLess(row['selected_margin_pct'],9)
        self.assertEqual(before,{r['id']:r['statistical_pct'] for r in a['matrix']['rows']})
        rebuilt=captured_prices(state['matrix_quotes']['100'],a['matrix']['rows'])
        self.assertTrue(any(g.get('quote_source')=='synthetic_combo' for g in rebuilt.values()))
        fresh=build_matrix(statistical_analysis(match(),NOW),NOW)
        attach(fresh,{},state,META,'2026-10-26T19:30:00+00:00')
        cached=next(r for r in fresh['matrix']['rows'] if r['spec']['kind']=='halves_total')
        self.assertEqual(cached['quote_source'],'synthetic_combo')
        self.assertEqual(cached['provider_updated_at'],NOW)
        self.assertEqual(cached['quote_status'],'cached_stable')

    def test_direct_offer_precedes_larger_synthetic_price(self):
        a=build_matrix(statistical_analysis(match(),NOW),NOW)
        row=next(r for r in a['matrix']['rows'] if r['spec']['kind']=='halves_total')
        group=self.combo();groups={'synthetic':group}
        synthetic=ledger_offers({},100,groups,NOW,START)[row['id']][0]
        direct={**deepcopy(synthetic),'odd':2.5,'first_odd':2.5,'quote_source':'bookmaker',
                'quote_provenance':{},'bookmaker_id':'1','bookmaker':'Direct','bet_id':999}
        analyse_prices(row,[synthetic,direct],NOW,START)
        self.assertEqual(row['odd'],2.5);self.assertEqual(row['quote_source'],'bookmaker')


class TargetedRefresh(unittest.TestCase):
    def client(self):
        client=Mock();client.request_count=0
        def bounded(path,params,max_calls):
            client.request_count+=1
            return ({'response':{'requests':{'current':1000,'limit_day':75000}}} if path=='status' else feed()),1
        client.get_bounded.side_effect=bounded
        return client

    def test_saved_first_missing_stale_and_four_hour_boundary(self):
        self.assertTrue(needs_refresh({},saved(),META,NOW))
        self.assertTrue(needs_refresh(feed(at='2026-10-26T16:00:00+00:00'),saved(),META,NOW))
        self.assertFalse(needs_refresh(feed(),saved(),META,NOW))
        self.assertFalse(needs_refresh({}, {},META,NOW))
        self.assertFalse(needs_refresh({},saved(),META,'2026-10-26T15:59:59+00:00'))
        self.assertFalse(needs_refresh({},saved(),META,START))

    def test_once_per_cycle_cooldown_and_cap_include_status(self):
        c=self.client();state=saved();refresh=NearKickoffRefresher(c,state,2)
        self.assertTrue(refresh.recover({},META,NOW)['response'])
        refresh.recover({},META,NOW)
        self.assertEqual(c.request_count,2);self.assertEqual(refresh.report['fixtures_recovered'],1)
        again=NearKickoffRefresher(c,state,2);again.recover({},META,NOW)
        self.assertEqual(c.request_count,2);self.assertEqual(again.report['cooldown_skips'],1)
        c=self.client();refresh=NearKickoffRefresher(c,saved(),1)
        self.assertEqual(refresh.recover({},META,NOW),{})
        self.assertEqual(c.request_count,1);self.assertEqual(refresh.report['fixtures_attempted'],0)

    def test_low_or_unknown_quota_and_incomplete_response_fail_closed(self):
        for value in (None, {'response':{'requests':{'current':74999,'limit_day':75000}}}):
            c=self.client();c.get_bounded.side_effect=lambda *a:(value,0)
            refresh=NearKickoffRefresher(c,saved());self.assertEqual(refresh.recover({},META,NOW),{})
            self.assertEqual(refresh.report['quota_skips'],1)
        c=self.client();original=c.get_bounded.side_effect
        c.get_bounded.side_effect=lambda path,*args: original(path,*args) if path=='status' else ({'response':[]},0)
        self.assertEqual(NearKickoffRefresher(c,saved()).recover({},META,NOW),{})


class HttpBudget(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('bounded_quote_engine',Path(__file__).resolve().parents[1]/'src/3appdays.py')
        cls.e=importlib.util.module_from_spec(spec);sys.modules[spec.name]=cls.e;spec.loader.exec_module(cls.e)

    def test_retries_and_pagination_never_exceed_budget_or_return_partial(self):
        c=self.e.APIClient('test-only');c._throttle=Mock();c.session=Mock()
        c.session.get.return_value=Mock(status_code=500,text='test')
        with patch.object(self.e,'API_RETRIES',(0,0,0)):
            result,used=c.get_bounded('odds',{'fixture':100},2)
        self.assertIsNone(result);self.assertEqual(used,2)
        self.assertEqual(c.session.get.call_count,2)
        c.session.get.reset_mock()
        page={'paging':{'current':1,'total':2},**feed()}
        c.session.get.return_value=Mock(status_code=200,json=Mock(return_value=page))
        result,used=c.get_bounded('odds',{'fixture':100},1)
        self.assertIsNone(result);self.assertEqual(used,1)
        self.assertEqual(c.session.get.call_count,1)
        self.assertIsNone(c._http_remaining)

    def test_mid_pagination_cutoff_and_enricher_restored(self):
        c=self.e.APIClient('test-only');c._throttle=Mock();c.session=Mock()
        enricher=c.odds_enricher=Mock()
        pages=[{'paging':{'current':1,'total':3},**feed()},
               {'response':{'requests':{'current':1000,'limit_day':75000}}},
               {'paging':{'current':2,'total':3},**feed(book=2)}]
        c.session.get.side_effect=[Mock(status_code=200,json=Mock(return_value=p)) for p in pages]
        result,used=c.get_bounded('odds',{'fixture':100},3)
        self.assertIsNone(result);self.assertEqual(used,3)
        self.assertEqual(c.session.get.call_count,3)
        self.assertIs(c.odds_enricher,enricher);enricher.enrich.assert_not_called()


class CoverageMeasurement(unittest.TestCase):
    def test_equivalent_asian_rows_are_not_double_counted(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'auditor'))
        from quote_coverage import measurements
        groups,_=collect(feed([bet(50,2.5),bet(6),bet(26)]),100,NOW)
        result=measurements(groups,NOW,START)
        self.assertEqual(result['real'],1)
        self.assertEqual(result['synthetic'],1)
        self.assertEqual(result['usable'],2)
        self.assertEqual(result['valid_operator_markets'],2)
        self.assertEqual(result['standard_rows'],1)


if __name__=='__main__': unittest.main()
