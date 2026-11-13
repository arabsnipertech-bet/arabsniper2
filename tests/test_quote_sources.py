import importlib.util
import sys
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from quote_sources import normalize_payload, synthetic_combo_groups, synthetic_margin, direct_combo_groups
from matrix_quotes import collect, ledger_offers, analyse_prices, attach
from odds_collection import scheduled_odds
from quote_history import OPENING_SNAPSHOT, QuoteHistory
from audit_storage import read_store
from v7_probability import price_usable
from refresh_opportunities import captured_prices, refresh_day
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
    def test_opening_survives_old_provider_timestamp_and_real_movement(self):
        meta = dict(META, kickoff='2026-10-29T20:00:00Z')
        state = {}
        analysis = build_matrix(statistical_analysis(match(), NOW), NOW)
        def snapshot(at, odd, observed):
            data = feed([bet(5, 2.5, odd, 1.75)], at=at)
            data['response'][0]['fixture']['date'] = meta['kickoff']
            attach(analysis, data, state, meta, observed)
            return next(r for r in analysis['matrix']['rows'] if r['spec'] == {
                'kind': 'total', 'period': 'ft', 'side': 'over', 'line': 2.5})

        initial = snapshot(NOW, 2.0, NOW)
        self.assertEqual(initial['opening_odd'], 2.0)
        next_day = '2026-10-27T18:00:00Z'
        stable = snapshot(NOW, 2.0, next_day)
        self.assertEqual((stable['opening_odd'], stable['odd'], stable['quote_status'], stable['delta_pct'], stable['oci']),
                         (2.0, 2.0, 'cached_stable', 0., 0.))
        self.assertTrue(price_usable(stable, next_day, meta['kickoff']))
        update = '2026-10-28T18:00:00Z'
        changed = snapshot(update, 2.2, update)
        self.assertEqual((changed['opening_odd'], changed['odd'], changed['quote_status']), (2.0, 2.2, 'recent'))
        self.assertAlmostEqual(changed['delta_pct'], 10.)
        replay = snapshot(update, 1.9, '2026-10-29T17:00:00Z')
        self.assertEqual((replay['opening_odd'], replay['odd'], replay['delta_pct'], replay['oci']), (2.0, 2.2, 0., 0.))
        self.assertAlmostEqual(replay['opening_delta_pct'], 10.)
        self.assertFalse(replay['movement_confirmed'])
        self.assertFalse(price_usable(replay, meta['kickoff'], meta['kickoff']))

    def test_fair_and_overround_use_complete_book_by_period_and_line(self):
        f = feed([bet(5, 2.5, 2., 1.85)], book=1)
        f['response'] += feed([bet(6, 1.5, 2.1, 1.78), bet(26, 1.5, 2.3, 1.65)], book=2)['response']
        partial = feed([bet(5, 2.5, 3., 1.6)], book=3)
        partial['response'][0]['bookmakers'][0]['bets'][0]['values'].pop()
        f['response'] += partial['response']
        a = build_matrix(statistical_analysis(match(), NOW), NOW)
        attach(a, f, {}, META, NOW)
        for period, line, book, over, under in [('ft', 2.5, '1', 2., 1.85),
                                                ('ht', 1.5, '2', 2.1, 1.78),
                                                ('sh', 1.5, '2', 2.3, 1.65)]:
            row = next(r for r in a['matrix']['rows'] if r['spec'] == {
                'kind': 'total', 'period': period, 'side': 'over', 'line': line})
            self.assertEqual((row['bookmaker_id'], row['odd']), (book, over))
            self.assertAlmostEqual(row['selected_margin_pct'], 100*(1/over+1/under-1))
            self.assertAlmostEqual(row['selected_market_fair_pct'], 100/over/(1/over+1/under))

    def test_snapshot_and_cache_restore_after_failed_analysis(self):
        import tempfile

        with tempfile.TemporaryDirectory() as folder:
            journal = QuoteHistory(folder)
            journal.observe(feed(), NOW, {'fixture': 100})
            journal.flush()
            reopened = QuoteHistory(folder)
            state = {}
            later = '2026-10-26T19:00:00+00:00'
            reopened.restore_openings(state, META, later)
            a = build_matrix(statistical_analysis(match(), NOW), NOW)
            attach(a, {}, state, META, later)
            row = next(r for r in a['matrix']['rows'] if r['spec'] == {
                'kind': 'total', 'period': 'ft', 'side': 'over', 'line': 2.5})
            self.assertEqual((row['opening_odd'], row['odd'], row['quote_status']), (1.9, 1.9, 'cached_stable'))
            reopened.save_openings([{'fixture': META}], later)
            snapshot = read_store(Path(folder)/OPENING_SNAPSHOT)
            market, = snapshot['odds']['100']['markets'].values()
            self.assertEqual(market['bookmaker_id'], '999')
            first = market['outcomes']['over']
            self.assertEqual(first, {'opening_odd': 1.9, 'observed_at': NOW})

    def test_calendar_uses_league_and_utc_then_fetches_incomplete_fixture_at_any_distance(self):
        client = Mock(prefetched_odds={})
        client.odds_enricher.allowed.return_value = False
        kickoff = '2026-10-28T00:30:00+01:00'
        winner = {'id': 1, 'name': 'Match Winner', 'values': [
            {'value': side, 'odd': odd} for side, odd in [('Home', 2.1), ('Draw', 3.2), ('Away', 3.5)]]}
        calendar = feed([winner, bet(5, 2.5)])
        calendar['response'][0]['fixture']['date'] = kickoff
        partial = feed([bet(5, 2.5)], book=2)
        partial['response'][0]['fixture'] = {'id': 101, 'date': kickoff}
        calendar['response'] += partial['response']
        direct = feed([winner], book=777, at='2026-10-24T18:00:00+00:00')
        direct['response'][0]['fixture'] = {'id': 101, 'date': kickoff}
        client.get.side_effect = [calendar, direct]
        meta = dict(META, kickoff=kickoff, league_id=101, season=2026, status_short='NS')
        report = scheduled_odds(client, [meta, dict(meta, fixture_id=101),
                                        dict(meta, fixture_id=102, status_short='PST')], '2026-10-28', lambda: NOW)
        self.assertEqual([c.args[1] for c in client.get.call_args_list], [
            {'date': '2026-10-27', 'league': 101, 'season': 2026}, {'fixture': 101}])
        self.assertEqual(client.get.call_args_list[0].kwargs['odds_fixture_ids'], {'100', '101'})
        self.assertFalse(client.get.call_args_list[1].kwargs['enrich_odds'])
        client.odds_enricher.enrich.assert_not_called()
        self.assertEqual(set(client.prefetched_odds), {'100', '101'})
        self.assertEqual(report['complete_primary'], 2)
        self.assertEqual({r['bookmakers'][0]['id'] for r in client.prefetched_odds['101']['response']}, {2, 777})

    def test_old_complete_pair_from_any_operator_precedes_new_partial_quote(self):
        stable = feed([bet(50, 2.5)], book=777, at='2026-10-24T18:00:00+00:00')
        partial = feed([bet(5, 2.5)], book=1)
        partial['response'][0]['bookmakers'][0]['bets'][0]['values'].pop()
        data = {'response': partial['response'] + stable['response']}
        a = build_matrix(statistical_analysis(match(), NOW), NOW)
        attach(a, data, {}, META, NOW)
        over = next(r for r in a['matrix']['rows'] if r['spec'] == {
            'kind': 'total', 'period': 'ft', 'side': 'over', 'line': 2.5})
        self.assertEqual((over['odd'], over['bookmaker_id'], over['quote_status']), (1.9, '777', 'cached_stable'))
        self.assertAlmostEqual(over['selected_market_fair_pct'], 50.)
        self.assertAlmostEqual(over['selected_margin_pct'], 100*(2/1.9-1))
        self.assertFalse(over['movement_confirmed'])
        self.assertFalse(over['opportunity']['eligible'])

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
                feed([bet(50,2.5)],at='2026-10-21T17:59:00+00:00'),
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
    def direct(self):
        return {'id':184,'name':'To Score in Both Halves','values':[{'value':'Yes','odd':1.9},{'value':'No','odd':1.9}]}

    def test_real_provider_combo_precedes_product_and_keeps_actual_complement(self):
        raw=feed([self.direct(),bet(6),bet(26)])
        groups,_=collect(raw,100,NOW)
        self.assertFalse(any(g.get('quote_source')=='synthetic_combo' for g in groups.values()))
        direct=next(g for g in groups.values() if g.get('bet_id')==184)
        self.assertEqual(direct['odds'],{'over':1.9,'not_over':1.9})
        a=build_matrix(statistical_analysis(match(),NOW),NOW);state={}
        attach(a,raw,state,META,NOW)
        row=next(r for r in a['matrix']['rows'] if r['spec']['kind']=='halves_total')
        self.assertEqual(row['odd'],1.9);self.assertTrue(row['quote_executable'])
        self.assertAlmostEqual(row['selected_margin_pct'],5.263157894736836)
        restored=captured_prices(state['matrix_quotes']['100'],a['matrix']['rows'])
        self.assertTrue(any(g.get('bet_id')==184 for g in restored.values()))

    def test_team_specific_both_halves_never_prices_match_combo(self):
        for bid,name in ((48,'To Score in Both Halves - Home'),(111,'Home team to score in both halves'),
                         (112,'Away team to score in both halves')):
            item=self.direct();item.update(id=bid,name=name)
            self.assertEqual(direct_combo_groups(feed([item]),100,NOW),{})
        item=self.direct();item.update(id=113,name='Both teams to score in both halves')
        actual=next(iter(direct_combo_groups(feed([item]),100,NOW).values()))
        self.assertEqual(actual['specs']['over'], {'kind':'both_halves','period':'ft','side':'both'})
        item=self.direct();item['name']='Home team will score in both halves'
        self.assertEqual(direct_combo_groups(feed([item]),100,NOW),{})
        item=self.direct();item['values'][0]['odd']=2.1
        self.assertEqual(direct_combo_groups(feed([self.direct(),item,self.direct()]),100,NOW),{})
        item=self.direct();item['values'].pop()
        groups,_=collect(feed([item,bet(6),bet(26)]),100,NOW)
        self.assertTrue(any(g.get('quote_source')=='synthetic_combo' for g in groups.values()))

    def test_partial_refresh_cannot_drop_a_complete_direct_combo(self):
        from odds_collection import coverage, merge_payload, BET_IDS
        raw=feed([self.direct()]);partial=self.direct();partial['values'].pop()
        later=feed([partial],at='2026-10-26T18:01:00+00:00')
        merged=merge_payload(raw,later,100,'2026-10-26T18:02:00+00:00')
        self.assertIn(184,BET_IDS['combo_ht_sh'])
        self.assertIn(('combo_ht_sh','999'),coverage(raw,100,NOW,False))
        self.assertTrue(coverage(raw,100,NOW,False) <= coverage(merged,100,'2026-10-26T18:02:00+00:00',False))

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


    def test_refresh_day_prefers_cached_direct_combo_over_larger_product_from_another_book(self):
        assessment = build_matrix(statistical_analysis(match(), NOW), NOW)
        state = {}
        direct, _ = collect(feed([self.direct()], book=7), 100, NOW)
        derived, _ = collect(feed([bet(6), bet(26)], book=8), 100, NOW)
        ledger_offers(state, 100, {**direct, **derived}, NOW, START)
        day = {'date': '2026-10-26', 'matches': [{'fixture': dict(META), 'multimarket': assessment}]}
        refresh_day(day, state, '2026-10-26T19:30:00+00:00')
        combo = next(row for row in assessment['matrix']['rows'] if row['spec']['kind'] == 'halves_total')
        self.assertEqual((combo['quote_status'], combo['quote_source'], combo['bookmaker_id']),
                         ('cached_stable', 'bookmaker', '7'))
        self.assertAlmostEqual(combo['odd'], 1.9)
        self.assertAlmostEqual(combo['selected_margin_pct'], (2/1.9-1)*100)
        self.assertTrue(combo['quote_executable'])


class TargetedRefresh(unittest.TestCase):
    def client(self, used=1000, limit=75000):
        client=Mock();client.request_count=0
        def bounded(path,params,max_calls):
            client.request_count+=1
            return ({'response':{'requests':{'current':used,'limit_day':limit}}} if path=='status' else feed()),1
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

    def test_reserve_allows_recovery_on_small_and_large_plans(self):
        for limit, remaining, reserve in ((1000, 51, 50), (75000, 501, 500)):
            client = self.client(limit-remaining, limit)
            refresh = NearKickoffRefresher(client, saved())
            self.assertTrue(refresh.recover({}, META, NOW)['response'])
            self.assertEqual(refresh.reserve, reserve)
            self.assertEqual(client.get_bounded.call_args.args[2], 1)


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

    def test_retry_after_is_honored_for_http_and_json_limits(self):
        now = self.e.datetime.fromisoformat('2026-11-13T17:00:00+00:00')
        for status, retry_after in ((429, '11'), (200, 'Fri, 13 Nov 2026 17:00:11 GMT')):
            with self.subTest(status=status), patch.object(self.e, 'time') as clock, patch.object(self.e, 'datetime', wraps=self.e.datetime) as calendar:
                client = self.e.APIClient('test-only', min_interval=0)
                client.session = Mock()
                clock.monotonic.return_value = 100
                calendar.now.return_value = now
                rejected = Mock(status_code=status, headers={'Retry-After': retry_after}, text='limited',
                                json=Mock(return_value={'errors': {'rateLimit': 'limited'}}))
                accepted = Mock(status_code=200, headers={}, json=Mock(return_value=feed()))
                client.session.get.side_effect = [rejected, accepted]
                payload, used = client.get_bounded('fixtures', {'id': 100}, 2)
                self.assertIsNotNone(payload)
                self.assertEqual(used, 2)
                clock.sleep.assert_called_once_with(11)

    def test_long_retry_after_does_not_spend_budget_on_subsequent_queries(self):
        client = self.e.APIClient('test-only')
        client.session = Mock()
        client.session.get.return_value = Mock(status_code=429, headers={'Retry-After': '120'}, text='limited')
        with patch.object(self.e, 'time') as clock:
            clock.monotonic.return_value = 100
            self.assertEqual(client.get_bounded('fixtures', {'id': 100}, 3), (None, 1))
            self.assertEqual(client.get_bounded('odds', {'fixture': 100}, 3), (None, 0))
            self.assertEqual(client.provider_retry_at, 220)
            clock.sleep.assert_not_called()
        self.assertEqual(client.last_error['errors'], 'provider_cooldown')

    def test_zero_daily_quota_defers_the_next_request_after_a_success(self):
        now = self.e.datetime.fromisoformat('2026-11-13T17:00:00+00:00')
        client = self.e.APIClient('test-only')
        client.session = Mock()
        client.session.get.return_value = Mock(status_code=200, headers={'x-ratelimit-requests-remaining': '0'},
                                               json=Mock(return_value=feed()))
        with patch.object(self.e, 'time') as clock, patch.object(self.e, 'datetime', wraps=self.e.datetime) as calendar:
            clock.monotonic.return_value = 100
            calendar.now.return_value = now
            self.assertIsNotNone(client.get('fixtures', {'id': 100}))
            self.assertEqual(client.get_bounded('fixtures', {'id': 101}, 2), (None, 0))
            self.assertEqual(client.provider_retry_at, 100 + 7 * 3600)
            clock.sleep.assert_not_called()


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
