import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from statistics_first import (evaluate, statistical_analysis, component, history, build_selection_board,
                              MODEL_ID, TARGETS, CONFIG)
from multimarket_model import fair, freeze_evaluations
from multimarket_audit import settle, build_report
from daily_audit import build_daily
from homepage_preview import build_preview
from homepage_results import project

NOW = '2026-10-26T08:00:00+00:00'
KICKOFF = '2026-10-26T20:00:00+00:00'


def team_history(side='home', goals=3, conceded=3, ht=1, ht_against=1):
    return [{'fixture_id':f'{side}-{i}', 'kickoff':f'2026-10-{25-i:02}T07:00:00+00:00',
             'date':f'2026-10-{25-i:02}', 'season':2026, 'friendly':False,
             'side':side if i % 2 == 0 else ('away' if side == 'home' else 'home'),
             'research_ft':{'scored':goals,'conceded':conceded},
             'team_goals_ft':goals, 'team_conceded_ft':conceded,
             'halftime_available':True, 'team_goals_ht':ht, 'team_conceded_ht':ht_against}
            for i in range(8)]


def match(fid='100'):
    return {'fixture':{'fixture_id':fid, 'season':2026, 'home_id':10,'away_id':20,
            'kickoff':KICKOFF, 'status_short':'NS', 'match':'Synthetic home – away'},
        'statistics':{'home':{'last_matches':team_history()},
                      'away':{'last_matches':team_history('away')},
                      'league':{'sample':30,'ht_sample':30,'avg_home_goals':1.4,
                                'avg_away_goals':1.1,'avg_home_ht':.6,'avg_away_ht':.45}}}


def book(name='A', points=(2.4,2.1), under=2.):
    trace=[{'observed_at':f'2026-10-26T0{6+i}:00:00+00:00',
            'odds':{'over':value,'under':under}} for i,value in enumerate(points)]
    p, margin = fair(trace[-1]['odds'])
    return {'bookmaker':name,'bookmaker_id':name,'observed_at':NOW,'provider_updated_at':NOW,
            'odds':trace[-1]['odds'],'fair':p,'overround_pct':margin,'trace':trace}


def evaluated(fid='100'):
    m=match(fid);m['multimarket']=evaluate(m,NOW);return m


class StatisticsTests(unittest.TestCase):
    def test_quotes_cannot_change_stats_admission_or_ranking_and_edge_is_not_capped_by_book(self):
        a=evaluate(match(),NOW)
        b=evaluate(match(),NOW,{'over25_ft':[book(),book('B',(2.5,2.2))]})
        self.assertEqual(a['input_sha256'],b['input_sha256'])
        self.assertEqual(a['rates'],b['rates'])
        for x,y in zip(a['markets'],b['markets']):
            for key in ('probabilities','statistical_candidate','statistical_score','sensitivity'):
                self.assertEqual(x[key],y[key])
        row=b['markets'][0]
        self.assertEqual(row['market_confirmation']['status'],'confirmed')
        self.assertIn('over25_from_above_2',row['market_confirmation']['patterns'])
        self.assertAlmostEqual(row['price_assessment']['estimated_return_pct'],row['statistical_pct']*row['odd']-100)
        self.assertEqual(row['price_assessment']['reference_probability_pct'],row['statistical_pct'])

    def test_isolated_goleada_loses_influence_without_assuming_next_match_zero(self):
        rows=team_history(goals=1)
        rows[0]['team_goals_ft']=6
        robust=component(rows,'home','for','ft',1.4)
        raw=component(rows,'home','for','ft',1.4,use_robust=False)
        normal=component(team_history(goals=1),'home','for','ft',1.4)
        self.assertLess(robust['rate'],raw['rate'])
        self.assertGreater(robust['rate'],normal['rate'])
        self.assertGreater(robust['peak_reduction'],0)
        consistent=component(team_history(goals=3),'home','for','ft',1.4)
        self.assertEqual(consistent['peak_reduction'],0)

    def test_drought_requires_real_opportunities_not_debt(self):
        rows=team_history(goals=0,ht=0)
        a=component(rows,'home','for','ft',1.4)
        for row in rows:row['team_xg_for']=2
        b=component(rows,'home','for','ft',1.4)
        self.assertFalse(a['under_conversion'])
        self.assertEqual(a['xg_adjustment'],0)
        self.assertTrue(b['under_conversion'])
        self.assertGreater(b['rate'],a['rate'])
        self.assertLess(b['rate'],2)
        self.assertEqual(CONFIG['drought_probability_bonus'],0)

    def test_fulltime_xg_cannot_boost_halftime(self):
        m=match();a=statistical_analysis(m,NOW)
        for side in ('home','away'):
            for r in m['statistics'][side]['last_matches']:r['team_xg_for']=2
        b=statistical_analysis(m,NOW)
        self.assertNotEqual(a['rates']['home_ft'],b['rates']['home_ft'])
        self.assertEqual(a['rates']['home_ht'],b['rates']['home_ht'])
        self.assertEqual(a['rates']['away_ht'],b['rates']['away_ht'])

    def test_history_rejects_future_other_season_duplicates_friendlies_and_missing_scores(self):
        rows=team_history()
        rows[0]['kickoff']=KICKOFF;rows[1]['season']=2025;rows[2]['friendly']=True
        rows[3]['research_ft']['scored']=None
        rows += [deepcopy(rows[4])]
        self.assertEqual(len(history(rows,NOW,2026)),4)

    def test_short_or_missing_ht_or_league_does_not_fabricate_selection(self):
        for what in ('short','ht','league','invalid_ht'):
            m=match()
            if what=='short':m['statistics']['home']['last_matches']=team_history()[:2]
            elif what=='league':m['statistics']['league']['sample']=0
            else:
                for row in m['statistics']['home']['last_matches']:
                    if what=='ht':row['halftime_available']=False
                    else:row['team_goals_ht']=10
            a=evaluate(m,NOW,{'over25_ft':[book(),book('B')]})
            rows=[r for r in a['markets'] if r['market'].endswith('_ht' if what in ('ht','invalid_ht') else '_ft')]
            self.assertFalse(any(r['statistical_candidate'] for r in rows))
            self.assertTrue(all(r['market_confirmation']['status']=='not_evaluated' for r in rows))

    def test_h2h_never_double_counts_recent_match_and_change_is_bounded(self):
        m=match();h={'fixture_id':'home-0','kickoff':'2026-10-20T06:00:00Z',
                    'home_id':10,'away_id':20,'total_ft':8,'total_ht':4,'halftime_available':True}
        m['statistics']['h2h']={'matches':[h,dict(h,fixture_id='extra1'),dict(h,fixture_id='extra2')]}
        a=evaluate(m,NOW)
        self.assertEqual(a['h2h']['overlapping_excluded'],1)
        self.assertEqual(a['h2h']['used'],2)
        self.assertLessEqual(abs(a['h2h']['effects']['ft']['relative_change']),CONFIG['h2h_max_rate_change'])

    def test_confirmation_rejects_single_operator_stale_and_margin_only_drop(self):
        for books in ([book()], [dict(book(),provider_updated_at='2026-10-25T08:00:00Z'),dict(book('B'),provider_updated_at='2026-10-25T08:00:00Z')]):
            self.assertNotEqual(evaluate(match(),NOW,{'over25_ft':books})['markets'][0]['market_confirmation']['status'],'confirmed')
        b=book();b['trace'][0]['odds']['under']=2.3;b['odds']['under']=1.3;b['trace'][-1]['odds']['under']=1.3
        c=deepcopy(b);c.update(bookmaker='B',bookmaker_id='B')
        self.assertNotEqual(evaluate(match(),NOW,{'over25_ft':[b,c]})['markets'][0]['market_confirmation']['status'],'confirmed')

    def test_probabilities_are_coherent(self):
        p={r['market']:r['statistical_pct'] for r in evaluate(match(),NOW)['markets']}
        self.assertLessEqual(p['gg_ht'],p['over15_ht'])
        self.assertLessEqual(p['over35_ft'],p['over25_ft'])


class PublicationTests(unittest.TestCase):
    def test_preserved_stale_price_loses_confirmation_without_changing_statistics(self):
        m=match();b=book();b['movement']={'first_odds':{'over':2.4,'under':2}}
        m['multimarket']=evaluate(m,NOW,{'over25_ft':[b,book('B')]})
        before=deepcopy(m['multimarket']['markets'][0]['probabilities'])
        build_selection_board({'matches':[m]},'2026-10-26T16:00:00Z')
        row=m['multimarket']['markets'][0]
        self.assertEqual(row['quote_status'],'stale')
        self.assertEqual(row['market_confirmation']['status'],'unavailable')
        self.assertIsNone(row['odd'])
        self.assertEqual(row['display_odd'],2.1)
        self.assertEqual(row['probabilities'],before)

    def test_top_five_statistics_only_and_no_refill_after_kickoff(self):
        day={'matches':[evaluated(str(i)) for i in range(8)]}
        for i,m in enumerate(day['matches']):
            for r in m['multimarket']['markets']:
                r['statistical_score']=60+i;r['odd']=10-i
        board=build_selection_board(day,NOW)
        self.assertEqual([r['fixture_id'] for r in board['markets']['over25_ft']['selected']],['7','6','5','4','3'])
        for m in day['matches'][6:]:m['fixture']['kickoff']='2026-10-26T07:00:00Z'
        before=deepcopy(day['matches'][7]['multimarket'])
        board=build_selection_board(day,NOW)
        self.assertEqual(len(board['markets']['over25_ft']['selected']),3)
        self.assertEqual(board['markets']['over25_ft']['started_selections'],2)
        self.assertEqual(before,day['matches'][7]['multimarket'])
        empty={'matches':[evaluated()]}
        for r in empty['matches'][0]['multimarket']['markets']:r['statistical_candidate']=False
        self.assertEqual(build_selection_board(empty,NOW)['markets']['over25_ft']['selected'],[])

    def test_freeze_all_selections_updates_and_public_denominator(self):
        day={'matches':[evaluated()]};build_selection_board(day,NOW)
        output={'days':[day]};store={};freeze_evaluations(store,output,NOW)
        self.assertTrue(store.get('analysis_updates'))
        result={'status':'FT','home_ft':3,'away_ft':2,'home_ht':1,'away_ht':1}
        settle(store, {'100':result})
        report=build_daily(store,'2026-10-26',NOW)
        rows=[r for r in report['markets'] if r['snapshot']=='latest' and r['published_selection']]
        self.assertEqual(len(rows),4)
        self.assertEqual(len(project(report,'2026-10-26',model=MODEL_ID)),4)
        self.assertTrue(all(r['outcome']=='won' for r in project(report,'2026-10-26',model=MODEL_ID)))
        self.assertEqual(len({(r['fixture_id'],r['market']) for r in project(report,'2026-10-26',model=MODEL_ID)}),4)
        audit=build_report(store)
        self.assertTrue(any(r['group']=='independent_model_variants' for r in audit['groups']))
        self.assertTrue(any(r['feature']=='selected_unavailable' for r in audit['feature_comparisons']))

    def test_public_preview_never_reintroduces_book_agreement_or_forced_pick(self):
        day={'date':'2026-10-26','updated_at':NOW,'matches':[evaluated(str(i)) for i in range(6)],
             'multimarket_method':{'model_id':MODEL_ID}}
        build_selection_board(day,NOW)
        for m in day['matches']:
            for r in m['multimarket']['markets']:r.update(odd=2.1,book_probability_pct=20)
        p=build_preview(day,now=datetime.fromisoformat(NOW))
        self.assertEqual(len(p['selections']),3)
        self.assertEqual(p['selections'],build_preview(day,now=datetime.fromisoformat(NOW))['selections'])
        for m in day['matches']:
            for r in m['multimarket']['markets']:r['published_selection']=False
        self.assertEqual(build_preview(day,now=datetime.fromisoformat(NOW))['selections'],[])


if __name__ == '__main__':unittest.main()
