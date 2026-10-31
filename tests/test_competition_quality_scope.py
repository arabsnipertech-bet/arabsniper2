from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from gold_competitions import SCOPE_ID, browser_data, enforce_day, fixture_allowed, profile
from live_watchlist import build as live_publication
from matrix_pipeline import build_board, complete_day
from odds_collection import OddsEnricher
from test_policy_engine import engine
from test_statistics_first import NOW, KICKOFF
from test_opportunity_policy import row as goal_row
from test_platinum_policy import CASES
from lateral_matrix import MODEL_ID
from near_kickoff_quotes import NearKickoffRefresher

EXCLUDED = [
    ('Ukraine','Persha Liga'), ('Ukraine','Persha Liha - Championship Round'), ('Ukraine','Druha Liga - Group A'),
    ('Poland','II Liga - East'), ('Poland','II Liga - Group North'), ('Poland','III Liga - Group 2'),
    ('Norway','2. Division - Group 2'), ('Norway','2. Division - Group 7'), ('Norway','3. divisjon - Avdeling 3'),
    ('Slovakia','2. liga'), ('Slovakia','3. liga - Center'), ('Slovakia','III. liga - West'),
    ('Bulgaria','Third League - Southwest'), ('Bulgaria','Third League - Group 8'),
    ('Czech-Republic','3. liga - CFL A'), ('Czechia','ČFL - Group C'), ('Czechia','MSFL'),
    ('Austria','Regionalliga West'), ('Austria','Regionalliga - Tirol'), ('Austria','Landesliga'),
    ('Romania','Liga III - Serie 1'), ('Romania','Liga 3 - Seria 12'), ('Romania','Liga IV'),
    ('Germany','3. Liga'), ('France','National'), ('Netherlands','Tweede Divisie'),
    ('Portugal','Liga 3'), ('Scotland','League One'), ('Spain','Primera División RFEF - Group 2'),
    ('Scotland','League Two'), ('England','League Two Women'), ('England','League Two U21'),
]
ALLOWED = [
    ('England','League Two'), ('England','English League Two'), ('England','Sky Bet League Two'),
    ('England','EFL League Two'), ('England','League One'), ('Italy','Serie C - Girone B'),
    ('Poland','I Liga'), ('Norway','1. Division'), ('Ukraine','Premier League'),
    ('Slovakia','Super Liga'), ('Bulgaria','Second League'), ('Czechia','2. Liga'),
    ('Austria','2. Liga'), ('Romania','Liga II'), ('Romania','Cupa României'),
    ('Japan','J3 League'), ('China','League Two')
]


def fixture(fid, country, name):
    return {'fixture': {'id':fid,'date':KICKOFF,'status':{'short':'NS'}},
            'league':{'country':country,'name':name,'id':42,'season':2026},
            'teams':{'home':{'name':'Home','id':11},'away':{'name':'Away','id':22}}}


def meta(raw):
    return {'fixture_id':str(raw['fixture']['id']), 'country':raw['league']['country'],
            'league':raw['league']['name'], 'home':'Home', 'away':'Away', 'status_short':'NS','kickoff':KICKOFF}


class CompetitionQualityScope(unittest.TestCase):
    def test_exclusions_and_quality_exceptions_are_country_scoped(self):
        for country,name in EXCLUDED+ALLOWED:
            with self.subTest(country=country,name=name):
                self.assertEqual(fixture_allowed(fixture(1,country,name)),(country,name) in ALLOWED)
        for name in ('League Two','English League Two','EFL League Two'):
            self.assertEqual(profile({'country':'England','name':name})['tier'],'senior_division_4')
        reserve = fixture(1,'England','League Two')
        reserve['teams']['home']['name'] = 'United U21'
        self.assertFalse(fixture_allowed(reserve))

    def test_blacklist_precedes_stats_odds_and_supplemental_discovery(self):
        client = Mock()
        candidate = fixture(101,'Norway','2. Division - Group 1')
        self.assertIsNone(engine.build_fixture_output(client,candidate,{},{}))
        client.get.assert_not_called()
        collector = OddsEnricher(client,{},NOW)
        collector.register_fixture(meta(candidate))
        empty={'collection':{'complete':True},'response':[]}
        self.assertIs(collector.enrich(empty,101),empty)
        self.assertEqual(collector.calls,0)
        collector.reserve_priority([{'fixture':meta(candidate)}],NOW)
        self.assertFalse(collector.priority_fixtures)
        refresher=NearKickoffRefresher(client,{})
        self.assertIs(refresher.recover(empty,meta(candidate),NOW),empty)
        client.get.assert_not_called()

    def test_saved_blacklisted_match_cannot_reappear_in_board_or_live_publication(self):
        old, new = fixture(7,'Romania','Liga III - Serie 1'), fixture(8,'England','League Two')
        archived={'fixture':meta(old),'gold_scope':{'policy_id':'gold-senior-whitelist-v2.1','allowed':True},
                  'multimarket':{'model_id':MODEL_ID,'matrix':{'rows':[], 'quote_monitoring':{'radar_state':'radar_watch'}}}}
        original=deepcopy(archived)
        day={'date':'2026-10-26','matches':[archived,{'fixture':meta(new)}]}
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder,'data','arab_quote_day1.json');target.parent.mkdir()
            target.write_text(json.dumps(day))
            self.assertEqual([r['fixture_id'] for r in live_publication(Path(folder))['fixtures']],['8'])
        build_board(day,NOW)
        self.assertEqual([m['fixture']['fixture_id'] for m in day['matches']],['8'])
        self.assertEqual(day['gold_scope']['policy_id'],SCOPE_ID)
        self.assertEqual(day['gold_scope']['exclusion_reasons']['excluded_lower_division'],1)
        self.assertEqual(archived,original)
        before=deepcopy(day);enforce_day(day);self.assertEqual(day,before)

    def test_league_two_reaches_discovery_and_ordinary_gold_platinum_gates(self):
        good, bad = fixture(42,'England','League Two'),fixture(43,'Poland','II Liga - East')
        class Client:
            odds_enricher=None
            def __init__(self): self.calls=[]
            def get(self,path,params,**kwargs):
                self.calls.append((path,dict(params)))
                return {'response':[good,bad] if path=='fixtures' else []}
        client=Client()
        day={'date':'2026-10-26','matches':[]}
        complete_day({'fixture_meta':meta,'_result_payload':lambda x:None},client,day,NOW)
        self.assertEqual([m['fixture']['fixture_id'] for m in day['matches']],['42'])
        self.assertIn(('odds',{'fixture':42}),client.calls)
        self.assertNotIn(('odds',{'fixture':43}),client.calls)
        self.assertFalse(day['matches'][0]['multimarket'].get('best_candidate'))
        # Run priced snapshots through the same board that emits both tiers.
        gold_candidate=goal_row()
        platinum_candidate={**deepcopy(CASES['base']), 'provider_updated_at':NOW, 'quote_observed_at':NOW}
        assessment={'model_id':MODEL_ID,'matrix':{'rows':[gold_candidate,platinum_candidate]}}
        day['matches'][0]['multimarket']=assessment
        build_board(day,NOW)
        self.assertEqual(assessment['matrix']['opportunity_policy']['best_market_id'],gold_candidate['id'])
        self.assertEqual(assessment['matrix']['platinum_policy']['best_market_id'],platinum_candidate['id'])

    def test_compiled_browser_rules_and_versioned_assets_match_policy(self):
        script=(ROOT/'web/competition-scope.js').read_bytes()
        data=json.loads(re.search(rb'^  const rules = (.*);$',script,re.M)[1])
        self.assertEqual(data,browser_data())
        url='/competition-scope.js?v='+sha256(script).hexdigest()[:12]
        for page,next_script in [('index.html','homepage.js'),('gold-dashboard-live.html','/lateral-matrix.js')]:
            html=(ROOT/'web'/page).read_text()
            self.assertIn(f'<script src="{url}"></script><script src="{next_script}?v=',html)
