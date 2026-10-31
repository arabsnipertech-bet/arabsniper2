from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from lateral_matrix import specification as S
from matrix_pipeline import quote_monitoring
from odds_collection import OddsEnricher, adaptive_cooldown, fixture_priority
from test_quote_sources import bet, feed

NOW = datetime(2026,10,31,8,tzinfo=timezone.utc)


def meta(hours, score=62):
    return {'fixture_id':100,'kickoff':(NOW+timedelta(hours=hours)).isoformat(),'competition_quality_score':score,
            'league_id':8,'date':'2026-11-01','season':2026}


class AdaptivePolling(TestCase):
    def test_boundaries_and_neutral_tier(self):
        for hours, score, kind, period, seconds in [
            (19,69,'total','ht',28800),(18,69,'total','ht',10800),
            (19,69,'halves_total','ft',28800),(19,70,'total','ht',10800),
            (19,69,'total','ft',10800),(7,60,'btts','ht',10800),
            (6,90,'result','ft',2700),(.1,50,'total','sh',2700)]:
            with self.subTest(hours=hours,score=score,period=period):
                self.assertEqual(adaptive_cooldown(meta(hours,score),kind,period,NOW),seconds)
        self.assertEqual(adaptive_cooldown({},'total','ht',NOW),10800)
        self.assertEqual(adaptive_cooldown({**meta(22,84),'competition_tier':'senior_division_2'},'total','ht',NOW),28800)

    def test_cold_first_query_only_ft_then_eight_hour_probe_and_hot_retry(self):
        state = {'bookmakers':[{'id':29}]}
        payload = feed([bet(5,2.5)],at=NOW.isoformat())
        metadata = meta(29)
        payload['response'][0]['fixture']['date'] = metadata['kickoff']
        def collect(when):
            requests = []
            client = Mock(); client.request_count = 0
            def get(path,params,use_cache=False):
                client.request_count += 1
                if path == 'status': return {'response':{'requests':{'current':10,'limit_day':75000}}}
                requests.append(params)
                return {'response':[]}
            client.get.side_effect=get
            worker = OddsEnricher(client,state,when.isoformat(),market_catalog=[
                {'id':5,'name':'Goals Over/Under'}, {'id':6,'name':'Goals Over/Under First Half'}])
            worker.register_fixture(metadata); worker.enrich(payload,100)
            return requests
        first=collect(NOW)
        self.assertEqual(first,[{'fixture':100,'bet':5}])
        self.assertNotIn(6,[r.get('bet') for r in collect(NOW+timedelta(hours=7,minutes=59))])
        self.assertIn(6,[r.get('bet') for r in collect(NOW+timedelta(hours=8))])
        self.assertIn(6,[r.get('bet') for r in collect(NOW+timedelta(hours=24))])
        self.assertNotIn(6,[r.get('bet') for r in collect(NOW+timedelta(hours=24,minutes=44))])
        self.assertIn(6,[r.get('bet') for r in collect(NOW+timedelta(hours=24,minutes=45))])

    def test_no_extra_spend_on_empty_far_fixture_and_ordering(self):
        client = Mock()
        worker = OddsEnricher(client,{},NOW.isoformat())
        worker.register_fixture(meta(31))
        empty = {'response':[],'collection':{'complete':True}}
        self.assertIs(worker.enrich(empty,100),empty)
        client.get.assert_not_called()
        prior={'multimarket':{'matrix':{'rows':[{'spec':S('result','ft','1'),'odd':2.,'quote_status':'recent'}]}}}
        self.assertLess(fixture_priority(meta(11)['kickoff'],{},NOW), fixture_priority(meta(31)['kickoff'],{},NOW))
        self.assertLess(fixture_priority(meta(24)['kickoff'],prior,NOW), fixture_priority(meta(17)['kickoff'],{},NOW))


def dashboard():
    specs = [S('total','ht','over',1.5),S('total','ft','over',2.5),S('total','ft','over',3.5),
             S('total','ht','over',2.5),S('btts','ht','yes'), S('result','ft','2')]
    return {'rows':[{'id':f'r{i}','spec':specs[i] if i<len(specs) else S('handicap','ft','home',-.5),
             'odd':2.,'quote_status':'recent','selected_margin_pct':7.,'provider_updated_at':NOW.isoformat(),
             'quote_observed_at':NOW.isoformat(),'quality':'supported','ev_pct':5.} for i in range(54)]}


def monitor(matrix):
    quote_monitoring(matrix,NOW.isoformat(),meta(2)['kickoff'])
    return matrix['quote_monitoring']


class RadarCoverage(TestCase):
    def test_54_prices_can_mean_missing_core_balanced_or_forming_pressure(self):
        source = dashboard()
        self.assertEqual(monitor(source)['radar_state'],'balanced')
        absent = deepcopy(source)
        for row in absent['rows'][:5]:
            row['spec']=S('result','ft','1')
        self.assertEqual((monitor(absent)['radar_state'],monitor(absent)['priced_markets']),('waiting_quotes',54))
        source['rows'][5].update(oci=34.2, agreeing_books=4,paired_books=6,speed_pp_per_hour=.8)
        self.assertEqual((monitor(source)['radar_state'],monitor(source)['watch_id']),('radar_watch','r5'))
        self.assertIn('r5',monitor(source)['row_ids'])
        self.assertNotIn('opportunity',source['rows'][5])

    def test_pressure_bounds_expiry_and_fair_alternative(self):
        original = dashboard()
        row = original['rows'][5]
        row.update(oci=-25,agreeing_books=3,paired_books=5,speed_pp_per_hour=.8)
        self.assertEqual(monitor(original)['radar_state'],'radar_watch')
        for patch in [{'ev_pct':0},{'quality':'limited'},{'agreeing_books':2},{'speed_pp_per_hour':.79},
                      {'oci':40},{'oci':24.99},{'quote_source':'synthetic_combo','quote_executable':False},
                      {'provider_updated_at':(NOW-timedelta(hours=1,seconds=1)).isoformat()}]:
            with self.subTest(patch=patch):
                m=deepcopy(original);m['rows'][5].update(patch)
                self.assertEqual(monitor(m)['radar_state'],'balanced')
        row.update(oci=None,fair_delta_pp=-1.2)
        self.assertEqual(monitor(original)['radar_state'],'radar_watch')
        quote_monitoring(original,meta(3)['kickoff'],meta(2)['kickoff'])
        self.assertIsNone(original['quote_monitoring']['watch_id'])
