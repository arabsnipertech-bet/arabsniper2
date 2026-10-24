import sys
import unittest
from copy import deepcopy
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from external_odds import Recovery, parse_pairs, parse_index, parse_catalog, INDEX
from urllib.error import HTTPError

META = {'fixture_id': 1, 'country': 'Italy', 'league': 'Serie C - Girone C',
        'home': 'Scafatese', 'away': 'Barletta', 'kickoff': '2026-10-24T14:30:00+02:00'}
INDEX_HTML = '<a href="/it/odds/match/123-scafatese-vs-barletta">Scafatese Calcio 1922 – Barletta</a>'

def page(line='2.5'):
    return f'''<a href="/it/odds/league/123-italy-serie-c">Italy, Serie C</a><h1>Scafatese Calcio 1922 – Barletta <span>Over/Under {line}</span></h1>
    <p class="od-matchmeta">Saturday 24 October 2026 · 12:30 UKT</p>
    <table class="od-table od-mbtable"><tr><th>Bookmaker</th><th>Handicap</th><th>Over</th><th>Under</th></tr>
    <tr><td>BWin</td><td>{line}</td><td>2.00</td><td>1.70</td></tr></table>'''

def match(odd=None):
    return {'fixture': deepcopy(META), 'multimarket': {'markets': [
        {'market':'over25_ft','selection':side,'odd':odd,'statistical_pct':61,
         'book_probability_pct':None,'movement':{'confirmed':False}}
        for side in ('over','under')]}}

class ExternalOddsTests(unittest.TestCase):
    def test_non_italian_fixture_is_recovered(self):
        for nation in ('England', 'Spain', 'Japan', 'Brazil'):
            r=Recovery(fetch=lambda url: INDEX_HTML if url==INDEX else page().replace('Italy, Serie C', nation+', League'))
            m=match();m['fixture'].update(country=nation,league='League')
            r.apply(m,'2026-10-24T08:00:00+00:00')
            self.assertEqual(m['multimarket']['markets'][0]['odd'],2.)

    def test_wrong_country_is_rejected(self):
        self.assertEqual(parse_pairs(page(),{**META,'country':'Spain'},'2.5'),[])

    def test_catalog_finds_future_fixture(self):
        catalog='<optgroup label="Italy"><option value="/it/odds/league/123-serie-c">Serie C (25)</option></optgroup>'
        self.assertEqual(len(parse_catalog(catalog)),1)
        def fetch(url):
            if url==INDEX:return catalog
            if '/league/' in url:return INDEX_HTML
            return page().replace('24 October','27 October').replace('Saturday','Tuesday')
        r=Recovery(fetch=fetch);m=match();m['fixture']['kickoff']='2026-10-27T14:30:00+02:00'
        r.apply(m,'2026-10-24T08:00:00+00:00')
        self.assertEqual(m['multimarket']['markets'][0]['odd'],2.)

    def test_access_rejection_stops_further_requests(self):
        def fetch(url):raise HTTPError(url,403,'Forbidden',{},None)
        r=Recovery(fetch=fetch);r.read(INDEX);r.read(INDEX+'other')
        self.assertEqual(r.report['requests'],1)
        self.assertTrue(r.disabled)

    def test_parses_both_sides_same_operator(self):
        self.assertEqual(parse_pairs(page(), META, '2.5')[0]['odds'], {'over':2.,'under':1.7})

    def test_identity_date_line_and_header_fail_closed(self):
        for html in (page().replace('24 October','25 October'), page().replace('Scafatese Calcio 1922','Barletta'),
                     page('3.5'), page().replace('<th>Over</th><th>Under</th>','<th>Under</th><th>Over</th>')):
            self.assertEqual(parse_pairs(html,META,'2.5'),[])

    def test_bad_values_and_exchange_rejected(self):
        for html in (page().replace('2.00','NaN'),page().replace('1.70','-'),page().replace('BWin','BetfairExchange')):
            self.assertEqual(parse_pairs(html,META,'2.5'),[])

    def test_ambiguous_names_rejected(self):
        self.assertEqual(parse_index(INDEX_HTML + INDEX_HTML.replace('123-','456-')), {})

    def test_fill_only_missing_no_signal_change(self):
        r=Recovery(fetch=lambda url: INDEX_HTML if url==INDEX else page())
        m=match(); r.apply(m,'2026-10-24T08:00:00+00:00')
        over,under=m['multimarket']['markets']
        self.assertEqual((over['odd'],under['odd']),(2.,1.7))
        self.assertEqual(over['statistical_pct'],61)
        self.assertIsNone(over['book_probability_pct'])
        self.assertFalse(over['movement']['confirmed'])
        self.assertIsNone(over['quote']['provider_updated_at'])
        self.assertTrue(over['quote']['indicative'])

    def test_api_returns_and_wins_without_external_request(self):
        r=Recovery(fetch=lambda url: self.fail('API priced fixture must not request external data'))
        m=match(1.9); before=deepcopy(m);r.apply(m,'2026-10-24T08:00:00+00:00')
        self.assertEqual(m,before)

    def test_partial_api_market_preserved(self):
        r=Recovery(fetch=lambda url: INDEX_HTML if url==INDEX else page())
        m=match();m['multimarket']['markets'][0]['odd']=1.9
        r.apply(m,'2026-10-24T08:00:00+00:00')
        self.assertEqual(m['multimarket']['markets'][0]['odd'],1.9)
        self.assertEqual(m['multimarket']['markets'][1]['odd'],1.7)

    def test_no_live_or_unknown_kickoff(self):
        r=Recovery(fetch=lambda url: self.fail('not prematch'))
        for kickoff in ('2026-10-24T07:00:00+00:00',None,'invalid'):
            m=match();m['fixture']['kickoff']=kickoff;r.apply(m,'2026-10-24T08:00:00+00:00')
            self.assertIsNone(m['multimarket']['markets'][0]['odd'])

    def test_provider_failure_and_request_cap(self):
        def fail(url): raise OSError('source unavailable')
        r=Recovery(fetch=fail);m=match();r.apply(m,'2026-10-24T08:00:00+00:00')
        self.assertIsNone(m['multimarket']['markets'][0]['odd'])
        self.assertEqual(r.report['errors'],1)
        r=Recovery(fetch=lambda url:INDEX_HTML,max_requests=1);r.apply(match(),'2026-10-24T08:00:00+00:00')
        self.assertEqual(r.report['requests'],1)

if __name__ == '__main__': unittest.main()
