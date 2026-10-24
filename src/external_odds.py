"""Bounded, indicative FT prices from public OddStorm comparison pages.

Never enters the API-Football ledger, calibration, movement or Gold signals.
The public preview is delayed; observed_at is NOT a bookmaker update time.
"""
import math
import re
import time
import unicodedata
from copy import deepcopy
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

BASE = 'https://www.oddstorm.com'
INDEX = BASE + '/it/odds/league/2180990-italy-serie-c'
MARKETS = {'over25_ft': '2.5', 'over35_ft': '3.5'}
PREFERRED = ('BWin', 'Bet365', 'Unibet', 'Pinnacle', 'Ladbrokes', 'BetWay')


def text(html):
    return ' '.join(unescape(re.sub(r'<[^>]*>', ' ', html)).split())


def team(value):
    value = ''.join(c for c in unicodedata.normalize('NFKD', value or '') if not unicodedata.combining(c)).lower()
    value = re.sub(r'\b(?:calcio|fc|ac|as|asd|us|ssd|1922|1908|1898)\b', '', value)
    return re.sub(r'[^a-z0-9]', '', value)


def parse_index(html):
    # Preserve home/away ordering. Exact normalized names only, no fuzzy match.
    found = {}
    for href, label in re.findall(r'<a\b[^>]*href="(/it/odds/match/[^"?]+)"[^>]*>(.*?)</a>', html, re.S):
        parts = text(label).split(' – ')
        if len(parts) == 2:
            key = tuple(map(team, parts))
            found.setdefault(key, set()).add(href)
    return {key: next(iter(paths)) for key, paths in found.items() if len(paths) == 1}


def parse_pairs(html, meta, line):
    heading = re.search(r'<h1\b[^>]*>(.*?)</h1>', html, re.S)
    date = re.search(r'<p\b[^>]*class="od-matchmeta"[^>]*>(.*?)</p>', html, re.S)
    if not heading or not date:
        return []
    title = text(heading[1])
    suffix = ' Over/Under ' + line
    if not title.endswith(suffix):
        return []
    names = title[:-len(suffix)].split(' – ')
    if len(names) != 2 or tuple(map(team, names)) != (team(meta.get('home')), team(meta.get('away'))):
        return []
    try:
        source_day = datetime.strptime(text(date[1]).split(' · ')[0], '%A %d %B %Y').date()
        fixture_day = datetime.fromisoformat(str(meta.get('kickoff', '')).replace('Z', '+00:00')).date()
        if source_day != fixture_day:
            return []
    except (ValueError, KeyError):
        return []
    table = re.search(r'<table\b[^>]*class="od-table od-mbtable"[^>]*>(.*?)</table>', html, re.S)
    if not table:
        return []
    headers = [text(c) for c in re.findall(r'<th\b[^>]*>(.*?)</th>', table[1], re.S)]
    if len(headers) != 4 or headers[1:] != ['Handicap', 'Over', 'Under']:
        return []
    result = []
    for row in re.findall(r'<tr\b[^>]*>(.*?)</tr>', table[1], re.S):
        cells = [text(c) for c in re.findall(r'<td\b[^>]*>(.*?)</td>', row, re.S)]
        if len(cells) != 4 or cells[1] != line or cells[0] not in PREFERRED:
            continue
        try:
            over, under = float(cells[2]), float(cells[3])
            if not all(math.isfinite(v) and 1 < v <= 100 for v in (over, under)):
                continue
            # Reject corrupt pairs; this is validation, not a model input.
            if not .95 <= 1 / over + 1 / under <= 1.35:
                continue
        except ValueError:
            continue
        result.append({'bookmaker': cells[0], 'odds': {'over': over, 'under': under}})
    return sorted(result, key=lambda r: PREFERRED.index(r['bookmaker']))


class Recovery:
    def __init__(self, fetch=None, max_requests=40, budget_seconds=90):
        self.fetch = fetch or self._get
        self.max_requests = max_requests
        self.budget_seconds = budget_seconds
        self.elapsed = 0.
        self.index = None
        self.cache = {}
        self.report = {'source': 'OddStorm', 'requests': 0, 'errors': 0, 'markets_recovered': 0, 'fixtures': {}}

    @staticmethod
    def _get(url):
        if urlsplit(url).hostname != 'www.oddstorm.com' or not url.startswith(BASE + '/it/odds/'):
            raise ValueError('external source not allowed')
        req = Request(url, headers={'User-Agent': 'ArabSniper/1.0 (public prematch comparison)', 'Accept': 'text/html'})
        with urlopen(req, timeout=8) as response:
            if urlsplit(response.url).hostname != 'www.oddstorm.com':
                raise ValueError('unexpected redirect')
            body = response.read(2_000_001)
            if len(body) > 2_000_000:
                raise ValueError('response too large')
            return body.decode('utf-8')

    def read(self, url):
        if url in self.cache:
            return self.cache[url]
        if self.report['requests'] >= self.max_requests or self.elapsed >= self.budget_seconds:
            return ''
        started = time.monotonic()
        self.report['requests'] += 1
        try:
            self.cache[url] = self.fetch(url)
        except Exception:
            self.report['errors'] += 1
            self.cache[url] = ''
        finally:
            self.elapsed += time.monotonic() - started
        return self.cache[url]

    def apply(self, output, as_of):
        meta = output.get('fixture', {})
        if meta.get('country') != 'Italy' or not str(meta.get('league', '')).startswith('Serie C') or meta.get('result'):
            return
        try:
            now = datetime.fromisoformat(as_of.replace('Z', '+00:00'))
            kickoff = datetime.fromisoformat(str(meta.get('kickoff', '')).replace('Z', '+00:00'))
            if now.tzinfo is None or kickoff.tzinfo is None or not 0 < (kickoff - now).total_seconds() <= 48 * 3600:
                return
        except (ValueError, KeyError, TypeError):
            return
        rows = output.get('multimarket', {}).get('markets', [])
        missing = {r['market'] for r in rows if r.get('market') in MARKETS and not valid_odd(r.get('odd'))}
        if not missing:
            return
        if self.index is None:
            self.index = parse_index(self.read(INDEX))
        path = self.index.get((team(meta.get('home')), team(meta.get('away'))))
        if not path:
            return
        recovered = {}
        for market in sorted(missing):
            url = urljoin(BASE, path) + '/over-under?h=' + MARKETS[market]
            pairs = parse_pairs(self.read(url), meta, MARKETS[market])
            if not pairs:
                continue
            quote = {**pairs[0], 'source': 'OddStorm', 'source_url': url,
                     'observed_at': datetime.now(timezone.utc).isoformat(),
                     'provider_updated_at': None, 'indicative': True,
                     'freshness': 'public_delayed_preview', 'opening_certified': False}
            recovered[market] = quote
        if not recovered:
            return
        # Display-only patch. No external quote enters API history or calibration.
        def patch(value):
            if isinstance(value, dict):
                q = recovered.get(value.get('market'))
                side = value.get('selection')
                if q and side in q['odds'] and 'odd' in value and not valid_odd(value['odd']):
                    value['odd'] = q['odds'][side]
                    value['quote'] = deepcopy(q)
                    value['quote_source'] = 'external_indicative'
                    value['expected_value_pct'] = None
                    value['price_assessment'] = None
                    value['movement'] = {'confirmed': False, 'paired_books': 0, 'reason': 'quota_esterna_senza_storico'}
                for child in value.values():
                    if isinstance(child, (dict, list)):
                        patch(child)
            elif isinstance(value, list):
                for child in value:
                    patch(child)
        patch(output.get('multimarket', {}))
        output['external_quotes'] = recovered
        self.report['markets_recovered'] += len(recovered)
        self.report['fixtures'][str(meta.get('fixture_id'))] = recovered


def valid_odd(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 1
