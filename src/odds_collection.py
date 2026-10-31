"""Budgeted, measurable recovery of complete bookmaker markets from API-Football.

Supplemental queries never synthesize an outcome or merge partial market sides.
Persistent cooldowns avoid repeatedly asking the same empty question.
"""
from copy import deepcopy
from datetime import datetime, timezone
import time
from multimarket_model import collect_quotes, stamp
from quote_sources import direct_combo_groups, normalize_payload
from market_discovery import identify, clean, VERSION as DISCOVERY_VERSION

TARGETS = ('1x2_ft', 'over25_ft', 'over35_ft', 'over15_ht', 'over25_ht', 'gg_ht', 'over05_ht', 'over05_sh', 'combo_ht_sh')
BET_IDS = {'1x2_ft':(1,), 'over25_ft':(5,50,336), 'over35_ft':(5,50,336),
           'over15_ht':(6,72), 'over25_ht':(6,72), 'gg_ht':(34,),
           'over05_ht':(6,72), 'over05_sh':(26,), 'combo_ht_sh':(184,)}
# Compatibility export only. Runtime routes are discovered from the catalogue
# and the entire fixture payload; these historical IDs are not admission rules.
SEMANTIC_TARGETS = {('result','ft'):('1x2_ft',),
    ('total','ft'):('over25_ft','over35_ft'), ('asian_total','ft'):('over25_ft','over35_ft'),
    ('total','ht'):('over15_ht','over25_ht','over05_ht'),
    ('asian_total','ht'):('over15_ht','over25_ht','over05_ht'),
    ('btts','ht'):('gg_ht',), ('total','sh'):('over05_sh',),
    ('halves_total','ft'):('combo_ht_sh',)}


def hours_to_kickoff(meta, now):
    start, current = stamp(meta.get('kickoff')), stamp(now)
    return (start-current).total_seconds()/3600 if start and current else None


def secondary_competition(meta):
    score = meta.get('competition_quality_score')
    # The current whitelist assigns the same admission score (84) to every
    # allowed division. Use its explicit lower-division tier for polling only.
    return (type(score) in (int, float) and score < 70 or
            score in (None, 84) and meta.get('competition_tier') in ('senior_division_2', 'senior_division_3'))


def adaptive_cooldown(meta, market_kind, period, now):
    hours = hours_to_kickoff(meta, now)
    if hours is not None and 0 < hours <= 6:
        return 2700
    if (hours is not None and hours > 18 and secondary_competition(meta)
            and (period in ('ht', 'sh') or market_kind == 'halves_total')):
        return 28800
    return 10800


def fixture_priority(kickoff, previous, now):
    hours = hours_to_kickoff({'kickoff': kickoff}, now)
    rows = ((previous.get('multimarket') or {}).get('matrix') or {}).get('rows', [])
    priced = [r.get('spec', {}) for r in rows if r.get('quote_status') in ('recent', 'cached_stable')
              and isinstance(r.get('odd'), (float, int)) and r['odd'] > 1]
    result = any(s.get('kind') == 'result' and s.get('period') == 'ft' for s in priced)
    goals = {(s.get('period'), s.get('line')) for s in priced
             if s.get('kind') in ('total', 'asian_total') and s.get('side') == 'over'}
    gaps = result and not {('ht', 1.5), ('ft', 2.5), ('ft', 3.5)} <= goals
    return (0 if hours is not None and 0 < hours <= 12 or gaps else 1,
            hours if hours is not None else float('inf'))


def discovery_routes(catalog, rows, counts, *, with_identity=False):
    routes={}
    observed=[bet for row in rows for book in row.get('bookmakers',[]) for bet in book.get('bets',[])]
    for bet in [*catalog,*observed]:
        identity=identify(bet.get('name'))
        if not identity or bet.get('id') is None: continue
        targets=SEMANTIC_TARGETS.get((identity['kind'],identity['period']),())
        if targets and min(counts[t] for t in targets)<8:
            routes[bet['id']]=(min(counts[t] for t in targets), identity['kind'], identity['period'])
    ordered = sorted(routes, key=lambda bid: (routes[bid][0], str(bid)))
    return [(bid, *routes[bid][1:]) for bid in ordered] if with_identity else ordered


def coverage(payload, fid, now, target_only=True):
    normalized = normalize_payload(payload or {}, now)
    quotes = collect_quotes(normalized, now)
    result = {(market, book) for market, books in quotes.get(str(fid), {}).items()
              if not target_only or market in TARGETS for book in books}
    result.update(('combo_ht_sh', group['bookmaker_id'])
                  for group in direct_combo_groups(normalized, fid, now).values())
    return result


def merge_payload(base, extra, fid, now):
    """Keep each bet whole, newest provider snapshot wins; never join odds sides."""
    result = {key: deepcopy(value) for key, value in base.items() if key != 'response'}
    current = stamp(now)
    records = {}
    for supplemental, payload in ((False, base), (True, extra or {})):
        for row in payload.get('response', []):
            if str(row.get('fixture', {}).get('id')) != str(fid):
                continue
            updated = stamp(row.get('update'))
            kickoff = stamp(row.get('fixture', {}).get('date'))
            if supplemental and (not updated or not kickoff or kickoff <= current or
                                 not 0 <= (current-updated).total_seconds() <= 21600):
                continue
            for book in row.get('bookmakers', []):
                for bet in book.get('bets', []):
                    key = (str(book.get('id')), str(bet.get('id')) if bet.get('id') is not None else 'name:'+clean(bet.get('name')))
                    previous = records.get(key)
                    if previous and (not updated or previous[0] and updated <= previous[0]):
                        continue
                    records[key] = (updated, row, book, bet)
    groups = {}
    for updated, row, book, bet in records.values():
        # Group only identical snapshot timestamps, retaining whole markets.
        key = (row.get('update'), str(book.get('id')))
        if key not in groups:
            # Copy each selected subtree once, excluding the arrays rebuilt here.
            # No full-feed or full-book copies inside the per-market loop.
            groups[key] = {**{k: deepcopy(v) for k, v in row.items() if k != 'bookmakers'},
                           'bookmakers': [{**{k: deepcopy(v) for k, v in book.items() if k != 'bets'}, 'bets': []}]}
        groups[key]['bookmakers'][0]['bets'].append(deepcopy(bet))
    result['response'] = list(groups.values())
    result['results'] = len(groups)
    return result


class OddsEnricher:
    def __init__(self, client, state, now=None, max_calls=1200, max_seconds=300, market_catalog=None):
        self.client, self.state = client, state
        self.fixed_now = now
        self.now = now or datetime.now(timezone.utc).isoformat()
        self.elapsed = 0.0
        self.fixtures = {}
        self.priority_fixtures = set()
        self.max_calls, self.max_seconds = max_calls, max_seconds
        self.calls = 0
        self.quota_remaining = None
        self.quota_checked_at = None
        self.quota_calls = 0
        self.reserve = 7500
        self.stats = dict(odds_targeted_requests=0, odds_targeted_books_recovered=0,
                          odds_targeted_markets_recovered=0, odds_targeted_empty=0,
                          odds_targeted_errors=0, odds_targeted_no_gain=0,
                          odds_targeted_cooldown_skips=0, odds_targeted_budget_stops=0,
                          odds_targeted_fixtures=0, odds_targeted_unproductive_skips=0)
        self.report = []
        self.catalog = state.get('bookmakers', [])
        self.market_catalog = market_catalog if market_catalog is not None else state.get('market_catalog', [])
        if market_catalog is not None: state['market_catalog']=deepcopy(market_catalog)
        if state.get('discovery_version') and state['discovery_version'] != DISCOVERY_VERSION:
            # Old no-gain decisions used stricter market labels. Let the
            # bounded collector reassess them without touching prices or history.
            state.pop('route_health', None)
            state.pop('queries', None)
        state['discovery_version']=DISCOVERY_VERSION

    def allowed(self, check_limits=True):
        if check_limits and (self.calls >= self.max_calls or self.elapsed >= self.max_seconds):
            return False
        # Recheck provider quota to account for other consumers and UTC resets.
        if self.quota_checked_at is None or time.monotonic()-self.quota_checked_at > 60:
            status = self.client.get('status', {}, use_cache=False)
            info = (status or {}).get('response', {}).get('requests', {})
            used, limit = info.get('current'), info.get('limit_day')
            self.quota_remaining = limit-used if type(used) is int and type(limit) is int else None
            self.reserve = max(7500, int(limit*.1)) if type(limit) is int else 7500
            self.quota_calls = self.client.request_count
            self.quota_checked_at = time.monotonic()
        remaining = None if self.quota_remaining is None else self.quota_remaining-(self.client.request_count-self.quota_calls)
        return remaining is not None and remaining > self.reserve+10

    def refresh_catalog(self):
        seen = stamp(self.state.get('catalog_at'))
        if seen and 0 <= (stamp(self.now)-seen).total_seconds() < 86400 and self.catalog:
            return
        if not self.allowed():
            return
        payload = self.client.get('odds/bookmakers', {}, use_cache=False)
        if payload and payload.get('response'):
            self.catalog = [{'id':b['id'],'name':b.get('name') or str(b['id'])}
                            for b in payload['response'] if b.get('id') is not None]
            self.state.update(bookmakers=self.catalog, catalog_at=self.now)

    def reserve_priority(self, prior_matches, now):
        self.priority_fixtures = {str(m['fixture']['fixture_id']) for m in prior_matches
            if (m.get('fixture') or {}).get('status_short') in ('NS', 'TBD')
            and (hours_to_kickoff(m['fixture'], now) or 0) > 0
            and fixture_priority(m['fixture'].get('kickoff'), m, now)[0] == 0}

    def register_fixture(self, meta):
        """Trusted fixture metadata from the fixtures endpoint, never inferred odds."""
        self.fixtures[str(meta["fixture_id"])] = dict(meta)

    def enrich(self, base, fid):
        self.now = self.fixed_now or datetime.now(timezone.utc).isoformat()
        prioritized = str(fid) in self.priority_fixtures
        self.priority_fixtures.discard(str(fid))
        if not base or not base.get('collection', {}).get('complete'):
            return base
        rows = base.get('response', [])
        meta = self.fixtures.get(str(fid), {})
        # Verify kickoff from independently fetched fixture metadata for empty odds.
        starts = [stamp(r.get('fixture', {}).get('date')) for r in rows if str(r.get('fixture', {}).get('id'))==str(fid)]
        if not starts and meta:
            starts = [stamp(meta.get("kickoff"))]
        if not starts or any(t is None or t <= stamp(self.now) for t in starts):
            return base
        before_all = coverage(base, fid, self.now, target_only=False)
        before = {item for item in before_all if item[0] in TARGETS}
        current_all, current = before_all, before
        present_books = {str(b['id']) for r in rows for b in r.get('bookmakers', [])}
        counts = {k:sum(m==k for m,b in before) for k in TARGETS}
        # Query sparsest goal markets first, but do not alter eligibility thresholds.
        start = stamp(meta.get('kickoff')) or min(starts)
        meta = {**meta, 'kickoff': start.isoformat()}
        hours = hours_to_kickoff(meta, self.now)
        if hours <= 0:
            return base
        if hours > 30 and not present_books:
            self.stats['odds_targeted_unproductive_skips'] += 1
            return base
        distant = hours > 18 and secondary_competition(meta)
        routes = discovery_routes(self.market_catalog, rows, counts, with_identity=True)
        identities = {bid: (kind, period) for bid, kind, period in routes}
        markets = list(identities)
        history = self.state.setdefault('queries', {})
        cursor = self.state.setdefault('cursors', {}).get(str(fid), 0)
        missing = [b['id'] for b in self.catalog if not distant and str(b['id']) not in present_books]
        missing = missing[cursor % len(missing):]+missing[:cursor % len(missing)] if missing else []
        # Keep catalogue expansion reachable even when all eight query slots have
        # sparse markets: interleave unknown operators and exact market routes.
        queries = []
        for index in range(max(len(markets), len(missing))):
            if index < len(markets): queries.append(('bet', markets[index]))
            if index < len(missing): queries.append(('bookmaker', missing[index]))
        uncovered = not before
        if uncovered and not distant and not queries:
            queries.append(('fixture',fid))  # Unfiltered discovery when even the catalogue is unavailable.
        if uncovered and not distant and all(meta.get(k) is not None for k in ('league_id','season','date')):
            queries.insert(0, ('league_date', meta['date']))
        result = base
        attempts = 0
        # Retain bounded calls for known priority fixtures on subsequent calendar days.
        call_limit = self.max_calls
        if not prioritized and hours > 12 and not (counts['1x2_ft'] and any(not counts[k] for k in ('over15_ht','over25_ft','over35_ft'))):
            call_limit = max(0, self.max_calls - 8*len(self.priority_fixtures))
        for kind, value in queries:
            route_kind, period = identities[value] if kind == 'bet' else (kind, 'ft')
            cooldown = adaptive_cooldown(meta, route_kind, period, self.now)
            key = f'{fid}:{kind}:{value}'
            checked = stamp(history.get(key))
            if distant and cooldown == 28800 and not checked:
                # Start the eight-hour observation window without spending a call.
                deferred = self.state.setdefault('deferred_since', {})
                checked = stamp(deferred.setdefault(key, self.now))
            if checked and (stamp(self.now)-checked).total_seconds() < cooldown:
                self.stats['odds_targeted_cooldown_skips'] += 1
                continue
            health = self.state.setdefault('route_health', {}).setdefault(kind, {})
            paused = stamp(health.get('paused_at'))
            if paused and 0 <= (stamp(self.now)-paused).total_seconds() < 21600:
                if not uncovered and hours > 6:
                    self.stats['odds_targeted_unproductive_skips'] += 1
                    continue
            elif paused:
                health.clear()
            if attempts >= 8:
                break
            if self.calls >= call_limit or not self.allowed():
                self.stats['odds_targeted_budget_stops'] += 1
                break
            start_calls = self.client.request_count
            params = ({'league':meta['league_id'],'season':meta['season'],'date':meta['date'],'timezone':'Europe/Rome'}
                      if kind == 'league_date' else {'fixture':fid,kind:value})
            started = time.monotonic()
            try:
                if callable(getattr(type(self.client),'get_bounded',None)):
                    extra,_ = self.client.get_bounded('odds',params,call_limit-self.calls)
                else:
                    extra = self.client.get('odds', params, use_cache=False)
            finally:
                self.elapsed += time.monotonic()-started
            used = self.client.request_count-start_calls
            self.calls += max(1,used)
            self.stats['odds_targeted_requests'] += max(1,used)
            attempts += 1
            if extra is None or extra.get('collection',{}).get('complete') is False:
                self.stats['odds_targeted_errors'] += 1
                continue  # Failed request is not cached as an empty result.
            history[key] = self.now
            health['no_gain_streak'] = health.get('no_gain_streak', 0)+1
            if health['no_gain_streak'] >= 24:
                health['paused_at'] = self.now
            if kind == 'bookmaker':
                self.state['cursors'][str(fid)] = cursor = cursor+1
            if not extra.get('response'):
                self.stats['odds_targeted_empty'] += 1
                continue
            candidate = merge_payload(result,extra,fid,self.now)
            candidate_all = coverage(candidate,fid,self.now,target_only=False)
            after = {item for item in candidate_all if item[0] in TARGETS}
            # Don't discard a usable market in favour of a partial targeted response.
            if not current_all <= candidate_all:
                self.stats['odds_targeted_no_gain'] += 1
                continue
            result = candidate
            if not after-current:
                self.stats['odds_targeted_no_gain'] += 1
            else:
                health.clear()
            current_all, current = candidate_all, after
        after = current
        new_books = {b for m,b in after}-{b for m,b in before}
        self.stats['odds_targeted_books_recovered'] += len(new_books)
        self.stats['odds_targeted_markets_recovered'] += len(after-before)
        self.stats['odds_targeted_fixtures'] += bool(attempts)
        self.report.append({'fixture_id':str(fid),'raw_books':len(present_books),
                            'usable_before':counts, 'usable_after':{k:sum(m==k for m,b in after) for k in TARGETS},
                            'new_books':sorted(new_books),'attempts':attempts,
                            'match':meta.get('match'), 'league':meta.get('league'),
                            'country':meta.get('country'), 'date':meta.get('date'),
                            'baseline_empty':not bool(rows), 'recovered_from_empty':not bool(rows) and bool(after)})
        return result

    def finish(self):
        cutoff = stamp(self.now).timestamp()-7*86400
        self.state['queries'] = {k:v for k,v in self.state.get('queries',{}).items() if stamp(v) and stamp(v).timestamp() >= cutoff}
        self.state['deferred_since'] = {k:v for k,v in self.state.get('deferred_since',{}).items() if k not in self.state['queries'] and stamp(v) and stamp(v).timestamp() >= cutoff}
        active = {k.split(':')[0] for k in self.state['queries']}
        self.state['cursors'] = {k:v for k,v in self.state.get('cursors',{}).items() if k in active}
        self.state.update(updated_at=self.now, last_run=dict(self.stats), coverage=self.report)
        return self.state

