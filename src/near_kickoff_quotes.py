"""One bounded recovery per fixture/cycle, only for previously observed prices."""
import os
import time

from lateral_matrix import base_specs, market_id
from matrix_quotes import collect
from odds_collection import merge_payload, coverage
from quote_sources import fresh, number, stamp, synthetic_margin
from refresh_opportunities import captured_prices


def needs_refresh(payload, state, meta, now):
    start, observed = stamp(meta.get('kickoff')), stamp(now)
    if not start or not observed or not 0 < (start-observed).total_seconds() <= 14400: return False
    fid = str(meta['fixture_id'])
    history = state.get('matrix_quotes', {}).get(fid)
    if not history: return False
    saved = captured_prices(history, [{'spec': spec} for spec in base_specs()])
    expected = {market_id(spec) for group in saved.values() for token, spec in group['specs'].items()
                if number(group['odds'].get(token)) is not None and group['odds'][token] > 1}
    current, _ = collect(payload, fid, now)
    available = {market_id(spec) for group in current.values()
                 if fresh(group.get('provider_updated_at'), now, meta['kickoff']) and
                    (group['complete'] or synthetic_margin(group) is not None)
                 for spec in group['specs'].values()}
    return bool(expected-available)


class NearKickoffRefresher:
    def __init__(self, client, state, max_calls=40):
        self.client, self.state = client, state
        self.max_calls = max(0, min(200, int(max_calls)))
        self.calls, self.attempted = 0, set()
        self.quota, self.quota_at, self.checked = None, 0, None
        self.reserve = 7500
        self.report = {'cap': self.max_calls, 'http_calls': 0, 'fixtures_attempted': 0,
                       'fixtures_recovered': 0, 'budget_skips': 0, 'quota_skips': 0,
                       'cooldown_skips': 0, 'errors': 0}
        state.setdefault('near_kickoff_quotes', {})['last_run'] = self.report

    def request(self, path, params, limit=None):
        budget = self.max_calls-self.calls
        if limit is not None: budget = min(budget, max(0, limit))
        if budget <= 0: return None
        before = self.client.request_count
        try:
            result, _ = self.client.get_bounded(path, params, budget)
            return result
        finally:
            self.calls += self.client.request_count-before
            self.report['http_calls'] = self.calls

    def recover(self, payload, meta, now):
        fid = str(meta['fixture_id'])
        if fid in self.attempted or not needs_refresh(payload, self.state, meta, now): return payload
        self.attempted.add(fid)
        data = self.state['near_kickoff_quotes']
        cooldown = data.setdefault('last_attempt', {})
        cutoff = stamp(now).timestamp()-7*86400
        for key in list(cooldown):
            if not stamp(cooldown[key]) or stamp(cooldown[key]).timestamp() < cutoff: del cooldown[key]
        prior = stamp(cooldown.get(fid))
        if prior and 0 <= (stamp(now)-prior).total_seconds() < 1800:
            self.report['cooldown_skips'] += 1
            return payload
        if self.calls >= self.max_calls:
            self.report['budget_skips'] += 1
            return payload
        if self.checked is None or time.monotonic()-self.checked > 60:
            info = (self.request('status', {}) or {}).get('response', {}).get('requests', {})
            used, limit = info.get('current'), info.get('limit_day')
            self.quota = limit-used if type(used) is int and type(limit) is int and limit > 0 else None
            self.reserve = max(7500, int(limit*.1)) if type(limit) is int else 7500
            self.quota_at, self.checked = self.client.request_count, time.monotonic()
        available = None if self.quota is None else self.quota-(self.client.request_count-self.quota_at)-self.reserve
        if available is None or available <= 0:
            self.report['quota_skips'] += 1
            return payload
        if self.calls >= self.max_calls:
            self.report['budget_skips'] += 1
            return payload
        cooldown[fid] = now
        self.report['fixtures_attempted'] += 1
        extra = self.request('odds', {'fixture': int(fid)}, available)
        if not extra or not extra.get('collection', {}).get('complete'):
            self.report['errors'] += 1
            return payload
        candidate = merge_payload(payload or {}, extra, fid, now)
        # Never replace a complete original pair with a partial targeted response.
        if not coverage(payload, fid, now, False) <= coverage(candidate, fid, now, False): return payload
        if not needs_refresh(candidate, self.state, meta, now): self.report['fixtures_recovered'] += 1
        return candidate


def recover_near_kickoff(client, payload, state, meta, now):
    if not hasattr(client, 'get_bounded'): return payload
    refresher = getattr(client, 'near_kickoff_refresher', None)
    if refresher is None:
        try: cap = int(os.environ.get('NEAR_KICKOFF_MAX_HTTP_CALLS', '40'))
        except ValueError: cap = 40
        refresher = client.near_kickoff_refresher = NearKickoffRefresher(client, state, cap)
    return refresher.recover(payload, meta, now)
