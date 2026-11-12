"""Read the live cache, with a bounded official-provider fallback."""
from datetime import timedelta
import os

import requests
from multimarket_model import stamp

from .selection import rome_day, utc

CACHE = 'https://arabsniperbet.com/api/live-scores'
API = 'https://v3.football.api-sports.io/'
MAX_CALLS = 26
MAX_EVENT_CALLS = 195
RESERVE = 1000


def public_cache():
    response = requests.get(CACHE, headers={'Accept': 'application/json'}, timeout=(5, 10))
    response.raise_for_status()
    value = response.json()
    if value.get('schema_version') != 1 or value.get('enabled') is not True:
        raise ValueError('Live cache disabled')
    return value


def provider(path, key, params=None):
    response = requests.get(API + path, params=params, headers={'x-apisports-key': key}, timeout=(5, 12))
    response.raise_for_status()
    value = response.json()
    if value.get('errors'):
        raise ValueError('Official score provider did not confirm the request')
    return value['response'], response.headers


def official_fixtures(store, now, params, log, *, budget='score_budget', limit=MAX_CALLS):
    key = os.environ.get('API_SPORTS_KEY')
    if not key:
        return []
    state = store.state['runtime'].get(budget)
    if not state or state['date'] != rome_day(now):
        info, _ = provider('status', key)
        quota = info.get('requests') or {}
        if not info.get('subscription', {}).get('active') or not all(
                type(quota.get(name)) is int for name in ('current', 'limit_day')):
            return []
        state = {'date': rome_day(now), 'calls': 1, 'remaining': quota['limit_day'] - quota['current']}
        store.state['runtime'][budget] = state
        store.save()
    if state['calls'] >= limit or state['remaining'] <= RESERVE:
        log('live_scores', 'provider_budget_unavailable', budget=budget)
        return []
    state['calls'] += 1
    state['remaining'] -= 1
    store.save()
    rows, headers = provider('fixtures', key, params)
    remaining = headers.get('x-ratelimit-requests-remaining')
    if remaining and remaining.isdecimal():
        state['remaining'] = min(state['remaining'], int(remaining))
    else:
        # Future requests require an authoritative remaining-quota signal.
        state['remaining'] = 0
    store.save()
    return rows


def fixture_view(row, seen, *, with_events=False):
    meta = row.get('fixture') or {}
    phase = meta.get('status') or {}
    score = row.get('score') or {}
    view = {'fixture_id': str(meta.get('id', '')), 'source': 'API-Football', 'observed_at': seen,
            'status': phase.get('short'), 'refresh_failed': False,
            'elapsed': phase.get('elapsed'), 'extra': phase.get('extra'),
            'goals': row.get('goals'), 'halftime': score.get('halftime'), 'fulltime': score.get('fulltime')}
    if with_events:
        view.update(events=row.get('events'), teams=row.get('teams'))
    return view


def feed(fixtures):
    return {'schema_version': 1, 'enabled': True, 'fixtures': fixtures}


def detailed(store, fixture, now, clock, log):
    rows = official_fixtures(store, now, {'id': fixture}, log, budget='event_score_budget', limit=MAX_EVENT_CALLS)
    if len(rows) != 1 or str((rows[0].get('fixture') or {}).get('id')) != fixture:
        return {}
    return feed({fixture: fixture_view(rows[0], utc(clock()), with_events=True)})


def read(store, states, now, clock, log):
    try:
        cached = public_cache()
    except (requests.RequestException, ValueError) as exc:
        log('live_scores', 'cache_unavailable', error=type(exc).__name__)
        cached = {}
    active = {state['pick']['fixture_id'] for state in states if not state['done']
              and stamp(state['pick']['kickoff']) <= now
              < stamp(state['pick']['kickoff']) + timedelta(hours=5)}
    fixtures = cached.get('fixtures') or {}
    missing = {fid for fid in active if not stamp((fixtures.get(fid) or {}).get('observed_at'))
               or (fixtures.get(fid) or {}).get('refresh_failed')
               or (fixtures[fid].get('status') not in ('FT', 'AET', 'PEN') and
                   now - stamp(fixtures[fid]['observed_at']) > timedelta(minutes=6))}
    rows = official_fixtures(store, now, {'ids': '-'.join(sorted(missing))}, log) if missing else []
    if not rows:
        return cached
    seen = utc(clock())
    fixtures = dict(fixtures)
    for row in rows:
        meta = row.get('fixture') or {}
        fid = str(meta.get('id', ''))
        if fid not in missing:
            continue
        fixtures[fid] = fixture_view(row, seen)
    return feed(fixtures)
