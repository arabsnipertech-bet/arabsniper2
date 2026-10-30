"""Persistent raw history only: revalidate completed fixtures before each scan.

No odds, predictions or derived statistics are cached here. Every consumer keeps
its existing as-of, season, score-quality and competition filters.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from audit_storage import read_store, write_store

ROME = ZoneInfo('Europe/Rome')
TTL = timedelta(hours=12)
FINISHED = {'FT', 'AET', 'PEN'}


def instant(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result if result.tzinfo is not None else None
    except ValueError:
        return None


def descriptor(path, params):
    """Exact request shape, including sample length; unrelated endpoints bypass."""
    if type(params.get('last')) is not int or params['last'] <= 0:
        return None
    if path == 'fixtures' and params.get('status') == 'FT':
        if set(params) == {'team', 'last', 'status'}:
            return {'teams': {str(params['team'])}, 'league': None}
        if set(params) == {'league', 'season', 'last', 'status'}:
            return {'teams': set(), 'league': f"{params['league']}::{params['season']}"}
    if (path == 'fixtures/headtohead' and set(params) == {'h2h', 'last'}
            and re.fullmatch(r'\d+-\d+', str(params.get('h2h')))):
        return {'teams': set(str(params['h2h']).split('-')), 'league': None}
    return None


def valid_payload(payload):
    if not isinstance(payload, dict) or payload.get('errors'):
        return False
    rows = payload.get('response')
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        return False
    paging = payload.get('paging') or {}
    return (isinstance(paging, dict) and paging.get('total', 1) == 1
            and paging.get('current', 1) == 1
            and all(isinstance(row.get('fixture'), dict) for row in rows))


def finished_event(row):
    fixture, teams, league = row.get('fixture') or {}, row.get('teams') or {}, row.get('league') or {}
    status = (fixture.get('status') or {}).get('short')
    if status not in FINISHED:
        return None
    home, away = (teams.get('home') or {}).get('id'), (teams.get('away') or {}).get('id')
    if any(value is None for value in (fixture.get('id'), home, away, league.get('id'), league.get('season'))):
        raise ValueError('Completed fixture lacks cache invalidation metadata')
    identity = {'id': fixture['id'], 'date': fixture.get('date'), 'status': status,
                'home': home, 'away': away, 'league': league['id'], 'season': league['season'],
                'goals': row.get('goals'), 'score': row.get('score')}
    signature = sha256(json.dumps(identity, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return str(fixture['id']), signature, {str(home), str(away)}, f"{league['id']}::{league['season']}"


class HistoryCache:
    def __init__(self, path, clock=lambda: datetime.now(timezone.utc), reuse=True):
        self.path, self.clock, self.reuse = Path(path), clock, reuse
        try:
            saved = read_store(self.path, {})
        except (OSError, ValueError, TypeError, KeyError):
            saved = {}
        self.entries = (saved.get('entries', {}) if isinstance(saved, dict)
                        and saved.get('schema_version') == 1 else {})
        if not isinstance(self.entries, dict): self.entries = {}
        self.by_team, self.by_league = defaultdict(dict), defaultdict(dict)
        self.prepared, self.validated = False, False
        self.fetched = set()
        self.report = dict(hits=0, misses=0, expired=0, invalidated=0, history_requests=0,
                           validation_requests=0, validation_errors=0, validation_bypasses=0)

    def now(self):
        now = self.clock()
        if now.tzinfo is None: raise ValueError('History cache clock requires a timezone')
        return now

    def fresh(self, entry, now):
        at = instant(entry.get('saved_at')) if isinstance(entry, dict) else None
        return at is not None and timedelta() <= now-at < TTL

    def observe(self, payload):
        if not valid_payload(payload): return False
        try:
            events = [event for row in payload['response'] if (event := finished_event(row))]
        except (ValueError, TypeError, AttributeError):
            return False
        for fid, signature, teams, league in events:
            for team in teams: self.by_team[team][fid] = signature
            self.by_league[league][fid] = signature
        return True

    def prepare(self, fetch):
        if self.prepared: return
        self.prepared = True
        now = self.now()
        times = [instant(entry['saved_at']) for entry in self.entries.values()
                 if self.reuse and self.fresh(entry, now)]
        # A fixture that kicked off yesterday can finish after a cache saved
        # just after midnight. Include the preceding kickoff date as well.
        day = min([now, *times]).astimezone(ROME).date() - timedelta(days=1)
        end = now.astimezone(ROME).date()
        self.validated = True
        # At most three calendar dates with the overnight safety overlap.
        # These broad reads replace thousands of per-team freshness checks.
        while day <= end:
            self.report['validation_requests'] += 1
            payload = fetch('fixtures', {'date': day.isoformat(), 'timezone': 'Europe/Rome'})
            if not self.observe(payload):
                self.validated = False
                self.report['validation_errors'] += 1
            day += timedelta(days=1)

    def observed_for(self, desc):
        observed = {}
        for team in desc['teams']: observed.update(self.by_team[team])
        if desc['league']: observed.update(self.by_league[desc['league']])
        return observed

    def get(self, path, params, fetch):
        desc = descriptor(path, params)
        if desc is None: return fetch(path, params)
        self.prepare(fetch)
        key = path + '?' + json.dumps(params, sort_keys=True, separators=(',', ':'))
        now, entry = self.now(), self.entries.get(key)
        observed = self.observed_for(desc)
        if entry is not None and not self.fresh(entry, now):
            self.report['expired'] += 1
            self.entries.pop(key, None)
            entry = None
        if (entry is not None and valid_payload(entry.get('payload'))
                and isinstance(entry.get('seen'), dict)):
            changed = any(entry['seen'].get(fid) != signature for fid, signature in observed.items())
            if changed:
                self.report['invalidated'] += 1
                self.entries.pop(key, None)
            elif key in self.fetched or self.reuse and self.validated:
                self.report['hits'] += 1
                return deepcopy(entry['payload'])
            elif self.reuse and not self.validated:
                self.report['validation_bypasses'] += 1
        self.report['misses'] += 1
        self.report['history_requests'] += 1
        payload = fetch(path, params)
        if valid_payload(payload):
            # A game may have finished between the calendar read and this
            # response. Include it in the snapshot already fetched from HTTP.
            seen = self.observed_for(desc)
            try:
                for row in payload['response']:
                    event = finished_event(row)
                    if event:
                        fid, signature, teams, league = event
                        if desc['teams'] & teams or desc['league'] == league:
                            seen[fid] = signature
            except (ValueError, TypeError, AttributeError):
                return payload
            self.entries[key] = {'saved_at': self.now().isoformat(), 'seen': seen,
                                 'payload': deepcopy(payload)}
            self.fetched.add(key)
        return payload

    def save(self):
        now = self.now()
        self.entries = {key: entry for key, entry in self.entries.items() if self.fresh(entry, now)}
        write_store(self.path, {'schema_version': 1, 'ttl_hours': 12, 'updated_at': now.isoformat(),
                               'entries': self.entries, 'last_run': dict(self.report)})
