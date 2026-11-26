"""Read already stored provider scores before spending the shared API allowance."""
import json
import subprocess
from urllib.request import urlopen

from multimarket_model import stamp
from v7_archive import FINISHED, ROME, VOID, checked_score, merge_verified_result, rescheduled_result

LOOKBACK_DAYS = 35
LIVE_URL = 'https://arabsniperbet.com/api/live-scores'
SCORE_FIELDS = ('home_ft', 'away_ft', 'home_ht', 'away_ht')
MAX_CACHE_BYTES = 2*1024*1024


def cached_feed():
    """Read only the Worker cache; GET never invokes its provider refresh.

    Use the same bounded transport as the existing score-health watchdog when
    urllib cannot read the public route. There is no API-Football fallback here.
    """
    try:
        with urlopen(LIVE_URL, timeout=8) as response:
            raw = response.read(MAX_CACHE_BYTES+1)
    except OSError:
        result = subprocess.run(('curl', '-fsS', '--max-time', '12', '--max-filesize',
                                 str(MAX_CACHE_BYTES), '-H', 'Accept: application/json', LIVE_URL),
                                check=True, capture_output=True, timeout=15)
        raw = result.stdout
    if len(raw) > MAX_CACHE_BYTES:
        raise ValueError('Live cache too large')
    return json.loads(raw)


def result_for_day(result, day):
    """A confirmed move to another Rome date closes the old schedule without a score."""
    kickoff = stamp(result.get('provider_kickoff'))
    if not kickoff or kickoff.astimezone(ROME).date().isoformat() == str(day):
        return result
    return checked_score({**{key: result.get(key) for key in ('source', 'transport', 'provider_observed_at')},
                          'status': 'PST', 'rescheduled_to': kickoff.isoformat(),
                          'provider_status': result.get('status')})


def needs_result(score):
    if not score:
        return True
    if rescheduled_result(score):
        return False
    return score.get('status') not in VOID and (score.get('status') not in FINISHED
                                               or any(score.get(field) is None for field in SCORE_FIELDS))


def worker_score(row):
    status = row.get('status')
    ft, ht = row.get('fulltime') or {}, row.get('halftime') or {}
    if status == 'FT' and (ft.get('home') is None or ft.get('away') is None):
        ft = row.get('goals') or {}
    return checked_score({'status': status, 'home_ht': ht.get('home'), 'away_ht': ht.get('away'),
                          'home_ft': ft.get('home'), 'away_ft': ft.get('away'),
                          'extra_time': row.get('extratime'), 'penalties': row.get('penalties'),
                          'source': 'API-Football', 'transport': 'live_worker',
                          'provider_observed_at': row.get('observed_at'),
                          'provider_kickoff': row.get('provider_kickoff')})


def read_live_scores(fixtures, now):
    try:
        data = cached_feed()
        if not (data.get('schema_version') == 1 and data.get('enabled') is True
                and isinstance(data.get('fixtures'), dict)):
            raise ValueError('Unavailable live score schema')
        scores = {}
        for fid, kickoff in fixtures.items():
            row = data['fixtures'].get(fid) or {}
            observed, began = stamp(row.get('observed_at')), stamp(kickoff)
            if not (row.get('fixture_id') == fid and row.get('source') == 'API-Football'
                    and stamp(row.get('provider_kickoff')) and observed and began and began < observed <= now):
                continue
            result = result_for_day(worker_score(row), began.astimezone(ROME).date())
            if result['status'] in FINISHED | VOID or rescheduled_result(result):
                scores[fid] = result
        return scores
    except (OSError, TypeError, ValueError, AttributeError, subprocess.SubprocessError) as error:
        print('Stored live scores unavailable: '+type(error).__name__, flush=True)
        return {}


def use_stored_scores(fixtures, saved, live):
    scores, disputes = dict(saved), set()
    for fid in fixtures.keys() & live.keys():
        old, incoming = scores.get(fid, {}), live[fid]
        if old.get('status') in FINISHED | VOID and (
                old.get('status') != incoming.get('status') or
                any(old.get(field) is not None and incoming.get(field) is not None
                    and old[field] != incoming[field] for field in SCORE_FIELDS)):
            disputes.add(fid)
        else:
            scores[fid] = merge_verified_result(old, incoming)
    return scores, disputes


def as_output(scores):
    return {'days': [{'matches': [{'fixture': {'fixture_id': fid, 'result': score}}
                                  for fid, score in scores.items()]}]}
