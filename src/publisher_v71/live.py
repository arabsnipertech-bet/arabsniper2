"""Follow the original prematch match using official scores and in-game events."""
from datetime import timedelta

from multimarket_model import stamp
from v7_decision import fingerprint

from . import live_events
from .selection import LEAD, project_pick, rome_day, utc

ACTIVE = {'1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE'}
FINAL = {'FT', 'AET', 'PEN'}
STOPPED = {'PST', 'CANC', 'ABD', 'AWD', 'WO', 'SUSP', 'INT'}
PHASES = {'1H': 'Primo tempo', 'HT': 'Intervallo', '2H': 'Secondo tempo'}
AFTER_FIRST_HALF = (ACTIVE - {'1H', 'LIVE'}) | FINAL
EXPIRED = timedelta(hours=5)
EVENT_MONITOR_GRACE = timedelta(minutes=2)


def status_view(pick, status, at, score=None, *, halftime=None, events=(), animation=None):
    row = project_pick(pick)
    row.pop('category')
    row.update(status=status, observed_at=utc(at))
    if score:
        row.update(home=score[0], away=score[1])
    if halftime:
        row['halftime'] = halftime
    if events:
        row['events'] = events
    if animation:
        row['animation'] = animation
    return row


def score_pair(source):
    if not isinstance(source, dict):
        return None
    pair = [source.get('home'), source.get('away')]
    return pair if all(type(n) is int and n >= 0 for n in pair) else None


def observe(pick, feed, now):
    fid = pick['fixture_id']
    row = feed.get('fixtures', {}).get(fid, {})
    seen = stamp(row.get('observed_at'))
    status = row.get('status')
    kickoff = stamp(pick['kickoff'])
    if (feed.get('enabled') is not True or feed.get('schema_version') != 1
            or row.get('source') != 'API-Football' or str(row.get('fixture_id')) != fid
            or row.get('refresh_failed') is not False or not seen
            or not kickoff <= seen <= now or now-seen > (EXPIRED if status in FINAL else timedelta(minutes=7))
            or status not in ACTIVE | FINAL | STOPPED):
        return None
    score = score_pair(row.get('fulltime') if status in FINAL else row.get('goals'))
    halftime = score_pair(row.get('halftime')) if status in AFTER_FIRST_HALF else None
    if status == 'HT':
        score = halftime = halftime or score
    if not score and status not in STOPPED:
        return None
    if halftime and score and any(half > total for half, total in zip(halftime, score)):
        halftime = None
    sample = {'at': utc(seen), 'status': status, 'score': score, 'halftime': halftime,
              **{key: row[key] for key in ('elapsed', 'extra') if type(row.get(key)) is int}}
    if 'events' in row:
        sample['events'] = live_events.from_fixture(row)
    return sample


def needs_halftime(state, now):
    return (state.get('done') and state['view']['status'] == 'Finale'
            and 'halftime' not in state['view']
            and now < stamp(state['pick']['kickoff']) + EXPIRED)


def monitor_current(state, now):
    last = stamp(state.get('event_monitor_at'))
    return bool(last and timedelta() <= now - last < EVENT_MONITOR_GRACE)


def advance(state, feed, now, reservable):
    """Apply fresh live scores and confirm corrections with independent readings."""
    done = state.get('done')
    if done and not needs_halftime(state, now):
        return None
    pick = state['pick']
    kickoff = stamp(pick['kickoff'])
    sample = observe(pick, feed, now)
    if done:
        if sample and sample['status'] in FINAL and sample['halftime']:
            return status_view(pick, 'Finale', stamp(sample['at']), sample['score'], halftime=sample['halftime'],
                               events=state['view'].get('events', ()))
        return None

    confirmed = state.get('confirmed') or {}
    confirmed_at = stamp(confirmed.get('at'))
    if sample and confirmed_at and stamp(sample['at']) <= confirmed_at:
        sample = None
    if sample:
        last = state.get('candidate')
        status, score = sample['status'], sample['score']
        signature = (status, score)
        independent = last and (last['status'], last['score']) == signature and (
            stamp(sample['at']) - stamp(last['at']) >= timedelta(seconds=45))
        previous_score = confirmed.get('score')
        correction = previous_score and score and any(new < old for old, new in zip(previous_score, score))
        earlier_phase = (status == '1H' and confirmed.get('status') in AFTER_FIRST_HALF
                         or status == 'HT' and confirmed.get('status') in AFTER_FIRST_HALF - {'HT'})
        current_score = status in ACTIVE and not earlier_phase and (status == 'HT' or not correction)
        if status in FINAL or current_score or independent:
            state['last_coverage'] = sample['at']
            stage = 'Finale' if status in FINAL else PHASES.get(status, 'In corso')
            if status in STOPPED:
                stage = 'Gara sospesa o rinviata' if status in {'PST', 'SUSP', 'INT'} else 'Gara interrotta o annullata'
            halftime = sample['halftime'] or state['view'].get('halftime')
            if halftime and score and any(half > total for half, total in zip(halftime, score)):
                halftime = None
            state['confirmed'] = {key: sample[key] for key in ('at', 'status', 'score', 'halftime')}
            state['candidate'] = None
            state['done'] = status in FINAL | (STOPPED - {'SUSP', 'INT'})
            view = state['view']
            events, animation = view.get('events', ()), None
            if score and status in ACTIVE | FINAL:
                events, animation = live_events.update(state, sample, previous_score, correction=correction)
            changed = (not confirmed or previous_score != score or view['status'] != stage
                       or view.get('halftime') != halftime)
            if status in FINAL and (correction or confirmed.get('status') not in ACTIVE):
                animation = None
            if status in ACTIVE and not animation and not changed:
                animation = view.get('animation')
            if changed or list(view.get('events', ())) != list(events) or animation != view.get('animation'):
                return status_view(pick, stage, stamp(sample['at']), score, halftime=halftime,
                                   events=events, animation=animation)
            return None
        if not last or (last['status'], last['score']) != signature:
            state['candidate'] = {key: sample[key] for key in ('at', 'status', 'score')}

    reserve = state.get('reserve')
    missing = not sample and kickoff + timedelta(minutes=18) <= now and not state.get('last_coverage')
    stopped = state.get('done') or (state.get('confirmed') or {}).get('status') in STOPPED
    if ((missing or stopped) and reserve and reserve['fixture_id'] in reservable
            and now + LEAD <= stamp(reserve['kickoff']) and not state.get('used_reserve')):
        state.update(pick=reserve, reserve=None, used_reserve=True, candidate=None, confirmed=None,
                     last_coverage=None, done=False, event_history={}, event_monitor_at=None)
        return status_view(reserve, 'Prematch', now)
    if now >= kickoff + EXPIRED:
        state['done'] = True
        return status_view(pick, 'Copertura non disponibile', now)
    return None


def start_state(primary, reserve, now):
    return {'created_at': utc(now), 'day': rome_day(stamp(primary['kickoff'])), 'pick': primary,
            'reserve': reserve, 'used_reserve': False, 'done': False,
            'view': status_view(primary, 'Prematch', now)}


def revision(view):
    return fingerprint(view)[:12]
