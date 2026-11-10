"""Follow the original prematch match using only the existing score cache."""
from datetime import timedelta

from multimarket_model import stamp
from v7_decision import fingerprint

from .selection import LEAD, project_pick, rome_day, utc

ACTIVE = {'1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE'}
FINAL = {'FT', 'AET', 'PEN'}
STOPPED = {'PST', 'CANC', 'ABD', 'AWD', 'WO', 'SUSP', 'INT'}
EXPIRED = timedelta(hours=5)


def status_view(pick, status, at, score=None):
    row = project_pick(pick)
    row.pop('category')
    row.update(status=status, observed_at=utc(at))
    if score:
        row.update(home=score[0], away=score[1])
    return row


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
    source = (row.get('fulltime') if status in FINAL else row.get('goals')) or {}
    score = [source.get('home'), source.get('away')]
    if not all(type(n) is int and n >= 0 for n in score):
        if status not in STOPPED:
            return None
        score = None
    return {'at': utc(seen), 'status': status, 'score': score}


def advance(state, feed, now, reservable):
    """Require two independent cache observations and use the reserve once."""
    if state.get('done'):
        return None
    pick = state['pick']
    kickoff = stamp(pick['kickoff'])
    sample = observe(pick, feed, now)
    if sample:
        last = state.get('candidate')
        status, score = sample['status'], sample['score']
        signature = (status, score)
        independent = last and (last['status'], last['score']) == signature and (
            stamp(sample['at']) - stamp(last['at']) >= timedelta(seconds=45))
        if status in FINAL or independent:
            confirmed = state.get('confirmed')
            state['last_coverage'] = sample['at']
            previous_score = confirmed.get('score') if confirmed else None
            stage = 'Finale' if status in FINAL else 'Intervallo' if status == 'HT' else 'In corso'
            if status in STOPPED:
                stage = 'Gara sospesa o rinviata' if status in {'PST', 'SUSP', 'INT'} else 'Gara interrotta o annullata'
            state['confirmed'] = sample
            state['done'] = status in FINAL | (STOPPED - {'SUSP', 'INT'})
            if (confirmed is None or previous_score != score or state['view']['status'] != stage):
                return status_view(pick, stage, stamp(sample['at']), score)
            return None
        if not last or (last['status'], last['score']) != signature:
            state['candidate'] = sample

    reserve = state.get('reserve')
    missing = not sample and kickoff + timedelta(minutes=18) <= now and not state.get('last_coverage')
    stopped = state.get('done') or (state.get('confirmed') or {}).get('status') in STOPPED
    if ((missing or stopped) and reserve and reserve['fixture_id'] in reservable
            and now + LEAD <= stamp(reserve['kickoff']) and not state.get('used_reserve')):
        state.update(pick=reserve, reserve=None, used_reserve=True, candidate=None, confirmed=None,
                     last_coverage=None, done=False)
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
