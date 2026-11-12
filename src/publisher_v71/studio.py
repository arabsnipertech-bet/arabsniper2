"""One study suggestion per channel and afternoon or evening kickoff window."""
from multimarket_model import stamp

from . import showcase
from .selection import LEAD, QUOTE_AGE, ROME, SPAN, WINDOW, fresh, rome_day

PREVIEW_FIELDS = ('fixture_id', 'match', 'league', 'kickoff')


def window(kickoff):
    local = stamp(kickoff).astimezone(ROME)
    start = next((hour for hour in (20, 18, 16) if local.hour >= hour), None)
    return f'{local.date()}/{start}' if start is not None else None


def candidates(boards, now):
    today = rome_day(now)
    rows = {}
    for board in boards:
        if board.get('date') != today:
            continue
        for item, fixture in showcase.upcoming(board, now):
            kickoff = stamp(fixture['kickoff'])
            if not window(fixture['kickoff']) or not LEAD < kickoff - now <= WINDOW + SPAN:
                continue
            pick = next((p for p in showcase.proposals(item, now) if p['study_only']), None)
            if pick is None:
                continue
            if not fresh(pick.get('quote_at'), now, QUOTE_AGE):
                pick = {key: pick[key] for key in ('category', 'market', 'probability', 'study_only')}
            rows.setdefault(fixture['fixture_id'], {**fixture, **pick})
    return rows


def group_for(pool, posts, excluded, now, *, channel='telegram'):
    covered, attempted = set(), set(excluded)
    for post in posts:
        if post['channel'] != channel or post['kind'] not in ('best', 'studio'):
            continue
        free = post['view']['free']
        status = post['status']
        if (status in ('sending', 'uncertain', 'sent')
                or status in ('ready', 'retry') and stamp(post['expires_at']) > now):
            covered.add(window(free['kickoff']))
        attempted.add(free['fixture_id'])
    available = sorted((p for p in pool.values() if p['fixture_id'] not in attempted),
                       key=lambda p: (p['kickoff'], p['fixture_id']))
    for free in available:
        slot = window(free['kickoff'])
        kickoff = stamp(free['kickoff'])
        if slot in covered or not LEAD < kickoff - now <= WINDOW:
            continue
        previews = [{key: row[key] for key in PREVIEW_FIELDS}
                    for row in available if row['fixture_id'] != free['fixture_id']
                    and kickoff <= stamp(row['kickoff']) <= kickoff + SPAN][:2]
        return f"{slot}/{free['fixture_id']}", {'free': free, 'previews': previews}
    return None
