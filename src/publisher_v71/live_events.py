"""Match API-Football's in-game events to the confirmed scoreboard."""
from collections import Counter

DETAILS = {'Normal Goal': 'goal', 'Own Goal': 'goal',
           'Penalty': 'penalty', 'Missed Penalty': 'missed'}
LABELS = {'goal': '⚽ GOOOOLLL!', 'penalty': '⚽ RIGORE SEGNATO!',
          'missed': '❌ RIGORE SBAGLIATO', 'notice': '↩️ RETTIFICA DEL PUNTEGGIO'}


def minute_order(event):
    return event.get('elapsed') or 0, event.get('extra') or 0


def from_fixture(row):
    if not isinstance(row.get('events'), list):
        return None
    teams = [((row.get('teams') or {}).get(side) or {}).get('id') for side in ('home', 'away')]
    events = {}
    for raw in row['events']:
        if not isinstance(raw, dict) or raw.get('type') != 'Goal' or not isinstance(raw.get('detail'), str):
            continue
        kind = DETAILS.get(raw['detail'])
        comment = str(raw.get('comments') or '').lower().replace('-', '').replace(' ', '')
        if not kind or 'shootout' in comment:
            continue
        time = raw.get('time') or {}
        team = raw.get('team') or {}
        player = raw.get('player') or {}
        elapsed, extra, team_id = time.get('elapsed'), time.get('extra'), team.get('id')
        if (type(elapsed) is not int or not 0 < elapsed <= 120 or team_id not in teams
                or type(team_id) is not int):
            continue
        extra = extra if type(extra) is int and 0 < extra <= 60 else None
        event = {'kind': kind, 'elapsed': elapsed, 'extra': extra}
        if isinstance(team.get('name'), str):
            event['team'] = team['name'].strip()[:55]
        key = f'{kind}/{elapsed}/{extra or 0}/{team_id}/{player.get("id") or 0}'
        events[key] = (team_id, event)
    return teams, events


def update(state, sample, previous_score, *, correction):
    """Keep a short caption history and identify the animation for this update."""
    history = state.setdefault('event_history', {})
    scored = history.get('scored', [])
    missed = history.get('missed', {})
    seen_missed = set(history.get('seen_missed', missed))
    fallback = history.get('fallback')
    new = []
    score = sample['score']
    old_total = sum(previous_score or (0, 0))
    extra_goals = sum(score) - old_total
    details = sample.get('events')
    aligned = False
    if details is not None:
        teams, events = details
        goals = [(team, event) for team, event in events.values() if event['kind'] != 'missed']
        counts = Counter(team for team, _ in goals)
        aligned = (len(goals) == sum(score) and [counts[team] for team in teams] == score)
        if aligned:
            updated = sorted((event for _, event in goals), key=minute_order)
            new.extend(event for index, event in enumerate(updated)
                       if index >= old_total or (event['kind'] == 'penalty' and event not in scored))
            scored, fallback = updated, None
        latest_missed = {key: event for key, (_, event) in events.items() if event['kind'] == 'missed'}
        new.extend(event for key, event in latest_missed.items() if key not in seen_missed)
        seen_missed.update(latest_missed)
        missed = latest_missed if aligned else {**missed, **latest_missed}
    if correction:
        if not aligned:
            scored, fallback = [], None
        new.append({'kind': 'notice', 'elapsed': sample.get('elapsed'), 'extra': sample.get('extra'),
                    'observed': True})
    elif extra_goals > 0 and not aligned:
        fallback = {'kind': 'goal', 'elapsed': sample.get('elapsed'), 'extra': sample.get('extra'),
                    'observed': True}
        new.append(fallback)
    history.update(scored=scored, fallback=fallback, missed=missed, seen_missed=sorted(seen_missed))
    recent = sorted([*scored, *missed.values(), *([fallback] if fallback else [])], key=minute_order)
    animation = ('notice' if correction else max(new, key=minute_order)['kind'] if new else None)
    return recent[-4:], animation


def copy(event):
    parts = [LABELS[event['kind']]]
    if event.get('team'):
        parts.append(event['team'])
    if event.get('elapsed') and not event.get('observed'):
        extra = f'+{event["extra"]}' if event.get('extra') else ''
        parts.append(f'{event["elapsed"]}{extra}′')
    return ' · '.join(parts)
