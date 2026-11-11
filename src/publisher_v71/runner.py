"""Single production tick for Telegram, Facebook and the public homepage."""
from datetime import datetime, date, timedelta, timezone
from itertools import chain
from pathlib import Path
import json
import os
import sys

from gold_storage import client
from multimarket_model import stamp
from v7_probability import VERSION

from . import cutover, legacy, live, public_feed, report, scores
from .channels import Telegram, Facebook
from .delivery import attempt, can_send, queue
from .editorial import academy
from .selection import (LEAD, ROME, group_for, group_id, project_group,
                        published_candidates, rome_day, utc)
from .storage import Storage

WORKING_DIRECTORY = Path(__file__).resolve().parents[2]


def clock():
    return datetime.now(timezone.utc)


def local_minute(now):
    local = now.astimezone(ROME)
    return local.hour * 60 + local.minute


def event_log(kind, status, **values):
    print(json.dumps({'kind': kind, 'status': status, **values}, ensure_ascii=False), flush=True)


def delivery_log(post, status):
    result = post.get('receipt') or {}
    event_log(post['kind'], status, channel=post['channel'], ref=post['marker'],
              id=result.get('id'), attempts=post['attempts'])


def read_local(path):
    with (WORKING_DIRECTORY / path).open(encoding='utf-8') as source:
        return json.load(source)


def safe_read(name, fn, fallback):
    try:
        return fn()
    except Exception as exc:
        event_log(name, 'unavailable', error=type(exc).__name__)
        return fallback


def read_board(store, number):
    board = safe_read(f'gold_board_day{number}', lambda: store.read(
        f'arab_quote_day{number}.json', {}, root=True)[0], {})
    return board if isinstance(board, dict) and isinstance(board.get('matches'), list) else {}


def group_posts(store, group):
    return [post for post in store.state['posts'].values()
            if post['kind'] == 'best' and post['key'] == group['id']]


def first_publication(store, group):
    times = [stamp(p['receipt']['at']) for p in group_posts(store, group) if p['status'] == 'sent']
    if group.get('homepage_at'):
        times.append(stamp(group['homepage_at']))
    return min(times, default=None)


def archive_availability(store, picks, now):
    for pick in picks:
        kickoff = stamp(pick['kickoff'])
        if not timedelta() < kickoff - now <= timedelta(minutes=60):
            continue
        day = rome_day(kickoff)
        store.freeze(f'{day}/gold/{pick["fixture_id"]}.json', pick)


def free_receipts(store, day):
    for group in store.state['groups'].values():
        pick = group['picks'][0]
        kickoff = stamp(pick['kickoff'])
        if rome_day(kickoff) != day:
            continue
        at = first_publication(store, group)
        if at and at < kickoff:
            receipt = {**pick, 'observed_at': utc(at), 'group_id': group['id']}
            store.freeze(f'{day}/public/{pick["fixture_id"]}.json', receipt)
    return list(store.snapshots(f'{day}/public/'))


def add_group(store, picks, now, channels):
    key = group_id(picks)
    group = {'id': key, 'picks': picks, 'created_at': utc(now), 'eligible_home': False}
    first = stamp(picks[0]['kickoff'])
    until = min(first - LEAD, *(stamp(p['expires_at']) for p in picks))
    if key in store.state['groups'] or until <= now:
        return False
    group = store.remember(f'{rome_day(first)}/groups/{key}.json', group)
    picks = group['picks']
    first = stamp(picks[0]['kickoff'])
    until = min(first - LEAD, *(stamp(p['expires_at']) for p in picks))
    if until <= now:
        return False
    store.state['groups'][key] = group
    reserved_ids = store.state['runtime'].get('reserved_fixtures', [])
    store.state['runtime']['reserved_fixtures'] = sorted(set(reserved_ids).union(p['fixture_id'] for p in picks))
    view = project_group(group, now)
    today = rome_day(first)
    representative = (picks[0]['tier'] in ('GOLD', 'PLATINUM') and picks[0]['role'] == 'most_probable'
                      and not any(p['channel'] == 'facebook' and p['kind'] == 'best'
                                  and rome_day(stamp(p['view']['free']['kickoff'])) == today
                                  and p['status'] in ('ready', 'retry', 'sending', 'uncertain', 'sent')
                                  for p in store.state['posts'].values()))
    queue(store, 'best', key, view,
          [name for name in channels if name != 'facebook' or representative], now, until)
    event_log('best', 'group_queued', group=key, count=len(picks), kickoff=picks[0]['kickoff'])
    return True



def plan_live(store, now, candidates, channels):
    today = rome_day(now)
    states = store.state['live']
    if 'telegram' not in channels:
        return
    upcoming = [group for group in store.state['groups'].values()
                if rome_day(stamp(group['picks'][0]['kickoff'])) >= today
                and stamp(group['picks'][0]['kickoff']) >= now + LEAD
                and first_publication(store, group)]
    watchlist = safe_read('live_watchlist', lambda: read_local('data/live_watchlist.json'), {})
    watched = {str(p['fixture_id']) for p in watchlist.get('fixtures', [])}
    for group in sorted(upcoming, key=lambda g: g['picks'][0]['kickoff']):
        primary, *reserved = group['picks']
        kickoff = stamp(primary['kickoff'])
        day = rome_day(kickoff)
        if day in states or primary['fixture_id'] not in watched:
            continue
        pool = [*reserved, *candidates.values()]
        reserve = next((p for p in pool if p['fixture_id'] in watched
                        and kickoff + timedelta(minutes=45) <= stamp(p['kickoff'])
                        and rome_day(stamp(p['kickoff'])) == day), None)
        state = store.remember(f'{day}/live/original.json', live.start_state(primary, reserve, now))
        kickoff = stamp(state['pick']['kickoff'])
        if kickoff < now + LEAD:
            continue
        states[day] = state
        queue(store, 'live', day + '/start', state['view'], ['telegram'], now, kickoff - LEAD)
    if not any(not state['done'] for state in states.values()):
        return
    feed = safe_read('live_scores', lambda: scores.read(store, states.values(), now, clock, event_log), {})
    now = clock()
    for day, state in states.items():
        start_post = store.state['posts'].get(f'telegram/live/{day}/start') or {}
        if start_post.get('status') != 'sent':
            continue
        change = live.advance(state, feed, now, candidates)
        if not change:
            continue
        state['view'] = change
        until = (stamp(change['kickoff']) - LEAD if change['status'] == 'Prematch'
                 else now + timedelta(hours=4))
        suffix = day + '/' + live.revision(change)
        queue(store, 'live', suffix, change, ['telegram'], now, until,
              edit=start_post['receipt']['id'])
        if change['status'] == 'Finale' and 'facebook' in channels:
            queue(store, 'live', suffix, change, ['facebook'], now, until)


def plan_report(store, now, channels, yesterday):
    previous = str(yesterday)
    reports = store.state['reports']
    existing = reports.get(previous)
    if existing and 'triad_examples' in existing['view']:
        return
    audit = safe_read('auditor', lambda: report.certified(store, yesterday, with_fixture_labels=True), None)
    if not audit:
        event_log('report', 'awaiting_certified_auditor', day=previous)
        return
    if existing:
        # Refresh the homepage view while preserving the frozen outbox and receipts.
        existing['view'] = {**existing['view'], 'triad_examples': report.triad_examples(audit)}
        return
    view = safe_read('report', lambda: report.build(audit, list(store.snapshots(previous + '/gold/')),
                                                   free_receipts(store, previous)), None)
    if not view:
        event_log('report', 'cohort_incomplete_or_empty', day=previous)
        return
    saved = store.remember(previous + '/report.json', view)
    view = {**saved, 'triad_examples': saved.get('triad_examples', view['triad_examples'])}
    reports[previous] = {'created_at': utc(now), 'view': view}
    queue(store, 'report', previous, view, channels, now, now + timedelta(hours=12))


def plan_editorial(store, now, channels, best_queued, trial_ok):
    today = date.fromisoformat(rome_day(now))
    minute = local_minute(now)
    yesterday = today - timedelta(days=1)
    previous = str(yesterday)
    reports = store.state['reports']
    if (minute >= 10 * 60 + 45
            and str(today) > rome_day(stamp(store.state['legacy']['handoff']['created_at']))):
        plan_report(store, now, channels, yesterday)
    if (16 * 60 + 45 <= minute <= 20 * 60
            and (not best_queued or minute >= 17 * 60 + 45)):
        week = [entry['view'] for key, entry in reports.items()
                if date.fromisoformat(key).isocalendar()[:2] == today.isocalendar()[:2]]
        view = academy(today, (reports.get(previous) or {}).get('view'), week)
        queue(store, 'academy', str(today), view, channels, now, now + timedelta(hours=5))
    if (trial_ok and today.weekday() in (1, 5) and 12 * 60 + 15 <= minute <= 14 * 60
            and not best_queued):
        # The bounded Gold-engine campaign owns Telegram's trial advertising
        # during these dates; keep Facebook's existing editorial schedule.
        gold_channels = [c for c in channels if c != 'telegram' or
                         not date(2026, 11, 12) <= today <= date(2026, 11, 18)]
        if gold_channels:
            queue(store, 'gold', str(today), {}, gold_channels, now, now + timedelta(hours=3))


def record_home_receipt(store, view, revision, at):
    if not at:
        return
    for row in view['groups']:
        group = store.state['groups'].get(row['id'])
        if (group and not group.get('homepage_at')
                and stamp(group['created_at']) <= at < stamp(group['picks'][0]['kickoff'])):
            group.update(homepage_at=utc(at), homepage_revision=revision)


def publish_home(store, candidates, now, boards):
    for group in store.state['groups'].values():
        if not group.get('homepage_at'):
            view = project_group(group, now)
            until = stamp(group['picks'][0]['kickoff']) - LEAD
            group['eligible_home'] = bool(view and not can_send(
                {'kind': 'best', 'view': view, 'expires_at': utc(until)}, now, candidates))
        elif group['picks'][0]['fixture_id'] in candidates:
            current = candidates[group['picks'][0]['fixture_id']]
            free = group['picks'][0]
            group['eligible_home'] = all(current[key] == free[key] for key in ('kickoff', 'market', 'odd', 'probability'))
    past = safe_read('historical_results', lambda: read_local('data/arab_quote_results_public.json'), {})
    upcoming = chain(boards, (read_board(store, number) for number in (3, 4, 5)))
    view = public_feed.projection(store.state, now, past, boards=upcoming)
    result = safe_read('homepage', lambda: public_feed.publish(view), None)
    if result:
        revision, previous = result
        if previous:
            record_home_receipt(store, *previous)
        record_home_receipt(store, view, revision, clock())
        store.save()
        event_log('homepage', 'published', revision=revision, groups=len(view['groups']),
                  previews=len(view['gold_previews']), scheduled=len(view['gold_schedule']),
                  recap_examples=len((view['report'] or {}).get('triad_examples', [])))


def tick(store, services, gh_token, *, trial_ok):
    now = clock()
    store.load()
    store.prune(now)
    store.state['runtime']['trial_verified'] = trial_ok
    candidate_feed = read_local(legacy.PUBLIC_PATH)
    if candidate_feed.get('publisher') != 'v7.1':
        event_log('cutover', 'awaiting_safe_public_projection')
        return
    if not store.state['legacy']:
        ids = safe_read('cutover', lambda: legacy.drain_and_import(gh_token, now), None)
        if ids is None:
            event_log('cutover', 'awaiting_legacy_drain')
            return
        store.state['legacy']['handoff'] = {'ids': ids, 'created_at': utc(now)}
        store.save()
        event_log('cutover', 'exclusive', legacy_fixtures=len(ids))

    boards = [read_board(store, number) for number in (1, 2)]
    picks = safe_read('best_candidates', lambda: published_candidates(boards, now), [])
    candidates = {p['fixture_id']: p for p in picks}
    if picks:
        safe_read('gold_availability', lambda: archive_availability(store, picks, now), None)
    seen = {p['fixture_id'] for group in store.state['groups'].values() for p in group['picks']}
    seen.update(store.state['legacy']['handoff']['ids'])
    seen.update(store.state['runtime'].get('reserved_fixtures', []))
    group = group_for([p for p in picks if p['fixture_id'] not in seen], now)
    best = bool(group and add_group(store, group, now, services))
    if not best:
        event_log('best', 'no_new_eligible_group', candidates=len(picks))
    plan_editorial(store, now, services, best, trial_ok)
    store.save()

    for post in list(store.state['posts'].values()):
        if (post['channel'] in services and post['kind'] != 'live'
                and stamp(post['expires_at']) + timedelta(hours=12) > now):
            attempt(store, post, services[post['channel']], candidates, clock, delivery_log)
    plan_live(store, clock(), candidates, services)
    store.save()
    live_posts = [p for p in store.state['posts'].values() if p['kind'] == 'live']
    for post in sorted(live_posts, key=lambda p: p['created_at']):
        if (post['channel'] in services and
                stamp(post['expires_at']) + timedelta(hours=12) > now):
            if post.get('edit_id') and post['status'] in ('ready', 'retry'):
                if any(p['channel'] == post['channel'] and p.get('edit_id') == post['edit_id']
                       and p['created_at'] > post['created_at'] for p in live_posts):
                    post.update(status='skipped', reason='superseded_live_update')
                    store.save()
                    continue
            attempt(store, post, services[post['channel']], candidates, clock, delivery_log)
    publish_home(store, candidates, clock(), boards)


def preflight(store):
    now = clock()
    boards = []
    for number in (1, 2):
        board, _ = store.read(f'arab_quote_day{number}.json', root=True)
        if not isinstance(board.get('matches'), list) or not stamp(board.get('updated_at')):
            raise ValueError('Invalid private Gold board')
        boards.append(board)
        published = sum(((((m.get('multimarket') or {}).get('matrix') or {}).get('v7')) or {}).get('version') == VERSION
                        for m in board['matches'])
        event_log('gold_board', 'validated_shape', day=board['date'], matches=len(board['matches']), v7=published)
    event_log('best', 'available', candidates=len(published_candidates(boards, now)))
    audit = report.certified(store, now.astimezone(ROME).date() - timedelta(days=1))
    event_log('auditor', 'certified' if audit else 'not_certified', day=audit['date'] if audit else None)


def main():
    if sys.argv[1:] == ['--preflight']:
        preflight(Storage(client(), os.environ['R2_BUCKET']))
        return
    if (not cutover.ENABLED or os.environ.get('PUBLISHER_DISABLED') == 'true'
            or os.environ.get('GITHUB_REPOSITORY') != legacy.REPO
            or os.environ.get('GITHUB_REF') != 'refs/heads/main'):
        event_log('publisher', 'disabled_or_non_production')
        return
    store = Storage(client(), os.environ['R2_BUCKET'])
    services = {'telegram': Telegram(os.getenv('BOT_TOKEN'))}
    if os.environ.get('FACEBOOK_ENABLED') == 'true':
        services['facebook'] = Facebook(os.getenv('META_TOKEN'))
    trial_ok = cutover.TRIAL_PROVEN and os.environ.get('TRIAL_DISABLED') != 'true'
    if trial_ok:
        trial_ok = safe_read('trial_webhook', lambda: services['telegram'].trial_healthy(clock()), False)
    tick(store, services, os.environ['GH_TOKEN'], trial_ok=trial_ok)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Never echo upstream request URLs, headers or private forecasts.
        event_log('publisher', 'cycle_aborted', error=type(error).__name__)
        raise SystemExit(1) from None

