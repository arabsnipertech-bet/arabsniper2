"""Single production tick for Telegram, Facebook and the public homepage."""
from datetime import datetime, date, timedelta, timezone
from itertools import chain
from pathlib import Path
import json
import os
import sys
import time

from gold_storage import client
from multimarket_model import stamp
from v7_probability import VERSION

from . import cutover, legacy, live, public_feed, report, scores, studio
from .channels import Telegram, Facebook
from .delivery import attempt, can_send, queue
from .editorial import academy, campaign_active
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
    board = safe_read(f'gold_board_day{number}', lambda: store.gold_board(number), {})
    if board and number <= 2:
        event_log(f'gold_board_day{number}', 'read', day=board['date'],
                  fixtures=len(board['matches']), bytes=board['source_bytes'])
    return board


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
    due = {day: state for day, state in states.items() if live.needs_halftime(state, now) or (
        not state['done'] and not live.monitor_current(state, now))}
    if not due:
        return
    feed = safe_read('live_scores', lambda: scores.read(store, due.values(), now, clock, event_log), {})
    now = clock()
    for day, state in due.items():
        update_live(store, day, state, feed, now, candidates, channels)


def live_receipt(store, day):
    post = store.state['posts'].get(f'telegram/live/{day}/start') or {}
    if post.get('status') == 'sent':
        return post['receipt']['id']


def update_live(store, day, state, feed, now, candidates, channels):
    receipt = live_receipt(store, day)
    if not receipt:
        return
    was_done = state['done']
    change = live.advance(state, feed, now, candidates)
    if not change:
        return
    state['view'] = change
    until = (stamp(change['kickoff']) - LEAD if change['status'] == 'Prematch'
             else now + timedelta(hours=4))
    suffix = day + '/' + live.revision(change)
    queue(store, 'live', suffix, change, ['telegram'], now, until, edit=receipt)
    if change['status'] == 'Finale' and not was_done and 'facebook' in channels:
        queue(store, 'live', suffix, change, ['facebook'], now, until)


def plan_report(store, now, channels, yesterday):
    previous = str(yesterday)
    reports = store.state['reports']
    existing = reports.get(previous)
    if (existing and 'triad_examples' in existing['view']
            and not existing['view'].get('recovered', {}).get('pending')):
        return
    audit = safe_read('auditor', lambda: report.certified(store, yesterday, with_fixture_labels=True), None)
    if not audit:
        event_log('report', 'awaiting_certified_auditor', day=previous)
        return
    if existing:
        view = existing['view']
        if view.get('recovered', {}).get('pending'):
            if stamp(audit['generated_at']) <= stamp(view['certified_at']):
                return
            updated = safe_read('report', lambda: report.build(audit, list(store.snapshots(previous + '/gold/')),
                                                              free_receipts(store, previous)), None)
            if updated:
                existing['view'] = updated
        else:
            # An older report can gain examples without creating a second delivery.
            existing['view'] = {**view, 'triad_examples': report.triad_examples(audit)}
        return
    view = safe_read('report', lambda: report.build(audit, list(store.snapshots(previous + '/gold/')),
                                                   free_receipts(store, previous)), None)
    if not view:
        event_log('report', 'cohort_incomplete_or_empty', day=previous)
        return
    key = previous + ('/report-recovered.json' if 'recovered' in view else '/report.json')
    saved = store.remember(key, view)
    view = {**saved, 'triad_examples': saved.get('triad_examples', view['triad_examples'])}
    reports[previous] = {'created_at': utc(now), 'view': view}
    queue(store, 'report', previous, view, channels, now, now + timedelta(hours=12))
    event_log('report', 'queued', day=previous, examples=len(view['triad_examples']),
              pending_fixtures=(view.get('recovered') or {}).get('pending_fixtures'))


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
        # The bounded campaign owns trial advertising on both social channels.
        if not campaign_active(today):
            queue(store, 'gold', str(today), {}, channels, now, now + timedelta(hours=3))


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
    if os.environ.get('GITHUB_REPOSITORY') == cutover.GROWTH:
        store.stage_homepage(view)
        previous = safe_read('homepage_receipt', lambda: public_feed.published_revision(
            public_feed.PUBLIC_REPOSITORY), None)
        if previous:
            record_home_receipt(store, *previous)
            store.save()
        event_log('homepage', 'ready_for_publication', groups=len(view['groups']),
                  previews=len(view['gold_previews']), scheduled=len(view['gold_schedule']))
        return
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


def tick(store, services, gh_token, *, trial_ok, repository=None):
    now = clock()
    store.load()
    if repository and repository != cutover.owner(store):
        event_log('publisher', 'owned_by_other_repository')
        return
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
    agenda = studio.agenda(boards, now)
    agenda_pool = {}
    if agenda:
        agenda_pool = {row['fixture_id']: row for row in agenda['fixtures']}
        until = min(stamp(row['kickoff']) for row in agenda['fixtures'])
        queue(store, 'agenda', agenda['date'], agenda, services, now, until)
    study_channels = [channel for channel in ('telegram', 'facebook') if channel in services]
    study_pool = safe_read('studio_board', lambda: studio.candidates(boards, now), {}) if study_channels else {}
    best_id = group_id(group) if best else None
    for channel in study_channels:
        if best_id and f'{channel}/best/{best_id}' in store.state['posts']:
            continue
        suggestion = studio.group_for(study_pool, store.state['posts'].values(), seen, now, channel=channel)
        if suggestion:
            key, view = suggestion
            queue(store, 'studio', key, view, [channel], now, stamp(view['free']['kickoff']) - LEAD)
            event_log('studio', 'group_queued', channel=channel, group=key, count=1 + len(view['previews']))
        elif local_minute(now) >= 15 * 60:
            event_log('studio', 'no_due_suggestion', channel=channel, candidates=len(study_pool))
    plan_editorial(store, now, services, best, trial_ok)
    store.save()

    for post in list(store.state['posts'].values()):
        if (post['channel'] in services and post['kind'] != 'live'
                and stamp(post['expires_at']) + timedelta(hours=12) > now):
            source = {'studio': study_pool, 'agenda': agenda_pool}.get(post['kind'], candidates)
            attempt(store, post, services[post['channel']], source, clock, delivery_log)
    plan_live(store, clock(), candidates, services)
    store.save()
    send_live(store, services, candidates, now)
    publish_home(store, candidates, clock(), boards)


def send_live(store, services, candidates, now, *, day=None):
    live_posts = [p for p in store.state['posts'].values() if p['kind'] == 'live'
                  and (day is None or p['key'].startswith(day + '/'))]
    for post in sorted(live_posts, key=lambda p: p['created_at']):
        if (post['channel'] in services and
                stamp(post['expires_at']) + timedelta(hours=12) > now):
            if post.get('edit_id') and post['status'] in ('ready', 'retry', 'sending', 'uncertain'):
                if any(p['channel'] == post['channel'] and p.get('edit_id') == post['edit_id']
                       and p['created_at'] > post['created_at'] for p in live_posts):
                    post.update(status='skipped', reason='superseded_live_update')
                    store.save()
                    continue
            attempt(store, post, services[post['channel']], candidates, clock, delivery_log)


def preflight(store):
    now = clock()
    boards = []
    for number in (1, 2):
        board = store.gold_board(number)
        if not isinstance(board.get('matches'), list) or not stamp(board.get('updated_at')):
            raise ValueError('Invalid private Gold board')
        boards.append(board)
        published = sum(((((m.get('multimarket') or {}).get('matrix') or {}).get('v7')) or {}).get('version') == VERSION
                        for m in board['matches'])
        event_log('gold_board', 'validated_shape', day=board['date'], matches=len(board['matches']), v7=published)
    event_log('best', 'available', candidates=len(published_candidates(boards, now)))
    audit = report.certified(store, now.astimezone(ROME).date() - timedelta(days=1))
    event_log('auditor', 'certified' if audit else 'not_certified', day=audit['date'] if audit else None)


def production_enabled():
    return (cutover.ENABLED and os.environ.get('PUBLISHER_DISABLED') != 'true'
            and os.environ.get('GITHUB_REPOSITORY') in (cutover.PRIMARY, cutover.GROWTH)
            and os.environ.get('GITHUB_REF') == 'refs/heads/main')


def production_store():
    source = client()
    bucket = os.environ['R2_BUCKET']
    if os.environ.get('GITHUB_REPOSITORY') != cutover.GROWTH:
        return Storage(source, bucket)

    import boto3
    from botocore.config import Config
    from botocore.credentials import RefreshableCredentials
    from botocore.session import get_session
    from .bridge import session_credentials

    session = get_session()
    session._credentials = RefreshableCredentials.create_from_metadata(
        metadata=session_credentials(), refresh_using=session_credentials, method='publisher-r2')
    editorial = boto3.Session(botocore_session=session).client(
        's3', endpoint_url=source.meta.endpoint_url, region_name='auto',
        config=Config(connect_timeout=7, read_timeout=20, retries={'total_max_attempts': 2}))
    return Storage(source, bucket, editorial=editorial)


def running_owner(store):
    repository = cutover.owner(store)
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a', encoding='utf-8') as target:
            target.write(f'owner={repository}\n')
    return repository


def change_owner(store, repository, *, verify=None):
    """Switch the owner inside the same lock that protects the sending outbox."""
    for _ in range(10):
        with store.cycle(clock()) as acquired:
            if acquired:
                store.load()
                if not store.etag or not store.state['legacy'].get('handoff'):
                    raise ValueError('No existing Publisher receipts to hand off')
                if cutover.owner(store) != repository:
                    if verify:
                        verify(store)
                    store.state['runtime'][cutover.OWNER_FIELD] = repository
                    store.save()
                event_log('publisher', 'delivery_owner', repository=repository)
                return
        time.sleep(3)
    raise RuntimeError('Publisher delivery cycle is busy')


def social_services():
    services = {'telegram': Telegram(os.getenv('BOT_TOKEN'))}
    if os.environ.get('FACEBOOK_ENABLED') == 'true':
        services['facebook'] = Facebook(os.getenv('META_TOKEN'))
    return services


def main():
    args = sys.argv[1:]
    if args == ['--preflight']:
        preflight(Storage(client(), os.environ['R2_BUCKET']))
        return
    if not production_enabled():
        event_log('publisher', 'disabled_or_non_production')
        return
    repository = os.environ['GITHUB_REPOSITORY']
    store = production_store()
    if args in (['--activate-growth'], ['--restore-primary']):
        if repository != cutover.GROWTH:
            raise ValueError('Publisher handoff must run from Growth main')
        if args == ['--restore-primary']:
            change_owner(store, cutover.PRIMARY)
            return
        from .preflight import verify

        change_owner(store, cutover.GROWTH, verify=lambda ledger: verify(ledger, activation=True))
    elif args:
        raise ValueError('Unsupported Publisher arguments')
    store.load()
    if running_owner(store) != repository:
        event_log('publisher', 'owned_by_other_repository')
        return
    services = social_services()
    trial_ok = cutover.TRIAL_PROVEN and os.environ.get('TRIAL_DISABLED') != 'true'
    if trial_ok:
        trial_ok = safe_read('trial_webhook', lambda: services['telegram'].trial_healthy(clock()), False)
    for _ in range(10):
        with store.cycle(clock()) as acquired:
            if acquired:
                tick(store, services, os.environ['GH_TOKEN'], trial_ok=trial_ok, repository=repository)
                return
        time.sleep(3)
    event_log('publisher', 'deferred_to_next_cycle')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Never echo upstream request URLs, headers or private forecasts.
        event_log('publisher', 'cycle_aborted', error=type(error).__name__)
        raise SystemExit(1) from None


