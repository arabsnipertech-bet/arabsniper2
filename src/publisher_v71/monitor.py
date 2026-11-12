"""Poll one announced Telegram live fixture and use the Publisher's edit outbox."""
from datetime import timedelta
import os
import time

from gold_storage import client
from multimarket_model import stamp

from . import live, runner, scores
from .storage import Storage

POLL_SECONDS = 60
MATCH_WINDOW = timedelta(minutes=190)


def poll(store, services, fixture):
    with store.cycle(runner.clock()) as acquired:
        if not acquired:
            return 'busy'
        store.load()
        now = runner.clock()
        match = next(((day, state) for day, state in store.state['live'].items()
                      if state['pick']['fixture_id'] == fixture and runner.live_receipt(store, day)), None)
        if not match:
            return 'no_announced_live'
        day, state = match
        kickoff = stamp(state['pick']['kickoff'])
        if state['done'] or now >= kickoff + MATCH_WINDOW or now < kickoff - timedelta(minutes=20):
            return 'finished'
        if now < kickoff:
            return 'waiting_for_kickoff'
        feed = runner.safe_read('live_event_feed', lambda: scores.detailed(
            store, fixture, now, runner.clock, runner.event_log), {})
        now = runner.clock()
        if feed:
            runner.update_live(store, day, state, feed, now, {}, services)
            observed_at = feed['fixtures'][fixture]['observed_at']
            if observed_at == (state.get('confirmed') or {}).get('at'):
                state['event_monitor_at'] = observed_at
            store.save()
        runner.send_live(store, services, {}, now, day=day)
        return 'finished' if state['done'] else 'polled' if feed else 'provider_unavailable'


def main():
    fixture = os.environ.get('LIVE_FIXTURE_ID', '')
    if not runner.production_enabled() or not fixture.isascii() or not fixture.isdecimal():
        runner.event_log('live_monitor', 'disabled_or_invalid_fixture')
        return
    store = Storage(client(), os.environ['R2_BUCKET'])
    services = runner.social_services()
    deadline = time.monotonic() + 210 * 60
    while time.monotonic() < deadline:
        started = time.monotonic()
        result = poll(store, services, fixture)
        runner.event_log('live_monitor', result, fixture=fixture)
        if result in ('finished', 'no_announced_live'):
            return
        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        runner.event_log('live_monitor', 'cycle_aborted', error=type(error).__name__)
        raise SystemExit(1) from None
