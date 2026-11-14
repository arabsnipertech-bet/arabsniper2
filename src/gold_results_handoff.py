"""Keep one live relay and one verification writer during the repository handoff."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
import os
import sys

from gold_storage import client
from publisher_v71 import bridge, cutover
from publisher_v71.storage import Storage, encoded


def origin():
    repository = os.environ.get('GITHUB_REPOSITORY')
    if repository not in (cutover.PRIMARY, cutover.GROWTH) or os.environ.get('GITHUB_REF') != 'refs/heads/main':
        raise ValueError('Gold results require production main')
    return repository


def storage():
    return Storage(client(), os.environ['R2_BUCKET'])


def owner(store):
    record, _ = store.read(bridge.GOLD_RESULTS_OWNER, root=True, limit=512)
    repository = record['repository'] if record else cutover.PRIMARY
    if repository not in (cutover.PRIMARY, cutover.GROWTH):
        raise ValueError('Invalid Gold results owner')
    return repository


@contextmanager
def cycle(task):
    repository = origin()
    store = storage()
    if owner(store) != repository:
        print('Gold results are owned by the other repository.', flush=True)
        yield None
        return
    if repository == cutover.GROWTH:
        store.editorial = bridge.gold_results_storage(store.client, store.bucket)
    now = datetime.now(timezone.utc)
    with store.cycle(now, key=f'{bridge.GOLD_RESULTS_PREFIX}{task}-cycle.json', root=True) as acquired:
        if not acquired or owner(store) != repository:
            print('Gold results work is already running or changed owner.', flush=True)
            yield None
        else:
            yield store


def report_repair(store, *, watchlist, audit):
    if origin() != cutover.GROWTH or not (watchlist or audit):
        return
    now = datetime.now(timezone.utc)
    value = {'watchlist': bool(watchlist), 'audit': str(audit) if audit else ''}
    old, _ = store.read(bridge.GOLD_RESULTS_REPAIR, root=True, limit=512)
    if old and all(old.get(key) == item for key, item in value.items()) and (
            timedelta() <= now - datetime.fromisoformat(old['at']) < timedelta(minutes=20)):
        return
    value['at'] = now.isoformat()
    store.editorial.put_object(Bucket=store.bucket, Key=bridge.GOLD_RESULTS_REPAIR,
                               Body=encoded(value), ContentType='application/json')


def change_owner(target):
    repository = origin()
    if repository != target:
        raise ValueError('Change the Gold results owner from the target repository')
    store = storage()
    if target == cutover.GROWTH:
        store.editorial = bridge.gold_results_storage(store.client, store.bucket)
    now = datetime.now(timezone.utc)
    prefix = bridge.GOLD_RESULTS_PREFIX
    with (store.cycle(now, key=prefix + 'relay-cycle.json', root=True) as relay,
          store.cycle(now, key=prefix + 'health-cycle.json', root=True) as health):
        if not relay or not health:
            raise RuntimeError('A Gold results cycle is already running')
        if owner(store) == repository:
            return
        if target == cutover.GROWTH:
            from publisher_v71.scores import public_cache

            current = public_cache()
            if current.get('poll_interval_seconds') != 300:
                raise ValueError('Expected the live scores feed at five minutes')
            # Check the exact write grants without discarding existing relay or alert state.
            for key, initial in zip(bridge.GOLD_RESULTS_KEYS, ({}, []), strict=True):
                saved, tag = store.read(key, root=True, limit=200_000)
                condition = {'IfMatch': tag} if tag else {'IfNoneMatch': '*'}
                store.editorial.put_object(Bucket=store.bucket, Key=key, Body=encoded(saved if tag else initial),
                                           ContentType='application/json', **condition)
        value = {'repository': repository, 'activated_at': now.isoformat()}
        _, tag = store.read(bridge.GOLD_RESULTS_OWNER, root=True, limit=512)
        condition = {'IfMatch': tag} if tag else {'IfNoneMatch': '*'}
        store.editorial.put_object(Bucket=store.bucket, Key=bridge.GOLD_RESULTS_OWNER,
                                   Body=encoded(value), ContentType='application/json', **condition)
        print(json.dumps(value), flush=True)


def read_repair():
    repository = origin()
    if repository != cutover.PRIMARY:
        raise ValueError('Gold archive repairs require the primary repository')
    request, _ = storage().read(bridge.GOLD_RESULTS_REPAIR, root=True, limit=512)
    if not request or not timedelta() <= datetime.now(timezone.utc) - datetime.fromisoformat(request['at']) < timedelta(minutes=20):
        return
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as out:
        if request['watchlist'] is True:
            print('watchlist_repair=true', file=out)
        if request['audit']:
            print(f"recovery_date={date.fromisoformat(request['audit'])}", file=out)


if __name__ == '__main__':
    if sys.argv[1:] == ['repair']:
        read_repair()
    elif sys.argv[1:] == ['activate']:
        change_owner(cutover.GROWTH)
    elif sys.argv[1:] == ['restore-primary']:
        change_owner(cutover.PRIMARY)
    else:
        raise SystemExit('Expected activate, repair or restore-primary')
