"""Wake Growth from the existing Fast watchdog and forward its public home view."""
from base64 import b64encode, urlsafe_b64encode
from contextlib import closing
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import os
import sys

from telegram_watchdog import GitHub, wake_live_monitor, wake_publisher

from . import cutover, public_feed
from .runner import event_log, production_store
from .storage import PREFIX, encoded

SESSION_KEY = PREFIX + 'growth-session.json'
SESSION_TTL = timedelta(hours=2)


def session_credentials():
    from gold_storage import client

    source = client()
    bucket = os.environ['R2_BUCKET']
    with closing(source.get_object(Bucket=bucket, Key=SESSION_KEY)['Body']) as body:
        credentials = json.loads(body.read(4096))
    expires = datetime.fromisoformat(credentials['expiry_time'])
    if expires <= datetime.now(timezone.utc) + timedelta(minutes=4):
        raise RuntimeError('Growth Publisher storage session is stale')
    return credentials


def refresh_session(store, now):
    try:
        stored, _ = store.read('growth-session.json', limit=4096)
        if stored and datetime.fromisoformat(stored['expiry_time']) > now + timedelta(minutes=75):
            return
    except (TypeError, KeyError, ValueError):
        pass
    access_key = os.environ['R2_ACCESS_KEY_ID']
    account = os.environ['R2_ACCOUNT_ID']
    expires = now + SESSION_TTL
    claims = {
        'sub': account, 'iss': access_key, 'aud': f'{account}.r2.cloudflarestorage.com',
        'iat': int(now.timestamp()), 'exp': int(expires.timestamp()), 'bucket': store.bucket,
        'actions': ['GetObject', 'ListObjectsV2', 'PutObject', 'DeleteObject'],
        'paths': {'prefixPaths': [PREFIX], 'objectPaths': []},
    }

    def segment(data):
        return urlsafe_b64encode(encoded(data).rstrip(b'\n')).rstrip(b'=')

    unsigned = b'.'.join((segment({'alg': 'HS256', 'typ': 'JWT'}), segment(claims)))
    signature = hmac.digest(os.environ['R2_SECRET_ACCESS_KEY'].encode(), unsigned, 'sha256')
    jwt = unsigned + b'.' + urlsafe_b64encode(signature).rstrip(b'=')
    credentials = {'access_key': access_key, 'secret_key': sha256(jwt).hexdigest(),
                   'token': b64encode(b'jwt/' + jwt).decode(), 'expiry_time': expires.isoformat()}
    store.editorial.put_object(Bucket=store.bucket, Key=SESSION_KEY, Body=encoded(credentials),
                              ContentType='application/json', CacheControl='private, no-store')
    event_log('publisher_bridge', 'editorial_session_refreshed')


def wake(store, now, *, dry_run=False):
    if not dry_run:
        refresh_session(store, now)
    store.load()
    repository = cutover.owner(store)
    if repository == cutover.PRIMARY:
        token = os.environ['GITHUB_TOKEN']
    else:
        token = os.environ['PUBLISHER_GROWTH_DISPATCH_TOKEN']
    api = GitHub(token, repository=repository)
    event_log('publisher_watchdog', wake_publisher(api, now, dry_run=dry_run), repository=repository)
    event_log('telegram_live_watchdog', wake_live_monitor(api, now, dry_run=dry_run), repository=repository)


def ready(store):
    token = os.environ.get('PUBLISHER_GROWTH_DISPATCH_TOKEN')
    if not token:
        event_log('publisher_bridge', 'growth_dispatch_token_missing')
        return
    api = GitHub(token, repository=cutover.GROWTH)
    for filename in ('publisher-v71.yml', 'telegram-live.yml'):
        if api.request('workflows/' + filename).get('state') != 'active':
            raise ValueError('A required Growth workflow is not active')
    store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + cutover.READY_KEY,
                              Body=b'{"growth_workflows_active":true}',
                              ContentType='application/json', CacheControl='private, no-store')
    event_log('publisher_bridge', 'growth_dispatch_verified')


def publish_home(store, now):
    with store.cycle(now) as acquired:
        if not acquired:
            event_log('homepage_bridge', 'publisher_busy')
            return
        store.load()
        if cutover.owner(store) != cutover.GROWTH:
            return
        view, _ = store.read('homepage.json', limit=100_000)
        if view is None:
            event_log('homepage_bridge', 'waiting_for_growth')
            return
        if (not isinstance(view, dict) or view.keys() != public_feed.HOME_FIELDS
                or view['schema_version'] != 1 or view['publisher'] != 'v7.1'):
            raise ValueError('Invalid public home projection')
        generated = public_feed.stamp(view['updated_at'])
        if not generated or not now - timedelta(hours=1) <= generated <= now + timedelta(seconds=30):
            event_log('homepage_bridge', 'waiting_for_current_projection')
            return
        revision, _ = public_feed.publish(view)
        event_log('homepage_bridge', 'published', revision=revision)


def main():
    if (os.environ.get('GITHUB_REPOSITORY') != cutover.PRIMARY
            or os.environ.get('GITHUB_REF') != 'refs/heads/main'):
        raise ValueError('Publisher bridge requires primary main')
    now = datetime.now(timezone.utc)
    store = production_store()
    if sys.argv[1:] == ['--homepage']:
        publish_home(store, now)
    elif sys.argv[1:] == ['--prepare']:
        refresh_session(store, now)
        ready(store)
    else:
        wake(store, now, dry_run=os.environ.get('FAST_BACKUP_DRY_RUN') == 'true')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        event_log('publisher_bridge', 'failed', error=type(error).__name__)
        raise SystemExit(1) from None
