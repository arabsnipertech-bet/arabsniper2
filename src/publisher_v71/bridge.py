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
WRAP_KEY = 'growth-wrap-key.json'
SESSION_TTL = timedelta(hours=2)


def session_credentials(source, bucket):
    from nacl.public import PrivateKey, SealedBox

    with closing(source.get_object(Bucket=bucket, Key=SESSION_KEY)['Body']) as body:
        envelope = json.loads(body.read(8192))
    key = PrivateKey(b64decode(os.environ['PUBLISHER_SESSION_PRIVATE_KEY'], validate=True))
    credentials = json.loads(SealedBox(key).decrypt(b64decode(envelope['sealed'], validate=True)))
    expires = datetime.fromisoformat(credentials['expiry_time'])
    if expires.isoformat() != envelope['expiry_time'] or expires <= datetime.now(timezone.utc) + timedelta(minutes=4):
        raise RuntimeError('Growth Publisher storage session is stale')
    return credentials


def refresh_session(store, now):
    wrapping, _ = store.read(WRAP_KEY, limit=512)
    if not wrapping:
        return
    try:
        stored, _ = store.read('growth-session.json', limit=8192)
        if (stored and stored.get('sealed') and stored.get('recipient') == wrapping['public_key']
                and datetime.fromisoformat(stored['expiry_time']) > now + timedelta(minutes=75)):
            return
    except (TypeError, KeyError, ValueError):
        pass
    from nacl.public import PublicKey, SealedBox

    access_key = os.environ['R2_ACCESS_KEY_ID']
    account = os.environ['R2_ACCOUNT_ID']
    expires = now + SESSION_TTL
    claims = {
        'sub': account, 'iss': access_key, 'aud': f'{account}.r2.cloudflarestorage.com',
        'iat': int(now.timestamp()), 'exp': int(expires.timestamp()), 'bucket': store.bucket,
        'actions': ['GetObject', 'ListObjectsV2', 'PutObject'],
        'paths': {'prefixPaths': [PREFIX], 'objectPaths': []},
    }

    def segment(data):
        return urlsafe_b64encode(encoded(data).rstrip(b'\n')).rstrip(b'=')

    unsigned = b'.'.join((segment({'alg': 'HS256', 'typ': 'JWT'}), segment(claims)))
    signature = hmac.digest(os.environ['R2_SECRET_ACCESS_KEY'].encode(), unsigned, 'sha256')
    jwt = unsigned + b'.' + urlsafe_b64encode(signature).rstrip(b'=')
    credentials = {'access_key': access_key, 'secret_key': sha256(jwt).hexdigest(),
                   'token': b64encode(b'jwt/' + jwt).decode(), 'expiry_time': expires.isoformat()}
    recipient = wrapping['public_key']
    box = SealedBox(PublicKey(b64decode(recipient, validate=True)))
    envelope = {'recipient': recipient, 'expiry_time': credentials['expiry_time'],
                'sealed': b64encode(box.encrypt(encoded(credentials))).decode()}
    store.editorial.put_object(Bucket=store.bucket, Key=SESSION_KEY, Body=encoded(envelope),
                              ContentType='application/json', CacheControl='private, no-store')
    event_log('publisher_bridge', 'editorial_session_refreshed')


def wake(store, now, *, dry_run=False):
    if not dry_run:
        refresh_session(store, now)
    store.load()
    repository = cutover.owner(store)
    primary_api = GitHub(os.environ['GITHUB_TOKEN'])
    if repository == cutover.PRIMARY:
        api = primary_api
    else:
        api = GitHub(os.environ['PUBLISHER_GROWTH_DISPATCH_TOKEN'], repository=cutover.GROWTH)
        publish_home_if_due(store, primary_api, now, dry_run=dry_run)
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a', encoding='utf-8') as target:
            target.write(f'owner={repository}\n')
    event_log('publisher_watchdog', wake_publisher(api, now, dry_run=dry_run), repository=repository)
    event_log('telegram_live_watchdog', wake_live_monitor(api, now, dry_run=dry_run), repository=repository)


def publish_home_if_due(store, api, now, *, dry_run=False):
    view, tag = store.read('homepage.json', limit=100_000)
    receipt, _ = store.read('homepage-receipt.json', limit=512)
    if not view or not tag or tag == (receipt or {}).get('etag'):
        return
    if not dry_run and not any(run['status'] != 'completed' for run in
                               api.runs(cutover.HOME_BRIDGE, now - timedelta(minutes=15))):
        api.request(f'workflows/{cutover.HOME_BRIDGE}/dispatches', {'ref': 'main'})


def prepare(store, now):
    """Install the Publisher's secrets in the private Growth repository."""
    from nacl.public import PrivateKey, PublicKey, SealedBox
    import requests

    store.load()
    if cutover.owner(store) != cutover.PRIMARY:
        raise ValueError('The Publisher is already owned by Growth')
    token = os.environ['PUBLISHER_GROWTH_DISPATCH_TOKEN']
    api = GitHub(token, repository=cutover.GROWTH)
    for filename in ('publisher-v71.yml', 'telegram-live.yml'):
        if api.request('workflows/' + filename).get('state') != 'active':
            raise ValueError('A required Growth workflow is not active')
    primary_api = GitHub(os.environ['GITHUB_TOKEN'])
    if primary_api.request(f'workflows/{cutover.HOME_BRIDGE}').get('state') != 'active':
        raise ValueError('The workflow that publishes the home projection is unavailable')
    primary_api.request(f'workflows/{cutover.HOME_BRIDGE}/dispatches', {'ref': 'main'})
    key_info = api.request('secrets/public-key')
    github_box = SealedBox(PublicKey(b64decode(key_info['key'], validate=True)))
    source_names = ('TELEGRAM_BOT_TOKEN', 'FACEBOOK_PAGE_ACCESS_TOKEN', 'API_SPORTS_KEY')
    values = {name: os.environ[name] for name in source_names if os.environ.get(name)}
    if len(values) != len(source_names):
        raise ValueError('A required Publisher secret is missing in the primary repository')
    private_key = PrivateKey.generate()
    public_key = b64encode(bytes(private_key.public_key)).decode()
    values['PUBLISHER_SESSION_PRIVATE_KEY'] = b64encode(bytes(private_key)).decode()
    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json',
               'X-GitHub-Api-Version': '2022-11-28'}
    for name, value in values.items():
        response = requests.put(f'https://api.github.com/repos/{cutover.GROWTH}/actions/secrets/{name}',
                                headers=headers, timeout=(5, 15), allow_redirects=False, json={
                                    'key_id': key_info['key_id'],
                                    'encrypted_value': b64encode(github_box.encrypt(value.encode())).decode()})
        if response.status_code not in (201, 204):
            raise RuntimeError(f'GitHub rejected the setup of {name} with HTTP {response.status_code}')
        event_log('publisher_bridge', 'secret_configured', name=name)
    store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + WRAP_KEY,
                              Body=encoded({'public_key': public_key}),
                              ContentType='application/json', CacheControl='private, no-store')
    refresh_session(store, now)
    if os.environ.get('ACTIVATE_GROWTH') == 'true':
        store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + cutover.READY_KEY,
                                  Body=b'{"growth_workflows_active":true}',
                                  ContentType='application/json', CacheControl='private, no-store')
        api.request('workflows/publisher-v71.yml/dispatches', {
            'ref': 'main', 'inputs': {'dry_run': 'false', 'operation': 'activate-growth'}})
    else:
        api.request('workflows/publisher-v71.yml/dispatches', {
            'ref': 'main', 'inputs': {'dry_run': 'true'}})
    event_log('publisher_bridge', 'growth_preflight_dispatched')


def publish_home(store, now):
    store.load()
    if cutover.owner(store) != cutover.GROWTH:
        return
    with store.cycle(now) as acquired:
        if not acquired:
            event_log('homepage_bridge', 'publisher_busy')
            return
        store.load()
        if cutover.owner(store) != cutover.GROWTH:
            return
        view, tag = store.read('homepage.json', limit=100_000)
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
        store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + 'homepage-receipt.json',
                                   Body=encoded({'etag': tag, 'revision': revision}),
                                   ContentType='application/json', CacheControl='private, no-store')
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
        prepare(store, now)
    else:
        wake(store, now, dry_run=os.environ.get('FAST_BACKUP_DRY_RUN') == 'true')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        event_log('publisher_bridge', 'failed', error=type(error).__name__)
        raise SystemExit(1) from None
