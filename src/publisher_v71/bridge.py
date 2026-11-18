"""Wake Growth from the existing Fast watchdog and forward its public home view."""
from base64 import b64decode, b64encode, urlsafe_b64encode
from contextlib import closing
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import hmac
import json
import os
import sys

from telegram_watchdog import GitHub, wake_live_monitor, wake_publisher

from . import cutover, public_feed
from .storage import PREFIX, encoded

SESSION_KEY = PREFIX + 'growth-session.json'
WRAP_KEY = 'growth-wrap-key.json'
SESSION_TTL = timedelta(hours=2)
GOLD_RESULTS_PREFIX = 'live-scores/growth/'
GOLD_RESULTS_KEYS = ('live-scores/provider-relay.json', 'live-scores/verification-incidents.json')
GOLD_RESULTS_OWNER = GOLD_RESULTS_PREFIX + 'owner.json'
GOLD_RESULTS_SESSION = 'growth-gold-results-session.json'
GOLD_RESULTS_WORKFLOW = 'gold-results.yml'
GOLD_RESULTS_REPAIR = GOLD_RESULTS_PREFIX + 'repair.json'
PRIMARY_GOLD_REPAIR = 'live-scores-health.yml'
GOLD_RESULTS_WATCHDOG = timedelta(minutes=30)


def session_credentials(source, bucket, *, key=SESSION_KEY):
    from nacl.public import PrivateKey, SealedBox

    with closing(source.get_object(Bucket=bucket, Key=key)['Body']) as body:
        envelope = json.loads(body.read(8192))
    private_key = PrivateKey(b64decode(os.environ['PUBLISHER_SESSION_PRIVATE_KEY'], validate=True))
    credentials = json.loads(SealedBox(private_key).decrypt(b64decode(envelope['sealed'], validate=True)))
    expires = datetime.fromisoformat(credentials['expiry_time'])
    if expires.isoformat() != envelope['expiry_time'] or expires <= datetime.now(timezone.utc) + timedelta(minutes=4):
        raise RuntimeError('Growth Publisher storage session is stale')
    return credentials


def refresh_session(store, now, *, name='growth-session.json', prefix=PREFIX,
                    objects=(), actions=('GetObject', 'ListObjectsV2', 'PutObject'), force=False):
    wrapping, _ = store.read(WRAP_KEY, limit=512)
    if not wrapping:
        return
    try:
        stored, _ = store.read(name, limit=8192)
        if (not force and stored and stored.get('sealed') and stored.get('recipient') == wrapping['public_key']
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
        'actions': list(actions),
        'paths': {'prefixPaths': [prefix], 'objectPaths': list(objects)},
    }

    def segment(data):
        return urlsafe_b64encode(encoded(data).rstrip(b'\n')).rstrip(b'=')

    unsigned = b'.'.join((segment({'alg': 'HS256', 'typ': 'JWT'}), segment(claims)))
    signature = hmac.digest(os.environ['R2_SECRET_ACCESS_KEY'].encode(), unsigned, 'sha256')
    jwt = unsigned + b'.' + urlsafe_b64encode(signature).rstrip(b'=')
    credentials = {'access_key': access_key, 'secret_key': sha256(jwt).hexdigest(),
                   'token': b64encode(b'jwt/' + jwt).decode(), 'expiry_time': expires.isoformat()}
    verify_session_write(store, credentials, prefix)
    recipient = wrapping['public_key']
    box = SealedBox(PublicKey(b64decode(recipient, validate=True)))
    envelope = {'recipient': recipient, 'expiry_time': credentials['expiry_time'],
                'sealed': b64encode(box.encrypt(encoded(credentials))).decode()}
    store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + name, Body=encoded(envelope),
                              ContentType='application/json', CacheControl='private, no-store')
    from .runner import event_log

    event_log('publisher_bridge', 'editorial_session_refreshed', file=name)


def verify_session_write(store, credentials, prefix):
    """Check the replacement session before handing it to either Growth writer."""
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    key = prefix + 'diagnostics/session-write.json'
    with boto3.client('s3', endpoint_url=store.client.meta.endpoint_url, region_name='auto',
                      aws_access_key_id=credentials['access_key'],
                      aws_secret_access_key=credentials['secret_key'],
                      aws_session_token=credentials['token'],
                      config=Config(connect_timeout=5, read_timeout=10,
                                    retries={'total_max_attempts': 2})) as client:
        try:
            obj = client.get_object(Bucket=store.bucket, Key=key)
            obj['Body'].close()
            condition = {'IfMatch': obj['ETag']}
        except ClientError as exc:
            if exc.response['Error']['Code'] != 'NoSuchKey':
                raise
            condition = {'IfNoneMatch': '*'}
        body = encoded({'expires': credentials['expiry_time']})
        client.put_object(Bucket=store.bucket, Key=key, Body=body, **condition)


def gold_results_storage(source, bucket):
    import boto3
    from botocore.config import Config
    from botocore.credentials import RefreshableCredentials
    from botocore.session import get_session

    def refresh():
        return session_credentials(source, bucket, key=PREFIX + GOLD_RESULTS_SESSION)

    session = get_session()
    session._credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method='gold-results-r2')
    return boto3.Session(botocore_session=session).client(
        's3', endpoint_url=source.meta.endpoint_url, region_name='auto',
        config=Config(connect_timeout=7, read_timeout=20, retries={'total_max_attempts': 2}))


def confirmed_idle(now):
    """Treat a stale decision, new watchlist or unavailable Worker as a normal recovery."""
    import requests
    from pathlib import Path

    try:
        watch = json.loads(Path('data/live_watchlist.json').read_text(encoding='utf-8'))
        response = requests.get('https://arabsniperbet.com/api/live-scores?preflight=1',
                                timeout=(2, 3), allow_redirects=False, stream=True)
        with response:
            if response.status_code != 200:
                return False
            body = next(response.iter_content(chunk_size=513), b'')
        if len(body) > 512:
            return False
        result = json.loads(body)
        if result.get('decision') != 'idle':
            return False
        cron = datetime.fromisoformat(result['cron_at'])
        generation = datetime.fromisoformat(result['watch_generated_at'])
        return (generation == datetime.fromisoformat(watch['generated_at']) and
                timedelta() <= now - generation < timedelta(hours=4) and
                timedelta() <= now - cron < timedelta(minutes=2))
    except (OSError, ValueError, AttributeError, KeyError, TypeError, requests.RequestException):
        return False


def wake_gold_results(store, api, primary, now, *, dry_run=False):
    owner, _ = store.read(GOLD_RESULTS_OWNER, root=True, limit=512)
    if owner and owner['repository'] == cutover.PRIMARY:
        if any(run['status'] != 'completed' for run in primary.runs('live-scores-relay.yml', now - timedelta(minutes=20))):
            return 'primary_already_running'
        if dry_run:
            return 'primary_restored'
        primary.request('workflows/live-scores-relay.yml/dispatches', {'ref': 'main'})
        return 'dispatched_primary'
    if not api:
        return 'growth_token_unavailable'
    if not dry_run:
        refresh_session(store, now, name=GOLD_RESULTS_SESSION, prefix=GOLD_RESULTS_PREFIX,
                        objects=GOLD_RESULTS_KEYS, actions=('GetObject', 'PutObject'))
    ready = owner is not None
    path = 'workflows/' + GOLD_RESULTS_WORKFLOW
    if api.request(path).get('state') != 'active':
        return 'workflow_disabled'
    # Growth owns the independent periodic check. Dispatch externally only if
    # that check has stopped arriving, while continuing to renew R2 sessions.
    runs = api.runs(GOLD_RESULTS_WORKFLOW, now - GOLD_RESULTS_WATCHDOG)
    if any(run['status'] != 'completed' for run in runs):
        return 'already_running'
    if ready and any(run.get('conclusion') == 'success' and
                     now - datetime.fromisoformat(run['updated_at']) < GOLD_RESULTS_WATCHDOG
                     for run in runs):
        return 'recent_cycle'
    if dry_run:
        return 'would_dispatch'
    # Keep an independent full cycle at least hourly when the external scheduler is active.
    # The 08:07 Europe/Rome reconciliation in Growth remains independently scheduled.
    if ready:
        try:
            previous = api.runs(GOLD_RESULTS_WORKFLOW, now - timedelta(hours=1))
        except RuntimeError:
            previous = []
        latest = max(previous, key=lambda run: run.get('created_at', ''), default={})
        if latest.get('conclusion') == 'success' and confirmed_idle(now):
            return 'worker_confirmed_idle'
    operation = 'fallback' if ready else 'activate'
    api.request(path + '/dispatches', {'ref': 'main', 'inputs': {'operation': operation}})
    return 'dispatched_' + operation


def wake(store, now, *, dry_run=False):
    from .runner import event_log

    store.load()
    repository = cutover.owner(store)
    if repository == cutover.GROWTH and not dry_run:
        refresh_session(store, now)
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


def wake_results(store, now):
    from .runner import event_log

    token = os.environ.get('PUBLISHER_GROWTH_DISPATCH_TOKEN')
    wrapping, _ = store.read(WRAP_KEY, limit=512)
    if not wrapping:
        return
    dry_run = os.environ.get('FAST_BACKUP_DRY_RUN') == 'true'
    api = GitHub(token, repository=cutover.GROWTH) if token else None
    primary = GitHub(os.environ['GITHUB_TOKEN'])
    try:
        status = wake_gold_results(store, api, primary, now, dry_run=dry_run)
    except RuntimeError as error:
        if str(error) != 'GitHub HTTP 404':
            raise
        status = 'workflow_unavailable'
    event_log('gold_results_watchdog', status, repository=cutover.GROWTH)
    if not dry_run:
        repair, _ = store.read(GOLD_RESULTS_REPAIR, root=True, limit=512)
        if repair and timedelta() <= now - datetime.fromisoformat(repair['at']) < timedelta(minutes=20):
            runs = primary.runs(PRIMARY_GOLD_REPAIR, now - timedelta(minutes=20))
            # The primary workflow has access to the verified checkpoint and its own Actions token.
            if not any(run.get('display_title') == 'Gold results repair requested by Growth'
                       and (run['status'] != 'completed' or
                            run.get('conclusion') == 'success' and
                            datetime.fromisoformat(run['created_at']) >= datetime.fromisoformat(repair['at']))
                       for run in runs):
                primary.request(f'workflows/{PRIMARY_GOLD_REPAIR}/dispatches', {
                    'ref': 'main', 'inputs': {'repair': 'true'}})
                event_log('gold_results_watchdog', 'repair_dispatched', repository=cutover.PRIMARY)


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
    from .runner import event_log

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
    from .runner import event_log

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
    from .runner import production_store

    if (os.environ.get('GITHUB_REPOSITORY') != cutover.PRIMARY
            or os.environ.get('GITHUB_REF') != 'refs/heads/main'):
        raise ValueError('Publisher bridge requires primary main')
    now = datetime.now(timezone.utc)
    store = production_store()
    if sys.argv[1:] == ['--homepage']:
        publish_home(store, now)
    elif sys.argv[1:] == ['--prepare']:
        prepare(store, now)
    elif sys.argv[1:] == ['--gold-results']:
        wake_results(store, now)
    else:
        wake(store, now, dry_run=os.environ.get('FAST_BACKUP_DRY_RUN') == 'true')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        from .runner import event_log

        event_log('publisher_bridge', 'failed', error=type(error).__name__)
        raise SystemExit(1) from None
