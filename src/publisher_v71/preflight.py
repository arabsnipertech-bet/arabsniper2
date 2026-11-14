"""Verify a Growth installation without sending a post."""
import os

import requests

from . import cutover, public_feed, runner, scores
from .channels import CHANNEL, GRAPH, PAGE
from .storage import PREFIX


def verify(store, *, activation=False):
    store.load()
    if not store.etag or not store.state['legacy'].get('handoff'):
        raise ValueError('Existing delivery receipts are missing')
    runner.event_log('preflight', 'existing_receipts', owner=cutover.owner(store),
                     posts=len(store.state['posts']), live=len(store.state['live']))
    if activation:
        ready, _ = store.read(cutover.READY_KEY, limit=512)
        if ready != {'growth_workflows_active': True}:
            raise ValueError('The external watchdog has not verified dispatch access to Growth')

    probe, tag = store.read('preflight/status.json', limit=512)
    condition = {'IfMatch': tag} if probe is not None else {'IfNoneMatch': '*'}
    store.editorial.put_object(Bucket=store.bucket, Key=PREFIX + 'preflight/status.json',
                               Body=b'{"verified":true}', ContentType='application/json', **condition)
    runner.event_log('preflight', 'editorial_storage_verified')

    services = runner.social_services()
    telegram = services['telegram']
    bot = telegram.call('getMe')
    channel = telegram.call('getChat', {'chat_id': '@' + CHANNEL})
    member = telegram.call('getChatMember', {'chat_id': channel['id'], 'user_id': bot['id']})
    if (bot.get('username', '').lower() != 'arabsniperbot'
            or channel.get('username', '').lower() != CHANNEL or not member.get('can_post_messages')):
        raise ValueError('Telegram channel permission validation failed')
    runner.event_log('preflight', 'telegram_verified')

    if 'facebook' not in services:
        raise ValueError('Facebook must be enabled for the Publisher handoff')
    headers = {'Authorization': 'Bearer ' + os.environ['META_TOKEN']}
    identity = requests.get(GRAPH + 'me', params={'fields': 'id'}, headers=headers,
                            timeout=(5, 10), allow_redirects=False)
    identity.raise_for_status()
    posts = requests.get(GRAPH + PAGE + '/published_posts', params={'fields': 'id', 'limit': 1},
                         headers=headers, timeout=(5, 10), allow_redirects=False)
    posts.raise_for_status()
    if identity.json().get('id') != PAGE or not isinstance(posts.json().get('data'), list):
        raise ValueError('Facebook Page validation failed')
    runner.event_log('preflight', 'facebook_verified')

    quota, _ = scores.provider('status', os.environ['API_SPORTS_KEY'])
    if quota.get('subscription', {}).get('active') is not True:
        raise ValueError('API-Football authorization is unavailable')
    scores.public_cache()
    runner.preflight(store)

    public_feed.published_revision(public_feed.PUBLIC_REPOSITORY)
    runner.event_log('preflight', 'homepage_source_verified')


if __name__ == '__main__':
    try:
        verify(runner.production_store())
    except Exception as error:
        # Provider exception bodies and URLs can contain credentials.
        runner.event_log('preflight', 'failed', error=type(error).__name__)
        raise SystemExit(1) from None
