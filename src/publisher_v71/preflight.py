"""Verify a Growth installation without sending a post."""
import importlib.util
import os
from pathlib import Path
import uuid

import requests

from telegram_watchdog import GitHub

from . import cutover, public_feed, runner, scores
from .channels import CHANNEL, GRAPH, PAGE
from .editorial import campaign_active
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

    probe = PREFIX + 'preflight/' + uuid.uuid4().hex
    tag = store.editorial.put_object(Bucket=store.bucket, Key=probe, Body=b'{}',
                                    IfNoneMatch='*')['ETag']
    try:
        store.editorial.put_object(Bucket=store.bucket, Key=probe, Body=b'{"verified":true}', IfMatch=tag)
    finally:
        store.editorial.delete_object(Bucket=store.bucket, Key=probe)
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
    api = GitHub(os.environ['PUBLISHER_PRIMARY_ACTIONS_TOKEN'])
    if api.request('workflows/publisher-home-bridge.yml').get('state') != 'active':
        raise ValueError('The workflow that publishes the home projection is unavailable')
    api.request('workflows/publisher-home-bridge.yml/dispatches', {'ref': 'main'})
    runner.event_log('preflight', 'homepage_source_verified')
    if (os.environ.get('TRIAL_DISABLED') != 'true'
            and campaign_active(runner.clock().astimezone(runner.ROME).date())):
        script = Path(__file__).resolve().parents[2] / 'scripts/gold-campaign-facebook.py'
        spec = importlib.util.spec_from_file_location('verify_campaign', script)
        campaign = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(campaign)
        campaign.verify_ready()


if __name__ == '__main__':
    try:
        verify(runner.production_store())
    except Exception as error:
        # Provider exception bodies and URLs can contain credentials.
        runner.event_log('preflight', 'failed', error=type(error).__name__)
        raise SystemExit(1) from None
