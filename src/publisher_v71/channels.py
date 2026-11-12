"""Posting transports and receipt reconciliation. No credentials in logs."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
import json
import re

import requests

from multimarket_model import stamp
from .selection import utc

CHANNEL = 'arabsniper_signals'
PAGE = '604216843376166'
GRAPH = 'https://graph.facebook.com/v25.0/'
CHANNEL_URL = 'https://t.me/s/' + CHANNEL


@dataclass
class DeliveryError(Exception):
    kind: str
    reason: str
    delay: int = 0


def retry_after(value):
    return min(3600, int(value)) if str(value).isdigit() else 0


class TelegramHistory(HTMLParser):
    def __init__(self, marker):
        super().__init__(convert_charrefs=True)
        self.marker = marker
        self.matches = {}
        self.message_id = self.at = None
        self.in_caption = False

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        post = re.fullmatch(CHANNEL + r'/(\d+)', attrs.get('data-post', ''))
        if post:
            self.message_id, self.at, self.in_caption = post[1], None, False
        if tag == 'time' and self.message_id:
            self.at = stamp(attrs.get('datetime'))
            if self.message_id in self.matches:
                self.matches[self.message_id] = self.at
        if tag == 'div' and 'tgme_widget_message_text' in attrs.get('class', '').split():
            self.in_caption = True

    def handle_endtag(self, tag):
        if tag == 'div':
            self.in_caption = False

    def handle_data(self, data):
        if self.in_caption and self.message_id and self.marker in data:
            self.matches[self.message_id] = self.at


class Telegram:
    def __init__(self, token, session=None):
        self.token = token
        self.http = session or requests.Session()

    def call(self, method, data=None, files=None, *, edit=False):
        if not self.token:
            raise DeliveryError('rejected', 'telegram_missing_credentials')
        try:
            response = self.http.post(f'https://api.telegram.org/bot{self.token}/{method}',
                                      data=data, files=files, timeout=(7, 18), allow_redirects=False)
            body = response.json()
        except requests.ConnectTimeout:
            raise DeliveryError('retry', 'telegram_connect_timeout') from None
        except (requests.RequestException, ValueError):
            raise DeliveryError('uncertain', 'telegram_unverified_response') from None
        if response.ok and body.get('ok') is True:
            return body['result']
        code = body.get('error_code')
        if code == 429 and body.get('ok') is False:
            raise DeliveryError('retry', 'telegram_rate_limit',
                                retry_after((body.get('parameters') or {}).get('retry_after')))
        if edit and code == 400 and 'message is not modified' in body.get('description', ''):
            return None
        kind = 'rejected' if body.get('ok') is False and code in (400, 401, 403, 404) else 'uncertain'
        raise DeliveryError(kind, f'telegram_http_{response.status_code}')

    def trial_healthy(self, now):
        from urllib.parse import urlsplit
        hook = self.call('getWebhookInfo')
        url = urlsplit(hook.get('url', ''))
        allowed = hook.get('allowed_updates') or ['message']
        last_error = hook.get('last_error_date')
        return (url.scheme == 'https' and url.hostname in
                ('arabsniperbet.com', 'stripe-checker.arabsnipertech.workers.dev')
                and url.path == '/api/telegram-webhook' and 'message' in allowed
                and (not last_error or now.timestamp() - last_error > 3600)
                and hook.get('pending_update_count', 0) < 20)

    def send(self, post, image):
        data = {'chat_id': '@' + CHANNEL, 'caption': post['text'], 'disable_notification':
                'true' if post['kind'] == 'live' else 'false'}
        if post.get('edit_id'):
            target = {'chat_id': data['chat_id'], 'message_id': post['edit_id']}
            media = json.dumps({'type': 'photo', 'media': 'attach://live_card', 'caption': post['text']},
                               ensure_ascii=False)
            try:
                result = self.call('editMessageMedia', {**target, 'media': media},
                                   files={'live_card': ('gold-classic.jpg', image, 'image/jpeg')}, edit=True)
            except DeliveryError as exc:
                if (exc.kind, exc.reason) != ('rejected', 'telegram_http_400'):
                    raise
                result = self.call('editMessageCaption', {**target, 'caption': post['text']}, edit=True)
        else:
            result = self.call('sendPhoto', data, files={'photo': ('gold-classic.jpg', image, 'image/jpeg')})
        if result is None and post.get('edit_id'):
            return {'id': str(post['edit_id']), 'at': utc(datetime.now(timezone.utc))}
        if result.get('chat', {}).get('username', '').lower() != CHANNEL or not result.get('message_id'):
            raise DeliveryError('uncertain', 'telegram_destination_unverified')
        field = 'edit_date' if post.get('edit_id') else 'date'
        at = datetime.fromtimestamp(result.get(field) or result['date'], timezone.utc)
        return {'id': str(result['message_id']), 'at': utc(at)}

    def reconcile(self, post, now):
        page = self.http.get(CHANNEL_URL, timeout=(7, 15))
        page.raise_for_status()
        parser = TelegramHistory(post['marker'])
        parser.feed(page.text)
        earliest = stamp(post['created_at']) - timedelta(minutes=1)
        matches = [(mid, at) for mid, at in parser.matches.items()
                   if at and (str(post.get('edit_id')) == mid or earliest <= at <= now)]
        if len(matches) == 1:
            mid, at = matches[0]
            return {'id': mid, 'at': utc(now if post.get('edit_id') else at)}


class Facebook:
    def __init__(self, token, session=None):
        self.token = token
        self.http = session or requests.Session()
        self.verified = False

    def request(self, method, path, **kwargs):
        if not self.token:
            raise DeliveryError('rejected', 'meta_missing_credentials')
        try:
            response = self.http.request(method, GRAPH + path, timeout=(7, 20), allow_redirects=False,
                                         headers={'Authorization': 'Bearer ' + self.token}, **kwargs)
            body = response.json()
        except requests.ConnectTimeout:
            raise DeliveryError('retry', 'meta_connect_timeout') from None
        except (requests.RequestException, ValueError):
            raise DeliveryError('uncertain', 'meta_unverified_response') from None
        if response.ok and 'error' not in body:
            return body
        code = (body.get('error') or {}).get('code')
        if code in (4, 17, 32, 613) or (response.status_code == 429 and code):
            raise DeliveryError('retry', 'meta_rate_limit', retry_after(response.headers.get('Retry-After')))
        kind = 'rejected' if code in (10, 100, 190, 200) or response.status_code in (401, 403) else 'uncertain'
        raise DeliveryError(kind, f'meta_http_{response.status_code}')

    def identify(self):
        if self.verified:
            return
        try:
            value = self.request('GET', 'me', params={'fields': 'id'})
        except DeliveryError as exc:
            # No publication was attempted during identity verification.
            raise DeliveryError('retry' if exc.kind == 'uncertain' else exc.kind, exc.reason, exc.delay) from None
        if value.get('id') != PAGE:
            raise DeliveryError('rejected', 'meta_page_mismatch')
        self.verified = True

    def send(self, post, image):
        self.identify()
        result = self.request('POST', PAGE + '/photos', data={'message': post['text']},
                              files={'source': ('gold-classic.jpg', image, 'image/jpeg')})
        if not result.get('post_id'):
            raise DeliveryError('uncertain', 'meta_missing_post_receipt')
        return {'id': result['post_id'], 'at': utc(datetime.now(timezone.utc))}

    def reconcile(self, post, now):
        self.identify()
        since = stamp(post['created_at']) - timedelta(minutes=1)
        response = self.request('GET', PAGE + '/published_posts',
                                params={'fields': 'id,message,created_time', 'limit': 100,
                                        'since': int(since.timestamp())})
        matches = [item for item in response['data'] if post['marker'] in item.get('message', '')
                   and stamp(item.get('created_time')) and since <= stamp(item['created_time']) <= now]
        if len(matches) == 1:
            return {'id': matches[0]['id'], 'at': utc(stamp(matches[0]['created_time']))}
