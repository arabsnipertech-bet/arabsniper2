"""Persistent per-channel outbox. A lost response never causes an automatic resend."""
from datetime import timedelta

from multimarket_model import stamp
from v7_decision import fingerprint

from .channels import DeliveryError
from .editorial import card, message
from .selection import LEAD, utc

MAX_ATTEMPTS = 3


def queue(store, kind, key, view, channels, now, until, *, edit=None):
    for channel in channels:
        post_id = f'{channel}/{kind}/{key}'
        if post_id in store.state['posts']:
            continue
        marker = 'AS71-' + fingerprint(post_id)[:16].upper()
        store.state['posts'][post_id] = {
            'id': post_id, 'channel': channel, 'kind': kind, 'key': key,
            'created_at': utc(now), 'expires_at': utc(until), 'view': view,
            'marker': marker, 'text': message(kind, view, marker, channel),
            'status': 'ready', 'attempts': 0, 'edit_id': edit,
        }


def can_send(post, now, candidates):
    if now >= stamp(post['expires_at']):
        return 'expired'
    if post['kind'] == 'best':
        view = post['view']
        for p in (view['free'], *view['previews']):
            latest = candidates.get(p['fixture_id'])
            if not latest or (latest['kickoff'], latest['category']) != (p['kickoff'], p['category']):
                return 'source_changed'
            if stamp(latest['expires_at']) <= now:
                return 'quote_expired'
        public = candidates[view['free']['fixture_id']]
        if (public['market'], public['odd'], public['probability']) != (
                view['free']['market'], view['free']['odd'], view['free']['probability']):
            return 'quote_or_decision_changed'
    if post['kind'] == 'live' and post['view']['status'] == 'Prematch':
        if stamp(post['view']['kickoff']) - now < LEAD:
            return 'late_prematch'
    return None


def attempt(store, post, service, candidates, clock, emit):
    now = clock()
    if post['status'] in ('uncertain', 'sending'):
        try:
            receipt = service.reconcile(post, now)
        except Exception as exc:
            emit(post, 'reconcile_' + type(exc).__name__)
            return
        if not receipt:
            if post['status'] == 'sending':
                post.update(status='uncertain', reason='interrupted_after_claim')
                store.save()
            if now < stamp(post['expires_at']) + timedelta(hours=12):
                emit(post, 'receipt_unconfirmed')
            return
        post.update(status='sent', receipt=receipt)
        store.save()
        emit(post, 'reconciled')
        return
    if post['status'] not in ('ready', 'retry'):
        return
    if post.get('retry_at') and now < stamp(post['retry_at']):
        return
    blocked = can_send(post, now, candidates)
    if blocked:
        post.update(status='skipped', reason=blocked)
        store.save()
        emit(post, blocked)
        return

    try:
        image = None if post.get('edit_id') else card(post['kind'], post['view'])
    except (OSError, ValueError) as exc:
        post.update(status='failed', reason='render_' + type(exc).__name__)
        store.save()
        emit(post, post['reason'])
        return
    now = clock()
    blocked = can_send(post, now, candidates)
    if blocked:
        post.update(status='skipped', reason=blocked)
        store.save()
        emit(post, blocked)
        return
    post.update(status='sending', attempts=post['attempts'] + 1, attempt_at=utc(now))
    store.save()
    try:
        receipt = service.send(post, image)
    except DeliveryError as exc:
        if exc.kind == 'retry' and post['attempts'] < MAX_ATTEMPTS:
            delay = max(exc.delay, 30 * (4 ** (post['attempts'] - 1)))
            post.update(status='retry', retry_at=utc(clock() + timedelta(seconds=delay)), reason=exc.reason)
        else:
            post.update(status='uncertain' if exc.kind == 'uncertain' else 'failed', reason=exc.reason)
        store.save()
        emit(post, post['status'] + '_' + exc.reason)
        return
    except Exception as exc:
        post.update(status='uncertain', reason='transport_' + type(exc).__name__)
        store.save()
        emit(post, post['reason'])
        return
    post.update(status='sent', receipt=receipt)
    store.save()
    emit(post, 'sent')
