"""Recover only a positively identified public channel receipt after uncertain send."""
from datetime import datetime, timedelta
from html import unescape
import re


def normalized(text):
    return ' '.join(unescape(re.sub(r'<[^>]*>', ' ', text)).split())


def caption(text):
    if len(text.encode('utf-16-le')) // 2 > 1024:
        return text[:950].rsplit('\n', 1)[0] + '\n18+ · arabsniperbet.com'
    return text


def confirmed_message(page, post, now):
    from telegram_digest import ROME
    expected = ' '.join(caption(post['text']).split())
    due = datetime.fromisoformat(f"{post['day']}T{post['slot']}").replace(tzinfo=ROME)
    matches = []
    blocks = re.split(r'data-post="arabsniper_signals/(\d+)"', page)
    for index in range(1, len(blocks), 2):
        message_id, block = blocks[index], blocks[index+1]
        text = re.search(r'<div class="tgme_widget_message_text[^\"]*"[^>]*>(.*?)</div>', block, re.S)
        time = re.search(r'<time[^>]*datetime="([^\"]+)"', block)
        if not text or not time or normalized(text.group(1)) != expected:
            continue
        at = datetime.fromisoformat(time.group(1).replace('Z', '+00:00'))
        if at.tzinfo and due <= at <= now and at < due+timedelta(minutes=30):
            matches.append(int(message_id))
    if len(matches) != 1:
        raise RuntimeError('Telegram receipt remains uncertain; automatic resend blocked')
    return matches[0]


def recover(saved, name, now):
    import json
    from pathlib import Path
    import requests
    from telegram_digest import gh
    from social_card import render
    gh('run', 'download', str(saved['workflow_run']['id']), '-n', name+'-claim', '-D', 'claim')
    post = json.loads(Path('claim/post.json').read_text())
    if name != f"tg-{post['day']}-{post['slot'].replace(':', '')}":
        raise ValueError('Claim identity mismatch')
    response = requests.get('https://t.me/s/arabsniper_signals', timeout=25)
    response.raise_for_status()
    post['message_id'] = confirmed_message(response.text, post, now)
    Path('post.json').write_text(json.dumps(post, ensure_ascii=False))
    render(post, 'social-card.jpg')
    print('Reconciled existing Telegram message', post['message_id'])
