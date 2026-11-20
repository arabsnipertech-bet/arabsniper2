"""Read back public Telegram embeds for receipt IDs. Never call the Telegram Bot API."""
import io
import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit
from bs4 import BeautifulSoup
from PIL import Image, ImageChops, ImageStat
import requests
from publisher_v71.editorial import card

ledger=json.loads((Path(os.environ['SHADOW_INPUT'])/'state.json').read_bytes())
latest={}
for post in sorted(ledger['posts'].values(),key=lambda p:(p.get('receipt') or {}).get('at','')):
    receipt=post.get('receipt') or {}
    mid=str(receipt.get('id',''))
    if post.get('status')=='sent' and post.get('channel')=='telegram' and re.fullmatch(r'[0-9]+',mid):
        latest[mid]=post
seen=set()
for mid,post in sorted(latest.items(),key=lambda pair:int(pair[0]),reverse=True):
    kind=(post['kind'],bool(post.get('edit_id')))
    if kind in seen: continue
    seen.add(kind)
    try:
        page=requests.get(f'https://t.me/arabsniper_signals/{mid}',params={'embed':1},
                          timeout=(4,10),allow_redirects=False)
        doc=BeautifulSoup(page.content,'html.parser')
        item=doc.select_one(f'[data-post="arabsniper_signals/{mid}"]')
        if item is None:
            print(kind,'embed unavailable',page.status_code);continue
        body=item.select_one('.tgme_widget_message_text')
        if body is None:
            print(kind,'caption unavailable');continue
        for br in body.find_all('br'): br.replace_with('\n')
        actual=body.get_text().strip()
        print(kind,'receipt link verified','caption exact',actual==post['text'].strip(),
              'marker present',post['marker'] in actual)
        photo=item.select_one('.tgme_widget_message_photo_wrap')
        if not photo:
            print(kind,'no public still image');continue
        match=re.search(r"background-image:\s*url\(['\"]?(https://[^)'\"]+)",photo.get('style',''))
        if not match:
            print(kind,'public photo URL unavailable');continue
        url=match.group(1)
        host=urlsplit(url).hostname or ''
        if not (re.fullmatch(r'cdn\d*\.cdn-telegram\.org',host) or host.endswith('.cdn-telegram.org') or host.endswith('.telesco.pe') or re.fullmatch(r'cdn\d*\.telegram-cdn\.org',host)):
            print(kind,'public photo host excluded',host);continue
        with requests.get(url,timeout=(4,10),stream=True,allow_redirects=False) as media:
            media.raise_for_status()
            buf=media.raw.read(4_000_001)
            if len(buf)>4_000_000: continue
        with Image.open(io.BytesIO(buf)) as remote,Image.open(io.BytesIO(card(post['kind'],post['view']))) as ref:
            if max(remote.size)>3000:
                print(kind,'oversized public preview');continue
            diff=ImageChops.difference(remote.convert('RGB'),ref.convert('RGB').resize(remote.size,Image.Resampling.LANCZOS))
            print(kind,'public photo',remote.size,'mean absolute channel error',round(max(ImageStat.Stat(diff).mean),3))
    except (requests.RequestException,OSError,ValueError) as exc:
        print(kind,'public comparison unavailable',type(exc).__name__)
