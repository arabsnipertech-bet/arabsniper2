"""Replay delivered records against the Python Worker. Log metrics only, never captions."""
import io
import json
import os
from pathlib import Path
import time
from datetime import datetime
from PIL import Image, ImageChops, ImageStat
import requests
import subprocess
from publisher_v71 import editorial
from telegram_live_art import animation

root = Path(os.environ['SHADOW_INPUT'])
ledger = json.loads((root / 'state.json').read_bytes())
posts = sorted((p for p in ledger['posts'].values() if p.get('channel') == 'telegram'
                and p.get('status') == 'sent' and (p.get('receipt') or {}).get('id')),
               key=lambda p:p['created_at'], reverse=True)
if not posts:
    raise ValueError('No receipt-backed posts to compare')
seen = set()
samples = []
for p in posts:
    group = (p['kind'], bool(p.get('edit_id')), bool(p.get('view',{}).get('animation')))
    if group not in seen:
        seen.add(group)
        samples.append(p)

def cpu_ticks():
    ticks = 0
    for comm in Path('/proc').glob('[0-9]*/comm'):
        try:
            if comm.read_text().strip() == 'workerd':
                fields = (comm.parent/'stat').read_text().split(') ',1)[1].split()
                ticks += int(fields[11]) + int(fields[12])
        except (FileNotFoundError, PermissionError):
            continue
    return ticks

def call(post, mode, trial=False):
    before = cpu_ticks()
    start = time.perf_counter()
    res = requests.post('http://127.0.0.1:8787',json={'post':post,'mode':mode,'trial_ok':trial},timeout=180)
    if res.status_code != 200:
        raise RuntimeError(f'Worker {mode} HTTP {res.status_code}')
    print(post['kind'],mode,'bytes',len(res.content),'ms wall',round(1000*(time.perf_counter()-start)),
          'ms workerd process CPU', round(1000*(cpu_ticks()-before)/os.sysconf('SC_CLK_TCK')),flush=True)
    return res.content

def compare_images(reference, raw):
    with Image.open(io.BytesIO(reference)) as a, Image.open(io.BytesIO(raw)) as b:
        if (a.format,a.size,getattr(a,'n_frames',1)) != (b.format,b.size,getattr(b,'n_frames',1)):
            raise ValueError('Different media dimensions, format or frame count')
        worst = 0
        for i in range(getattr(a,'n_frames',1)):
            a.seek(i); b.seek(i)
            if a.info.get('duration') != b.info.get('duration'):
                raise ValueError('Different frame timing')
            diff = ImageChops.difference(a.convert('RGB'),b.convert('RGB'))
            worst = max(worst, max(ImageStat.Stat(diff).mean))
        print(a.format,a.size,getattr(a,'n_frames',1),'worst channel mean absolute pixel error',round(worst,3),
              'byte identical',reference==raw,flush=True)
        return worst

from PIL import features, __version__, ImageFont
from unittest.mock import patch
print('native renderer', __version__, 'raqm',features.check('raqm'),'freetype',features.version('freetype2'),
      'jpeg',features.version('jpg'),flush=True)
print('worker renderer',requests.post('http://127.0.0.1:8787',json={'mode':'features'},timeout=10).json(),flush=True)
native_font = ImageFont.truetype
def basic_font(*args, **kwargs):
    kwargs['layout_engine'] = ImageFont.Layout.BASIC
    return native_font(*args, **kwargs)

failures = []
for post in samples:
    kind, view = post['kind'], post['view']
    try:
        if post['id'] != '/'.join(('telegram',kind,post['key'])) or post['marker'] not in post['text']:
            raise ValueError('Invalid recorded identity')
        if post.get('edit_id') and str(post['edit_id']) != str(post['receipt']['id']):
            raise ValueError('Edit receipt mismatch')
        args = {'post':post}
        replay = subprocess.run(['node',str(Path(__file__).with_name('replay-envelope.mjs'))],
                                input=json.dumps(args).encode(),capture_output=True,check=True)
        proposal = json.loads(replay.stdout)
        method = 'editMessageMedia' if post.get('edit_id') else 'sendPhoto'
        expected = (post['id'],post['marker'],post['text'],method,post.get('edit_id'))
        if tuple(proposal[k] for k in ('id','marker','caption','method','message_id')) != expected:
            raise ValueError('Worker envelope mismatch')
        at = datetime.fromisoformat(post['created_at'])
        flags = [trial for trial in (False,True) if editorial.message(kind,view,post['marker'],'telegram',
                 now=at,trial_ok=trial) == post['text']]
        if not flags or call(post,'caption',flags[0]).decode() != post['text']:
            raise ValueError('Recorded caption mismatch')
        worker_jpeg = call(post,'jpeg')
        if compare_images(editorial.card(kind,view),worker_jpeg) > 1:
            editorial.font.cache_clear()
            try:
                with patch.object(ImageFont,'truetype',side_effect=basic_font):
                    print('BASIC layout control',kind,flush=True)
                    compare_images(editorial.card(kind,view),worker_jpeg)
            finally:
                editorial.font.cache_clear()
            failures.append((kind,'JPEG','visual mismatch'))
        if post.get('edit_id') and view.get('animation'):
            badge = f"{view['home']}–{view['away']} · {view['status'].upper()}"
            if compare_images(animation(view['animation'],badge), call(post,'gif')) > 1:
                raise ValueError('GIF visual mismatch')
    except Exception as exc:
        failures.append((kind,type(exc).__name__,str(exc)[:100]))
print('receipt-backed samples',len(samples),'of',len(posts),'failures',failures,flush=True)
if failures:
    raise SystemExit(1)
