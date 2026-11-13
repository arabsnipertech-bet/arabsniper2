"""Bounded official-score recovery through the existing private R2 bucket."""
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import time
import subprocess
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urlencode

KEY = 'live-scores/provider-relay.json'
FINISHED = {'FT','AET','PEN'}
ADMIN = {'PST','CANC','ABD','AWD','WO'}
BATCH_SIZE = 20
DAILY_CAP = 600

def stamp(value):
    try:
        d = datetime.fromisoformat(value.replace('Z','+00:00'))
        return d if d.tzinfo else None
    except (AttributeError, ValueError):
        return None

def due(watch, live, now):
    fixtures = live.get('fixtures') or {}
    limited = (live.get('status') == 'provider_rate_limit' and
               (live.get('diagnostic') or {}).get('limit_scope') != 'daily')
    candidates = {}
    for f in watch.get('fixtures', []):
        fid, kickoff = str(f.get('fixture_id','')), stamp(f.get('kickoff'))
        if not fid.isascii() or not fid.isdecimal() or not kickoff:
            continue
        if not now-timedelta(hours=72) < kickoff <= now:
            continue
        row = fixtures.get(fid, {})
        if row.get('status') in ADMIN:
            continue
        complete = row.get('status') in FINISHED and all(
            type((row.get(part) or {}).get(side)) is int
            for part in ('halftime','fulltime') for side in ('home','away'))
        observed = stamp(row.get('observed_at'))
        active = now - kickoff <= timedelta(hours=3)
        freshness = timedelta(minutes=2 if limited and active else 4)
        if not complete and (not observed or now - observed >= freshness):
            candidates[fid] = (0 if active else 1,
                observed or datetime.min.replace(tzinfo=timezone.utc), kickoff)
    # The Worker retains watched fixtures when the published list rolls over.
    health = live.get('health') or {}
    for fid in health.get('stale_live_fixture_ids', []):
        if str(fid).isascii() and str(fid).isdecimal():
            observed = stamp(fixtures.get(str(fid), {}).get('observed_at'))
            candidates[str(fid)] = (-1, observed or datetime.min.replace(tzinfo=timezone.utc), now)
    for fid in health.get('overdue_fixture_ids', []):
        if str(fid).isascii() and str(fid).isdecimal():
            observed = stamp(fixtures.get(str(fid),{}).get('observed_at'))
            candidates.setdefault(str(fid), (1, observed or datetime.min.replace(tzinfo=timezone.utc), now-timedelta(hours=72)))
    limit = BATCH_SIZE * (2 if limited else 1)
    return sorted(candidates, key=lambda fid:(candidates[fid],fid))[:limit]

def allowed(state, now):
    last = stamp(state.get('attempted_at'))
    retry = stamp(state.get('retry_after'))
    return (not last or now-last >= timedelta(minutes=4)) and (
        not retry or retry <= now) and (
        state.get('day') != now.date().isoformat() or state.get('calls',DAILY_CAP) <= DAILY_CAP - 2)


def retry_at(headers, now):
    value = (headers.get('Retry-After') or '').strip()
    try:
        result = now + timedelta(seconds=int(value)) if value.isascii() and value.isdecimal() else parsedate_to_datetime(value)
        return result if result.tzinfo and result > now else None
    except (TypeError, ValueError, OverflowError):
        return None

def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    now = datetime.now(timezone.utc)
    def read(url, headers=None):
        with urlopen(Request(url,headers=headers or {}),timeout=15) as response:
            return json.load(response), response.headers
    watch = json.loads(Path('data/live_watchlist.json').read_text())
    def public_feed():
        public = subprocess.run(('curl','-fsS','--max-time','15','-H','Accept: application/json',
            'https://arabsniperbet.com/api/live-scores'),check=True,capture_output=True)
        return json.loads(public.stdout)
    live = public_feed()
    ids = due(watch, live, now)
    if not ids and live.get('status') == 'provider_rate_limit':
        retry = stamp((live.get('health') or {}).get('retry_after'))
        if retry and now < retry <= now + timedelta(seconds=60):
            time.sleep((retry - now).total_seconds() + 5)
            now = datetime.now(timezone.utc)
            live = public_feed()
            ids = due(watch, live, now)
    if (live.get('diagnostic') or {}).get('limit_scope') == 'daily':
        print('Official daily quota exhausted; zero provider calls.')
        return
    if not ids:
        print('No missing or stale tracked results; zero provider calls.')
        return
    storage = boto3.client('s3',region_name='auto',
        endpoint_url='https://'+os.environ['R2_ACCOUNT_ID']+'.r2.cloudflarestorage.com',
        aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
        config=Config(connect_timeout=5,read_timeout=15,retries={'total_max_attempts':2}))
    bucket = os.environ['R2_BUCKET']
    try:
        state = json.loads(storage.get_object(Bucket=bucket,Key=KEY)['Body'].read())
    except ClientError as error:
        if error.response['Error']['Code'] not in ('NoSuchKey','404'):
            raise
        state = {}
    if not allowed(state, now):
        print('Relay cooldown or 600-request daily cap; zero provider calls.')
        return
    if state.get('day') != now.date().isoformat():
        state.update(day=now.date().isoformat(),calls=0)
    state['attempted_at'] = now.isoformat()
    # The relay may be released before the compatible Worker during a rollout.
    if len(ids) > BATCH_SIZE and live.get('health', {}).get('relay_fixture_capacity') != BATCH_SIZE * 2:
        ids = ids[:BATCH_SIZE]
    def save():
        storage.put_object(Bucket=bucket,Key=KEY,Body=json.dumps(state,separators=(',',':')).encode(),
                           ContentType='application/json')
    def defer(headers, *, limited=False):
        checked = datetime.now(timezone.utc)
        retry = retry_at(headers, checked)
        if limited:
            retry = max(retry or checked, checked + timedelta(minutes=4))
        if retry:
            state['retry_after'] = retry.isoformat()
            save()
        return retry is not None
    def provider(path):
        state['calls'] += 1
        save()  # Failed calls count too; no automatic provider retries.
        try:
            value, headers = read('https://v3.football.api-sports.io/'+path,
                {'x-apisports-key':os.environ['API_SPORTS_KEY'],'Cache-Control':'no-cache'})
        except HTTPError as error:
            if error.code == 429:
                defer(error.headers, limited=True)
            raise ValueError(f'Official provider HTTP {error.code}') from None
        if value.get('errors'):
            defer(headers, limited='rateLimit' in value['errors'])
            raise ValueError('Provider rejected bounded relay request')
        return value['response'], headers
    quota, headers = provider('status')
    usage = quota.get('requests') or {}
    used, limit = usage.get('current'), usage.get('limit_day')
    if quota.get('subscription',{}).get('active') is not True or not all(type(x) is int for x in (used,limit)):
        raise ValueError('Provider quota unavailable')
    reserve = max(1000, (limit + 9) // 10)
    rows = []
    captured = None
    for offset in range(0, len(ids), BATCH_SIZE):
        minute = headers.get('x-ratelimit-remaining') or ''
        if (defer(headers) or limit - used <= reserve or (minute.isdecimal() and int(minute) <= 2)
                or state['calls'] >= DAILY_CAP):
            print('Official reserve or cooldown reached; remaining fixtures deferred.')
            break
        batch = ids[offset:offset+BATCH_SIZE]
        time.sleep(.25)
        received, headers = provider('fixtures?'+urlencode({'ids':'-'.join(batch),'timezone':'Europe/Rome'}))
        if not isinstance(received, list) or len(received) > BATCH_SIZE:
            raise ValueError('Invalid official response')
        daily = headers.get('x-ratelimit-requests-remaining') or ''
        used = max(used + 1, limit - int(daily)) if daily.isdecimal() else used + 1
        # Keep the first batch if the next one is rejected. The oldest capture sets freshness.
        captured = captured or datetime.now(timezone.utc).isoformat()
        rows.extend({k:r.get(k) for k in ('fixture','goals','score')} for r in received
                    if isinstance(r, dict) and str((r.get('fixture') or {}).get('id')) in batch)
        state.update(schema_version=1,source='API-Football',captured_at=captured,fixtures=rows)
        save()
    if not captured:
        return
    print(json.dumps({'relay_saved':len(rows),'fixture_ids':[r['fixture']['id'] for r in rows],
                      'calls_today':state['calls'],'captured_at':state['captured_at']}))

if __name__ == '__main__':
    run()
