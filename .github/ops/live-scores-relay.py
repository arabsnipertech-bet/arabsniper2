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
LIVE = {'1H','HT','2H','ET','BT','P','LIVE'}
SCHEDULED = {'NS','TBD'}
BATCH_SIZE = 20
DAILY_CAP = 600


class ProviderThrottled(Exception):
    """A deferred attempt with the provider cooldown already recorded."""


def stamp(value):
    try:
        d = datetime.fromisoformat(value.replace('Z','+00:00'))
        return d if d.tzinfo else None
    except (AttributeError, ValueError):
        return None

def temporarily_limited(live):
    return (live.get('status') in ('provider_rate_limit', 'minute_quota_low') and
            (live.get('diagnostic') or {}).get('limit_scope') != 'daily')


def needs_midpoint(watch, live, now):
    if not temporarily_limited(live):
        return False
    fixtures = live.get('fixtures') or {}
    health = live.get('health') or {}
    if health.get('stale_live_fixtures') or health.get('live_tracking_fixture_ids'):
        return True
    for f in watch.get('fixtures', []):
        kickoff = stamp(f.get('kickoff'))
        if kickoff is None or not now - timedelta(hours=3) < kickoff <= now:
            continue
        status = (fixtures.get(str(f.get('fixture_id'))) or {}).get('status')
        if status in LIVE or status in (None, 'NS', 'TBD') and now - kickoff < timedelta(minutes=15):
            return True
    return False


def has_score(pair):
    pair = pair or {}
    return all(type(pair.get(side)) is int and pair[side] >= 0 for side in ('home', 'away'))


def is_fresh(row, now, interval):
    observed = stamp(row.get('observed_at'))
    return observed is not None and not row.get('refresh_failed') and (
        -timedelta(seconds=30) <= now - observed < interval
    )


def scheduled_retry(age):
    if age >= timedelta(days=1):
        return timedelta(hours=2)
    return timedelta(hours=1) if age >= timedelta(hours=3) else timedelta(minutes=15)


def due(watch, live, now):
    fixtures = live.get('fixtures') or {}
    limited = temporarily_limited(live)
    health = live.get('health') or {}
    stale = set(health.get('stale_live_fixture_ids', []))
    tracked = set(health.get('live_tracking_fixture_ids', []))
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
        status = row.get('status')
        final = status in FINISHED and (
            has_score(row.get('fulltime')) or status == 'FT' and has_score(row.get('goals')))
        if final and has_score(row.get('halftime')):
            continue
        observed = stamp(row.get('observed_at'))
        age = now - kickoff
        active = fid in tracked or age <= timedelta(hours=3)
        if final:
            freshness = timedelta(minutes=30 if age < timedelta(days=1) else 120)
        elif status in SCHEDULED and age >= timedelta(minutes=15):
            freshness = scheduled_retry(age)
        else:
            freshness = timedelta(minutes=4)
        if fid in stale or not is_fresh(row, now, freshness):
            candidates[fid] = (2 if final else 0 if active else 1,
                observed or datetime.min.replace(tzinfo=timezone.utc), kickoff)
    # The Worker retains watched fixtures when the published list rolls over.
    for fid in stale:
        if str(fid).isascii() and str(fid).isdecimal():
            observed = stamp(fixtures.get(str(fid), {}).get('observed_at'))
            candidates[str(fid)] = (-1, observed or datetime.min.replace(tzinfo=timezone.utc), now)
    for fid in health.get('overdue_fixture_ids', []):
        row = fixtures.get(str(fid), {})
        interval = timedelta(hours=2) if row.get('status') in SCHEDULED else timedelta(minutes=4)
        if str(fid).isascii() and str(fid).isdecimal() and not is_fresh(row, now, interval):
            observed = stamp(row.get('observed_at'))
            candidates.setdefault(str(fid), (1, observed or datetime.min.replace(tzinfo=timezone.utc), now-timedelta(hours=72)))
    for fid in health.get('provider_pending_fixture_ids', []):
        row = fixtures.get(str(fid), {})
        if row.get('status') in SCHEDULED and str(fid).isascii() and str(fid).isdecimal() and not is_fresh(
            row, now, timedelta(hours=1)
        ):
            observed = stamp(row.get('observed_at'))
            candidates.setdefault(str(fid), (1, observed or datetime.min.replace(tzinfo=timezone.utc), now))
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
    if (os.environ.get('GITHUB_EVENT_NAME') == 'workflow_run' and os.environ.get('GITHUB_OUTPUT')
            and needs_midpoint(watch, live, now)):
        with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as out:
            out.write('live_followup=true\n')
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
        print('No tracked results are due for recovery; zero provider calls.')
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
                raise ProviderThrottled from None
            raise ValueError(f'Official provider HTTP {error.code}') from None
        if value.get('errors'):
            limited = 'rateLimit' in value['errors']
            defer(headers, limited=limited)
            if limited:
                raise ProviderThrottled
            raise ValueError('Provider rejected bounded relay request')
        return value['response'], headers
    try:
        quota, headers = provider('status')
    except ProviderThrottled:
        print('::warning title=Gold score recovery::API-Football limited the quota check. Recovery deferred.')
        return
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
        try:
            received, headers = provider('fixtures?'+urlencode({'ids':'-'.join(batch),'timezone':'Europe/Rome'}))
        except ProviderThrottled:
            print('::warning title=Gold score recovery::API-Football limited the fixture request. Recovery deferred.')
            break
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
