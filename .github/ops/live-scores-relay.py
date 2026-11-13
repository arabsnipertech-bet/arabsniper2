"""Official-score recovery through the existing private R2 bucket, at most two API calls/run."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen
from urllib.parse import urlencode

KEY = 'live-scores/provider-relay.json'
FINISHED = {'FT','AET','PEN'}
ADMIN = {'PST','CANC','ABD','AWD','WO'}

def stamp(value):
    try:
        d = datetime.fromisoformat(value.replace('Z','+00:00'))
        return d if d.tzinfo else None
    except (AttributeError, ValueError):
        return None

def due(watch, live, now):
    fixtures = live.get('fixtures') or {}
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
        if not complete and (not observed or now-observed >= timedelta(minutes=4)):
            candidates[fid] = kickoff
    # Overdue IDs come from the retained DO watchlist, which also spans midnight.
    for fid in live.get('health',{}).get('overdue_fixture_ids',[]):
        if str(fid).isascii() and str(fid).isdecimal():
            candidates.setdefault(str(fid), now-timedelta(hours=72))
    return sorted(candidates, key=lambda fid:(candidates[fid],fid))[:20]

def allowed(state, now):
    last = stamp(state.get('attempted_at'))
    return (not last or now-last >= timedelta(minutes=4)) and (
        state.get('day') != now.date().isoformat() or state.get('calls',600) <= 598)

def run():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    now = datetime.now(timezone.utc)
    def read(url, headers=None):
        with urlopen(Request(url,headers=headers or {}),timeout=15) as response:
            return json.load(response), response.headers
    live, _ = read('https://arabsniperbet.com/api/live-scores', {'Cache-Control':'no-cache'})
    watch = json.loads(Path('data/live_watchlist.json').read_text())
    ids = due(watch, live, now)
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
    def save():
        storage.put_object(Bucket=bucket,Key=KEY,Body=json.dumps(state,separators=(',',':')).encode(),
                           ContentType='application/json')
    def provider(path):
        state['calls'] += 1
        save()  # Failed calls count too; no automatic provider retries.
        value, headers = read('https://v3.football.api-sports.io/'+path,
            {'x-apisports-key':os.environ['API_SPORTS_KEY'],'Cache-Control':'no-cache'})
        if value.get('errors'):
            raise ValueError('Provider rejected bounded relay request')
        return value['response'], headers
    quota, headers = provider('status')
    usage = quota.get('requests') or {}
    used, limit = usage.get('current'), usage.get('limit_day')
    if quota.get('subscription',{}).get('active') is not True or not all(type(x) is int for x in (used,limit)):
        raise ValueError('Provider quota unavailable')
    if limit-used <= max(1000,(limit+9)//10) or headers.get('x-ratelimit-remaining') in ('0','1','2'):
        print('Official reserve reached; fixture request deferred.')
        return
    time.sleep(.25)
    rows, _ = provider('fixtures?'+urlencode({'ids':'-'.join(ids),'timezone':'Europe/Rome'}))
    if not isinstance(rows,list) or len(rows)>20:
        raise ValueError('Invalid official response')
    # Store only match state, never account identity or credentials.
    rows = [{k:r.get(k) for k in ('fixture','goals','score')} for r in rows
            if str(r.get('fixture',{}).get('id')) in ids]
    state.update(schema_version=1,source='API-Football',captured_at=datetime.now(timezone.utc).isoformat(),fixtures=rows)
    save()
    print(json.dumps({'relay_saved':len(rows),'fixture_ids':[r['fixture']['id'] for r in rows],
                      'calls_today':state['calls'],'captured_at':state['captured_at']}))

if __name__ == '__main__':
    run()
