"""Read-only score health and reconciliation for the existing Actions notifications."""
import argparse
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
import subprocess
from time import monotonic, sleep
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from gold_storage import client
from v7_archive import FINISHED, checked_score
from lateral_matrix import settle_spec

LIVE = 'https://arabsniperbet.com/api/live-scores'
FOOTBALL = 'https://v3.football.api-sports.io/'
ROME = ZoneInfo('Europe/Rome')
MAX_AGE = timedelta(minutes=8)
RATE_LIMIT_GRACE = timedelta(minutes=5)


def stamp(value):
    try:
        date = datetime.fromisoformat(value)
        return date if date.tzinfo else None
    except (TypeError, ValueError):
        return None


def get_json(url):
    result = subprocess.run(('curl', '-fsS', '--max-time', '12', '-H', 'Accept: application/json', url),
                            check=True, capture_output=True)
    return json.loads(result.stdout)


def recent_audit(day):
    s3, bucket = client(), os.environ['R2_BUCKET']
    with s3.get_object(Bucket=bucket, Key='state/current.json')['Body'] as stream:
        index = json.load(stream)['files']
    entry = index.get(f'auditarchive/prematch_{day}_v7.json')
    if not entry:
        return {}
    digest = entry['sha256']
    with s3.get_object(Bucket=bucket, Key='state/objects/' + digest)['Body'] as stream:
        raw = stream.read()
    if len(raw) != entry['bytes'] or sha256(raw).hexdigest() != digest:
        raise ValueError('Auditor checkpoint checksum mismatch')
    data = json.loads(raw)
    if not isinstance(data.get('fixtures'), dict):
        raise ValueError('Auditor result index cannot be reconciled')
    return data['fixtures']


def score(row):
    home, away = (row.get('fulltime') or {}).get('home'), (row.get('fulltime') or {}).get('away')
    if row.get('status') == 'FT' and (home is None or away is None):
        home, away = (row.get('goals') or {}).get('home'), (row.get('goals') or {}).get('away')
    ht = row.get('halftime') or {}
    return checked_score({'status': row.get('status'), 'home_ft': home, 'away_ft': away,
                          'home_ht': ht.get('home'), 'away_ht': ht.get('away')})


def reconcile(audit, fixtures):
    differences = []
    for fid, evidence in audit.items():
        row = fixtures.get(fid)
        if not row or row.get('status') not in FINISHED:
            continue
        result = score(row)
        if None in (result['home_ft'], result['away_ft']):
            continue
        audited = evidence.get('result') or {}
        if evidence.get('status') == 'settled':
            if any(result[k] != audited.get(k) for k in ('status', 'home_ft', 'away_ft', 'home_ht', 'away_ht')
                   if result[k] is not None):
                differences.append({'fixture_id': fid, 'kind': 'score_mismatch'})
                continue
        elif evidence.get('status') != 'settled':
            differences.append({'fixture_id': fid, 'kind': 'auditor_pending'})
            continue
        for role, pick in (evidence.get('triad') or {}).items():
            market = (pick or {}).get('market_id')
            if not market:
                continue
            expected = settle_spec(json.loads(market), result)
            saved = (evidence.get('outcomes') or {}).get(market)
            if expected is not None and (not saved or (saved.get('win'), saved.get('loss')) != expected):
                differences.append({'fixture_id': fid, 'kind': 'outcome_mismatch', 'role': role})
    return differences


def inspect_live(now):
    live = get_json(LIVE)
    if live.get('enabled') is not True or live.get('schema_version') != 1 or not isinstance(live.get('fixtures'), dict):
        raise ValueError('Invalid public score feed')
    health = live.get('health') or {}
    cron = stamp(health.get('last_cron_at'))
    last_poll = stamp(health.get('last_successful_poll_at'))
    reasons = []
    if not cron or now - cron > MAX_AGE:
        reasons.append('Cron Trigger missing or stale')
    if live.get('status') not in ('ok', 'idle'):
        reasons.append('Collector state: ' + str(live.get('status')))
    if live.get('status') == 'ok' and (not last_poll or now - last_poll > timedelta(minutes=12)):
        reasons.append('No recent successful provider poll')
    if health.get('overdue_fixtures'):
        reasons.append('Tracked fixtures overdue without a confirmed result')
    if health.get('stale_live_fixtures'):
        reasons.append('Tracked live scores missing or older than five minutes')
    summary = {'status': live.get('status'), 'checked_at': now.isoformat(),
               'health': health, 'diagnostic': live.get('diagnostic')}
    return live, summary, reasons


def short_rate_limit_retry(summary, reasons, now):
    if summary['status'] != 'provider_rate_limit' or len(reasons) != 1:
        return None
    health = summary['health']
    diagnostic = summary['diagnostic'] or {}
    retry_at = stamp(health.get('retry_after'))
    if (
        diagnostic.get('limit_scope') != 'daily'
        and health.get('overdue_fixtures') == 0
        and health.get('missing_halftime') == 0
        and not health.get('watchlist_status')
        and retry_at is not None
        and retry_at <= now + RATE_LIMIT_GRACE
    ):
        return retry_at
    return None


def inspect_with_rate_limit_recheck(now):
    live, summary, reasons = inspect_live(now)
    retry_at = short_rate_limit_retry(summary, reasons, now)
    if retry_at is None:
        return live, summary, reasons

    initial = {'checked_at': summary['checked_at'], 'retry_after': summary['health']['retry_after'],
               'diagnostic': summary['diagnostic']}
    print('Provider rate limit detected. Rechecking the stored feed for up to five minutes.', flush=True)
    deadline = monotonic() + RATE_LIMIT_GRACE.total_seconds()
    while retry_at is not None and (remaining := deadline - monotonic()) > 0:
        after_retry = (retry_at - now).total_seconds() + 5
        sleep(min(max(30, after_retry), remaining))
        now = datetime.now(timezone.utc)
        live, summary, reasons = inspect_live(now)
        retry_at = short_rate_limit_retry(summary, reasons, now)

    summary['rate_limit_recheck'] = {**initial, 'cleared': not reasons}
    if not reasons:
        print('::warning title=Gold live scores::The provider rate limit cleared during verification.', flush=True)
    return live, summary, reasons


def provider_results(ids):
    def fetch(path):
        with urlopen(Request(FOOTBALL + path, headers={'x-apisports-key': os.environ['API_SPORTS_KEY']}), timeout=15) as response:
            data = json.load(response)
            if data.get('errors'):
                raise ValueError('API-Football rejected the official verification request')
            return data['response'], response.headers
    status, headers = fetch('status')
    usage = status.get('requests') or {}
    used, limit = usage.get('current'), usage.get('limit_day')
    if status.get('subscription', {}).get('active') is not True or not all(type(x) is int for x in (used, limit)):
        raise ValueError('Unable to confirm API-Football quota')
    if limit - used <= max(1000, (limit + 9) // 10) or headers.get('x-ratelimit-remaining') in ('0', '1', '2'):
        raise ValueError('Official verification deferred due to API-Football quota')
    rows, _ = fetch('fixtures?' + urlencode({'ids': '-'.join(ids), 'timezone': 'Europe/Rome'}))
    return {str(row['fixture']['id']): score({'status': row['fixture']['status']['short'],
             'goals': row.get('goals'), **(row.get('score') or {})}) for row in rows
            if str(row.get('fixture', {}).get('id')) in ids}


def run(ids, compare_auditor):
    now = datetime.now(timezone.utc)
    inspect = inspect_live if ids else inspect_with_rate_limit_recheck
    live, summary, reasons = inspect(now)
    fixtures = live['fixtures']
    if compare_auditor:
        summary['auditor_days'] = []
        recovery_dates = []
        for age in (2, 1):
            day = now.astimezone(ROME).date() - timedelta(days=age)
            audit = recent_audit(day)
            differences = reconcile(audit, fixtures)
            summary['auditor_days'].append({'date': str(day), 'available': bool(audit),
                'differences': len(differences), 'sample': differences[:20]})
            if differences:
                recovery_dates.append(str(day))
        if recovery_dates:
            reasons.append('Discrepancies between acquired scores and Auditor certification')
            if os.environ.get('GITHUB_OUTPUT'):
                with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as out:
                    out.write('recovery_date=' + recovery_dates[0] + '\n')
    if ids:
        official = provider_results(ids)
        summary['verification'] = {fid: {'provider': official.get(fid), 'worker': score(fixtures[fid]) if fid in fixtures else None}
                                   for fid in ids}
        if any(not v['provider'] or v['provider']['status'] not in FINISHED or
               None in (v['provider']['home_ft'], v['provider']['away_ft']) or v['provider'] != v['worker']
               for v in summary['verification'].values()):
            reasons.append('Official fixture results have not all been confirmed and delivered by the Worker')
    message = json.dumps(summary, indent=2)
    print(message, flush=True)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as out:
        out.write('## Gold live results\n\n```json\n' + message + '\n```\n')
        for reason in reasons:
            out.write(f'\n- {reason}\n')
    if reasons:
        raise SystemExit(' / '.join(reasons))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fixtures', default='')
    parser.add_argument('--compare-auditor', action='store_true')
    args = parser.parse_args()
    ids = list(dict.fromkeys(args.fixtures.split(','))) if args.fixtures else []
    if len(ids) > 20 or any(not fid.isascii() or not fid.isdecimal() or not 1 <= len(fid) <= 16 for fid in ids):
        parser.error('Provide up to 20 comma-separated fixture IDs.')
    try:
        run(ids, args.compare_auditor)
    except Exception as error:
        raise SystemExit(f'Gold live results verification failed: {type(error).__name__}') from None
