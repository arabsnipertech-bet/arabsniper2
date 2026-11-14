"""Verify live score delivery, report provider coverage and reconcile the Auditor."""
import argparse
from contextlib import closing
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
RELAY_CRON_GRACE_SECONDS = 135
RELAY_POLL_SECONDS = 15
LIVE_FRESHNESS = timedelta(minutes=5)
LIVE_PHASES = {'1H', 'HT', '2H', 'ET', 'BT', 'P', 'LIVE'}
THROTTLED = {'provider_rate_limit', 'minute_quota_low'}
INCIDENT_KEY = 'live-scores/verification-incidents.json'
FEED_ERROR = 'Public score feed unavailable or invalid'
INVALID_FEED = 'Invalid public score feed'
INVALID_COUNTERS = 'Invalid score feed health counters'
OVERDUE_FAULT = 'Tracked fixtures overdue without a confirmed result'
STALE_FAULT = 'Tracked live scores missing or older than five minutes'
SCORE_FAULT_IDS = {OVERDUE_FAULT: 'overdue_fixture_ids', STALE_FAULT: 'stale_live_fixture_ids'}


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


def recent_audit(day, s3, bucket):
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
    try:
        live = get_json(LIVE)
    except (subprocess.SubprocessError, ValueError) as error:
        raise ValueError(FEED_ERROR) from error
    if (not isinstance(live, dict) or live.get('enabled') is not True
            or live.get('schema_version') != 1 or not isinstance(live.get('fixtures'), dict)):
        raise ValueError(INVALID_FEED)
    health = live.get('health') or {}
    for count in ('overdue_fixtures', 'stale_live_fixtures', 'missing_halftime'):
        if type(health.get(count)) is not int or health[count] < 0:
            raise ValueError(INVALID_COUNTERS)
    cron = stamp(health.get('last_cron_at'))
    last_poll = stamp(health.get('last_successful_poll_at'))
    reasons = []
    if not cron or not -timedelta(seconds=30) <= now - cron <= MAX_AGE:
        reasons.append('Cron Trigger missing or stale')
    status = live.get('status')
    if status not in ('ok', 'idle'):
        reasons.append(f'Collector state: {status}')
    watch_state = health.get('watchlist_status')
    if watch_state and watch_state != status:
        reasons.append(f'Fixture watchlist: {watch_state}')
    if status == 'ok' and (not last_poll or now - last_poll > timedelta(minutes=12)):
        reasons.append('No recent successful provider poll')
    if health.get('overdue_fixtures'):
        reasons.append(OVERDUE_FAULT)
    if health.get('stale_live_fixtures'):
        reasons.append(STALE_FAULT)
    summary = {'status': live.get('status'), 'checked_at': now.isoformat(),
               'health': health, 'diagnostic': live.get('diagnostic')}
    return live, summary, reasons


def temporary_limit(summary):
    return summary['status'] in THROTTLED and (summary['diagnostic'] or {}).get('limit_scope') != 'daily'


def collector_only(summary, reasons):
    return reasons == [f"Collector state: {summary['status']}"]


def short_rate_limit_retry(summary, reasons, now):
    if not temporary_limit(summary) or not collector_only(summary, reasons):
        return None
    retry_at = stamp(summary['health'].get('retry_after'))
    if retry_at is not None and retry_at <= now + RATE_LIMIT_GRACE:
        return retry_at
    return None


def current_delivery(live, summary, reasons, now):
    if not temporary_limit(summary) or not collector_only(summary, reasons):
        return None
    health = summary['health']
    live_due = health.get('live_tracking_fixtures')
    tracked_ids = health.get('live_tracking_fixture_ids')
    earliest = now - LIVE_FRESHNESS
    latest = now + timedelta(seconds=30)
    covered = set()
    active_scores = 0
    direct_fresh = relay_fresh = False
    for fid, row in live['fixtures'].items():
        observed = stamp(row.get('observed_at'))
        fresh = observed is not None and earliest <= observed <= latest and not row.get('refresh_failed')
        active = row.get('status') in LIVE_PHASES
        pair = row.get('goals') or {}
        live_score = active and all(type(pair.get(side)) is int and pair[side] >= 0 for side in ('home', 'away'))
        if active:
            active_scores += 1
            if not fresh or not live_score:
                return None
        if fresh and row.get('source') == 'API-Football':
            if active or row.get('status') in ('NS', 'TBD'):
                covered.add(fid)
            if row.get('transport') == 'GitHub Actions/R2':
                relay_fresh = True
            elif active:
                direct_fresh = True
    if type(live_due) is int:
        if live_due == active_scores == 0:
            return 'idle'
        if tracked_ids is not None:
            if not isinstance(tracked_ids, list) or len(tracked_ids) != live_due or any(
                    not isinstance(fid, str) or fid not in covered for fid in tracked_ids):
                return None
        elif live_due > len(covered):
            return None
    if relay_fresh and not (health.get('relay') or {}).get('error'):
        return 'relay'
    if direct_fresh:
        return 'direct'
    return None


def waiting_for_relay(summary, finished_at, now):
    cron = stamp(summary['health'].get('last_cron_at'))
    return (
        finished_at is not None
        and temporary_limit(summary)
        and cron is not None
        and now - MAX_AGE <= cron < finished_at <= now
    )


def inspect_with_rate_limit_recheck(now, relay_finished_at=None):
    live, summary, reasons = inspect_live(now)
    initial = {'checked_at': summary['checked_at'], 'retry_after': summary['health'].get('retry_after'),
               'diagnostic': summary['diagnostic']}
    started = monotonic()
    rechecked = False
    while temporary_limit(summary):
        delivery = current_delivery(live, summary, reasons, now)
        if delivery:
            summary['delivery_during_provider_limit'] = delivery
            reasons = []
            detail = {
                'idle': 'No live or overdue score updates are due.',
                'relay': 'The relay has delivered recent official scores.',
                'direct': 'The live feed still holds recent official scores.',
            }[delivery]
            print(f'::warning title=Gold live scores::API-Football limited the direct collector. {detail}', flush=True)
            break
        elapsed = monotonic() - started
        remaining = RATE_LIMIT_GRACE.total_seconds() - elapsed
        if remaining <= 0:
            break
        retry_at = short_rate_limit_retry(summary, reasons, now)
        if elapsed < RELAY_CRON_GRACE_SECONDS and waiting_for_relay(summary, relay_finished_at, now):
            delay = min(RELAY_POLL_SECONDS, RELAY_CRON_GRACE_SECONDS - elapsed)
        elif retry_at is not None:
            delay = max(30, (retry_at - now).total_seconds() + 5)
        else:
            break
        if not rechecked:
            print('Provider rate limit detected. Rechecking the public feed for up to five minutes.', flush=True)
        sleep(min(delay, remaining))
        rechecked = True
        now = datetime.now(timezone.utc)
        live, summary, reasons = inspect_live(now)

    if rechecked:
        cleared = not reasons and summary['status'] not in THROTTLED
        summary['rate_limit_recheck'] = {**initial, 'cleared': cleared}
        if cleared:
            print('::warning title=Gold live scores::The provider rate limit cleared during verification.', flush=True)
    return live, summary, reasons


def report_halftime_coverage(live, summary):
    count = summary['health']['missing_halftime']
    if not count:
        return
    missing = []
    for fid, row in live['fixtures'].items():
        if row.get('status') not in FINISHED:
            continue
        result = score(row)
        if None not in (result['home_ft'], result['away_ft']) and None in (result['home_ht'], result['away_ht']):
            missing.append(fid)
            if len(missing) == 20:
                break
    summary['missing_halftime_fixture_ids'] = missing
    print(f'::warning title=Gold halftime coverage::{count} finished fixtures have no confirmed halftime score. '
          'Final results are available. Markets that require halftime remain unverified.', flush=True)


def report_provider_pending(summary):
    count = summary['health'].get('provider_pending_fixtures', 0)
    if count:
        print(f'::warning title=Gold outstanding results::{count} fixtures still have no official final. '
              'The provider reports that they are scheduled or still in play.', flush=True)


def update_incidents(s3, bucket, reasons, summary):
    """Alert on new faults once and rearm them when the feed recovers."""
    from botocore.exceptions import ClientError

    if SCORE_FAULT_IDS.keys() & set(reasons):
        collector_variants = {f'Collector state: {status}' for status in THROTTLED | {'results_pending'}}
        if (summary.get('diagnostic') or {}).get('limit_scope') == 'daily':
            collector_variants.discard('Collector state: provider_rate_limit')
        reasons = [reason for reason in reasons if reason not in collector_variants]
    incidents = {}
    for reason in reasons:
        fixture_ids = summary['health'].get(SCORE_FAULT_IDS.get(reason), [])
        if fixture_ids:
            for fixture_id in fixture_ids:
                incidents[f'{reason} ({fixture_id})'] = reason
        else:
            incidents[reason] = reason
    try:
        with closing(s3.get_object(Bucket=bucket, Key=INCIDENT_KEY)['Body']) as body:
            saved = json.load(body)
    except ClientError as error:
        if error.response['Error']['Code'] not in ('NoSuchKey', '404'):
            raise
        saved = []
    if not isinstance(saved, list) or any(not isinstance(reason, str) for reason in saved):
        raise ValueError('Invalid live verification incident state')
    active = sorted(incidents)
    if active != saved:
        s3.put_object(Bucket=bucket, Key=INCIDENT_KEY, Body=json.dumps(active).encode(),
                      ContentType='application/json')
    return list(dict.fromkeys(reason for key, reason in incidents.items() if key not in saved))


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


def run(ids, compare_auditor, *, track_incidents=False):
    now = datetime.now(timezone.utc)
    try:
        if ids:
            live, summary, reasons = inspect_live(now)
        else:
            live, summary, reasons = inspect_with_rate_limit_recheck(now, stamp(os.environ.get('RELAY_FINISHED_AT')))
    except ValueError as error:
        if str(error) not in (FEED_ERROR, INVALID_FEED, INVALID_COUNTERS):
            raise
        live = {'fixtures': {}}
        summary = {'status': 'unavailable', 'checked_at': now.isoformat(), 'health': {}}
        reasons = [str(error)]
    if summary['health']:
        report_halftime_coverage(live, summary)
        report_provider_pending(summary)
    fixtures = live['fixtures']
    storage = client() if compare_auditor or track_incidents else None
    bucket = os.environ['R2_BUCKET'] if storage else None
    if compare_auditor and summary['health']:
        summary['auditor_days'] = []
        recovery_dates = []
        for age in (2, 1):
            day = now.astimezone(ROME).date() - timedelta(days=age)
            audit = recent_audit(day, storage, bucket)
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
    failures = update_incidents(storage, bucket, reasons, summary) if track_incidents else reasons
    note = ''
    if reasons:
        summary['verification_state'] = 'failed' if failures else 'known_incident'
        summary['faults'] = reasons
        if not failures:
            note = 'This incident is still open. A failure notification was already recorded.'
            print('::warning title=Gold live scores::This incident is still open. '
                  'A failure notification was already recorded.', flush=True)
    else:
        summary['verification_state'] = 'ok'
    message = json.dumps(summary, indent=2)
    print(message, flush=True)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as out:
        out.write('## Gold live results\n\n```json\n' + message + '\n```\n')
        for reason in reasons:
            out.write(f'\n- {reason}\n')
        if note:
            out.write(f'\n{note}\n')
    if failures:
        raise SystemExit(' / '.join(failures))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--fixtures', default='')
    parser.add_argument('--compare-auditor', action='store_true')
    parser.add_argument('--track-incidents', action='store_true')
    args = parser.parse_args()
    ids = list(dict.fromkeys(args.fixtures.split(','))) if args.fixtures else []
    if len(ids) > 20 or any(not fid.isascii() or not fid.isdecimal() or not 1 <= len(fid) <= 16 for fid in ids):
        parser.error('Provide up to 20 comma-separated fixture IDs.')
    try:
        run(ids, args.compare_auditor, track_incidents=args.track_incidents and not ids)
    except Exception as error:
        raise SystemExit(f'Gold live results verification failed: {type(error).__name__}') from None
