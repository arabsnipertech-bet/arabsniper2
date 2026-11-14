"""Verify and release only the existing live scores Worker, with version rollback."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID

ACCOUNT = 'dfdb02b4139b31c0ab4456a50eb371a2'
WORKER = 'arabsniper-live-scores'
CONFIG = 'workers/live-scores/wrangler.jsonc'
API = f'https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/workers/scripts/{WORKER}'
PUBLIC = 'https://arabsniperbet.com/api/live-scores'
WRANGLER = ('npx', '--yes', 'wrangler@4.161.0')
CRON = '* * * * *'
LEGACY_CRON = '*/2 * * * *'
EXPECTED_VARS = {'LIVE_ENABLED': 'true', 'MAX_CALLS_PER_DAY': '1500', 'RESERVE_REQUESTS': '1000'}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def api(path, data=None):
    request = Request(API + path, data=json.dumps(data).encode() if data is not None else None,
                      method='PUT' if data is not None else 'GET',
                      headers={'Authorization': 'Bearer ' + os.environ['CLOUDFLARE_API_TOKEN'],
                               'Content-Type': 'application/json', 'Cache-Control': 'no-cache'})
    try:
        with build_opener(NoRedirect()).open(request, timeout=20) as response:
            body = json.load(response)
    except HTTPError as error:
        raise RuntimeError(f'Cloudflare {path}: HTTP {error.code}') from None
    if body.get('success') is not True:
        raise RuntimeError(f'Cloudflare did not confirm {path}')
    return body['result']


def version_id(value):
    identifier = UUID(value)
    if str(identifier) != value:
        raise ValueError('Invalid Cloudflare version ID')
    return value


def active_version():
    deployments = api('/deployments')['deployments']
    versions = deployments[0]['versions'] if deployments else []
    if len(versions) != 1 or versions[0]['percentage'] != 100:
        raise RuntimeError('Expected a single active version for deterministic rollback')
    return version_id(versions[0]['version_id'])


def verify_configuration(*, allow_previous=False):
    settings = api('/settings')
    bindings = {binding['name']: binding for binding in settings['bindings']}
    score = bindings.get('SCORES') or {}
    if (score.get('type') != 'durable_object_namespace' or score.get('class_name') != 'ScoreStore'
            or score.get('script_name') not in (None, '', WORKER)):
        raise RuntimeError('SCORES does not match the existing ScoreStore namespace')
    if (bindings.get('API_SPORTS_KEY') or {}).get('type') != 'secret_text':
        raise RuntimeError('API_SPORTS_KEY binding is missing')
    for key, expected in EXPECTED_VARS.items():
        if (bindings.get(key) or {}).get('text') != expected:
            raise RuntimeError(f'Runtime setting {key} differs from the release configuration')
    cron = api('/schedules')['schedules']
    if [row['cron'] for row in cron] not in ([[CRON], [LEGACY_CRON]] if allow_previous else [[CRON]]):
        raise RuntimeError('Unexpected live scores Cron Trigger configuration')
    config = json.loads(Path(CONFIG).read_text())
    if (config.get('name') != WORKER or config.get('vars') != EXPECTED_VARS
            or config.get('triggers', {}).get('crons') != [CRON]):
        raise RuntimeError('Unexpected Wrangler target or variables')


def wrangler(*arguments):
    result = subprocess.run((*WRANGLER, *arguments, '--config', CONFIG), check=True, text=True,
                            stdout=subprocess.PIPE, timeout=100)
    print(result.stdout, end='', flush=True)
    return result.stdout


def public_response():
    result = subprocess.run(('curl', '-fsS', '--max-time', '12', '-H', 'Accept: application/json',
                             '-i', PUBLIC), check=True, capture_output=True)
    headers, _, body = result.stdout.rpartition(b'\r\n\r\n')
    if not body:
        raise ValueError('Missing production response')
    live = json.loads(body)
    if live.get('schema_version') != 1 or live.get('enabled') is not True or not isinstance(live.get('fixtures'), dict):
        raise ValueError('The production route does not serve an enabled live score feed')
    version = re.search(rb'(?im)^x-live-scores-version:\s*([a-f0-9-]{36})\s*$', headers)
    return version.group(1).decode() if version else None, live


def same_script(first, second):
    if not first or not second:
        return False
    def identity(identifier):
        data = api('/versions/' + version_id(identifier))
        return (data.get('resources') or {}).get('script', {}).get('etag')
    expected_etag, served_etag = identity(first), identity(second)
    return bool(expected_etag and expected_etag == served_etag)


def wait_active(expected, equivalent=False):
    for attempt in range(7):
        current = active_version()
        if current == expected or equivalent and same_script(expected, current):
            return current
        if attempt < 6:
            sleep(5)
    raise RuntimeError('Cloudflare active deployment did not converge to the expected version')


def verify_runtime(expected, previous, timeout=250):
    until = monotonic() + timeout
    first_cron = None
    while monotonic() < until:
        try:
            served, data = public_response()
        except (subprocess.SubprocessError, ValueError):
            served, data = None, {}
        cron = (data.get('health') or {}).get('last_cron_at')
        if served and served != expected and same_script(expected, served):
            verify_configuration()
            if active_version() != served:
                sleep(5)
                continue
            print(f'Equivalent Worker script deployed as {served}; expected upload {expected}.', flush=True)
            expected = served
        if served == expected and cron:
            first_cron = first_cron or cron
            if cron > first_cron or previous and cron > previous:
                print(f'Version {served} served on the production route. Cron advanced to {cron}.', flush=True)
                print(json.dumps({'status': data.get('status'), 'health': data.get('health'),
                                  'fixtures': len(data['fixtures'])}), flush=True)
                return served
        sleep(10)
    raise RuntimeError('The production route or Cron advancement could not be verified')


def summary(message):
    print(message, flush=True)
    with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as out:
        out.write(message + '\n\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('inspect', 'deploy', 'rollback'))
    parser.add_argument('--version', default='')
    args = parser.parse_args()
    if os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_REPOSITORY') != 'arabsnipertech-bet/arabsniper2':
        raise SystemExit('Use main of arabsnipertech-bet/arabsniper2.')
    if not os.environ.get('CLOUDFLARE_API_TOKEN'):
        raise SystemExit('GitHub Secret CLOUDFLARE_LIVE_SCORES_API_TOKEN is missing.')
    os.environ['CLOUDFLARE_ACCOUNT_ID'] = ACCOUNT
    verify_configuration(allow_previous=True)
    previous_schedule = api('/schedules')['schedules']
    previous = active_version()
    _, before = public_response()
    summary(f'Active Worker version and rollback reference: `{previous}`')
    if args.action == 'inspect':
        return
    if args.action == 'rollback':
        target = version_id(args.version)
        wrangler('rollback', target, '--yes', '--message', 'Live scores operational rollback')
        if active_version() != target:
            raise RuntimeError('Cloudflare did not confirm the requested rollback version')
        public_response()
        summary(f'Rollback active: `{target}`')
        return
    commit = os.environ['GITHUB_SHA']
    if not re.fullmatch('[a-f0-9]{40}', commit):
        raise RuntimeError('Cannot establish source commit')
    output = wrangler('versions', 'upload', '--keep-vars', '--tag', commit[:12], '--message', commit)
    match = re.search(r'Worker Version ID:\s*([a-f0-9-]{36})', output)
    if not match:
        raise RuntimeError('Cloudflare upload version ID is unavailable. No traffic was changed.')
    target = version_id(match[1])
    if active_version() != previous:
        raise RuntimeError('An intervening deployment changed the rollback baseline')
    summary(f'Uploaded `{target}` from `{commit}`')
    try:
        wrangler('versions', 'deploy', target + '@100%', '--yes', '--message', commit)
        wait_active(target, equivalent=True)
        if [row['cron'] for row in previous_schedule] != [CRON]:
            api('/schedules', [{'cron': CRON}])
        verified = verify_runtime(target, (before.get('health') or {}).get('last_cron_at'))
        verify_configuration()
    except (RuntimeError, ValueError, subprocess.SubprocessError, URLError) as error:
        summary('Release verification failed: ' + str(error))
        current = active_version()
        if current == target:
            wrangler('rollback', previous, '--yes', '--message', 'Live score release verification failed')
            wait_active(previous)
            if [row['cron'] for row in previous_schedule] != [CRON]:
                api('/schedules', [{'cron': row['cron']} for row in previous_schedule])
            public_response()
            summary(f'Rollback active: `{previous}`')
        raise
    summary(f'Production Worker version verified: `{verified}` (uploaded `{target}`)')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, subprocess.SubprocessError, URLError) as error:
        raise SystemExit(str(error)) from None
