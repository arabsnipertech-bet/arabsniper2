"""Conservative migration of previously exposed fixtures at the editorial cutover."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from io import BytesIO
import json
import re
from zipfile import ZipFile

import requests

from .selection import utc

REPO = 'arabsnipertech-bet/arabsniper2'
API = 'https://api.github.com/repos/' + REPO + '/'
PUBLIC_PATH = 'data/arab_quote_homepage.json'
WORKFLOWS = {
    'telegram-digest.yml', 'telegram-results.yml', 'telegram-live.yml',
    'telegram-sports-live.yml', 'telegram-daily-spunti.yml', 'telegram-evidence.yml',
    'telegram-gold-promo.yml', 'telegram-morning-correction.yml',
    'facebook-page.yml', 'facebook-recovery.yml',
}
ARTIFACT = re.compile(r'tg-(?:spunti-)?\d{4}-\d{2}-\d{2}-\d{4}-(?:claim|sent)')


def get(url, headers=None, **kwargs):
    response = requests.get(url, headers=headers, timeout=(6, 15), **kwargs)
    response.raise_for_status()
    return response


def fixture_ids(value):
    return {str(row.get('fixture_id', row.get('id', '')))
            for key in ('selections', 'social_spunti', 'social_selections', 'picks', 'reserved')
            for row in value.get(key, []) if isinstance(row, dict)}


def drain_and_import(token, now):
    headers = {'Authorization': 'Bearer ' + token, 'Accept': 'application/vnd.github+json'}
    def api(path, params):
        return get(API + path, headers, params=params).json()

    with ThreadPoolExecutor(max_workers=8) as workers:
        def old_runs(status):
            value = api('actions/runs', {'branch': 'main', 'status': status, 'per_page': 100})
            if value['total_count'] > 100:
                raise RuntimeError('Legacy run check incomplete')
            return any(run['path'].split('@')[0].rsplit('/', 1)[-1] in WORKFLOWS
                       for run in value['workflow_runs'])
        if any(workers.map(old_runs, ('in_progress', 'queued', 'waiting', 'pending', 'requested'))):
            return None
        cutoff = utc(now - timedelta(hours=48))
        source = api('commits', {'sha': 'main', 'path': PUBLIC_PATH, 'since': cutoff, 'per_page': 100})
        if len(source) == 100:
            raise RuntimeError('Public projection history exceeds bound')

        def old_feed(item):
            sha = item['sha']
            if not re.fullmatch(r'[0-9a-f]{40}', sha):
                raise ValueError('Invalid commit from history')
            return fixture_ids(get(f'https://raw.githubusercontent.com/{REPO}/{sha}/{PUBLIC_PATH}').json())
        exposed = set().union(*workers.map(old_feed, source))

        def artifact_page(page):
            return api('actions/artifacts', {'per_page': 100, 'created': '>=' + cutoff, 'page': page})
        first_page = artifact_page(1)
        pages = (first_page['total_count'] + 99) // 100
        if pages > 15:
            raise RuntimeError('Legacy receipt scan exceeds bound')
        recent = list(first_page['artifacts'])
        for page in workers.map(artifact_page, range(2, pages + 1)):
            recent.extend(page['artifacts'])

        def download(item):
            path = 'actions/artifacts/' + str(int(item['id'])) + '/zip'
            response = get(API + path, headers, stream=True)
            with response:
                data = response.raw.read(4_000_001)
            if len(data) > 4_000_000:
                raise ValueError('Legacy receipt archive too large')
            ids = set()
            with ZipFile(BytesIO(data)) as archive:
                for file in archive.infolist():
                    if file.filename.rsplit('/', 1)[-1] not in ('post.json', 'spunti-receipt.json'):
                        continue
                    if file.file_size > 200_000:
                        raise ValueError('Legacy receipt too large')
                    payload = json.loads(archive.read(file))
                    ids |= fixture_ids(payload)
                    ids.update(str(fid) for fid in payload.get('fixtures', []) if str(fid).isdigit())
            return ids
        items = {item['id']: item for item in recent
                 if item['created_at'].replace('Z', '+00:00') >= cutoff
                 and not item['expired'] and ARTIFACT.fullmatch(item['name'])}
        exposed.update(set().union(*workers.map(download, items.values())))
    return sorted(value for value in exposed if value.isascii() and value.isdecimal())
