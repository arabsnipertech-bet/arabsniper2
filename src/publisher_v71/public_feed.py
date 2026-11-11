"""Publish an allowlisted JSON document on an independent, single-file Git branch."""
from datetime import timedelta
import json
import os
import re
import subprocess
import time

from multimarket_model import stamp

from . import showcase
from .selection import project_group, utc
from .storage import encoded

BRANCH = 'publisher-public-v71'
REF = 'refs/heads/' + BRANCH


def historical(source):
    latest = source.get('latest_verified') or {}
    settled = latest.get('daily') or {}
    try:
        date = latest['date']
        stamp(date + 'T00:00:00Z').date()
        won, lost = settled['wins'], settled['losses']
    except (AttributeError, KeyError, TypeError):
        return None
    if not all(type(n) is int and n >= 0 for n in (won, lost)) or won + lost == 0:
        return None
    return {'date': date, 'won': won, 'lost': lost}


def projection(state, now, past=None, *, boards=()):
    report = max(state['reports'].values(), key=lambda item: item['view']['date'], default=None)
    academy = max((p['view'] for p in state['posts'].values() if p['kind'] == 'academy'),
                  key=lambda item: item['date'], default=None)
    live = max(state['live'].values(), key=lambda item: item['view']['observed_at'], default=None)
    groups = []
    for group in state['groups'].values():
        view = project_group(group, now)
        if view and group.get('eligible_home'):
            groups.append(view)
    previews, schedule = showcase.project(boards, now)
    return {
        'schema_version': 1, 'publisher': 'v7.1', 'updated_at': utc(now),
        'groups': sorted(groups, key=lambda item: item['free']['kickoff']),
        'gold_previews': previews,
        'gold_schedule': schedule,
        'live': live['view'] if live and now - stamp(live['view']['observed_at']) < timedelta(hours=24) else None,
        'report': report['view'] if report else None,
        'historical': historical(past or {}) if not report else None,
        'academy': academy,
        'trial_verified': state['runtime'].get('trial_verified', False),
    }


def git(*args, data=None):
    env = {**os.environ, 'GIT_AUTHOR_NAME': 'github-actions[bot]', 'GIT_COMMITTER_NAME': 'github-actions[bot]',
           'GIT_AUTHOR_EMAIL': '41898282+github-actions[bot]@users.noreply.github.com',
           'GIT_COMMITTER_EMAIL': '41898282+github-actions[bot]@users.noreply.github.com'}
    result = subprocess.run(['git', *args], input=data, env=env, check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=25)
    return result.stdout.decode().strip()


def branch_head():
    output = git('ls-remote', '--heads', 'origin', REF).split()
    return output[0] if output else None


def publish(view):
    # Build a fresh single-file tree. Neither the index nor main is ever a source.
    raw = encoded(view)
    if len(raw) > 100_000:
        raise ValueError('Public projection too large')
    blob = git('hash-object', '-w', '--stdin', data=raw)
    tree = git('mktree', data=f'100644 blob {blob}\thomepage.json\n'.encode())
    for attempt in range(3):
        parent = branch_head()
        previous = None
        if parent:
            git('fetch', '--no-tags', '--depth=1', 'origin', REF)
            parent = git('rev-parse', 'FETCH_HEAD')
            if not re.fullmatch(r'[0-9a-f]{40}', parent):
                raise ValueError('Unexpected public branch reference')
            existing = json.loads(git('show', parent + ':homepage.json'))
            if existing.get('publisher') != 'v7.1':
                raise ValueError('Unexpected existing public branch content')
            published = stamp(git('show', '-s', '--format=%cI', parent))
            previous = (existing, parent, published)
            if all(existing.get(key) == value for key, value in view.items() if key != 'updated_at'):
                return parent, previous
        args = ['-p', parent] if parent else []
        commit = git('commit-tree', tree, *args, '-m', 'Update ArabSniperBet public editorial feed')
        try:
            git('push', 'origin', f'{commit}:{REF}')
            return commit, previous
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            # A timed-out push may have succeeded. Read the authoritative ref.
            if branch_head() == commit:
                return commit, previous
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
