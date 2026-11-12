"""Preserve changes of market in the three authenticated Gold slots."""
from datetime import timedelta, timezone
import json

from gold_history import ROME, stamp
from market_scope import JOINT_KINDS, core_market
from v7_archive import valid_snapshot
from v7_decision import TRIAD_MARKETS
from v7_probability import finite


def fixture_key(fixture):
    fid = str(fixture.get('fixture_id', ''))
    kickoff = stamp(fixture.get('kickoff'))
    if fid.isdigit() and kickoff:
        return f'{fid}::{kickoff.astimezone(timezone.utc).isoformat()}'


def snapshot(match):
    return (((match.get('multimarket') or {}).get('matrix') or {}).get('v7') or {})


def keep_visible_days(history, now):
    yesterday = now.astimezone(ROME).date() - timedelta(days=1)
    for key, entry in list(history.items()):
        kickoff = stamp(entry.get('kickoff'))
        if not kickoff or kickoff.astimezone(ROME).date() < yesterday:
            del history[key]


def observe_day(history, day, now):
    for match in day.get('matches', []):
        fixture = match.get('fixture') or {}
        kickoff = stamp(fixture.get('kickoff'))
        if (not kickoff or now >= kickoff or fixture.get('status_short') not in ('NS', 'TBD')
                or fixture.get('result')):
            continue
        v7 = snapshot(match)
        key = fixture_key(fixture)
        captured = stamp(v7.get('captured_at'))
        if not key or v7.get('status') != 'published' or not captured:
            continue
        entry = history.get(key)
        if entry and captured <= stamp(entry['seen_at']):
            continue
        if not valid_snapshot(v7, fixture, now.isoformat()):
            continue
        entry = history.setdefault(key, {'kickoff': fixture['kickoff'], 'slots': {}})
        for role in TRIAD_MARKETS:
            signal = (v7.get('triad') or {}).get(role) or {}
            market_id = signal.get('market_id')
            record = (v7.get('markets') or {}).get(market_id)
            if not record or not record.get('label'):
                continue
            try:
                spec = json.loads(market_id)
            except (TypeError, ValueError):
                continue
            if not core_market(spec) or spec['kind'] not in ('result', 'total', 'btts', *JOINT_KINDS):
                continue
            versions = entry['slots'].setdefault(role, [])
            if versions and versions[-1]['market_id'] == market_id:
                continue
            price = record.get('odd')
            fair = record.get('model_fair_odd')
            versions.append(dict(market_id=market_id, label=record['label'],
                                 captured_at=v7['captured_at'],
                                 odd=price if finite(price) and price > 1 else None,
                                 model_fair_odd=fair if finite(fair) and fair > 1 else None))
        entry['seen_at'] = v7['captured_at']


def earlier_versions(match, history):
    entry = history.get(fixture_key(match.get('fixture') or {}))
    if not entry:
        return {}
    v7 = snapshot(match)
    captured = stamp(v7.get('captured_at'))
    if v7.get('status') != 'published' or not captured:
        return {}
    earlier = {}
    for role, versions in entry['slots'].items():
        market_id = ((v7.get('triad') or {}).get(role) or {}).get('market_id')
        if not market_id:
            continue
        visible = [version for version in versions if stamp(version['captured_at']) <= captured]
        if visible and visible[-1]['market_id'] == market_id:
            visible.pop()
        if visible:
            earlier[role] = visible
    return earlier
