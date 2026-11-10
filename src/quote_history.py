"""Lossless acquisition journal, separate from the bounded pricing cache.

The first observed price is immutable. Every successful HTTP acquisition is
retained, including unchanged prices, without making cached reads new evidence.
Files use the existing checksummed, content-addressed private R2 checkpoint.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from audit_storage import read_store, write_store
from multimarket_model import stamp

VERSION = 'observed-price-history-v1'
INDEX = 'arab_quote_price_history_index.json'
PREFIX = 'arab_quote_price_history_'
ROME = ZoneInfo('Europe/Rome')


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def partition_id(fid, observed):
    # Sixteen small date partitions bound rewrite cost during large weekends.
    return stamp(observed).astimezone(ROME).strftime('%Y%m%d')+'_'+hashlib.sha256(str(fid).encode()).hexdigest()[0]


class QuoteHistory:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.index = read_store(self.directory/INDEX, {'version': VERSION, 'fixtures': {}})
        if self.index.get('version') != VERSION:
            raise ValueError('Unknown quote history index')
        self.days, self.dirty = {}, set()
        self.index_dirty = False
        self.observations = 0

    def fixture(self, fid, kickoff, observed):
        fid = str(fid)
        existing = fid in self.index['fixtures']
        if not existing:
            self.index['fixtures'][fid] = partition_id(fid, observed)
            self.index_dirty = True
        day = self.index['fixtures'][fid]
        if not isinstance(day, str) or not re.fullmatch(r'\d{8}(?:_[0-9a-f])?', day):
            raise ValueError('Invalid quote history partition')
        if day not in self.days:
            if existing and not (self.directory/(PREFIX+day+'.json')).is_file():
                raise ValueError('Missing referenced quote history partition')
            self.days[day] = read_store(self.directory/(PREFIX+day+'.json'),
                                       {'version': VERSION, 'fixtures': {}})
            if self.days[day].get('version') != VERSION:
                raise ValueError('Unknown quote history partition')
        if existing and fid not in self.days[day]['fixtures']:
            raise ValueError('Missing referenced quote history fixture')
        fixture = self.days[day]['fixtures'].setdefault(fid, {'kickoff': kickoff, 'groups': {}})
        # A rescheduled fixture keeps the original baseline and partition.
        fixture['kickoff'] = kickoff
        self.dirty.add(day)
        return fixture

    def append(self, fid, kickoff, key, group, source, *, seed=False):
        at, updated = (stamp(group.get(k)) for k in ('observed_at', 'provider_updated_at'))
        start = stamp(kickoff)
        if not at or not updated or not start or updated > at or at >= start:
            return False
        if group.get('quote_source') == 'synthetic_combo' or group.get('quote_executable') is False:
            return False
        odds = {str(side): odd for side, odd in group.get('odds', {}).items()
                if type(odd) in (float, int) and odd > 1}
        if not odds:
            return False
        # Reject non-finite prices before any file or baseline is changed.
        fingerprint(odds)
        fixture = self.fixture(fid, kickoff, group['observed_at'])
        history = fixture['groups'].setdefault(key, {
            'bookmaker_id': str(group.get('bookmaker_id') or key.rsplit('|', 1)[-1]),
            'bookmaker': group.get('bookmaker'), 'bet_id': group.get('bet_id'),
            'quote_source': group.get('quote_source', 'bookmaker'),
            'quote_provenance': deepcopy(group.get('quote_provenance') or {}),
            'first_observation_type': 'first_observed_not_bookmaker_opening',
            'first': {}, 'observations': []})
        entry = {'observed_at': group['observed_at'], 'provider_updated_at': group['provider_updated_at'],
                 'odds': deepcopy(odds), 'complete': bool(group.get('complete')),
                 'source': deepcopy(source)}
        identity_value = {k: entry[k] for k in ('observed_at', 'provider_updated_at', 'odds', 'complete')}
        if source.get('acquisition_id'):
            identity_value['acquisition_id'] = source['acquisition_id']
        identity = fingerprint(identity_value)
        if any(p['sha256'] == identity for p in history['observations']):
            return False
        if seed and history['observations']:
            # Import only before the new journal starts. Never insert a newly
            # discovered older value ahead of an established first observation.
            if at < stamp(history['observations'][-1]['observed_at']):
                return False
        for side, odd in odds.items():
            history['first'].setdefault(side, {'odd': odd, 'observed_at': group['observed_at'],
                'provider_updated_at': group['provider_updated_at']})
        prior = history['observations'][-1]['odds'] if history['observations'] else {}
        entry['delta_from_first_pct'] = {side: 100*(odd/history['first'][side]['odd']-1) for side, odd in odds.items()}
        entry['delta_from_previous_pct'] = {side: 100*(odd/prior[side]-1) if side in prior else None for side, odd in odds.items()}
        entry['sha256'] = identity
        history['observations'].append(entry)
        self.observations += 1
        return True

    def seed_matrix(self, state):
        """Copy original, timestamped real points; leave the source ledger intact."""
        for fid, fixture in state.get('matrix_quotes', {}).items():
            for key, points in fixture.get('groups', {}).items():
                if not points or key.startswith('synthetic_'):
                    continue
                day = self.index['fixtures'].get(str(fid))
                if day:
                    existing = self.fixture(fid, fixture.get('kickoff'), points[0].get('observed_at'))
                    if key in existing['groups']:
                        continue
                for point in points:
                    if stamp(point.get('observed_at')) and stamp(point['observed_at']) <= datetime.now(timezone.utc):
                        self.append(fid, fixture.get('kickoff'), key, point,
                                    {'endpoint': 'legacy_matrix_ledger', 'migration': 'original_observed_point'}, seed=True)

    def observe(self, payload, observed_at, query):
        """Called only on a new complete provider response, before enrichment."""
        from matrix_quotes import collect
        if not payload.get('collection', {}).get('complete'):
            return
        starts = {str(r.get('fixture', {}).get('id')): r.get('fixture', {}).get('date')
                  for r in payload.get('response', [])}
        for fid, kickoff in starts.items():
            captured, _ = collect(payload, fid, observed_at)
            for key, group in captured.items():
                self.append(fid, kickoff, key, group,
                            {'provider': 'API-Football', 'endpoint': 'odds', 'query': dict(query),
                             'acquisition_id': payload['collection'].get('acquisition_id')})

    def flush(self):
        for day in sorted(self.dirty):
            write_store(self.directory/(PREFIX+day+'.json'), self.days[day])
        if self.index_dirty:
            # Index last, matching the private checkpoint's pointer-last rule.
            write_store(self.directory/INDEX, self.index)
        self.dirty.clear()
        self.index_dirty = False


def restore_for_fixtures(storage, bucket, root, manifest, fixture_ids):
    """Restore only the archive partitions needed by an incremental price pass."""
    from gold_storage import restore_subset
    root = Path(root)
    index_path = 'data/'+INDEX
    if index_path not in manifest:
        return
    restore_subset(storage, bucket, root, manifest, [index_path])
    mapping = read_store(root/index_path)['fixtures']
    paths = {'data/'+PREFIX+mapping[str(fid)]+'.json' for fid in fixture_ids if str(fid) in mapping}
    if not paths.issubset(manifest):
        raise ValueError('Missing private quote history partition')
    restore_subset(storage, bucket, root, manifest, paths)


def checkpoint():
    """Preserve acquisitions after a failed scan without publishing any Gold."""
    import os
    from gold_storage import client, publish
    root = Path(__file__).resolve().parents[1]
    paths = ['data/'+p.name for p in (root/'data').glob(PREFIX+'*.json')]
    if paths:
        publish(client(), os.environ['R2_BUCKET'], root, checkpoint_only=True, selected_paths=paths)


if __name__ == '__main__':
    checkpoint()
