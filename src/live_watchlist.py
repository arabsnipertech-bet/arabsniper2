"""Publish IDs and kickoffs only; the live service never reads/rebuilds forecasts."""
from pathlib import Path
from datetime import datetime, timezone
from audit_storage import read_store, atomic_bytes
from gold_competitions import excluded_known_meta
import json


def build(root=Path('.')):
    records = {}
    for path in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(path, {}).get('matches', []):
            f = match.get('fixture') or {}
            fid, kickoff = str(f.get('fixture_id', '')), f.get('kickoff')
            if fid.isdigit() and kickoff and not excluded_known_meta(f):
                records[fid] = {'fixture_id': fid, 'kickoff': kickoff}
    return {'schema_version': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
            'fixtures': list(records.values())}


def from_checkpoint(root):
    """Refresh the public ID projection without scans, provider calls or private writes."""
    import os
    from tempfile import TemporaryDirectory
    from gold_storage import client, previous_manifest, restore_subset
    now = datetime.now(timezone.utc)
    target = Path(root)/'data/live_watchlist.json'
    try:
        current = json.loads(target.read_bytes())
        age = (now - datetime.fromisoformat(current['generated_at'])).total_seconds()
        if current.get('schema_version') == 1 and current.get('fixtures') and 0 <= age <= 4 * 3600:
            return None  # A scan or another repair already updated main.
    except (OSError, ValueError, TypeError, KeyError):
        pass
    storage, bucket = client(), os.environ['R2_BUCKET']
    index = previous_manifest(storage, bucket)
    paths = [f'data/arab_quote_day{i}.json' for i in range(1, 6)]
    if not all(path in index for path in paths):
        raise ValueError('All five verified Gold days are required')
    with TemporaryDirectory(prefix='live-watchlist-') as folder:
        source = Path(folder)
        restore_subset(storage, bucket, source, index, paths)
        payload = build(source)
    if not payload['fixtures'] or not any(
            datetime.fromisoformat(f['kickoff']) > now
            for f in payload['fixtures']):
        raise ValueError('Verified watchlist does not cover upcoming fixtures')
    payload['source'] = 'verified_private_checkpoint'
    atomic_bytes(target,
                 json.dumps(payload,separators=(',',':')).encode()+b'\n')
    return payload


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-checkpoint', action='store_true')
    args = parser.parse_args()
    if args.from_checkpoint:
        payload = from_checkpoint(Path('.'))
        if payload is None:
            print('The published ID projection is already current; zero provider calls.')
        else:
            print(f"ID projection prepared: {len(payload['fixtures'])} fixtures; zero provider calls.")
    else:
        atomic_bytes(Path('data/live_watchlist.json'), json.dumps(build(),separators=(',',':')).encode()+b'\n')
