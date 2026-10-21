"""Publish IDs and kickoffs only; the live service never reads/rebuilds forecasts."""
from pathlib import Path
from datetime import datetime, timezone
from audit_storage import read_store, atomic_bytes
import json


def build(root=Path('.')):
    records = {}
    for path in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(path, {}).get('matches', []):
            f = match.get('fixture') or {}
            fid, kickoff = str(f.get('fixture_id', '')), f.get('kickoff')
            if fid.isdigit() and kickoff:
                records[fid] = {'fixture_id': fid, 'kickoff': kickoff}
    return {'schema_version': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
            'fixtures': list(records.values())}


if __name__ == '__main__':
    atomic_bytes(Path('data/live_watchlist.json'), json.dumps(build(),separators=(',',':')).encode()+b'\n')
