"""Republish Explorer presentation fields from five verified Gold files only."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from audit_storage import read_store
from explorer_lease import writer
from gold_explorer import project_match, publish, read
from gold_storage import GOLD, client, previous_manifest, restore_subset


def main():
    storage, bucket = client(fast_restore=True), os.environ['R2_BUCKET']
    manifest = previous_manifest(storage, bucket)
    paths = ['data/'+name for name in GOLD[:5]]
    if not all(path in manifest for path in paths):
        raise RuntimeError('Missing verified Gold days; Explorer unchanged')
    with TemporaryDirectory(prefix='explorer-presentation-') as folder:
        root = Path(folder)
        restore_subset(storage, bucket, root, manifest, paths, fast_restore=True)
        days = [read_store(root/path) for path in paths]
        now = datetime.now(timezone.utc)
        projected = [row for day in days for match in day.get('matches', [])
                     for row in project_match(match, now)]
        with writer(storage, bucket) as scoped:
            if scoped is None:
                raise RuntimeError('Explorer writer busy; no changes made')
            publish(scoped, bucket, root, days, 'refresh', now=now, legacy_limit=0,
                    source_manifest=manifest)
            current = read(scoped, bucket, 'explorer/current.json')
            if current['count'] != len(projected) or current['generated_at'] != now.isoformat():
                raise RuntimeError('Explorer presentation publication not verified')
        print(json.dumps(dict(status='published', rows=len(projected),
            matches=len({r['fixture_id'] for r in projected}),
            rows_with_1x2=sum(bool(r['match_odds']) for r in projected),
            rows_with_gold_reason=sum(bool(r['origin_badge'] and r['origin_tooltip']) for r in projected),
            gold_forecasts_changed=0, football_requests=0)), flush=True)


if __name__ == '__main__':
    main()
