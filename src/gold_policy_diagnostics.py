"""Read-only policy diagnosis from one immutable private checkpoint.

No puts, deletes, football calls, ledger writes, or public forecast exports.
Only the five daily files and their referenced chunks are downloaded.
"""
from collections import Counter, defaultdict
from datetime import datetime
import json
import os
from pathlib import Path
import re
import tempfile

from audit_storage import FORMAT, read_store, atomic_bytes
from gold_storage import sha
from gold_policy_audit import (ROME, MODEL_ID, STRATEGIES, policy_choices,
    requested_case, stamp, output_allowed, market_tier)
from matrix_quotes import expire


def inspect_checkpoint(s3, bucket, observed_at=None):
    now = observed_at or datetime.now(ROME).isoformat()
    manifest = json.loads(s3.get_object(Bucket=bucket, Key='state/current.json')['Body'].read())
    if manifest.get('version') != 1 or not isinstance(manifest.get('files'), dict):
        raise ValueError('Invalid checkpoint')
    downloaded = {}
    with tempfile.TemporaryDirectory(prefix='gold-readonly-') as directory:
        root = Path(directory)

        def get(path):
            item = manifest['files'][path]
            digest = item['sha256']
            if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
                raise ValueError('Invalid object identifier')
            if digest not in downloaded:
                downloaded[digest] = s3.get_object(Bucket=bucket, Key='state/objects/'+digest)['Body'].read()
            raw = downloaded[digest]
            if type(item['bytes']) is not int or len(raw) != item['bytes'] or sha(raw) != digest:
                raise ValueError('Checkpoint checksum mismatch')
            atomic_bytes(root/path, raw)
            return raw

        counts, cases, seen, source_times = defaultdict(Counter), [], set(), {}
        for day in range(1, 6):
            path = f'data/arab_quote_day{day}.json'
            value = json.loads(get(path))
            if isinstance(value, dict) and value.get('_storage') == FORMAT:
                for part in value['parts']:
                    digest = part['sha256']
                    if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
                        raise ValueError('Invalid audit chunk')
                    get(f'data/_audit_parts/{digest}.json.gz')
            data = read_store(root/path)
            source_times[path] = data.get('generated_at') or data.get('updated_at')
            for match in data.get('matches', []):
                meta = match.get('fixture') or {}
                fid, kickoff = str(meta.get('fixture_id')), stamp(meta.get('kickoff'))
                if fid in seen or not kickoff or kickoff <= stamp(now): continue
                seen.add(fid)
                case = requested_case(match, now)
                if case: cases.append(case)
                if not output_allowed(match) or meta.get('status_short') not in ('NS', 'TBD') or meta.get('result'): continue
                date = kickoff.astimezone(ROME).date().isoformat()
                counts[date]['allowed_upcoming'] += 1
                assessment = match.get('multimarket') or {}
                if assessment.get('model_id') != MODEL_ID:
                    counts[date]['pending_current_engine'] += 1
                    continue
                expire(assessment, meta, now)
                matrix = assessment.get('matrix') or {}
                rows = [r for r in matrix.get('rows', []) if market_tier(r) is not None]
                choices = policy_choices(rows, matrix.get('goal_context') or {})
                for strategy in STRATEGIES:
                    if choices[strategy] is not None:
                        counts[date][strategy] += 1
                        if strategy != 'published' and choices['published'] is None:
                            counts[date][strategy+'_extra'] += 1
        return {'read_only': True, 'api_football_calls': 0, 'checkpoint_writes': 0,
            'objects_read': len(downloaded), 'observed_at': now,
            'checkpoint_sha256': sha(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()),
            'source_times': source_times, 'counts': {d: dict(c) for d, c in sorted(counts.items())},
            'requested_cases': cases}


if __name__ == '__main__':
    from gold_storage import client
    try:
        report = inspect_checkpoint(client(), os.environ['R2_BUCKET'])
        print('GOLD_READ_ONLY_DIAGNOSTICS='+json.dumps(report, ensure_ascii=False, separators=(',', ':')), flush=True)
    except Exception as error:
        # Keep SDK headers/credentials out of logs, even on failure.
        raise SystemExit('Gold read-only diagnosis failed: '+type(error).__name__)
