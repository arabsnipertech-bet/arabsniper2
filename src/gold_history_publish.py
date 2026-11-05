"""Publish the additive history projection without running or rewriting a scan.
Runs under the same writer lock as normal private checkpoint publication.
"""
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from gold_history import project, FILE, ROME
from gold_storage import client, collect, write_manifest
from audit_storage import read_store, FORMAT


def download(s3, bucket, files, name, root):
    item = files[name]
    digest = item['sha256']
    if not re.fullmatch('[a-f0-9]{64}', digest):
        raise ValueError('Invalid object hash')
    raw = s3.get_object(Bucket=bucket, Key='state/objects/'+digest)['Body'].read()
    if len(raw) != item['bytes'] or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('Checkpoint checksum mismatch')
    path = root/name
    path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(raw)
    if name.endswith('.json'):
        value = json.loads(raw)
        if value.get('_storage') == FORMAT:
            for part in value['parts']:
                part_name = str(Path(name).parent/'_audit_parts'/(part['sha256']+'.json.gz'))
                download(s3, bucket, files, part_name, root)
    return path


def main():
    s3 = client(); bucket = os.environ['R2_BUCKET']
    raw = s3.get_object(Bucket=bucket, Key='state/current.json')['Body'].read()
    manifest = json.loads(raw); index = manifest['files']
    if manifest.get('version') != 1: raise ValueError('Unknown checkpoint')
    now = datetime.now(ROME); yesterday = (now.date()-timedelta(days=1)).isoformat()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        board = read_store(download(s3, bucket, index, 'data/arab_quote_day1.json', root))
        if FILE in index: download(s3, bucket, index, FILE, root)
        history = read_store(root/FILE, {})
        seed = None
        if yesterday not in history.get('days', {}):
            candidates = [o for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix='state/manifests/')
                          for o in page.get('Contents', []) if o['LastModified'].astimezone(ROME).date().isoformat() == yesterday]
            for item in sorted(candidates, key=lambda o:o['LastModified'], reverse=True)[:5]:
                old = json.loads(s3.get_object(Bucket=bucket, Key=item['Key'])['Body'].read())
                with tempfile.TemporaryDirectory() as seed_dir:
                    candidate = read_store(download(s3, bucket, old['files'], 'data/arab_quote_day1.json', Path(seed_dir)))
                    if candidate.get('date') == yesterday:
                        seed = candidate; break
        projection = project(root, board, now=now, seed=seed)
        # Preserve every unrelated checkpoint entry, including immutable audits.
        new_index = dict(index)
        for name, value in collect(root, index).items():
            digest = hashlib.sha256(value).hexdigest()
            s3.put_object(Bucket=bucket, Key='state/objects/'+digest, Body=value, Metadata={'sha256':digest})
            new_index[name] = {'sha256':digest, 'bytes':len(value)}
        if s3.get_object(Bucket=bucket, Key='state/current.json')['Body'].read() != raw:
            raise ValueError('Concurrent checkpoint update; retry under the writer lock')
        s3.put_object(Bucket=bucket, Key='arab_quote_day1.json',
                      Body=json.dumps(projection, ensure_ascii=False, separators=(',', ':')).encode(),
                      ContentType='application/json', CacheControl='private, no-store')
        write_manifest(s3, bucket, new_index)
        previous = projection.get('previous_day') or {}
        summary = {'today':board.get('date'), 'yesterday':previous.get('date'),
                   'archived_matches':len(previous.get('matches', [])),
                   'verified_prematch':sum(bool(m.get('_gold_history', {}).get('verified_prematch')) for m in previous.get('matches', [])),
                   'unchanged_current_matches':projection.get('matches') == board.get('matches')}
        print(json.dumps(summary))
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            Path(os.environ['GITHUB_STEP_SUMMARY']).write_text('## Gold - vista Ieri\n\n'+json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
