"""Delete only unreferenced immutable parts after verifying every current manifest.

JSON records, forecasts, results and Git history are never deleted. Unknown files
are retained. Run under the shared data-writer lock, with no concurrent writers.
"""
import argparse
import json
import re
from pathlib import Path
from audit_storage import FORMAT, read_store

ROOTS = ('data', 'auditordata', 'auditarchive')
PART = re.compile(r'[a-f0-9]{64}\.json\.gz\Z')


def clean(root, apply=False):
    root = Path(root)
    referenced = set()
    manifests = []
    # Plan the entire deletion only after all JSON and referenced chunks validate.
    for folder in ROOTS:
        base = root / folder
        for path in sorted(base.rglob('*.json')):
            if '_audit_parts' in path.parts:
                continue
            value = json.loads(path.read_bytes())
            if isinstance(value, dict) and '_storage' in value:
                if value['_storage'] != FORMAT:
                    raise ValueError(f'Unknown storage format: {path}')
                read_store(path)  # Full byte counts and SHA-256 validation.
                manifests.append(path)
                for part in value['parts']:
                    referenced.add(path.parent / '_audit_parts' / (part['sha256'] + '.json.gz'))
    candidates = []
    for folder in ROOTS:
        for path in (root / folder).rglob('_audit_parts/*.json.gz'):
            if path.is_symlink():
                raise ValueError(f'Unexpected symbolic link: {path}')
            if PART.fullmatch(path.name) and path not in referenced:
                candidates.append(path)
    report = {'verified_manifests': len(manifests), 'referenced_parts': len(referenced),
              'removed_parts' if apply else 'candidate_parts': len(candidates),
              'bytes': sum(p.stat().st_size for p in candidates)}
    if apply:
        for path in candidates:
            path.unlink()
        for path in manifests:
            read_store(path)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='.')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(clean(args.root, args.apply), indent=2))
