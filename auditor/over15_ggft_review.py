"""One-time read-only research export. No provider, publisher or remote writer."""
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from threading import Lock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from audit_daily_evidence import evidence_paths, receipt_path
from audit_storage import FORMAT, read_store
from gold_storage import client, previous_manifest, restore_subset, sha
from repair_certificate import encrypt

FIRST, LAST = date(2026, 11, 9), date(2026, 11, 22)


class ReadOnly:
    def __init__(self, s3):
        self.s3, self.reads, self.bytes, self.lock = s3, 0, 0, Lock()

    def get_object(self, *, Bucket, Key):
        if Bucket != 'arabsniper-gold' or not (Key == 'state/current.json' or
                re.fullmatch(r'state/objects/[0-9a-f]{64}', Key)):
            raise ValueError('Read outside the audit checkpoint')
        with self.lock:
            self.reads += 1
            if self.reads > 512:
                raise ValueError('Research object-read budget exceeded')
        response = self.s3.get_object(Bucket=Bucket, Key=Key)
        with self.lock:
            self.bytes += response.get('ContentLength', 0)
            if self.bytes > 128*1024*1024:
                response['Body'].close()
                raise ValueError('Research byte budget exceeded')
        return response


def target(key):
    spec = json.loads(key)
    return ((spec.get('kind'), spec.get('period'), spec.get('side'), spec.get('line'))
            == ('total', 'ht', 'over', 1.5) or
            (spec.get('kind'), spec.get('period'), spec.get('side')) == ('btts', 'ft', 'yes'))


def compact(archive, day):
    if archive.get('date') != str(day) or not isinstance(archive.get('fixtures'), dict):
        raise ValueError('Archive identity mismatch')
    return {**{key: archive.get(key) for key in
               ('version', 'date', 'generated_at', 'counts', 'rejected_forecasts')},
            'fixtures': archive['fixtures'],
            'samples': [row for row in archive.get('samples', []) if target(row['market'])]}


def main():
    if LAST >= datetime.now(ZoneInfo('Europe/Rome')).date():
        raise ValueError('Closed dates only')
    source = ReadOnly(client())
    index = previous_manifest(source, 'arabsniper-gold')
    dates = [FIRST+timedelta(days=i) for i in range((LAST-FIRST).days+1)]
    paths = [p for day in dates for p in
             (f'auditarchive/prematch_{day}_v7.json', receipt_path(day)) if p in index]
    report = dict(version=1, read_only=True, provider_calls=0, remote_writes=0,
                  source_revision=os.environ.get('GITHUB_SHA'),
                  collected_at=datetime.now(ZoneInfo('Europe/Rome')).isoformat(),
                  requested_dates=[str(day) for day in dates],
                  sources={p:index[p] for p in paths}, days={}, missing=[])
    with TemporaryDirectory() as folder:
        root = Path(folder)
        restore_subset(source, 'arabsniper-gold', root, index, paths)
        for day in dates:
            path = f'auditarchive/prematch_{day}_v7.json'
            if path not in index:
                report['missing'].append(str(day))
                continue
            stored = json.loads((root/path).read_bytes()) if (root/path).stat().st_size <= 1024*1024 else {}
            if stored.get('_storage') == FORMAT and stored['bytes'] > 256*1024*1024:
                raise ValueError('Archive exceeds the bounded research scope')
            archive = compact(read_store(root/path), day)
            receipt = read_store(root/receipt_path(day), {})
            hashes = receipt.get('sha256', {})
            sealed = (receipt.get('version') == 1 and receipt.get('date') == str(day)
                      and set(hashes) == evidence_paths(day)
                      and all(index.get(p, {}).get('sha256') == digest for p, digest in hashes.items()))
            archive.update(evidence_receipt=receipt, receipt_matches_current_manifest=sealed,
                           source_sha256=sha((root/path).read_bytes()))
            report['days'][str(day)] = archive
        if not report['days']:
            raise ValueError('No saved V7 archive available')
        report.update(r2_gets=source.reads, r2_compressed_bytes=source.bytes)
        encrypt(report, (ROOT/'auditor/over15-ggft-recipient-public.pem').read_bytes(),
                ROOT/'encrypted-review/over15-ggft.enc.json')
    print('Read-only research complete; encrypted archives exported; zero provider calls or remote writes.')


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit('Read-only research failed: '+type(error).__name__)
