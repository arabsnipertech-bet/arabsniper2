"""Read the exact authenticated Gold feeds; export privately for UI diagnosis.

Only R2 GETs, no provider calls, recalculation, checkpoint writes or public picks.
"""
import base64
from datetime import datetime
import gzip
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from gold_storage import client, sha
from opportunity_policy import goal_market


def collect_published(s3, bucket, date, observed_at):
    if datetime.strptime(date, '%Y-%m-%d').date().isoformat() != date:
        raise ValueError('Invalid publication date')
    sources, matches, seen = {}, [], set()
    scope = None
    for day in range(1, 6):
        key = f'arab_quote_day{day}.json'
        response = s3.get_object(Bucket=bucket, Key=key)
        raw = response['Body'].read()
        data = json.loads(raw)
        if not isinstance(data, dict) or not isinstance(data.get('matches'), list):
            raise ValueError('Invalid published Gold feed')
        sources[key] = {'sha256': sha(raw), 'bytes': len(raw),
            'generated_at': data.get('generated_at') or data.get('updated_at'),
            'last_modified': response['LastModified'].isoformat() if response.get('LastModified') else None}
        for match in data['matches']:
            fixture = match.get('fixture') or {}
            kickoff = datetime.fromisoformat(str(fixture.get('kickoff')).replace('Z', '+00:00'))
            if kickoff.tzinfo is None:
                raise ValueError('Published kickoff has no timezone')
            fid = str(fixture.get('fixture_id'))
            if kickoff.astimezone(ZoneInfo('Europe/Rome')).date().isoformat() != date or fid in seen:
                continue
            seen.add(fid)
            scope = scope or data.get('gold_scope')
            assessment = match.get('multimarket') or {}
            matrix = assessment.get('matrix') or {}
            matches.append({'fixture': fixture, 'gold_scope': match.get('gold_scope'),
                'source_feed': key, 'multimarket': {'model_id': assessment.get('model_id'),
                    'captured_at': assessment.get('captured_at'), 'matrix': {
                        'opportunity_policy': matrix.get('opportunity_policy'),
                        'rows': [r for r in matrix.get('rows', []) if goal_market(r) or r.get('spec', {}).get('kind') == 'halves_total']}}})
    return {'read_only': True, 'provider_calls': 0, 'checkpoint_writes': 0,
        'date': date, 'observed_at': observed_at, 'source_revision': os.environ.get('GITHUB_SHA'),
        'sources': sources, 'payload': {'gold_scope': scope, 'matches': matches}}


def encrypt_report(report, public_key_path, destination):
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    public = serialization.load_pem_public_key(Path(public_key_path).read_bytes())
    key, nonce, aad = AESGCM.generate_key(bit_length=256), os.urandom(12), b'arabsniper-published-gold-v1'
    plain = gzip.compress(json.dumps(report, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode())
    encrypted = AESGCM(key).encrypt(nonce, plain, aad)
    wrapped = public.encrypt(key, padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
    envelope = {k: base64.b64encode(v).decode() for k, v in {'key': wrapped, 'nonce': nonce, 'aad': aad, 'ciphertext': encrypted}.items()}
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(envelope))


if __name__ == '__main__':
    try:
        now = datetime.now(ZoneInfo('Europe/Rome'))
        date = os.environ.get('PUBLICATION_DATE') or now.date().isoformat()
        report = collect_published(client(), os.environ['R2_BUCKET'], date, now.isoformat())
        encrypt_report(report, ROOT/'auditor/published-recipient-public.pem', ROOT/'encrypted-review/published-gold.enc.json')
        print('Published Gold verified: '+str(len(report['payload']['matches']))+' fixtures; encrypted export only; no writes or provider calls.')
    except Exception as error:
        raise SystemExit('Published Gold verification failed: '+type(error).__name__)
