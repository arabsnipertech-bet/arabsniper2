"""Explicit repair retraining from verified compact R2 evidence, under writer lock.

Does not rewrite historical forecasts or change policy parameters. Scheduled
training keeps its existing time window; this entry point is reserved for the
separate learning-chain repair workflow.
"""
from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from adaptive_calibration import WEIGHTS, nightly_state
from audit_storage import atomic_bytes
from current_audit import TZ
from gold_storage import client, previous_manifest, sha, write_manifest
from recover_daily import restore_subset


def refresh(storage, bucket, root, now, *, certify_dates=()):
    index = previous_manifest(storage, bucket)
    paths = [p for p in index if p.startswith('auditarchive/prematch_') and
             p.endswith(('_daily.json', '_results.json', '_calibration.json', '_checkpoint.json'))]
    paths += [f'auditarchive/prematch_{day}_daily.md' for day in certify_dates]
    paths += [p for p in (WEIGHTS, 'data/arab_quote_prediction_audit.json') if p in index]
    restore_subset(storage, bucket, root, index, paths)
    # A dated archive without ANY report must still prevent a complete certificate.
    expected = {p.removeprefix('auditordata/prematch_').removesuffix('.json')
                for p in index if p.startswith('auditordata/prematch_') and p.endswith('.json')}
    state = nightly_state(root, now.astimezone(TZ).date())
    from datetime import date
    from adaptive_calibration import WINDOW
    evidence = state['training_evidence']
    seen = {r['date'] for r in evidence['files']}
    expected -= seen | set(state['evidence_provenance'].get('excluded_recovery_dates', ()))
    absent = [d for d in expected if 1 <= (now.astimezone(TZ).date()-date.fromisoformat(d)).days <= WINDOW]
    evidence['missing_dates'] = sorted(set(evidence['missing_dates']) | set(absent))
    evidence['complete'] = not evidence['missing_dates'] and not evidence['missing_results_dates']
    if not evidence['complete']:
        raise ValueError('Incomplete learning chain: '+json.dumps(evidence, sort_keys=True))
    state['trained_at'] = now.isoformat()
    raw = (json.dumps(state, sort_keys=True, indent=2, allow_nan=False)+'\n').encode()
    digest = sha(raw)
    certificate = None
    if certify_dates:
        from repair_certificate import build
        certificate = build(root, index, state, certify_dates, digest)
    storage.put_object(Bucket=bucket, Key='state/objects/'+digest, Body=raw)
    if previous_manifest(storage, bucket) != index:
        raise RuntimeError('Checkpoint advanced during calibration repair; retry under writer lock')
    write_manifest(storage, bucket, {**index, WEIGHTS: {'sha256': digest, 'bytes': len(raw)}})
    atomic_bytes(root/WEIGHTS, raw)
    if certificate is not None:
        atomic_bytes(root/'repair-certificate.json', json.dumps(certificate, allow_nan=False).encode())
    return state


def main():
    if (os.getenv('GITHUB_REF') != 'refs/heads/main' or os.getenv('GOLD_PRIVATE') != 'true'
            or os.getenv('GITHUB_WORKFLOW') != 'Repair learning chain'):
        raise ValueError('Explicit private learning-chain repair workflow required')
    from private_publication import publish
    with TemporaryDirectory() as folder:
        root = Path(folder)
        dates = json.loads(os.getenv('CERTIFY_DATES', '["2026-11-05","2026-11-06"]'))
        state = refresh(client(), os.environ['R2_BUCKET'], root, datetime.now(TZ), certify_dates=dates)
        from repair_certificate import encrypt
        encrypt(json.loads((root/'repair-certificate.json').read_text()),
                (ROOT/'auditor/quant-verification-public.pem').read_bytes(),
                ROOT/'encrypted-review/repair-certificate.enc.json')
        atomic_bytes(ROOT/WEIGHTS, (root/WEIGHTS).read_bytes())
        publish(ROOT, {WEIGHTS})
        print(json.dumps({'training_evidence': state['training_evidence'], 'trained_at': state['trained_at']}, sort_keys=True))


if __name__ == '__main__':
    main()
