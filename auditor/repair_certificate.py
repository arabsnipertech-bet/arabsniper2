"""Verified aggregate recovery report; private metrics leave CI only encrypted."""
import base64
import gzip
import json
import os
from pathlib import Path
from audit_storage import read_store
from auditor_history_chunks import digest


def build(root, index, state, dates, weights_sha256):
    root = Path(root)
    provenance = state['evidence_provenance']
    if not provenance.get('complete') or not set(dates) <= set(provenance.get('dates', [])):
        raise ValueError('Requested repair dates are missing from certified training provenance')
    evidence = {entry['date']:entry for entry in provenance['files']}
    days = {}
    for day in dates:
        prefix = f'auditarchive/prematch_{day}_'
        receipt = read_store(root/(prefix+'checkpoint.json'))
        expected = {prefix+suffix for suffix in ('results.json','calibration.json','daily.json','daily.md')}
        original = f'auditordata/prematch_{day}.json'
        if receipt.get('version') != 1 or receipt.get('day') != day or set(receipt.get('outputs', {})) != expected:
            raise ValueError('Incomplete repair seal: '+day)
        if index.get(original, {}).get('sha256') != receipt.get('input_sha256'):
            raise ValueError('Frozen input changed after repair: '+day)
        for path, checksum in receipt['outputs'].items():
            if index.get(path, {}).get('sha256') != checksum or digest(root/path) != checksum:
                raise ValueError('Repair output does not match seal: '+path)
        if evidence[day]['sha256'] != receipt['outputs'][prefix+'calibration.json']:
            raise ValueError('Training used different evidence from the sealed day: '+day)
        report = read_store(root/(prefix+'daily.json'))
        if report.get('audit_date') != day:
            raise ValueError('Incorrect report date: '+day)
        benchmark = report.get('four_level_benchmark') or {}
        decision = benchmark.get('decision_benchmark') or {}
        pending_summary = report.get('pending_summary')
        if pending_summary is not None and (pending_summary.get('pending') is not receipt.get('pending') or
                receipt.get('pending_summary') is not None and pending_summary != receipt['pending_summary']):
            raise ValueError('Pending counts do not match sealed day: '+day)
        versions = {}
        for snapshot in decision.get('snapshots', []):
            if snapshot.get('snapshot') == 'first':
                version = (snapshot.get('decisions') or {}).get('version', 'unknown')
                versions[version] = versions.get(version, 0)+1
        days[day] = dict(receipt=receipt, policy_id=report.get('policy_id'), source_revision=report.get('source_revision'),
            decision_path_comparison=decision.get('decision_path_comparison', {}),
            experiments=decision.get('experiments', {}), platinum_value_cohorts=decision.get('platinum_value_cohorts', {}),
            snapshot_count=decision.get('snapshot_count'), unavailable_snapshots=decision.get('unavailable_snapshots'),
            first_snapshot_versions=versions,
            pending_summary=pending_summary or dict(available=False, reason='legacy_sealed_day_without_pending_counts'),
            market_fair_protocols=decision.get('market_fair_protocols', {}),
            experiment_availability=decision.get('experiment_availability', {}))
    return dict(version=1, r2_chain_complete=True, dates=days,
        weights=dict(sha256=weights_sha256, trained_at=state.get('trained_at'), as_of=state.get('as_of'),
            evidence_provenance=provenance, mismatch_shrinkage_factor=state.get('mismatch_shrinkage_factor'),
            mismatch_symmetric_shadow=state.get('mismatch_symmetric_shadow')))


def encrypt(report, public_key, destination):
    # Same RSA-OAEP + AES-GCM construction used by the existing weekend export.
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    pub = serialization.load_pem_public_key(public_key)
    key = AESGCM.generate_key(bit_length=256)
    nonce, aad = os.urandom(12), b'arabsniper-repair-certificate-v1'
    plain = gzip.compress(json.dumps(report, separators=(',', ':'), allow_nan=False).encode())
    cipher = AESGCM(key).encrypt(nonce, plain, aad)
    wrapped = pub.encrypt(key, padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None))
    envelope = {k:base64.b64encode(v).decode() for k,v in dict(key=wrapped,nonce=nonce,aad=aad,ciphertext=cipher).items()}
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(envelope))
