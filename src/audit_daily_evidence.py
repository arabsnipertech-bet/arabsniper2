"""Private, date-specific proof of frozen forecasts and verified results."""
from pathlib import Path

from audit_storage import read_store, write_store
from auditor_history_chunks import digest
from multimarket_model import stamp


def receipt_path(day):
    return f'auditarchive/prematch_{day}_v7_evidence.json'


def evidence_paths(day):
    return {f'auditordata/prematch_{day}.json',
            *(f'auditarchive/prematch_{day}_{suffix}' for suffix in
              ('daily.json', 'daily.md', 'results.json', 'v7.json'))}


def read_receipt(root, day):
    receipt = read_store(Path(root)/receipt_path(day), {})
    if not (isinstance(receipt, dict) and receipt.get('version') == 1
            and receipt.get('date') == str(day) and stamp(receipt.get('checked_at'))
            and isinstance(receipt.get('fixtures'), dict)):
        return None
    return receipt


def verified_evidence(root, day):
    receipt = read_receipt(root, day)
    paths = evidence_paths(day)
    hashes = (receipt or {}).get('sha256', {})
    if not isinstance(hashes, dict) or hashes.keys() != paths:
        return None
    root = Path(root)
    if any(not (root/path).is_file() or hashes[path] != digest(root/path) for path in paths):
        return None
    return receipt


def seal_evidence(root, day, now, fixtures, scores, pending):
    root = Path(root)
    receipt = {'version': 1, 'date': str(day), 'checked_at': now.isoformat(),
               'sha256': {path: digest(root/path) for path in sorted(evidence_paths(day))},
               'fixtures': {fid: {'kickoff': kickoff, 'result': scores.get(fid), 'pending': fid in pending}
                            for fid, kickoff in sorted(fixtures.items())},
               'pending_fixtures': len(pending)}
    write_store(root/receipt_path(day), receipt)
    return receipt
