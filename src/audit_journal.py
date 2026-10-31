"""Immutable per-scan evidence; historical merges run only in the nightly Auditor."""
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_ledger import AuditLedger, capture_ledger
from audit_storage import read_store, write_store
from multimarket_model import stamp

PREFIX = 'arab_quote_scan_journal_'


def journal_path(directory, kind, observed_at):
    at = stamp(observed_at)
    if not at:
        raise ValueError('Journal requires an aware observation time')
    # UTC sorts scans chronologically across DST changes; each phase is separate.
    return Path(directory)/(PREFIX + at.astimezone(ZoneInfo('UTC')).strftime('%Y%m%dT%H%M%S%f')
                            + '_' + kind + '.json')


def capture_forecasts(ns, output):
    now = ns['now_rome']()
    path = journal_path(ns['DATA_DIR'], 'forecasts', now.isoformat())
    if path.exists():
        raise ValueError('Refusing to overwrite frozen scan evidence')
    # Deliberately start empty: no indexing or settlement of historical records.
    with AuditLedger() as ledger:
        ledger.metadata.update(schema_version=1, journal_type='forecasts')
        capture_ledger(ledger, output, ns['capture_prematch_predictions'], now)
        ledger.write(path)
    print('Frozen scan evidence: ' + path.name, flush=True)
    return path


def capture_policies(root, observed_at=None):
    from gold_policy_audit import capture as gold_capture
    from platinum_audit import capture as platinum_capture
    root = Path(root)
    now = observed_at or datetime.now(ZoneInfo('Europe/Rome')).isoformat()
    path = journal_path(root/'data', 'policies', now)
    if path.exists():
        raise ValueError('Refusing to overwrite frozen policy evidence')
    gold, platinum, seen = {'fixtures': {}}, {'fixtures': {}}, set()
    for day in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(day, {}).get('matches', []):
            fid = str(match.get('fixture', {}).get('fixture_id'))
            if fid in seen:
                continue
            seen.add(fid)
            # These existing functions validate decisions and kickoff boundaries.
            gold_capture(gold, match, now)
            platinum_capture(platinum, match, now)
    write_store(path, {'schema_version': 1, 'journal_type': 'policies',
                      'observed_at': now, 'gold': gold, 'platinum': platinum})
    print(f'Frozen current policy evidence: Gold {len(gold["fixtures"])}; '
          f'Platinum {len(platinum["fixtures"])}; no historical reports', flush=True)
    return path


def merge_policy(target, incoming, kind):
    for fid, item in incoming.get('fixtures', {}).items():
        if str(item.get('fixture', {}).get('fixture_id')) != fid:
            raise ValueError('Policy journal fixture mismatch')
        destination = target.setdefault('fixtures', {}).setdefault(fid,
            {**deepcopy(item), 'snapshots': []})
        known = {s['sha256'] for s in destination['snapshots']}
        for snapshot in item.get('snapshots', []):
            now, kickoff, decision = (stamp(snapshot.get(k)) for k in
                                      ('observed_at', 'kickoff', 'decision_at'))
            if not now or not kickoff or not decision or not decision <= now < kickoff:
                raise ValueError('Policy journal lacks prematch evidence')
            payload = {k: v for k, v in snapshot.items() if k != 'sha256'
                       and (kind != 'platinum' or k != 'observed_at')}
            digest = sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
            if digest != snapshot.get('sha256'):
                raise ValueError('Policy journal checksum mismatch')
            if digest not in known:
                destination['snapshots'].append(deepcopy(snapshot))
                known.add(digest)
        destination['snapshots'].sort(key=lambda s: stamp(s['observed_at']))


def merge_forecast(ledger, collection, key, record):
    if collection == 'analysis_updates' and key not in ledger.get(collection, {}):
        # This descriptive label depends on earlier scans; derive
        # it at ingestion without changing any frozen prediction.
        previous = [r for c, _, r in ledger.records(
            fixture_id=record.get('fixture', {}).get('fixture_id'))
            if c == collection and r.get('model_version') == record.get('model_version')
            and stamp(r.get('captured_at')) < stamp(record.get('captured_at'))]
        old = max(previous, key=lambda r: stamp(r['captured_at']), default={})
        first = (old.get('multimarket') or {}).get('best_candidate') or {}
        last = (record.get('multimarket') or {}).get('best_candidate') or {}
        record['update_reason'] = ('prima_analisi' if not previous else
            'proposta_cambiata' if (first.get('market'), first.get('selection')) !=
            (last.get('market'), last.get('selection')) else 'aggiornamento_dati_e_quote')
    # First forecast is immutable; later observations have unique keys.
    ledger.put(collection, key, record, overwrite=False)


def ingest(root, after_forecasts=None):
    """Merge in observation order; reruns preserve first forecasts and deduplicate."""
    from gold_policy_audit import STUDY_ID, POLICY_ID as GOLD_POLICY
    from platinum_policy import POLICY_ID as PLATINUM_POLICY
    root = Path(root)
    paths = sorted((root/'data').glob(PREFIX+'*.json'))
    forecasts = [p for p in paths if p.name.endswith('_forecasts.json')]
    policies = [p for p in paths if p.name.endswith('_policies.json')]
    if len(forecasts) + len(policies) != len(paths):
        raise ValueError('Unknown audit journal phase')
    if forecasts:
        target = root/'data/arab_quote_prematch_audit.json'
        with AuditLedger(target) as ledger:
            ledger.metadata.setdefault('schema_version', 1)
            ledger.add_collection('fixtures')
            for path in forecasts:
                with AuditLedger(path) as journal:
                    if journal.metadata.get('journal_type') != 'forecasts':
                        raise ValueError('Invalid forecast journal')
                    for collection, key, record in journal.records():
                        merge_forecast(ledger, collection, key, record)
                    ledger.metadata['updated_at'] = journal.metadata['updated_at']
            ledger.write(target)
            if after_forecasts is not None:
                after_forecasts(ledger)
    if policies:
        targets = {'gold': root/'data/arab_quote_gold_policy_audit.json',
                   'platinum': root/'data/arab_quote_platinum_audit.json'}
        stores = {'gold': read_store(targets['gold'], {'study_id': STUDY_ID,
                    'policy_id': GOLD_POLICY, 'fixtures': {}}),
                  'platinum': read_store(targets['platinum'], {'schema_version': 1,
                    'policy_id': PLATINUM_POLICY, 'fixtures': {}})}
        for path in policies:
            journal = read_store(path)
            if journal.get('schema_version') != 1 or journal.get('journal_type') != 'policies':
                raise ValueError('Invalid policy journal')
            for kind in targets:
                merge_policy(stores[kind], journal[kind], kind)
        for kind, target in targets.items():
            write_store(target, stores[kind])
    # Delete pending pointers only after every merge succeeds. Publication of the
    # private checkpoint commits both the merged history and these deletions.
    # A failed upload leaves remote journals available for an idempotent retry.
    for path in paths:
        path.unlink()
    print(f'Nightly journal ingestion complete: {len(paths)} batches', flush=True)
    return len(paths)


if __name__ == '__main__':
    import sys
    root = Path(__file__).resolve().parents[1]
    if sys.argv[1:] == ['capture']:
        capture_policies(root)
    elif sys.argv[1:] == ['ingest']:
        ingest(root)
    else:
        raise SystemExit('Expected capture or ingest')
