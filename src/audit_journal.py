"""Immutable per-scan evidence; historical merges run only in the nightly Auditor."""
from copy import deepcopy
from datetime import datetime, timezone
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
    from selection_studies import freeze_study
    studies, publications = {}, {}
    gold, platinum, seen = {'fixtures': {}}, {'fixtures': {}}, set()
    for day in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(day, {}).get('matches', []):
            fid = str(match.get('fixture', {}).get('fixture_id'))
            if fid in seen:
                continue
            seen.add(fid)
            # These existing functions validate decisions and kickoff boundaries.
            study = freeze_study(match, now)
            if study:
                studies[f'{fid}::{now}'] = study
            meta, assessment = match.get('fixture') or {}, match.get('multimarket') or {}
            at, kickoff = stamp(now), stamp(meta.get('kickoff'))
            if (at and kickoff and at < kickoff and meta.get('status_short') in ('NS','TBD')
                    and not meta.get('result') and assessment.get('model_id')):
                key = f"{assessment['model_id']}::{fid}::publication::{now}"
                publications[key] = deepcopy(dict(fixture=meta, captured_at=now,
                    model_version=assessment['model_id'], multimarket=assessment, publication_snapshot=True))
            gold_capture(gold, match, now)
            platinum_capture(platinum, match, now)
    write_store(path, {'schema_version': 1, 'journal_type': 'policies',
                      'observed_at': now, 'gold': gold, 'platinum': platinum, 'study_observations': studies, 'analysis_updates': publications})
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


class ForecastHistory:
    """Disposable compact lookup for recovery's descriptive update labels.

    The durable forecast is still inserted unchanged by merge_forecast. Keeping
    the preceding candidate on disk avoids parsing every earlier full payload
    for every new observation. Unusual legacy metadata uses the original path.
    """
    def __init__(self, ledger):
        self.ledger = ledger
        self.unsupported = set()
        ledger.db.execute('CREATE TABLE recovery_forecast_history ('
            'position INTEGER PRIMARY KEY, record_key TEXT UNIQUE, fixture_id TEXT, '
            'model_version TEXT, captured_at TEXT, candidate TEXT)')
        ledger.db.execute('CREATE INDEX recovery_forecast_lookup ON '
            'recovery_forecast_history(fixture_id,model_version,captured_at DESC,position ASC)')
        for key, record in ledger.get('analysis_updates', {}).items():
            self.add(key, record)

    def add(self, key, record):
        fid = (record.get('fixture') or {}).get('fixture_id')
        model = record.get('model_version')
        if fid is None or not isinstance(model, str):
            return
        observed = stamp(record.get('captured_at'))
        if observed is None:
            self.unsupported.add((str(fid), model))
            return
        candidate = (record.get('multimarket') or {}).get('best_candidate') or {}
        self.ledger.db.execute('INSERT OR IGNORE INTO recovery_forecast_history '
            '(record_key,fixture_id,model_version,captured_at,candidate) VALUES(?,?,?,?,?)',
            (str(key), str(fid), model,
             observed.astimezone(timezone.utc).isoformat(timespec='microseconds'),
             json.dumps(candidate, ensure_ascii=False, separators=(',', ':'), allow_nan=False)))

    def previous(self, record):
        fid = (record.get('fixture') or {}).get('fixture_id')
        model = record.get('model_version')
        observed = stamp(record.get('captured_at'))
        if (fid is None or not isinstance(model, str) or observed is None
                or (str(fid), model) in self.unsupported):
            return NotImplemented
        row = self.ledger.db.execute('SELECT candidate FROM recovery_forecast_history '
            'WHERE fixture_id=? AND model_version=? AND captured_at<? '
            'ORDER BY captured_at DESC,position ASC LIMIT 1',
            (str(fid), model, observed.astimezone(timezone.utc).isoformat(timespec='microseconds'))).fetchone()
        return {'multimarket': {'best_candidate': json.loads(row[0])}} if row else None


def merge_forecast(ledger, collection, key, record, *, history=None):
    if collection == 'analysis_updates' and key not in ledger.get(collection, {}):
        # This descriptive label depends on earlier scans; derive
        # it at ingestion without changing any frozen prediction.
        old = history.previous(record) if history is not None else NotImplemented
        if old is NotImplemented:
            previous = [r for c, _, r in ledger.records(
                fixture_id=record.get('fixture', {}).get('fixture_id'))
                if c == collection and r.get('model_version') == record.get('model_version')
                and stamp(r.get('captured_at')) < stamp(record.get('captured_at'))]
            old = max(previous, key=lambda r: stamp(r['captured_at']), default=None)
        has_previous = old is not None
        old = old or {}
        first = (old.get('multimarket') or {}).get('best_candidate') or {}
        last = (record.get('multimarket') or {}).get('best_candidate') or {}
        record['update_reason'] = ('prima_analisi' if not has_previous else
            'proposta_cambiata' if (first.get('market'), first.get('selection')) !=
            (last.get('market'), last.get('selection')) else 'aggiornamento_dati_e_quote')
    # First forecast is immutable; later observations have unique keys.
    inserted = ledger.put(collection, key, record, overwrite=False)
    if inserted and history is not None and collection == 'analysis_updates':
        history.add(key, record)


def pending_journals(root, before=None):
    paths = sorted((Path(root)/'data').glob(PREFIX+'*.json'))
    if before is not None:
        if before.tzinfo is None:
            raise ValueError('Journal cutoff requires a timezone')
        cutoff = PREFIX+before.astimezone(ZoneInfo('UTC')).strftime('%Y%m%dT%H%M%S%f')
        paths = [p for p in paths if p.name < cutoff]
    return paths


def ingest(root, after_forecasts=None, *, limit=None, partition_delta=False, before=None):
    """Merge in observation order; reruns preserve first forecasts and deduplicate."""
    from gold_policy_audit import STUDY_ID, POLICY_ID as GOLD_POLICY
    from platinum_policy import POLICY_ID as PLATINUM_POLICY
    root = Path(root)
    paths = pending_journals(root, before)
    if limit is not None:
        if limit < 1:
            raise ValueError('Journal limit must be positive')
        paths = paths[:limit]
    forecasts = [p for p in paths if p.name.endswith('_forecasts.json')]
    policies = [p for p in paths if p.name.endswith('_policies.json')]
    if len(forecasts) + len(policies) != len(paths):
        raise ValueError('Unknown audit journal phase')
    if forecasts or policies:
        target = root/'data/arab_quote_prematch_audit.json'
        with AuditLedger(target) as ledger, AuditLedger() as delta:
            delta.add_collection('fixtures')
            ledger.metadata.setdefault('schema_version', 1)
            ledger.add_collection('fixtures')
            for path in forecasts:
                with AuditLedger(path) as journal:
                    if journal.metadata.get('journal_type') != 'forecasts':
                        raise ValueError('Invalid forecast journal')
                    for collection, key, record in journal.records():
                        merge_forecast(ledger, collection, key, record)
                        delta.put(collection, key, ledger[collection][key], overwrite=False)
                    ledger.metadata['updated_at'] = journal.metadata['updated_at']
            for path in policies:
                journal = read_store(path)
                for key, record in journal.get('analysis_updates', {}).items():
                    merge_forecast(ledger, 'analysis_updates', key, record)
                    delta.put('analysis_updates', key, ledger['analysis_updates'][key], overwrite=False)
                for key, record in journal.get('study_observations', {}).items():
                    ledger.put('study_observations', key, record, overwrite=False)
                    delta.put('study_observations', key, ledger['study_observations'][key], overwrite=False)
            ledger.write(target)
            if after_forecasts is not None:
                after_forecasts(delta if partition_delta else ledger)
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
