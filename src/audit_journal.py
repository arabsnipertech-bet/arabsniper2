"""Immutable per-scan evidence; historical merges run only in the nightly Auditor."""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, time, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_ledger import AuditLedger, capture_ledger, kickoff_day
from audit_storage import read_store, write_store
from multimarket_model import stamp

PREFIX = 'arab_quote_scan_journal_'
DATED_PREFIX = 'arab_quote_scan_dated_'
KINDS = ('forecasts', 'policies')
ROME = ZoneInfo('Europe/Rome')
LEGACY_PRIORITY_DAYS = 7


def journal_path(directory, kind, observed_at, fixture_day=None):
    at = stamp(observed_at)
    if not at:
        raise ValueError('Journal requires an aware observation time')
    # UTC sorts scans chronologically across DST changes; each phase is separate.
    scope = '' if fixture_day is None else '_' + (fixture_day.replace('-', '') or 'undated')
    prefix = PREFIX if fixture_day is None else DATED_PREFIX
    return Path(directory)/(prefix + at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
                            + scope + '_' + kind + '.json')


def journal_fields(path):
    name = Path(path).name
    prefix = DATED_PREFIX if name.startswith(DATED_PREFIX) else PREFIX
    if not name.startswith(prefix) or not name.endswith('.json'):
        return None
    observed, *scope, kind = name[len(prefix):-5].split('_')
    if len(scope) != (1 if prefix == DATED_PREFIX else 0) or kind not in KINDS:
        raise ValueError('Unknown audit journal phase')
    return observed, scope[0] if scope else None, kind


def journals_for_day(paths, day, *, legacy_days=None):
    """Select exact kickoff-date shards and compatible legacy mixed-date journals."""
    end = datetime.combine(day+timedelta(days=1), time.min, ROME)
    cutoff = end.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    earliest = ((end-timedelta(days=legacy_days)).astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
                if legacy_days else '')
    target = day.strftime('%Y%m%d')
    for path in paths:
        fields = journal_fields(path)
        if fields and fields[0] < cutoff and (fields[1] == target or fields[1] is None and fields[0] >= earliest):
            yield path


def new_journal(directory, kind, observed_at):
    original = journal_path(directory, kind, observed_at)
    prefix = DATED_PREFIX + original.stem[len(PREFIX):].removesuffix('_'+kind)
    if original.exists() or any(original.parent.glob(prefix+'_*_'+kind+'.json')):
        raise ValueError('Refusing to overwrite frozen scan evidence')


def capture_forecasts(ns, output):
    now = ns['now_rome']()
    directory = ns['DATA_DIR']
    new_journal(directory, 'forecasts', now)
    paths = []
    # Deliberately start empty: no indexing or settlement of historical records.
    with AuditLedger() as ledger:
        ledger.metadata.update(schema_version=1, journal_type='forecasts')
        capture_ledger(ledger, output, ns['capture_prematch_predictions'], now)
        days = [day or '' for (day,) in ledger.db.execute('SELECT DISTINCT day FROM records ORDER BY day')]
        for day in days or ['']:
            path = journal_path(directory, 'forecasts', now, day)
            ledger.write(path, day=day)
            paths.append(path)
    print(f'Frozen scan evidence: {len(paths)} kickoff-date files', flush=True)
    return tuple(paths)


def capture_policies(root, observed_at=None):
    from gold_policy_audit import capture as gold_capture
    from platinum_audit import capture as platinum_capture
    root = Path(root)
    now = observed_at or datetime.now(ROME).isoformat()
    new_journal(root/'data', 'policies', now)
    from selection_studies import freeze_study

    def policy_journal():
        return {'schema_version': 1, 'journal_type': 'policies', 'observed_at': now,
                'gold': {'fixtures': {}}, 'platinum': {'fixtures': {}},
                'study_observations': {}, 'analysis_updates': {}}

    journals, seen = defaultdict(policy_journal), set()
    for day in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(day, {}).get('matches', []):
            fid = str(match.get('fixture', {}).get('fixture_id'))
            if fid in seen:
                continue
            seen.add(fid)
            meta, assessment = match.get('fixture') or {}, match.get('multimarket') or {}
            journal = journals[kickoff_day(meta) or '']
            # These existing functions validate decisions and kickoff boundaries.
            study = freeze_study(match, now)
            if study:
                journal['study_observations'][f'{fid}::{now}'] = study
            at, kickoff = stamp(now), stamp(meta.get('kickoff'))
            if (at and kickoff and at < kickoff and meta.get('status_short') in ('NS','TBD')
                    and not meta.get('result') and assessment.get('model_id')):
                key = f"{assessment['model_id']}::{fid}::publication::{now}"
                journal['analysis_updates'][key] = deepcopy(dict(fixture=meta, captured_at=now,
                    model_version=assessment['model_id'], multimarket=assessment, publication_snapshot=True))
            gold_capture(journal['gold'], match, now)
            platinum_capture(journal['platinum'], match, now)
    # The browser and social feeds do not need per-operator price evidence.
    from v7_archive import quote_evidence, read_quote_state, valid_snapshot
    forecasts = [(record, (record['multimarket'].get('matrix') or {}).get('v7'))
                 for journal in journals.values() for record in journal['analysis_updates'].values()]
    forecasts = [(record, v7) for record, v7 in forecasts if v7]
    quotes = read_quote_state(root/'data/arab_quote_state.json', {v7['fixture_id'] for _, v7 in forecasts})
    for record, v7 in forecasts:
        if not valid_snapshot(v7, record['fixture'], record['captured_at']):
            raise ValueError('Invalid V7 prematch publication evidence')
        record['v7_quote_evidence'] = quote_evidence(quotes.get(v7['fixture_id'], {}), v7)
        if 'triad' in v7:
            record.update({key:v7[key] for key in ('triad','tier','horizon','niche_id','badge')})
    paths = []
    for day, journal in sorted(journals.items() or {'': policy_journal()}.items()):
        path = journal_path(root/'data', 'policies', now, day)
        write_store(path, journal)
        paths.append(path)
    print(f'Frozen current policy evidence: {len(paths)} kickoff-date files', flush=True)
    return tuple(paths)


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
    directory = Path(root)/'data'
    def order(path):
        observed, scope, kind = journal_fields(path)
        return observed, kind, scope or ''

    paths = sorted((*directory.glob(PREFIX+'*.json'), *directory.glob(DATED_PREFIX+'*.json')), key=order)
    if before is not None:
        if before.tzinfo is None:
            raise ValueError('Journal cutoff requires a timezone')
        cutoff = before.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
        paths = [p for p in paths if journal_fields(p)[0] < cutoff]
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
        batches = {}
        for path in paths:
            observed, _, kind = journal_fields(path)
            batches.setdefault((observed, kind), []).append(path)
        paths = [path for batch in list(batches.values())[:limit] for path in batch]
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
            history = ForecastHistory(ledger)
            for path in forecasts:
                with AuditLedger(path) as journal:
                    if journal.metadata.get('journal_type') != 'forecasts':
                        raise ValueError('Invalid forecast journal')
                    for collection, key, record in journal.records():
                        merge_forecast(ledger, collection, key, record, history=history)
                        delta.put(collection, key, ledger[collection][key], overwrite=False)
                    ledger.metadata['updated_at'] = journal.metadata['updated_at']
            for path in policies:
                journal = read_store(path)
                for key, record in journal.get('analysis_updates', {}).items():
                    merge_forecast(ledger, 'analysis_updates', key, record, history=history)
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
