"""Deduplicated fixture counts behind the daily ledger's pending flag."""
from collections import Counter
from multimarket_model import stamp

COLLECTIONS = ('fixtures', 'quality_evaluations', 'multimarket_evaluations',
               'signal_observations', 'analysis_updates', 'study_observations')


def unresolved(record):
    outcomes = (record.get('settlement') or {}).get('outcomes')
    return not outcomes or any(value is None for value in outcomes.values())


def summarize(store):
    pending, gold, known = set(), set(), set()
    records, unknown = 0, 0
    reasons = Counter()
    for collection in COLLECTIONS:
        for _, record in store.get(collection, {}).items():
            meta = record.get('fixture') or {}
            fid = meta.get('fixture_id')
            fid = str(fid) if fid is not None else None
            if fid is not None:
                known.add(fid)
                at, kickoff = stamp(record.get('captured_at')), stamp(meta.get('kickoff'))
                rows = ((record.get('multimarket') or {}).get('matrix') or {}).get('rows', [])
                if not record.get('retrospective') and at and kickoff and at < kickoff and any(
                        (r.get('opportunity') or {}).get('eligible') is True and
                        (r.get('opportunity') or {}).get('rank') == 1 for r in rows):
                    gold.add(fid)
            if not unresolved(record):
                continue
            records += 1
            if fid is None:
                unknown += 1
            else:
                pending.add(fid)
            outcomes = (record.get('settlement') or {}).get('outcomes')
            reasons['missing_outcomes' if not outcomes else 'unresolved_outcome'] += 1
    non_gold = pending-gold
    return dict(version='daily-pending-fixtures-v1', available=True,
        pending=bool(records), pending_records_count=records,
        pending_fixtures_count=len(pending), non_gold_pending_fixtures_count=len(non_gold),
        gold_pending_fixtures_count=len(pending & gold),
        pending_records_without_fixture_id=unknown,
        non_gold_pending_fixture_ids=sorted(non_gold), gold_pending_fixture_ids=sorted(pending & gold),
        observed_fixtures_count=len(known), record_reasons=dict(reasons),
        gold_definition='any valid prematch eligible rank-1 Gold observed for the fixture',
        scope='all daily ledger collections; fixture IDs deduplicated; not shadow pending_choices')
