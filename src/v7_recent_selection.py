"""Re-elect published slots until T-60; retain the last recent real choice.

This clock never changes a probability, an EV, a quote timestamp or a journal.
The three-hour window covers two ordinary Fast slots. It is not a guarantee
that a scan ran at T-60, and an old or late choice must not claim that it did.
"""
from copy import deepcopy
from datetime import timedelta

from multimarket_model import stamp
from v7_probability import finite, utc

POLICY = dict(id='TRIAD_RECENT_SELECTION_V1', enabled=True,
              freeze_minutes=60, max_final_age_minutes=180)


def clock(as_of, kickoff, previous, policy=None):
    policy = POLICY if policy is None else policy
    if policy.get('enabled') is not True:
        return None
    at, start = stamp(as_of), stamp(kickoff)
    cutoff = start-timedelta(minutes=policy['freeze_minutes'])
    window = start-timedelta(minutes=policy['max_final_age_minutes'])
    reselect = at <= cutoff
    return dict(id=policy['id'], evaluated_at=utc(at), freeze_at=utc(cutoff),
                window_start=utc(window), reselect=reselect,
                recent_window=at >= window,
                status=('frozen' if at == cutoff else 'provisional') if reselect else 'frozen' if previous else 'late')


def recent_quote(record, timing):
    quote = record.get('quote') or {}
    observed, updated, at, window = (stamp(value) for value in
        (quote.get('quote_observed_at'), quote.get('provider_updated_at'),
         timing['evaluated_at'], timing['window_start']))
    return bool(observed and updated and window <= updated <= observed <= at
                and quote.get('bookmaker_id') is not None)


def routing_records(records, timing, previous):
    if not timing:
        return records
    if not timing['reselect'] and previous:
        # Missing slots stay missing after the deadline. Never slip a new
        # market into a frozen role using information from the last hour.
        held = {slot['market_id'] for slot in previous.values()}
        return {key: row for key, row in records.items() if key in held}
    if timing['recent_window']:
        # Stale executable prices remain available in the complete matrix and
        # the immutable journals, but cannot win the recent selection. Genuine
        # unpriced estimates may still appear explicitly as Studio.
        return {key: row for key, row in records.items()
                if not finite(row.get('odd')) or recent_quote(row, timing)}
    return records


def recent_commitment(slot, timing):
    proof = slot.get('selection_timing') or {}
    confirmed = stamp(proof.get('confirmed_at'))
    observed, updated = (stamp(proof.get(key)) for key in
                         ('quote_observed_at', 'provider_updated_at'))
    window, cutoff, at = (stamp(timing[k]) for k in
                          ('window_start', 'freeze_at', 'evaluated_at'))
    return bool(proof.get('id') == timing['id'] and confirmed and observed and updated
                and window <= confirmed <= min(cutoff, at)
                and window <= updated <= observed <= confirmed
                and proof.get('recent_quote') is True
                and stamp(slot.get('captured_at')) == confirmed
                and stamp(proof.get('freeze_at')) == cutoff)


def save(triad, records, previous, timing):
    """New confirmations refresh their own anchor; replaced choices stay intact."""
    previous = previous or {}
    saved = {}
    for role, pick in triad.items():
        old = previous.get(role)
        if not timing['reselect'] and old:
            slot = deepcopy(old)
            ready = recent_commitment(slot, timing)
            status = 'frozen' if ready else ('late' if
                (old.get('selection_timing') or {}).get('status') == 'late' else 'stale')
            proof = dict(slot.get('selection_timing') or {})
            proof.update(id=timing['id'], freeze_at=timing['freeze_at'], status=status)
            # Keep the original confirmation and source times. A later read is
            # never evidence of a new prematch observation.
            slot['selection_timing'] = proof
        elif pick and finite(pick.get('odd')) and pick['odd'] > 1:
            slot = {key: deepcopy(pick[key]) for key in ('market_id', 'label', 'odd', 'horizon')}
            slot.update(captured_at=timing['evaluated_at'], study_only=pick.get('study_only') is True)
            for key in ('ev_recovery', 'selection_policy'):
                if pick.get(key):
                    slot[key] = deepcopy(pick[key])
            record = records[pick['market_id']]
            proof = dict(id=timing['id'], confirmed_at=timing['evaluated_at'],
                         freeze_at=timing['freeze_at'], status='provisional' if timing['reselect'] else 'late',
                         recent_quote=recent_quote(record, timing),
                         **{key: (record.get('quote') or {}).get(key)
                            for key in ('quote_observed_at', 'provider_updated_at')})
            if timing['evaluated_at'] == timing['freeze_at'] and proof['recent_quote']:
                proof['status'] = 'frozen'
            slot['selection_timing'] = proof
            if old:
                if (old.get('market_id') != slot['market_id'] or
                        (old.get('selection_timing') or {}).get('id') != timing['id']):
                    slot['replaces'] = deepcopy(old)
                elif old.get('replaces'):
                    slot['replaces'] = deepcopy(old['replaces'])
        else:
            continue
        saved[role] = slot
    if not timing['reselect']:
        statuses = {slot['selection_timing']['status'] for slot in saved.values()}
        timing['status'] = ('stale' if 'stale' in statuses else
                            'late' if 'late' in statuses or not saved else 'frozen')
        # Incomplete/late evidence is a visible study, not a certified proposal.
        # No past commitment is edited: these are new publication objects.
        for role, pick in triad.items():
            slot = saved.get(role)
            if not slot or slot['selection_timing']['status'] != 'frozen':
                if pick:
                    pick.update(study_only=True, tier='BLU', badge='◆ BLU')
                if slot:
                    slot['study_only'] = True
    return saved
