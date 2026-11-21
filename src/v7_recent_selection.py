"""Record the latest actual prematch election without changing admission.

The archived first choice is history, not a lock on the current ranking.
Quote usability and quality remain the responsibility of the existing engine.
"""
from copy import deepcopy

from v7_probability import finite

POLICY_ID = 'TRIAD_LATEST_PREMATCH_V1'


def save(triad, captured_at, previous):
    saved = {}
    for role, pick in triad.items():
        if not pick or not finite(pick.get('odd')) or pick['odd'] <= 1:
            continue
        slot = {key: deepcopy(pick[key]) for key in ('market_id', 'label', 'odd', 'horizon')}
        slot.update(captured_at=captured_at, study_only=pick.get('study_only') is True,
                    selection_timing=dict(id=POLICY_ID, confirmed_at=captured_at))
        for key in ('ev_recovery', 'selection_policy'):
            if pick.get(key):
                slot[key] = deepcopy(pick[key])
        old = (previous or {}).get(role)
        if old:
            if (old.get('market_id') != slot['market_id'] or
                    (old.get('selection_timing') or {}).get('id') != POLICY_ID):
                slot['replaces'] = deepcopy(old)
            elif old.get('replaces'):
                slot['replaces'] = deepcopy(old['replaces'])
        saved[role] = slot
    return saved
