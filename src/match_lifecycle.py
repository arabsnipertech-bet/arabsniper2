"""Observed match status only. Never rewrite prematch probabilities or invent live odds."""
from copy import deepcopy


def observed_live_state(status_row, observed_at, previous=None):
    if not status_row:
        result = deepcopy(previous) if isinstance(previous, dict) else {}
        result.update(refresh_failed=True, attempted_at=observed_at)
        return result
    fixture = status_row.get('fixture') or {}
    status = fixture.get('status') or {}
    scores = status_row.get('score') or {}
    return {
        'status': status.get('short'), 'elapsed': status.get('elapsed'),
        'goals': deepcopy(status_row.get('goals') or {}),
        'halftime': deepcopy(scores.get('halftime') or {}),
        'fulltime': deepcopy(scores.get('fulltime') or {}),
        'observed_at': observed_at, 'refresh_failed': False,
        'source': 'fixtures_api', 'live_odds_available': False,
    }
