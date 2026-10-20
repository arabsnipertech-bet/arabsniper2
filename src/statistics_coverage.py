"""Observable collection quality; missing observations are never converted to zero."""
from datetime import datetime, timezone
from math import isfinite


def valid(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and value >= 0


def cache_reusable(record, now):
    if not isinstance(record, dict) or record.get('stats_schema', 0) < 2:
        return False
    complete = all(valid((record.get(side+'_stats') or {}).get(metric))
                   for side in ('home', 'away') for metric in ('xg', 'shots_on_target'))
    if complete:
        return True
    try:
        observed = datetime.fromisoformat(record['checked_at'].replace('Z', '+00:00'))
        current = datetime.fromisoformat(now.replace('Z', '+00:00'))
        if observed.tzinfo is None or current.tzinfo is None:
            return False
        age = (current-observed).total_seconds()
    except (KeyError, TypeError, ValueError):
        return False
    # Errors: retry after an hour. Valid partial/empty replies: once a day.
    ttl = 3600 if record.get('source') in ('api_statistics_unavailable', 'run_budget_exhausted', 'disabled') else 86400
    return 0 <= age < ttl


def profile_coverage(rows):
    mapping = {'xg_for':'team_xg_for', 'xg_against':'team_xg_against',
               'shots_for':'team_shots_on_target_for', 'shots_against':'team_shots_on_target_against'}
    counts = {name:sum(valid(r.get(field)) for r in rows) for name,field in mapping.items()}
    sources = {}
    for r in rows:
        key = r.get('xg_source') or 'not_recorded'
        sources[key] = sources.get(key, 0)+1
    return dict(sample=len(rows), **counts, source_counts=sources,
                opportunity_matches=sum(any(valid(r.get(mapping[k])) for k in ('xg_for','shots_for')) for r in rows),
                opponent_strength_adjusted=False)
