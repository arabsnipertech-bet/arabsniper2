"""Bounded daily public projection; Gold payloads never reach the homepage."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import math
from audit_storage import read_store, atomic_bytes
import json

ROME = ZoneInfo('Europe/Rome')
MARKETS = {'over15_ht', 'over25_ht', 'gg_ht', 'over25_ft', 'over35_ft'}

def stamp(value):
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt if dt.tzinfo else None
    except (TypeError, ValueError, AttributeError):
        return None

def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)

def build_preview(payload, previous=None, now=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ROME).date().isoformat()
    result = {'date': today, 'updated_at': now.isoformat(), 'source_updated_at': payload.get('updated_at'), 'selections': []}
    # Keep the day's public set and prematch snapshot stable: refreshes cannot leak more picks.
    if previous and previous.get('date') == today and previous.get('selections'):
        return previous
    updated = stamp(payload.get('updated_at'))
    if payload.get('date') != today or not updated or not 0 <= (now-updated).total_seconds() <= 86400:
        return result
    candidates = []
    for match in payload.get('matches', []):
        f = match.get('fixture', {})
        a = match.get('multimarket', {})
        r = a.get('proposals_by_min_odd', {}).get('1.5') or a.get('best_candidate') or {}
        kickoff = stamp(f.get('kickoff'))
        p, b, odd = r.get('statistical_pct'), r.get('book_probability_pct'), r.get('odd')
        if not kickoff or kickoff <= now or kickoff.astimezone(ROME).date().isoformat() != today:
            continue
        if f.get('status_short') != 'NS' or not f.get('fixture_id') or r.get('market') not in MARKETS:
            continue
        if r.get('primary_eligible') is not True or r.get('statistical_reliability') in ('context_conflict', 'fragile_halftime'):
            continue
        if not all(number(v) for v in (p,b,odd)) or not (50 <= p <= 100 and 50 <= b <= 100 and abs(p-b) <= 10 and odd >= 1.5):
            continue
        if (r.get('coverage') or 0) < .75:
            continue
        row = {'fixture_id': str(f['fixture_id']), 'match': f.get('match', ''), 'league': f.get('league', ''), 'kickoff': f['kickoff'],
               'label': r.get('label', ''), 'market': r['market'], 'odd': odd, 'statistical_pct': p, 'book_probability_pct': b,
               'captured_at': a.get('captured_at') or payload['updated_at']}
        candidates.append((min(p,b), r.get('support_score') or 0, row))
    candidates.sort(key=lambda x: (-x[0], -x[1], x[2]['kickoff'], x[2]['fixture_id']))
    seen = set()
    for _, _, row in candidates:
        if row['fixture_id'] in seen: continue
        seen.add(row['fixture_id']); result['selections'].append(row)
        if len(result['selections']) == 3: break
    return result

def publish(directory='data'):
    directory = Path(directory)
    destination = directory / 'arab_quote_homepage.json'
    value = build_preview(read_store(directory/'arab_quote_day1.json', {}), read_store(destination, {}))
    atomic_bytes(destination, (json.dumps(value, ensure_ascii=False, separators=(',', ':'))+'\n').encode())
    return value

if __name__ == '__main__': publish()
