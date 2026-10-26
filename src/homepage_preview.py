"""Bounded daily public projection; Gold payloads never reach the homepage."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import math
import random
from audit_storage import read_store, atomic_bytes
from statistics_first import MODEL_ID, TARGETS
from matrix_quotes import expire
from opportunity_policy import POLICY_ID, best_opportunity
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

def statistics_preview(payload, result, now):
    candidates = []
    for match in payload.get('matches', []):
        f, a = match.get('fixture') or {}, match.get('multimarket') or {}
        kickoff = stamp(f.get('kickoff'))
        if (a.get('model_id') != MODEL_ID or not kickoff or kickoff <= now or
                kickoff.astimezone(ROME).date().isoformat() != result['date'] or
                f.get('status_short') != 'NS' or not f.get('fixture_id')):
            continue
        for row in a.get('markets', []):
            p, odd, score = row.get('statistical_pct'), row.get('odd'), row.get('statistical_score')
            if (row.get('published_selection') is not True or row.get('market') not in TARGETS or
                    not all(number(v) for v in (p, odd, score)) or not (0 <= p <= 100 and odd > 1)):
                continue
            candidates.append((score, {'fixture_id':str(f['fixture_id']), 'match':f.get('match',''),
                'league':f.get('league',''), 'kickoff':f['kickoff'], 'label':row.get('label',''),
                'market':row['market'], 'odd':odd, 'statistical_pct':p,
                'market_confirmation':(row.get('market_confirmation') or {}).get('status'),
                'model_id':MODEL_ID, 'captured_at':a.get('captured_at') or payload['updated_at']}))
    candidates.sort(key=lambda pair:(-pair[0], pair[1]['kickoff'], pair[1]['fixture_id'], pair[1]['market']))
    unique = {}
    for _, row in candidates:
        unique.setdefault(row['fixture_id'], row)
    result.update(model_id=MODEL_ID, selections=list(unique.values())[:3])
    return result

def build_preview(payload, previous=None, now=None, rng=None):
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ROME).date().isoformat()
    result = {'date': today, 'updated_at': now.isoformat(), 'source_updated_at': payload.get('updated_at'), 'selections': []}
    rng = rng or random.SystemRandom()
    updated = stamp(payload.get('updated_at'))
    if payload.get('date') != today or not updated or not 0 <= (now-updated).total_seconds() <= 86400:
        return result
    if (payload.get('multimarket_method') or {}).get('model_id') == 'lateral-matrix-v5.0':
        candidates = []
        for match in payload.get('matches', []):
            f, a = match.get('fixture') or {}, match.get('multimarket') or {}
            kickoff = stamp(f.get('kickoff'))
            if (not kickoff or kickoff <= now or kickoff.astimezone(ROME).date().isoformat() != today
                    or f.get('status_short') not in ('NS', 'TBD')): continue
            # Preserve the saved snapshot while checking freshness at publication.
            view = {'matrix': {'rows': [dict(r) for r in a.get('matrix', {}).get('rows', [])]}}
            expire(view, f, now.isoformat())
            row = best_opportunity(view['matrix']['rows'])
            if row:
                candidates.append((row.get('ev_low_pct', -100), {'fixture_id': str(f['fixture_id']),
                    'match': f.get('match', ''), 'league': f.get('league', ''), 'kickoff': f['kickoff'],
                    'label': row['label'], 'market': row['spec']['kind']+'_'+row['period'],
                    'odd': row['odd'], 'statistical_pct': row['statistical_pct'],
                    'market_confirmation': row['opportunity']['signal'], 'category': row['category'],
                    'opportunity': row['opportunity'], 'oci': row.get('oci'),
                    'margin_pct': row.get('selected_margin_pct'),
                    'model_id': 'lateral-matrix-v5.0', 'captured_at': a.get('captured_at')}))
        candidates.sort(key=lambda x: (-x[0], x[1]['kickoff'], x[1]['fixture_id'], x[1]['label']))
        unique = {}
        for _, row in candidates: unique.setdefault(row['fixture_id'], row)
        result.update(model_id='lateral-matrix-v5.0', opportunity_policy=POLICY_ID, selections=list(unique.values())[:3],
                      analyzed_fixtures=(payload.get('matrix_summary') or {}).get('analyzed_fixtures', 0),
                      modeled_markets=(payload.get('matrix_summary') or {}).get('modeled_markets', 0))
        return result
    if ((payload.get('multimarket_method') or {}).get('model_id') == MODEL_ID or
            any((m.get('multimarket') or {}).get('model_id') == MODEL_ID for m in payload.get('matches', []))):
        return statistics_preview(payload, result, now)
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
    unique = {}
    for _, _, row in candidates:
        unique.setdefault(row['fixture_id'], row)
    pool = list(unique.values())
    selected = rng.sample(pool, min(3, len(pool)))
    previous_ids = {r.get('fixture_id') for r in (previous or {}).get('selections', [])}
    if (previous or {}).get('date') == today and len(pool) > 3 and {r['fixture_id'] for r in selected} == previous_ids:
        alternatives = [r for r in pool if r['fixture_id'] not in previous_ids]
        selected[-1] = rng.choice(alternatives)
    result['selections'] = sorted(selected, key=lambda r: (r['kickoff'], r['fixture_id']))
    return result

def publish(directory='data'):
    directory = Path(directory)
    destination = directory / 'arab_quote_homepage.json'
    value = build_preview(read_store(directory/'arab_quote_day1.json', {}), read_store(destination, {}))
    atomic_bytes(destination, (json.dumps(value, ensure_ascii=False, separators=(',', ':'))+'\n').encode())
    return value

if __name__ == '__main__': publish()

