"""Bounded daily public projection; Gold payloads never reach the homepage."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
import math
import random
from audit_storage import read_store, atomic_bytes
from statistics_first import MODEL_ID, TARGETS
from matrix_quotes import expire
from opportunity_policy import POLICY_ID, best_opportunity, goal_market
from gold_competitions import output_allowed, SCOPE_ID
from v7_decision import TIER_RANK, stored_offers
import json
from copy import deepcopy

ROME = ZoneInfo('Europe/Rome')
MARKETS = {'over15_ht', 'over25_ht', 'gg_ht', 'over25_ft', 'over35_ft'}
SECONDARY_PLATINUM_PRIORITY = 1.5

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

def board_spunti(fixture, assessment, now, *, blue_only=False):
    """Project only the board's selected markets and published Studio star."""
    matrix = assessment.get('matrix') or {}
    captured = stamp(assessment.get('captured_at'))
    if not captured or not 0 <= (now-captured).total_seconds() <= 5400 or fixture.get('result'):
        return
    if 'triad' in (matrix.get('v7') or {}):
        for role, selection, row in stored_offers(assessment, fixture, now.isoformat()):
            tier = selection['tier']
            if blue_only and tier != 'BLU':
                continue
            pick = {k: row.get(k) for k in ('odd', 'first_odd', 'delta_pct', 'bookmaker',
                                           'movement_confirmed', 'bookmakers_count', 'oci')}
            if row.get('historical_oci'):
                pick['oci'] = None
            pick.update(label=selection['label'], market=goal_market(row),
                        quote_status=row.get('quote_status'), tier={'BLU': 'Blu', 'GOLD': 'Gold', 'PLATINUM': 'Platinum'}[tier],
                        role=role, badge=selection['badge'], horizon=selection['horizon'], niche_id=selection['niche_id'],
                        fixture_id=str(fixture['fixture_id']), match=fixture['match'], kickoff=fixture['kickoff'],
                        country=fixture.get('country', ''), league=fixture.get('league', ''),
                        captured_at=(matrix['v7'] or {})['captured_at'],
                        provider_updated_at=row['provider_updated_at'], statistical_pct=100*selection['probability'],
                        margin_pct=row.get('selected_margin_pct'),
                        market_confirmation='confirmed' if tier == 'PLATINUM' else
                        (row.get('opportunity') or {}).get('signal'))
            yield (2-TIER_RANK[tier], -selection['probability'], fixture['kickoff'], pick['fixture_id']), pick
        return
    rows = {row.get('id'): row for row in matrix.get('rows', [])}
    for priority, tier, field in ((0, 'Platinum', 'platinum'), (1, 'Gold', 'opportunity'),
                                  (2, 'Solo Studio', 'study')):
        if field == 'study' and blue_only:
            from platinum_policy import flow_rejections
            eligible = [r for r in rows.values() if r.get('quote_executable') is True
                        and not flow_rejections(r, now.isoformat(), -2)
                        and stamp(r.get('provider_updated_at'))
                        and 0 <= (now-stamp(r['provider_updated_at'])).total_seconds() <= 28800]
            if not eligible:
                continue
            row = min(eligible, key=lambda r: (-r['oci'], r['delta_pct'], str(r['id'])))
            tier = 'Blu'
            observed = row['provider_updated_at']
            probability = row.get('statistical_pct')
            score = row['oci']
        elif field == 'study':
            ideas = (matrix.get('quote_monitoring') or {}).get('studio_ideas', [])
            row = next((idea for idea in ideas if idea.get('best_study') is True and not idea.get('suppressed')), {})
            observed = row.get('updated')
            score = row.get('poisson_pct')
            probability = score
        else:
            policy = matrix.get(field+'_policy') or {}
            row = rows.get(policy.get('best_market_id')) or {}
            decision = row.get(field) or {}
            if (decision.get('eligible') is not True or row.get('quote_executable') is not True
                    or row.get('quote_status') not in ('recent', 'cached_stable')):
                continue
            observed = row.get('provider_updated_at')
            score = row.get('oci') if field == 'platinum' else decision.get('playability_score')
            if decision.get('secondary_to_gold') is True:
                priority = SECONDARY_PLATINUM_PRIORITY
            probability = row.get('statistical_pct')
        quote_time = stamp(observed)
        if (not quote_time or not 0 <= (now-quote_time).total_seconds() <= 28800
                or not number(row.get('odd')) or row['odd'] <= 1 or not row.get('label')
                or not fixture.get('match') or not str(fixture.get('fixture_id', '')).isdigit()):
            continue
        pick = {k:row.get(k) for k in ('label','odd','first_odd','delta_pct','bookmaker',
                                       'movement_confirmed','bookmakers_count','oci')}
        if row.get('historical_oci'):
            pick['oci'] = None
        pick.update(market=row.get("market") if tier == "Solo Studio" else goal_market(row),
                    quote_status=row.get("quote_status"), tier=tier, fixture_id=str(fixture['fixture_id']), match=fixture['match'],
                    kickoff=fixture['kickoff'], country=fixture.get('country',''), league=fixture.get('league',''), captured_at=assessment['captured_at'],
                    provider_updated_at=observed, statistical_pct=probability,
                    margin_pct=row.get('selected_margin_pct'),
                    market_confirmation='confirmed' if field == 'platinum' else
                    (row.get('opportunity') or {}).get('signal'))
        yield (priority, -score if number(score) else float('inf'),
               fixture['kickoff'], pick['fixture_id']), pick


def social_board(payload, now, *, kickoff_from=None, kickoff_until=None, exclude=(), limit=3, blue_only=False, tier_order=None):
    today = now.astimezone(ROME).date().isoformat()
    updated = stamp(payload.get('updated_at'))
    if payload.get('date') != today or not updated or not 0 <= (now-updated).total_seconds() <= 5400:
        return []
    ranked = []
    for match in payload.get('matches', []):
        fixture = match.get('fixture') or {}
        kickoff = stamp(fixture.get('kickoff'))
        if (not kickoff or kickoff <= now or kickoff.astimezone(ROME).date().isoformat() != today
                or fixture.get('status_short') not in ('NS', 'TBD') or not output_allowed(match)):
            continue
        if (kickoff_from and kickoff < kickoff_from) or (kickoff_until and kickoff > kickoff_until) or str(fixture.get('fixture_id')) in exclude:
            continue
        ranked.extend(board_spunti(fixture, match.get('multimarket') or {}, now, blue_only=blue_only))
    # Social-only ordering; model qualification and other callers are unchanged.
    if tier_order:
        priorities = {tier: i for i, tier in enumerate(tier_order)}
        for index, (key, pick) in enumerate(ranked):
            priority = priorities.get(pick['tier'], len(priorities))
            if key[0] == SECONDARY_PLATINUM_PRIORITY:
                priority = max(priority, priorities.get('Gold', len(priorities)) + .5)
            ranked[index] = (priority, *key[1:]), pick
    distinct = {}
    for _, pick in sorted(ranked, key=lambda item: item[0]):
        distinct.setdefault(pick['fixture_id'], pick)
    return list(distinct.values())[:limit]


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
                    or f.get('status_short') not in ('NS', 'TBD') or not output_allowed(match)): continue
            # Preserve the saved snapshot while checking freshness at publication.
            matrix = a.get('matrix') or {}
            selection = None
            if 'triad' in (matrix.get('v7') or {}):
                selection, row = next(((pick, source)
                    for _, pick, source in stored_offers(a, f, now.isoformat())
                    if pick['tier'] in ('GOLD', 'PLATINUM')), (None, None))
            elif matrix.get('publication_model'):
                from v7_probability import price_usable
                selected = (matrix.get('opportunity_policy') or {}).get('best_market_id')
                row = next((r for r in matrix.get('rows', []) if r['id'] == selected
                            and r.get('probability_model') == matrix['publication_model']
                            and price_usable(r, now.isoformat(), f['kickoff'])), None)
            else:
                view = {**a, 'matrix': deepcopy(matrix)}
                expire(view, f, now.isoformat())
                selected = view['matrix']['opportunity_policy']['best_market_id']
                row = next((r for r in view['matrix']['rows'] if r['id'] == selected), None)
            if row:
                tier_meta = ({key: selection[key] for key in ('tier', 'badge', 'horizon', 'niche_id')}
                             if selection else {})
                candidates.append((row.get('ev_low_pct', -100), {**tier_meta, 'fixture_id': str(f['fixture_id']),
                    'match': f.get('match', ''), 'league': f.get('league', ''), 'country': f.get('country'), 'kickoff': f['kickoff'],
                    'label': row['label'], 'market': row['spec']['kind']+'_'+row['period'], 'spec': deepcopy(row['spec']),
                    'odd': row['odd'], 'statistical_pct': row['statistical_pct'],
                    'market_confirmation': row['opportunity']['signal'], 'category': row['category'],
                    'opportunity': row['opportunity'], 'oci': row.get('oci'),
                    'first_odd': row.get('first_odd'), 'delta_pct': row.get('delta_pct'),
                    'bookmaker': row.get('bookmaker'), 'bookmakers_count': row.get('bookmakers_count'),
                    'movement_confirmed': row.get('movement_confirmed'),
                    'competition_policy': SCOPE_ID,
                    'margin_pct': row.get('selected_margin_pct'),
                    'model_id': matrix.get('publication_model') or 'lateral-matrix-v5.0',
                    'captured_at': (matrix.get('v7') or {}).get('captured_at') if selection else a.get('captured_at'),
                    'provider_updated_at': row.get('provider_updated_at'),
                    'quote_executable': row.get('quote_executable', True)}))
        candidates.sort(key=lambda x: (-x[0], x[1]['kickoff'], x[1]['fixture_id'], x[1]['label']))
        unique = {}
        for _, row in candidates: unique.setdefault(row['fixture_id'], row)
        fields = ('fixture_id', 'match', 'league', 'kickoff', 'label', 'odd', 'captured_at', 'provider_updated_at',
                  'statistical_pct', 'margin_pct', 'market_confirmation', 'first_odd', 'delta_pct',
                  'bookmaker', 'bookmakers_count', 'movement_confirmed', 'oci')
        social = []
        for pick in unique.values():
            observed = stamp(pick.get('provider_updated_at'))
            captured = stamp(pick.get('captured_at'))
            if (pick['quote_executable'] is not True or not observed or not captured
                    or not captured <= now < stamp(pick['kickoff'])
                    or pick['opportunity'].get('policy_id') != POLICY_ID
                    or not pick['opportunity'].get('eligible')):
                continue
            social.append({field: pick[field] for field in fields})
            if len(social) == 4:
                break
        result['social_spunti'] = social_board(payload, now)
        result['social_selections'] = social if len(social) == 4 else []
        result.update(model_id='lateral-matrix-v5.0', opportunity_policy=POLICY_ID,
                      competition_policy=SCOPE_ID, selections=list(unique.values())[:3],
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
        row = {'fixture_id': str(f['fixture_id']), 'match': f.get('match', ''), 'league': f.get('league', ''), 'country': f.get('country'), 'kickoff': f['kickoff'],
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
    from publisher_v71.cutover import ENABLED
    if ENABLED:
        # The scheduled scans still write this public path. V7.1 owns the
        # editorial release and keeps reserved BESTs out of public Git history.
        now = datetime.now(timezone.utc)
        value = {'publisher': 'v7.1', 'date': now.astimezone(ROME).date().isoformat(),
                 'updated_at': now.isoformat(), 'selections': [], 'social_spunti': [],
                 'social_selections': []}
    else:
        value = build_preview(read_store(directory/'arab_quote_day1.json', {}), read_store(destination, {}))
    atomic_bytes(destination, (json.dumps(value, ensure_ascii=False, separators=(',', ':'))+'\n').encode())
    return value

if __name__ == '__main__': publish()

