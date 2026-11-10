"""Read executable V7 BESTs and build the PARTIAL public projection."""
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

from gold_competitions import output_allowed
from multimarket_model import stamp
from v7_archive import valid_snapshot
from v7_decision import fingerprint, stored_offers
from v7_probability import finite

ROME = ZoneInfo('Europe/Rome')
CATEGORIES = {
    'most_probable': 'Probabile',
    'recommended_alternative': 'Plausibile',
    'high_odds_shot': 'Colpaccio',
}
LEAD = timedelta(minutes=20)
WINDOW = timedelta(minutes=60)
SPAN = timedelta(minutes=90)
BOARD_AGE = timedelta(minutes=90)
QUOTE_AGE = timedelta(minutes=60)
PREVIEW_FIELDS = ('fixture_id', 'match', 'league', 'kickoff', 'category')
FREE_FIELDS = (*PREVIEW_FIELDS, 'market', 'odd', 'probability', 'captured_at',
               'quote_at', 'expires_at', 'reason')


def utc(value):
    return value.astimezone(timezone.utc).isoformat()


def rome_day(value):
    return value.astimezone(ROME).date().isoformat()


def clean(value, limit):
    return ' '.join(value.split())[:limit] if isinstance(value, str) else ''


def fresh(value, now, age):
    at = stamp(value)
    return at is not None and timedelta() <= now - at <= age


def reason(role, source):
    details = []
    delta = source.get('delta_pct')
    if source.get('movement_confirmed') is True and finite(delta) and abs(delta) >= .05:
        details.append(f"movimento quota confermato {delta:+.1f}%".replace('.', ','))
    margin = source.get('selected_margin_pct')
    if finite(margin) and 0 <= margin <= 100:
        details.append(f"aggio osservato {margin:.1f}%".replace('.', ','))
    lead = {
        'most_probable': 'Prima proposta della triade del modello',
        'recommended_alternative': 'Alternativa qualificata nella triade del modello',
        'high_odds_shot': 'Scenario a quota alta qualificato dal modello',
    }[role]
    return lead + ('. ' + ' · '.join(details).capitalize() if details else '. Quota verificata sullo snapshot prematch.')


def read_board(board, now):
    """Use the first executable role in the order emitted by V7."""
    if not fresh(board.get('updated_at'), now, BOARD_AGE):
        return
    for item in board.get('matches', []):
        f = item.get('fixture') or {}
        fid = str(f.get('fixture_id', ''))
        kickoff = stamp(f.get('kickoff'))
        if (not fid.isascii() or not fid.isdecimal() or not kickoff or not now < kickoff <= now + WINDOW + SPAN
                or board.get('date') != rome_day(kickoff) or f.get('status_short') != 'NS'
                or f.get('result') or not output_allowed(item)):
            continue
        match, league = clean(f.get('match'), 95), clean(f.get('league'), 70)
        if not (match and league):
            continue
        assessment = item.get('multimarket') or {}
        v7 = ((assessment.get('matrix') or {}).get('v7') or {})
        if not fresh(v7.get('captured_at'), now, BOARD_AGE) or not valid_snapshot(v7, f, utc(now)):
            continue
        for role, pick, source in stored_offers(assessment, f, utc(now)):
            quote_at = stamp(source.get('provider_updated_at'))
            label = clean(pick.get('label'), 100)
            if (not label or not quote_at or not fresh(source.get('provider_updated_at'), now, QUOTE_AGE)
                    or not 0 < pick['probability'] <= 1):
                continue
            yield {
                'fixture_id': fid, 'match': match, 'league': league, 'kickoff': utc(kickoff),
                'category': CATEGORIES[role], 'role': role, 'tier': pick['tier'],
                'market': label, 'market_id': pick['market_id'], 'odd': pick['odd'],
                'probability': pick['probability'], 'captured_at': v7['captured_at'],
                'quote_at': utc(quote_at), 'expires_at': utc(min(quote_at + QUOTE_AGE, kickoff)),
                'observed_at': utc(now), 'forecast_sha256': v7['sha256'], 'reason': reason(role, source),
            }
            break


def published_candidates(boards, now):
    distinct = {}
    for board in boards:
        for pick in read_board(board, now):
            previous = distinct.get(pick['fixture_id'])
            if previous is None or stamp(pick['captured_at']) > stamp(previous['captured_at']):
                distinct[pick['fixture_id']] = pick
    return sorted(distinct.values(), key=lambda pick: (pick['kickoff'], pick['fixture_id']))


def group_for(candidates, now):
    """Use kickoff order across fixtures and keep the model's BEST within each."""
    ready = [p for p in candidates if LEAD <= stamp(p['kickoff']) - now <= WINDOW]
    for anchor in ready:
        start = stamp(anchor['kickoff'])
        group = [p for p in candidates if start <= stamp(p['kickoff']) <= start + SPAN][:3]
        if len(group) > 1:
            return group
    return [pick for pick in ready
            if stamp(pick['kickoff']) - now <= timedelta(minutes=45)
            and pick['role'] == 'most_probable' and pick['tier'] in ('GOLD', 'PLATINUM')][:1]


def group_id(picks):
    return fingerprint(sorted((p['fixture_id'], p['kickoff']) for p in picks))[:20]


def project_pick(pick, free=False):
    # This is the only boundary used for BEST captions, cards and homepage JSON.
    return {field: pick[field] for field in FREE_FIELDS if free or field in PREVIEW_FIELDS}


def project_group(group, now):
    free, *reserved = group['picks']
    if min(stamp(free['kickoff']), stamp(free['expires_at'])) <= now:
        return None
    return {'id': group['id'], 'published_at': group['created_at'],
            'free': project_pick(free, True),
            'previews': [project_pick(p) for p in reserved if stamp(p['kickoff']) > now]}
