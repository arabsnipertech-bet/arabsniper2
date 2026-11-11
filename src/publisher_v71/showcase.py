"""Allowlisted homepage previews and fixture rotation from the private Gold boards."""
from gold_competitions import output_allowed
from multimarket_model import stamp
from quote_sources import PREMATCH_WINDOW
from v7_archive import valid_snapshot
from v7_probability import VERSION, finite, price_usable

from .selection import TRIAD_LABELS, clean, rome_day, utc

PREVIEW_LIMIT = 2
ROTATION_LIMIT = 8


def triad(item, now):
    fixture = item['fixture']
    matrix = (item.get('multimarket') or {}).get('matrix') or {}
    snapshot = matrix.get('v7') or {}
    slots = snapshot.get('triad') or {}
    at = utc(now)
    if (snapshot.get('status') != 'published' or not all(slots.get(role) for role in TRIAD_LABELS)
            or not valid_snapshot(snapshot, fixture, at)):
        return []
    captured = stamp(snapshot['captured_at'])
    sources = {row['id']: row for row in matrix.get('rows', [])}
    records = snapshot.get('markets') or {}
    result = []
    for role, category in TRIAD_LABELS.items():
        pick = slots.get(role) or {}
        record = records.get(pick.get('market_id')) or {}
        source = sources.get(pick.get('source_market_id')) or {}
        market = clean(record.get('label'), 70)
        probability = record.get('calibrated')
        if (not market or not finite(probability) or not 0 <= probability <= 1
                or pick.get('probability') != probability or source.get('probability_model') != VERSION
                or record.get('source_market_id') != source['id']):
            return []

        row = {'category': category, 'market': market, 'probability': probability,
               'study_only': pick.get('study_only') is True}
        odd = record.get('odd')
        if pick.get('quote_state') == 'model_estimate':
            fair_odd = record.get('model_fair_odd')
            if (odd is not None or pick.get('odd') is not None or pick.get('tier') != 'BLU'
                    or not row['study_only'] or not finite(fair_odd) or fair_odd <= 1
                    or pick.get('model_fair_odd') != fair_odd):
                return []
        else:
            quoted, observed = (stamp(source.get(key)) for key in ('provider_updated_at', 'quote_observed_at'))
            if (not finite(odd) or odd <= 1 or odd != pick.get('odd') or odd != source.get('odd')
                    or source.get('quote_status') not in ('recent', 'cached_stable')
                    or source.get('quote_executable') is not True or source.get('bookmaker_id') is None
                    or not quoted or not observed or not quoted <= observed <= captured
                    or now - quoted > PREMATCH_WINDOW
                    or (pick.get('tier') != 'BLU' and (source.get('prematch_stable')
                        or not price_usable(source, at, fixture['kickoff'])))):
                return []
            row.update(odd=odd, quote_at=utc(quoted))
        result.append(row)
    return result


def upcoming(board, now):
    rows = []
    for item in board.get('matches', []):
        fixture = item.get('fixture') or {}
        fid = str(fixture.get('fixture_id', ''))
        kickoff = stamp(fixture.get('kickoff'))
        if (not fid.isascii() or not fid.isdecimal() or not kickoff or kickoff <= now
                or board.get('date') != rome_day(kickoff) or fixture.get('status_short') != 'NS'
                or fixture.get('result') or not output_allowed(item)):
            continue
        match, league = clean(fixture.get('match'), 75), clean(fixture.get('league'), 45)
        if match and league:
            rows.append((item, {'fixture_id': fid, 'match': match, 'league': league, 'kickoff': utc(kickoff)}))
    return sorted(rows, key=lambda pair: (pair[1]['kickoff'], pair[1]['fixture_id']))


def project(boards, now):
    seen, schedule, previews = set(), [], []
    for board in boards:
        for item, row in upcoming(board, now):
            if row['fixture_id'] in seen:
                continue
            seen.add(row['fixture_id'])
            if len(schedule) < ROTATION_LIMIT:
                schedule.append(row)
            if len(previews) < PREVIEW_LIMIT:
                proposals = triad(item, now)
                if proposals:
                    previews.append({**row, 'proposals': proposals})
            if len(previews) == PREVIEW_LIMIT and len(schedule) == ROTATION_LIMIT:
                return previews, schedule
    return previews, schedule
