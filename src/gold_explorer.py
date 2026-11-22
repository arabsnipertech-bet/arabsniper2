"""Derived, archive-only Gold view. Never collects odds or computes probabilities.

The injected storage client may read/write only explorer/. Gold publication must
already be committed; callers retain the existing writer mutex.
"""
from datetime import datetime, timedelta, timezone
from hashlib import sha256, file_digest
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import open_verified_json, read_store
from multimarket_model import stamp
from quote_sources import PREMATCH_HOURS
from v7_archive import valid_snapshot
from v7_decision import TRIAD_LIMITS, TRIAD_MARKETS, AWAY, compatible, confirmed_triad_coherent, stored_offers
from v7_probability import finite, price_usable

ROME = ZoneInfo('Europe/Rome')
MARGIN = timedelta(minutes=5)
PREFIX = 'explorer/'
PAGE_SIZE = 250


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def signature(value):
    return sha256(encoded(value)).hexdigest()


def file_signature(path):
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


def read(storage, bucket, key, default=None):
    if not key.startswith(PREFIX):
        raise ValueError('Explorer scope')
    try:
        raw = storage.get_object(Bucket=bucket, Key=key)['Body'].read()
    except Exception as error:
        if getattr(error, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404'):
            return default
        raise
    if key.startswith(PREFIX+'objects/') and sha256(raw).hexdigest() != Path(key).stem:
        raise ValueError('Explorer checksum')
    return json.loads(raw)


def put(storage, bucket, key, value):
    if not key.startswith(PREFIX):
        raise ValueError('Explorer scope')
    storage.put_object(Bucket=bucket, Key=key, Body=encoded(value),
                       ContentType='application/json', CacheControl='private, no-store')


def object_put(storage, bucket, value):
    digest = signature(value)
    put(storage, bucket, PREFIX+'objects/'+digest+'.json', value)
    return digest


def pages(storage, bucket, rows):
    return [object_put(storage, bucket, {'version': 1, 'rows': rows[i:i+PAGE_SIZE]})
            for i in range(0, len(rows), PAGE_SIZE)]


def eligible(market, row, role):
    floor, low, high = TRIAD_LIMITS[role]
    if not (row['validated'] and market in TRIAD_MARKETS[role]
            and low <= row['odd'] <= high and row['probability'] >= floor):
        return False
    return role != 'high_odds_shot' or market != AWAY or row['odd'] >= 4.5 and (row['delta'] or 0) > 0


def quote_expiry(source, kickoff, usable):
    provider = stamp(source.get('provider_updated_at'))
    if not usable or not provider:
        return None
    expiry = provider+timedelta(hours=PREMATCH_HOURS)
    if source.get('quote_status') == 'recent':
        expiry = provider+timedelta(hours=6)
        if expiry > kickoff-timedelta(hours=3):
            expiry = max(kickoff-timedelta(hours=3), provider+timedelta(hours=1))
    return min(expiry, kickoff).isoformat()


def match_odds(matrix, snap, observed):
    """Copy the same-bookmaker 1X2 observations; never estimate missing prices."""
    result = {}
    for source in matrix.get('rows', []):
        spec = source.get('spec') or {}
        if (spec.get('kind') != 'result' or spec.get('period') != 'ft'
                or spec.get('side') not in ('1', 'x', '2')
                or spec.get('line') is not None or spec.get('extra') is not None):
            continue
        seen = stamp(source.get('quote_observed_at'))
        if seen and seen > stamp(snap['captured_at']):
            continue
        current = source.get('odd')
        if not finite(current) or current <= 1:
            current = source.get('last_odd')
        item = {key: source.get(key) for key in ('opening_odd', 'first_odd', 'first_observed_at',
            'quote_observed_at', 'provider_updated_at', 'bookmaker', 'bookmaker_id', 'quote_status')}
        usable = price_usable(source, observed.isoformat(), snap['kickoff'])
        item.update(odd=current, quote_usable=bool(usable),
                    quote_valid_until=quote_expiry(source, stamp(snap['kickoff']), usable))
        result[spec['side']] = item
    return result


def project_match(match, observed):
    meta = match.get('fixture') or {}
    matrix = (match.get('multimarket') or {}).get('matrix') or {}
    snap = matrix.get('v7') or {}
    if meta.get('status_short') not in ('NS', 'TBD') or meta.get('result'):
        return []
    if not valid_snapshot(snap, meta, observed.isoformat()):
        return []
    kickoff = stamp(snap['kickoff'])
    if observed > kickoff-MARGIN:
        return []
    sources = {r['id']: r for r in matrix.get('rows', []) if r.get('id')}
    official = snap.get('triad_commitments', snap.get('triad')) or {}
    coherent = confirmed_triad_coherent(snap.get('triad') or {})
    anchors = {pick['market_id']: pick.get('label') or 'Proposta Gold'
               for _, pick, _ in stored_offers(match.get('multimarket') or {}, meta, observed.isoformat())}
    episode = str(meta['fixture_id'])+'::'+kickoff.isoformat()
    prices = match_odds(matrix, snap, observed)
    rows = []
    for market, original in snap.get('markets', {}).items():
        probability = original.get('calibrated')
        if not finite(probability) or not 0 <= probability <= 1:
            continue
        try:
            spec = json.loads(market)
        except (ValueError, TypeError):
            continue
        source = sources.get(original.get('source_market_id'), {})
        usable = (price_usable(source, observed.isoformat(), snap['kickoff'])
                  and source.get('odd') == original.get('odd')
                  and stamp(source.get('quote_observed_at')) <= stamp(snap['captured_at']))
        ev, low = original.get('ev'), original.get('ev_low')
        confirmation = original.get('confirmation') or {}
        observation = confirmation.get('observation_only') is True
        valid = bool(not observation and usable and original.get('supported') is True and original.get('tier')
                     and finite(ev) and ev > 0 and finite(low) and low >= 0)
        opposed = []
        if not coherent:
            relation = 'inconsistent_gold_triad'
            valid = False
        elif not anchors:
            relation = 'no_confirmed_triad'
        elif market in anchors:
            relation = 'official'
        else:
            try:
                aligned = compatible(market, *anchors)
                opposed = [label for key, label in anchors.items() if not compatible(market, key)]
            except (TypeError, ValueError, KeyError):
                aligned = False
            relation = 'compatible' if aligned else 'opposed'
            if not aligned and not opposed:
                opposed = list(anchors.values())
        roles = [role for role, pick in official.items() if pick and pick.get('market_id') == market]
        quote = original.get('quote') or {}
        expiry = quote_expiry(source, kickoff, usable)
        row = dict(id=sha256((episode+'::'+market).encode()).hexdigest(), episode=episode,
                   fixture_id=str(meta['fixture_id']), home=meta.get('home'), away=meta.get('away'),
                   league=meta.get('league'), kickoff=kickoff.isoformat(),
                   date=kickoff.astimezone(ROME).date().isoformat(), market=market,
                   label=original.get('label'), kind=spec.get('kind'), period=spec.get('period'),
                   probability=probability, odd=original.get('odd'), opening=quote.get('opening_odd') or source.get('opening_odd'),
                   first_odd=quote.get('first_odd') or source.get('first_odd'), ev=ev, ev_low=low,
                   oci=(original.get('features') or {}).get('oci'),
                   delta=(original.get('features') or {}).get('net_delta_pct'),
                   reliability='supported' if original.get('supported') else 'limited',
                   category='official' if roles else 'alternative' if valid else
                   'observation' if original.get('supported') else 'unqualified',
                   roles=roles, official_references={role: official[role] for role in roles},
                   tier=original.get('tier'), validated=valid, niche=bool(original.get('niche_id')),
                   quote=quote, quote_usable=bool(usable), quote_valid_until=expiry,
                   captured_at=snap['captured_at'], published_at=observed.isoformat(),
                   forecast_sha256=snap['sha256'], exclusions=original.get('edge_exclusions', []))
        row.update(match_odds=prices, confirmation=confirmation, observation_only=observation,
                   coherence_status=relation, coherence_opposed_to=opposed,
                   coherence_forecast_sha256=snap['sha256'],
                   opening_delta_pct=(original.get('features') or {}).get('opening_delta_pct'),
                   last_leg_delta_pct=(original.get('features') or {}).get('delta_pct'),
                   oci_state=(original.get('features') or {}).get('oci_state'),
                   last_confirmed_oci=(original.get('features') or {}).get('last_confirmed_oci'),
                   origin_badge=original.get('origin_badge'),
                   origin_tooltip=original.get('origin_tooltip'),
                   motivation=next((pick.get('motivation') for pick in (snap.get('triad') or {}).values()
                                    if pick and pick.get('market_id') == market), None))
        row['ranking_roles'] = [role for role in TRIAD_MARKETS if eligible(market, row, role)]
        rows.append(row)
    return rows


def rank(rows, mode='best', diversify=True):
    role = {'recommended': 'recommended_alternative', 'shots': 'high_odds_shot'}.get(mode)
    candidates = [r for r in rows if r['validated']
                  and r.get('coherence_status') not in ('opposed', 'inconsistent_gold_triad')
                  and (not role or role in r['ranking_roles'])]
    def order(r):
        return ((r['probability'], r['ev_low'], r['ev']) if mode == 'probable' else
                (r['niche'], r['ev'], r['probability'])) + (r['market'], r['id'])
    result, seen = [], set()
    for row in sorted(candidates, key=order, reverse=True):
        if not diversify or row['fixture_id'] not in seen:
            result.append(row['id'])
            seen.add(row['fixture_id'])
    return result


def boards(rows):
    return {**{mode: rank(rows, mode) for mode in ('best', 'probable', 'recommended', 'shots')},
            'markets': {label: rank([r for r in rows if r['label'] == label])
                        for label in sorted({r['label'] for r in rows if r['label']})}}


def legacy_universe(path):
    """Documented V7 universe, never retrospectively certified Explorer rankings."""
    import ijson
    latest = {}
    with open_verified_json(path) as stream:
        for _, record in ijson.kvitems(stream, 'analysis_updates', use_float=True):
            observed = stamp(record.get('captured_at'))
            if not observed or record.get('retrospective') or not record.get('publication_snapshot'):
                continue
            for row in project_match(record, observed):
                if row['date'] != path.stem[len('prematch_'):]:
                    continue
                previous = latest.get(row['id'])
                if not previous or stamp(row['published_at']) > stamp(previous['published_at']):
                    latest[row['id']] = {**row, 'legacy_v7': True,
                                        'category': 'official' if row['roles'] else 'legacy'}
    return sorted(latest.values(), key=lambda r: r['id'])


def publish(storage, bucket, root, days, phase, now=None, scan_started=None, legacy_limit=1, source_manifest=None):
    now, root = now or datetime.now(timezone.utc), Path(root)
    catalog = read(storage, bucket, PREFIX+'catalog.json', {'version': 1, 'days': {}})
    if catalog.get('version') != 1:
        raise ValueError('Explorer version')
    active = {}
    if phase in ('night', 'fast', 'refresh'):
        for day in days:
            for match in day.get('matches', []):
                for row in project_match(match, now):
                    fresh = phase in ('night', 'fast') and (scan_started is None or
                        stamp(scan_started) and stamp(row['captured_at']) >= stamp(scan_started))
                    row['scan_phase'] = phase if fresh else 'retained'
                    active[row['id']] = row
    dates = {r['date'] for r in active.values()}
    dates.update(day for day in catalog['days'] if catalog['days'][day].get('pending_freeze') or day >= (now.astimezone(ROME).date()-timedelta(days=1)).isoformat())
    def source_sha(path):
        known = (source_manifest or {}).get(path.relative_to(root).as_posix(), {}).get('sha256')
        return known or file_signature(path)
    result_inputs, pending = {}, []
    for path in (root/'auditarchive').glob('prematch_*_v7.json'):
        day, checksum = path.name[len('prematch_'):-len('_v7.json')], source_sha(path)
        if catalog['days'].get(day, {}).get('result_sha') != checksum:
            result_inputs[day] = (path, checksum)
            dates.add(day)
    for path in sorted((root/'auditordata').glob('prematch_????-??-??.json'), reverse=True):
        day, checksum = path.stem[len('prematch_'):], source_sha(path)
        if catalog['days'].get(day, {}).get('legacy_sha') != checksum:
            pending.append((day, path, checksum))
    legacy_inputs = {day: (path, checksum) for day, path, checksum in pending[:legacy_limit]}
    dates.update(legacy_inputs)
    catalog['legacy_import_pending'] = len(pending)-len(legacy_inputs)
    for day in sorted(dates):
        key = PREFIX+'days/'+day+'.json'
        history = read(storage, bucket, key, {'date': day, 'scans': [], 'references': {}, 'results': {}, 'market_references': {}})
        rows = sorted([r for r in active.values() if r['date'] == day], key=lambda r: r['id'])
        identity = signature({'phase': phase, 'forecasts': sorted({r['forecast_sha256'] for r in rows})})
        if any(r['scan_phase'] in ('night', 'fast') for r in rows) and not any(s['identity'] == identity for s in history['scans']):
            for row in rows:
                saved = history['market_references'].setdefault(row['id'], {})
                kind = row['scan_phase']
                if kind in ('night', 'fast') and (kind != 'fast' or row['quote_usable']):
                    if kind == 'fast' or kind not in saved:
                        saved[kind] = {k: row[k] for k in ('odd', 'probability', 'ev', 'ev_low', 'captured_at', 'quote_valid_until')}
                row['night_reference'], row['fast_reference'] = saved.get('night'), saved.get('fast')
            snapshot = {'version': 1, 'date': day, 'phase': phase, 'published_at': now.isoformat(),
                        'pages': pages(storage, bucket, rows), 'count': len(rows), 'boards': boards(rows)}
            digest = object_put(storage, bucket, snapshot)
            history['scans'].append({'sha256': digest, 'identity': identity, 'phase': phase, 'published_at': now.isoformat()})
            for row in rows:
                if row['scan_phase'] == 'retained':
                    continue
                ref = history['references'].setdefault(row['episode'], {'kickoff': row['kickoff'],
                    'night': None, 'fast': None, 'final': None, 'frozen': False, 'fallback_night': False})
                if not ref['frozen'] and (phase != 'fast' or row['quote_usable']) and (phase == 'fast' or not ref['night']):
                    ref[phase] = digest
        for ref in history['references'].values():
            if not ref['frozen'] and now >= stamp(ref['kickoff'])-MARGIN:
                ref.update(frozen=True, final=ref['fast'] or ref['night'], fallback_night=not ref['fast'] and bool(ref['night']))
        info = dict(catalog['days'].get(day, {}))
        if day in result_inputs:
            result = read_store(result_inputs[day][0], {})
            history['results'] = result.get('fixtures', {})
            history['results_updated_at'] = result.get('generated_at')
            info['result_sha'] = result_inputs[day][1]
        if day in legacy_inputs:
            legacy = legacy_universe(legacy_inputs[day][0])
            history['legacy_pages'] = pages(storage, bucket, legacy)
            info['legacy_sha'], info['legacy_count'] = legacy_inputs[day][1], len(legacy)
        for row in rows:
            saved = history['market_references'].get(row['id'], {})
            row['night_reference'], row['fast_reference'] = saved.get('night'), saved.get('fast')
        put(storage, bucket, key, history)
        catalog['days'][day] = {**info, 'scans': len(history['scans']), 'results': len(history['results']),
            'pending_freeze': any(not r['frozen'] for r in history['references'].values())}
    if phase in ('night', 'fast', 'refresh'):
        rows = sorted(active.values(), key=lambda r: r['id'])
        put(storage, bucket, PREFIX+'current.json', {'version': 1, 'generated_at': now.isoformat(),
            'phase': phase, 'count': len(rows), 'pages': pages(storage, bucket, rows), 'margin_seconds': 300})
    catalog['updated_at'] = now.isoformat()
    put(storage, bucket, PREFIX+'catalog.json', catalog)


if __name__ == '__main__':
    import argparse
    import os
    from gold_storage import client, GOLD
    parser = argparse.ArgumentParser(description='Archive-only Explorer projection; no football collection')
    parser.add_argument('--seed-current', action='store_true')
    parser.add_argument('--complete-backfill', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    days = [read_store(root/'data'/name, {}) for name in GOLD[:5]] if args.seed_current else []
    from explorer_lease import writer
    with writer(client(), os.environ['R2_BUCKET']) as storage:
        if storage is None:
            raise SystemExit('Explorer writer busy; no changes made')
        publish(storage, os.environ['R2_BUCKET'], root, days, 'refresh' if args.seed_current else 'results',
                legacy_limit=None if args.complete_backfill else 1)
