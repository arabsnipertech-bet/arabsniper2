"""Exact API-Football market adapters, comparable observations, OCI and value.

The overround is calculated only from a complete same-book, same-line snapshot.
Overlapping double-chance and range quotes never form a fictitious partition.
OCI is an odds index, not traded volume or proof of informed money.
"""
from collections import Counter, defaultdict
from copy import deepcopy
from statistics import median
import re

from multimarket_model import stamp, number, fair, normalized, SPECS
from context_analysis import movement
from lateral_matrix import specification, market_id, matrix_rows
from opportunity_policy import annotate
from market_scope import CORE_BET_IDS, core_market, trim_matrix

# Verified API-Football prematch catalogue identities. Never identify a market
# by the presence of 'over' alone (corners/cards/team totals are different).
CATALOG = {
 1: ('Match Winner', 'result', 'ft'), 13: ('First Half Winner', 'result', 'ht'),
 3: ('Second Half Winner', 'result', 'sh'), 2: ('Home/Away', 'dnb', 'ft'),
 109: ('Draw No Bet (1st Half)', 'dnb', 'ht'), 182: ('Draw No Bet (2nd Half)', 'dnb', 'sh'),
 12: ('Double Chance', 'dc', 'ft'), 20: ('Double Chance - First Half', 'dc', 'ht'),
 33: ('Double Chance - Second Half', 'dc', 'sh'),
 5: ('Goals Over/Under', 'total', 'ft'), 6: ('Goals Over/Under First Half', 'total', 'ht'),
 26: ('Goals Over/Under - Second Half', 'total', 'sh'),
 50: ('Goal Line', 'asian_total', 'ft'), 72: ('Goal Line (1st Half)', 'asian_total', 'ht'),
 336: ('Goal Line', 'asian_total', 'ft'),
 8: ('Both Teams Score', 'btts', 'ft'), 34: ('Both Teams Score - First Half', 'btts', 'ht'),
 35: ('Both Teams To Score - Second Half', 'btts', 'sh'),
 16: ('Total - Home', 'team_total', 'ft'), 17: ('Total - Away', 'team_total', 'ft'),
 105: ('Home Team Total Goals(1st Half)', 'team_total', 'ht'),
 106: ('Away Team Total Goals(1st Half)', 'team_total', 'ht'),
 107: ('Home Team Total Goals(2nd Half)', 'team_total', 'sh'),
 108: ('Away Team Total Goals(2nd Half)', 'team_total', 'sh'),
 4: ('Asian Handicap', 'handicap', 'ft'), 19: ('Asian Handicap First Half', 'handicap', 'ht'),
 104: ('Asian Handicap (2nd Half)', 'handicap', 'sh'),
 25: ('Result/Total Goals', 'result_total', 'ft'), 51: ('Halftime Result/Total Goals', 'result_total', 'ht'),
 194: ('Result/Total Goals (2nd Half)', 'result_total', 'sh'),
 24: ('Results/Both Teams Score', 'result_btts', 'ft'),
 52: ('Halftime Result/Both Teams Score', 'result_btts', 'ht'),
 49: ('Total Goals/Both Teams To Score', 'btts_total', 'ft'),
 183: ('Double Chance/Total', 'dc_total', 'ft'),
 282: ('Double Chance/Both Teams To Score', 'dc_btts', 'ft'),
 7: ('HT/FT Double', 'htft', 'ft'),
 10: ('Exact Score', 'score', 'ft'), 31: ('Correct Score - First Half', 'score', 'ht'),
 62: ('Correct Score - Second Half', 'score', 'sh'),
 186: ('Total Goals Number By Ranges', 'range', 'ft'),
 187: ('Total Goals By Ranges (1st Half)', 'range', 'ht'),
 223: ('Number of Goals In Match (Range)', 'range', 'ft'),
}
SIDE = {'home': '1', '1': '1', 'draw': 'x', 'x': 'x', 'away': '2', '2': '2',
        'home draw': '1x', 'home or draw': '1x', '1x': '1x',
        'draw away': 'x2', 'draw or away': 'x2', 'x2': 'x2',
        'home away': '12', 'home or away': '12', '12': '12'}
HANDICAP_ADAPTER_VERSION = 2


def parse_value(bid, raw):
    if bid not in CATALOG: return None
    _, kind, period = CATALOG[bid]
    raw = str(raw).strip().lower().replace(',', '.')
    norm = normalized(raw)
    side, line, extra = '', None, None
    if kind in ('result', 'dc', 'dnb'):
        side = SIDE.get(norm)
        if side not in ({'1', 'x', '2'} if kind == 'result' else {'1', '2'} if kind == 'dnb' else {'1x', 'x2', '12'}): return None
    elif kind == 'btts':
        side = raw if raw in ('yes', 'no') else None
        if side is None: return None
    elif kind in ('total', 'asian_total', 'team_total'):
        m = re.fullmatch(r'(over|under)\s+(\d+(?:\.\d+)?)', raw)
        if not m: return None
        side, line = m[1], float(m[2])
        if line > 15 or abs(line*4-round(line*4)) > 1e-8: return None
        if kind == 'total' and line % 1 != .5: kind = 'asian_total'
        if kind == 'team_total':
            # The standard team-total adapter only accepts non-push lines.
            if line % 1 != .5: return None
            extra = 'home' if bid in (16, 105, 107) else 'away'
    elif kind == 'handicap':
        m = re.fullmatch(r'(home|away)\s+([+-]?\d+(?:\.\d+)?)', raw)
        if not m: return None
        side, line = m[1], float(m[2])
        if abs(line) > 10 or abs(line*4-round(line*4)) > 1e-8: return None
        # Prematch /odds labels use the HOME handicap for both selections:
        # Home -1.5 and Away -1.5 are opposite sides of the same line.
        # Our settlement spec instead stores the selected team's handicap.
        if side == 'away': line = -line
        if line == 0: line = 0.0
    elif kind in ('result_total', 'dc_total', 'btts_total'):
        m = re.fullmatch(r'(.+?)\s*[/&,]\s*(over|under)\s+(\d+(?:\.\d+)?)', raw)
        if not m and kind == 'btts_total':
            rev = re.fullmatch(r'(over|under)\s+(\d+(?:\.\d+)?)\s*[/&,]\s*(yes|no)', raw)
            if rev: m = (None, rev[3], rev[1], rev[2])
        if not m: return None
        side = m[1] if kind == 'btts_total' else SIDE.get(normalized(m[1]))
        allowed = {'yes', 'no'} if kind == 'btts_total' else {'1x', 'x2', '12'} if kind == 'dc_total' else {'1', 'x', '2'}
        line, extra = float(m[3]), m[2]
        if side not in allowed or line % 1 != .5 or line > 15: return None
    elif kind in ('result_btts', 'dc_btts', 'htft'):
        m = re.fullmatch(r'(.+?)\s*/\s*([^/]+)', raw)
        if not m: return None
        side = SIDE.get(normalized(m[1]))
        extra = SIDE.get(normalized(m[2])) if kind == 'htft' else m[2].strip()
        if side not in ({'1x', 'x2', '12'} if kind == 'dc_btts' else {'1', 'x', '2'}): return None
        if extra not in ({'1', 'x', '2'} if kind == 'htft' else {'yes', 'no'}): return None
    elif kind in ('range', 'score'):
        m = re.fullmatch(r'(\d+)\s*[-:]\s*(\d+)', raw)
        if not m or max(int(m[1]), int(m[2])) > 20: return None
        if kind == 'score': side = f'{int(m[1])}:{int(m[2])}'
        else:
            line, extra = float(m[1]), int(m[2])
            if line > extra: return None
    else: return None
    return specification(kind, period, side, line, extra)


def partition(spec):
    k, p, side = spec['kind'], spec['period'], spec['side']
    line, extra = spec.get('line'), spec.get('extra')
    group, token, expected = [k, p], side, None
    if k in ('total', 'asian_total', 'team_total'):
        group += [line, extra]; expected = {'over', 'under'}
    elif k == 'result': expected = {'1', 'x', '2'}
    elif k == 'dnb': expected = {'1', '2'}
    elif k == 'btts': expected = {'yes', 'no'}
    elif k == 'handicap':
        group += [(line if side == 'home' else -line) or 0.0]; expected = {'home', 'away'}
    elif k in ('result_total', 'btts_total', 'dc_total'):
        group += [line]; token = side+':'+extra
        if k != 'dc_total': expected = {s+':'+d for s in (('yes', 'no') if k == 'btts_total' else ('1', 'x', '2')) for d in ('over', 'under')}
    elif k in ('result_btts', 'dc_btts', 'htft'):
        token = side+':'+extra
        if k != 'dc_btts': expected = {s+':'+d for s in ('1', 'x', '2') for d in (('1', 'x', '2') if k == 'htft' else ('yes', 'no'))}
    elif k == 'range': token = f'{line:g}:{extra}'
    return '|'.join(map(str, group)), token, expected


def collect(payload, fid, observed_at):
    now = stamp(observed_at)
    groups, diagnostics = {}, Counter()
    for record in (payload or {}).get('response', []):
        if str((record.get('fixture') or {}).get('id')) != str(fid): continue
        updated, kickoff = stamp(record.get('update')), stamp((record.get('fixture') or {}).get('date'))
        if not now or updated and updated > now or kickoff and now >= kickoff: continue
        for book in record.get('bookmakers', []):
            if book.get('id') is None: continue
            for bet in book.get('bets', []):
                try: bid = int(bet.get('id'))
                except (ValueError, TypeError): continue
                if bid not in CATALOG:
                    diagnostics['unmodeled_bet_types'] += 1; continue
                if bid not in CORE_BET_IDS:
                    diagnostics['out_of_scope_bet_types'] += 1; continue
                if normalized(bet.get('name')) != normalized(CATALOG[bid][0]):
                    diagnostics['catalog_identity_mismatch'] += 1; continue
                local = {}
                for value in bet.get('values', []):
                    odd = number(value.get('odd'))
                    spec = parse_value(bid, value.get('value'))
                    if spec and not core_market(spec):
                        diagnostics['out_of_scope_values'] += 1; continue
                    if not spec or odd is None or odd <= 1:
                        diagnostics['unmapped_or_invalid_values'] += 1; continue
                    group, token, expected = partition(spec)
                    item = local.setdefault(group, {'odds': {}, 'specs': {}, 'expected': expected, 'ambiguous': False})
                    if token in item['odds'] and item['odds'][token] != odd: item['ambiguous'] = True
                    item['odds'][token], item['specs'][token] = odd, spec
                for group, item in local.items():
                    if item['ambiguous']:
                        diagnostics['ambiguous_snapshots'] += 1; continue
                    key = ('ah2|' if CATALOG[bid][1] == 'handicap' else '') + f'{bid}|{group}|{book["id"]}'
                    complete = item['expected'] is not None and set(item['odds']) == item['expected']
                    candidate = {k: item[k] for k in ('odds', 'specs')}
                    candidate.update(bookmaker_id=str(book['id']), bookmaker=str(book.get('name') or book['id']),
                        bet_id=bid, observed_at=observed_at, provider_updated_at=updated.isoformat() if updated else None,
                        complete=complete, group=f'{bid}|{group}')
                    old = groups.get(key)
                    if old and stamp(old['provider_updated_at']) and (not updated or updated < stamp(old['provider_updated_at'])): continue
                    groups[key] = candidate
    return groups, dict(diagnostics)


def migrate_handicap_history(fixture):
    """Re-pair legacy raw prices only at identical provider/observation times.

    Keep the original ledger for traceability. The versioned keys prevent a
    formerly mixed pair from contributing to OCI or overround after repair.
    """
    if fixture.get('handicap_adapter_version') == HANDICAP_ADAPTER_VERSION: return
    rebuilt = defaultdict(dict)
    for key, points in list(fixture.get('groups', {}).items()):
        parts = key.split('|')
        if len(parts) != 5 or parts[1] != 'handicap': continue
        bid, _, period, anchor, book = parts
        if int(bid) not in (4, 19, 104): continue
        for point in points:
            for side, odd in point.get('odds', {}).items():
                if side not in ('home', 'away'): continue
                raw_line = float(anchor) * (1 if side == 'home' else -1)
                spec = parse_value(int(bid), f'{side} {raw_line:g}')
                if not spec: continue
                group, _, _ = partition(spec)
                target = f'ah2|{bid}|{group}|{book}'
                when = (point.get('provider_updated_at'), point.get('observed_at'))
                snapshot = rebuilt[target].setdefault(when, {
                    'odds': {}, 'provider_updated_at': when[0], 'observed_at': when[1]})
                if side in snapshot['odds'] and snapshot['odds'][side] != odd:
                    raise ValueError('Conflicting historical handicap prices')
                snapshot['odds'][side] = odd
    for key, snapshots in rebuilt.items():
        if key in fixture['groups']: continue
        points = sorted(snapshots.values(), key=lambda p: (p['provider_updated_at'] or '', p['observed_at'] or ''))
        for point in points: point['complete'] = set(point['odds']) == {'home', 'away'}
        fixture['groups'][key] = points if len(points) <= 48 else [points[0], *points[-47:]]
    fixture['handicap_adapter_version'] = HANDICAP_ADAPTER_VERSION


def handicap_price_valid(row):
    return (row.get('spec', {}).get('kind') != 'handicap' or
            row.get('quote_adapter_version') == HANDICAP_ADAPTER_VERSION)


def safe_price_row(row):
    if handicap_price_valid(row): return row
    # Preserve the original frozen record. Consumers must not evaluate or
    # promote prices produced by the former handicap adapter.
    clean = dict(row)
    for key in ('odd', 'first_odd', 'last_odd', 'ev_pct', 'ev_low_pct', 'ev_high_pct',
                'oci', 'oci_previous', 'margin_pct', 'margin_delta_pp', 'delta_pct',
                'market_fair_pct', 'edge_pp', 'implied_pct', 'persistence',
                'speed_pp_per_hour', 'amplitude_pct', 'fair_delta_pp'):
        clean[key] = None
    clean.update(quote_status='invalid_adapter', category='no_quote',
                 category_reason='Quota handicap della versione precedente: associazione da aggiornare',
                 market_behavior='unavailable', margin_band='unavailable', margin_trend='unavailable',
                 theoretical_value=False, stable_value=False, paired_books=0, agreeing_books=0)
    return clean


def ledger_offers(state, fid, captured, observed_at, kickoff):
    now, start = stamp(observed_at), stamp(kickoff)
    if not now or not start or now >= start: return {}
    ledger = state.setdefault('matrix_quotes', {})
    for key in list(ledger):
        old_start = stamp(ledger[key].get('kickoff'))
        if old_start and (now-old_start).total_seconds() > 172800: del ledger[key]
    fixture = ledger.setdefault(str(fid), {'kickoff': kickoff, 'groups': {}})
    fixture['kickoff'] = kickoff
    migrate_handicap_history(fixture)
    offers = defaultdict(list)
    for key, group in captured.items():
        if not group.get('specs') or not all(core_market(s) for s in group['specs'].values()): continue
        points = fixture['groups'].setdefault(key, [])
        if not points:
            # Preserve valid operator histories already collected by V3/V4.
            # New markets start empty; no synthetic opening price is created.
            sample = next(iter(group['specs'].values()))
            for old_key, (bid, name, line, sides, _) in SPECS.items():
                if bid != group['bet_id'] or line != sample.get('line') or set(sides) != set(group['odds']): continue
                old_points = state.get('multimarket_quotes', {}).get(str(fid), {}).get(old_key, {}).get(group['bookmaker_id'], [])
                for point in old_points:
                    observed, updated = stamp(point.get('observed_at')), stamp(point.get('provider_updated_at'))
                    if not observed or not updated or not observed < start or observed > now or updated > now: continue
                    if set(point.get('odds', {})) != set(sides): continue
                    points.append({k: deepcopy(point.get(k)) for k in ('odds', 'observed_at', 'provider_updated_at')})
                    points[-1]['complete'] = True
                break
        at = stamp(group['provider_updated_at'])
        if points and stamp(points[-1].get('provider_updated_at')) and (not at or at < stamp(points[-1]['provider_updated_at'])):
            continue  # A delayed provider response must not reverse time.
        snap = {k: deepcopy(group[k]) for k in ('odds', 'observed_at', 'provider_updated_at', 'complete')}
        if not points or (points[-1]['odds'], points[-1]['provider_updated_at'], points[-1]['complete']) != (snap['odds'], snap['provider_updated_at'], snap['complete']):
            points.append(snap)
        if len(points) > 48: points[:] = [points[0], *points[-47:]]
        for token, spec in group['specs'].items():
            usable = [p for p in points if token in p['odds']]
            complete = [p for p in usable if p['complete'] and set(p['odds']) == set(group['odds'])]
            first = usable[0]
            offers[market_id(spec)].append({**group, 'spec': spec, 'token': token,
                'odd': group['odds'][token], 'first_odd': first['odds'][token],
                'first_at': first['observed_at'], 'trace': complete if group['complete'] else [],
                'raw_trace': usable})
    return offers


def analyse_prices(row, offers, as_of, kickoff):
    if not core_market(row.get('spec')):
        raise ValueError('Price analysis outside the active market scope')
    row['quote_adapter_version'] = HANDICAP_ADAPTER_VERSION
    row.update(selected_margin_pct=None, selected_margin_delta_pp=None,
               selected_movement=None, selected_market_odds=None,
               movement_confirmed=False, fair_direction_coherent=False)
    now, start = stamp(as_of), stamp(kickoff)
    max_hours = 1 if now and start and 0 < (start-now).total_seconds() <= 10800 else 6
    recent = [o for o in offers if stamp(o.get('provider_updated_at')) and
              0 <= (now-stamp(o['provider_updated_at'])).total_seconds() <= max_hours*3600]
    # Multiple provider bet IDs for the same settlement do not count as more books.
    unique = {}
    for offer in sorted(recent, key=lambda o: (o['provider_updated_at'], o['odd'], -o['bet_id'])):
        unique[offer['bookmaker_id']] = offer
    recent = list(unique.values())
    if not recent:
        row.update(quote_status='stale' if offers else 'missing', category='no_quote',
                   category_reason='Quota scaduta' if offers else 'Quota non disponibile',
                   market_behavior='unavailable', odd=None, ev_pct=None, ev_low_pct=None)
        row.update(oci=None, oci_previous=None, margin_pct=None, margin_delta_pp=None,
                   margin_band='unavailable', margin_trend='unavailable', paired_books=0,
                   agreeing_books=0, stable_value=False, theoretical_value=False)
        if offers:
            old = max(offers, key=lambda o: o.get('provider_updated_at') or '')
            row.update(last_odd=old['odd'], provider_updated_at=old.get('provider_updated_at'))
        return
    best = max(recent, key=lambda o: (o['odd'], o['provider_updated_at'], o['bookmaker_id']))
    compatible = [o for o in recent if o['complete'] and o['group'] == best['group']]
    # OCI is calculated separately per market partition: no mixed lines,
    # bookmakers, timestamps, partial markets or synthetic best-price overround.
    move = movement(compatible, best['token']) if best['complete'] else {}
    oci = move.get('oci')
    margins = [fair(o['odds'])[1] for o in compatible]
    book_p = median(fair(o['odds'])[0][best['token']] for o in compatible) if compatible else None
    margin = median(margins) if margins else None
    odd = best['odd']
    ev = row['win_weight']*(odd-1)-row['loss_weight']
    evs = [w*(odd-1)-lost for w, lost in row.get('sensitivity_weights', [[row['win_weight'], row['loss_weight']]])]
    strong = bool(move.get('confirmed') and move.get('fair_direction_coherent') and oci is not None and abs(oci) >= 40)
    behavior = 'convergence' if ev > 0 and strong and oci > 0 else 'divergence' if ev > 0 and strong and oci < 0 else 'neutral' if oci is not None else 'unavailable'
    robust_value = ev > 0 and min(evs) > 0 and row['quality'] == 'supported'
    category = 'no_edge'
    reason = 'Il prezzo non supera la quota di equilibrio del modello'
    if ev > 0:
        category, reason = 'value_watch', 'Valore teorico positivo, conferma o stabilità ancora insufficienti'
        if row['quality'] != 'supported': category, reason = 'limited_data', 'Valore teorico con storico limitato: osservazione, non segnale consolidato'
        elif robust_value:
            if behavior == 'convergence': category, reason = 'mature', 'Valore statistico stabile e movimento concorde sullo stesso mercato'
            elif behavior == 'divergence': category, reason = 'contrarian', 'Valore statistico stabile, movimento contrario: ipotesi da verificare'
            elif row['family'] == 'combo': category, reason = 'lateral', 'Combo con prezzo reale e valore teorico stabile; conferma diretta non disponibile'
    low_margin = 5 if row['family'] in ('1x2', 'totals') and row['period'] == 'ft' else 7
    margin_band = 'unavailable' if margin is None else 'low' if margin < low_margin else 'high' if margin > 9 else 'medium'
    margin_delta = move.get('median_overround_delta_pp')
    margin_trend = 'unavailable' if margin_delta is None else 'compressing' if margin_delta <= -1 else 'expanding' if margin_delta >= 1 else 'stable'
    row.update(quote_status='recent', odd=odd, first_odd=best['first_odd'],
        bookmaker=best['bookmaker'], bookmaker_id=best['bookmaker_id'], bet_id=best['bet_id'],
        first_observed_at=best['first_at'], provider_updated_at=best['provider_updated_at'],
        quote_observed_at=best['observed_at'], bookmakers_count=len(recent),
        implied_pct=100/odd, market_fair_pct=100*book_p if book_p is not None else None,
        fair_basis='settlement_weight' if row['spec']['kind'] in ('dnb', 'handicap', 'asian_total') else 'outcome',
        edge_pp=100*(row['win_weight']/(row['win_weight']+row['loss_weight'])-book_p) if book_p is not None and row['win_weight']+row['loss_weight'] else None,
        ev_pct=round(ev*100, 4), ev_low_pct=round(min(evs)*100, 4), ev_high_pct=round(max(evs)*100, 4),
        oci=oci, oci_previous=move.get('oci_previous'), oci_direction=move.get('direction'),
        paired_books=move.get('paired_books', 0), agreeing_books=move.get('agreeing_books', 0),
        delta_pct=100*(odd/best['first_odd']-1), amplitude_pct=move.get('amplitude_pct'),
        margin_pct=margin, margin_band=margin_band, margin_delta_pp=margin_delta, margin_trend=margin_trend,
        margin_anomaly=bool(margin is not None and (margin < 0 or margin > 30)),
        fair_delta_pp=move.get('median_fair_delta_pp'), market_behavior=behavior,
        category=category, category_reason=reason, theoretical_value=ev > 0,
        stable_value=robust_value, is_lateral=row['family'] in ('combo', 'asian', 'ranges'),
        movement_volatile=bool(move.get('has_direction_reversal') or (move.get('amplitude_pct') or 0) >= 8))
    operators = move.get('operators') or []
    selected = next((o for o in operators if o.get('bookmaker_id') == best['bookmaker_id']), None)
    own_trace = best.get('trace') or []
    own_margin_delta = (fair(own_trace[-1]['odds'])[1] - fair(own_trace[0]['odds'])[1]
                        if len(own_trace) >= 2 else None)
    row.update(selected_margin_pct=fair(best['odds'])[1] if best['complete'] else None,
               selected_margin_delta_pp=own_margin_delta,
               selected_market_odds=deepcopy(best['odds']) if best['complete'] and row['spec']['kind'] == 'result' and row['period'] == 'ft' else None,
               movement_confirmed=bool(move.get('confirmed')),
               fair_direction_coherent=bool(move.get('fair_direction_coherent')),
               selected_movement={
                   'delta_pct': selected['delta_pct'], 'fair_delta_pp': selected['leg_fair_delta_pp'],
                   'persistence': selected['persistence'], 'reversal': selected['reversal'],
                   'observed_from': selected['observed_from'], 'observed_to': selected['observed_to']
               } if selected else None)
    if operators:
        row.update(movement_hours=median(o['hours'] for o in operators),
                   persistence=median(o['persistence'] for o in operators),
                   speed_pp_per_hour=median(abs(o['leg_fair_delta_pp'])/o['hours'] for o in operators))


def attach(assessment, payload, state, meta, as_of):
    captured, diagnostics = collect(payload, meta['fixture_id'], as_of)
    offers = ledger_offers(state, meta['fixture_id'], captured, as_of, meta['kickoff'])
    matrix = assessment['matrix']
    trim_matrix(matrix)
    existing = {r['id'] for r in matrix['rows']}
    extra = {key: values[0]['spec'] for key, values in offers.items() if key not in existing}
    if extra: matrix['rows'].extend(matrix_rows(assessment, extra))
    for row in matrix['rows']:
        analyse_prices(row, offers.get(row['id'], []), as_of, meta['kickoff'])
    matrix['as_of'] = as_of
    matrix['coverage'] = {'modeled_markets': len(matrix['rows']),
        'priced_markets': sum(r['quote_status'] == 'recent' for r in matrix['rows']),
        'movement_markets': sum(r.get('oci') is not None for r in matrix['rows']),
        'categories': dict(Counter(r['category'] for r in matrix['rows'])),
        'adapters': diagnostics}
    matrix['market_notes'] = [
        'Aggio basso: prezzi più competitivi nel campione osservato; liquidità e volumi non misurati.',
        'OCI da variazioni sugli stessi operatori: non prova smart money, disinteresse o informazioni privilegiate.',
        'Conferma e divergenza si riferiscono al mercato esatto; nessun trasferimento automatico alle combo.',
        'Aggio in calo non implica una direzione: servono le variazioni delle probabilità depurate dal margine.']
    annotate(matrix, as_of, assessment)
    return assessment


def expire(assessment, meta, as_of):
    now, start = stamp(as_of), stamp(meta.get('kickoff'))
    if meta.get('result') or now and start and now >= start: return
    trim_matrix(assessment.setdefault('matrix', {}))
    age = 1 if now and start and 0 < (start-now).total_seconds() <= 10800 else 6
    for row in assessment.get('matrix', {}).get('rows', []):
        at = stamp(row.get('provider_updated_at'))
        if row.get('quote_status') == 'recent' and (not at or not now or not 0 <= (now-at).total_seconds() <= age*3600):
            row.update(last_odd=row.get('odd'), odd=None, quote_status='stale', category='no_quote',
                       category_reason='Quota non aggiornata', ev_pct=None, ev_low_pct=None,
                       ev_high_pct=None, oci=None, oci_previous=None, edge_pp=None,
                       margin_pct=None, margin_delta_pp=None, margin_band='unavailable',
                       margin_trend='unavailable', margin_anomaly=False, fair_delta_pp=None,
                       market_fair_pct=None, implied_pct=None, persistence=None,
                       speed_pp_per_hour=None, amplitude_pct=None, paired_books=0,
                       agreeing_books=0, theoretical_value=False,
                       selected_margin_pct=None, selected_margin_delta_pp=None,
                       selected_movement=None, selected_market_odds=None,
                       movement_confirmed=False, fair_direction_coherent=False,
                       market_behavior='unavailable', stable_value=False)
    annotate(assessment.get('matrix', {}), as_of, assessment)
