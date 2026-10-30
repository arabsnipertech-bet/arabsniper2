"""Exact half-line equivalences and explicitly estimated disjoint-half prices.

No outcome is assembled across operators, lines, periods or provider snapshots.
"""
from copy import deepcopy
from datetime import datetime
from math import isfinite
import re
from market_discovery import discover_payload

SOURCE_VERSION = 'quote-sources-v1'
TOTALS = {
    5: ('ft', False, ('Goals Over/Under',)),
    6: ('ht', False, ('Goals Over/Under First Half',)),
    26: ('sh', False, ('Goals Over/Under - Second Half',)),
    50: ('ft', True, ('Goal Line', 'Asian Goal Line', 'Total Goals Asian')),
    336: ('ft', True, ('Goal Line', 'Asian Goal Line', 'Total Goals Asian')),
    72: ('ht', True, ('Goal Line (1st Half)', 'Asian Goal Line (1st Half)',
                    'Total Goals Asian (1st Half)', 'Asian Goal Line - First Half',
                    'Total Goals Asian - First Half')),
}
TARGETS = frozenset((('ht', 1.5), ('ht', 2.5), ('ft', 2.5), ('ft', 3.5)))


def stamp(value):
    try:
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return value if value.tzinfo else None
    except (TypeError, ValueError):
        return None


def number(value):
    if isinstance(value, bool): return None
    try:
        value = float(value)
        return value if isfinite(value) else None
    except (TypeError, ValueError):
        return None


def norm(value):
    return re.sub(r'[^a-z0-9]+', ' ', str(value).lower()).strip()


def fresh(updated, now, kickoff):
    updated, now, kickoff = stamp(updated), stamp(now), stamp(kickoff)
    if not all((updated, now, kickoff)) or now >= kickoff: return False
    hours = 1 if (kickoff-now).total_seconds() <= 10800 else 6
    return 0 <= (now-updated).total_seconds() <= hours*3600


def total_pairs(payload):
    """Yield only whole, unambiguous Over/Under pairs from one raw bet."""
    for record in discover_payload(payload).get('response', []):
        fixture = record.get('fixture') or {}
        if fixture.get('id') is None: continue
        for book in record.get('bookmakers', []):
            if book.get('id') is None: continue
            for bet in book.get('bets', []):
                if bet.get('_discovery_rejected'): continue
                try: bid = int(bet.get('id'))
                except (TypeError, ValueError): continue
                if bid not in TOTALS: continue
                period, asian, names = TOTALS[bid]
                if norm(bet.get('name')) not in {norm(n) for n in names}: continue
                lines, invalid = {}, set()
                for value in bet.get('values', []):
                    match = re.fullmatch(r'(over|under)\s+(\d+(?:\.\d+)?)',
                                         str(value.get('value', '')).strip().lower().replace(',', '.'))
                    if not match: continue
                    side, line = match[1], float(match[2])
                    if line % 1 != .5: continue
                    odd = number(value.get('odd'))
                    pair = lines.setdefault(line, {})
                    if odd is None or odd <= 1 or side in pair and pair[side] != odd:
                        invalid.add(line)
                    else: pair[side] = odd
                for line, odds in lines.items():
                    if line in invalid or set(odds) != {'over', 'under'}: continue
                    yield {'fixture_id': str(fixture['id']), 'kickoff': fixture.get('date'),
                           'bookmaker_id': str(book['id']), 'bookmaker': str(book.get('name') or book['id']),
                           'bet_id': bid, 'bet_name': bet.get('name'), 'period': period,
                           'asian': asian, 'line': line, 'odds': odds,
                           'provider_updated_at': record.get('update'),
                           'source': deepcopy(bet.get('_quote_provenance') or {}),
                           'discovery': deepcopy(bet.get('_market_discovery') or {}),
                           '_record': record, '_book': book}


def normalize_payload(payload, observed_at):
    """Prefer a fresh complete native pair; otherwise alias the exact Asian .5.

    Generated pairs keep their actual source ID/time and never patch one side of
    a native market. Repeated normalization is idempotent.
    """
    result = deepcopy(payload or {})
    result['response'] = [r for r in result.get('response', []) if not r.get('_halfline_fallback')]
    result = discover_payload(result)
    pairs = list(total_pairs(result))
    native = {(p['fixture_id'], p['bookmaker_id'], p['period'], p['line'])
              for p in pairs if not p['asian'] and fresh(p['provider_updated_at'], observed_at, p['kickoff'])}
    candidates = {}
    for pair in pairs:
        key = (pair['fixture_id'], pair['bookmaker_id'], pair['period'], pair['line'])
        if (not pair['asian'] or (pair['period'], pair['line']) not in TARGETS | {('ht', .5)}
                or key in native or not fresh(pair['provider_updated_at'], observed_at, pair['kickoff'])):
            continue
        old = candidates.get(key)
        if old is None or (stamp(pair['provider_updated_at']), -pair['bet_id']) > (stamp(old['provider_updated_at']), -old['bet_id']):
            candidates[key] = pair
    for pair in candidates.values():
        bid = 6 if pair['period'] == 'ht' else 5
        provenance = {'version': SOURCE_VERSION, 'kind': 'asian_halfline',
                      'source_bet_id': pair['discovery'].get('source_bet_id',pair['bet_id']),
                      'source_bet_name': pair['discovery'].get('source_bet_name',pair['bet_name']),
                      'discovery': pair['discovery'],
                      'source_period': pair['period'], 'source_line': pair['line'],
                      'provider_updated_at': pair['provider_updated_at']}
        bet = {'id': bid, 'name': TOTALS[bid][2][0], '_quote_provenance': provenance,
               'values': [{'value': f'{side.title()} {pair["line"]:g}', 'odd': odd}
                          for side, odd in pair['odds'].items()]}
        record = {k: deepcopy(v) for k, v in pair['_record'].items() if k != 'bookmakers'}
        record.update(_halfline_fallback=True, bookmakers=[{
            'id': pair['_book']['id'], 'name': pair['bookmaker'], 'bets': [bet]}])
        result['response'].append(record)
    return result


def synthetic_combo_groups(payload, fid, observed_at):
    """Q_HT*Q_SH is an estimate, never a directly offered combined price."""
    groups = {}
    for pair in total_pairs(payload):
        if (pair['fixture_id'] != str(fid) or pair['asian'] or pair['line'] != .5
                or pair['period'] not in ('ht', 'sh')
                or not fresh(pair['provider_updated_at'], observed_at, pair['kickoff'])): continue
        margin = 100*(sum(1/v for v in pair['odds'].values())-1)
        if not 0 <= margin <= 9+1e-9: continue
        key = (pair['bookmaker_id'], stamp(pair['provider_updated_at']))
        group = groups.setdefault(key, {})
        component = {k: deepcopy(v) for k, v in pair.items() if not k.startswith('_')}
        component['margin_pct'] = margin
        previous = group.get(pair['period'])
        if previous is not None and previous['odds'] != component['odds']:
            group['ambiguous'] = True
        group[pair['period']] = component
    result = {}
    for (book, _), parts in groups.items():
        if set(parts) != {'ht', 'sh'}: continue
        ht, sh = parts['ht'], parts['sh']
        odd = ht['odds']['over']*sh['odds']['over']
        if not isfinite(odd): continue
        spec = {'kind': 'halves_total', 'period': 'ft', 'side': 'over', 'line': .5, 'extra': .5}
        key = f'synthetic_combo_v1|halves_total|ft|0.5|0.5|{book}'
        if any(str(p.get('discovery',{}).get('source_bet_id',p['bet_id']))!=str(p['bet_id']) for p in (ht,sh)):
            tokens='_'.join(p.get('discovery',{}).get('source_token',str(p['bet_id'])) for p in (ht,sh))
            key=f'synthetic_combo_discovery_v1|{tokens}|halves_total|ft|0.5|0.5|{book}'
        value = {'odds': {'over': odd}, 'specs': {'over': spec}, 'complete': False,
                 'bookmaker_id': book, 'bookmaker': ht['bookmaker'], 'bet_id': 0,
                 'group': 'synthetic_combo_v1|halves_total|ft|0.5|0.5',
                 'observed_at': observed_at, 'provider_updated_at': ht['provider_updated_at'],
                 'quote_source': 'synthetic_combo', 'quote_executable': False,
                 'quote_provenance': {'version': SOURCE_VERSION, 'kind': 'synthetic_combo',
                     'formula': 'Q_PT * Q_ST', 'assumption': 'independent_disjoint_halves',
                     'components': [ht, sh], 'margin_basis': 'each_component',
                     'component_margins_pct': [ht['margin_pct'], sh['margin_pct']]}}
        old = result.get(key)
        if old is None or stamp(value['provider_updated_at']) > stamp(old['provider_updated_at']): result[key] = value
    return result


def direct_combo_groups(payload, fid, observed_at):
    """Semantic match-total both-halves market, with an explicit Yes/No pair.

    Team-specific and BTTS-in-both-halves settlements are rejected by discovery.
    The canonical 184 adapter preserves the actual complement and source ID.
    """
    result = {}
    ambiguous = set()
    now = stamp(observed_at)
    for record in discover_payload(payload).get('response', []):
        fixture = record.get('fixture') or {}
        at, start = stamp(record.get('update')), stamp(fixture.get('date'))
        if str(fixture.get('id')) != str(fid) or not all((at, start, now)) or at > now or now >= start: continue
        for book in record.get('bookmakers', []):
            if book.get('id') is None: continue
            for bet in book.get('bets', []):
                if bet.get('_discovery_rejected'): continue
                if str(bet.get('id')) != '184' or norm(bet.get('name')) != 'to score in both halves': continue
                odds, invalid = {}, False
                for value in bet.get('values', []):
                    token = {'yes':'over', 'no':'not_over'}.get(norm(value.get('value')))
                    odd = number(value.get('odd'))
                    if token is None or odd is None or odd <= 1 or token in odds and odds[token] != odd:
                        invalid = True; continue
                    odds[token] = odd
                if invalid or set(odds) != {'over', 'not_over'}: continue
                group = '184|halves_total|ft|0.5|0.5'
                key = f'direct_combo_v1|{group}|{book["id"]}'
                discovery=bet.get('_market_discovery') or {}
                if discovery and str(discovery.get('source_bet_id'))!='184':
                    key=f'direct_combo_discovery_v1|{discovery["source_token"]}|{group}|{book["id"]}'
                candidate = {'odds':odds, 'specs':{'over':{'kind':'halves_total','period':'ft','side':'over','line':.5,'extra':.5}},
                    'complete':True, 'bookmaker_id':str(book['id']), 'bookmaker':str(book.get('name') or book['id']),
                    'bet_id':184, 'group':group, 'observed_at':observed_at, 'provider_updated_at':record['update'],
                    'quote_source':'bookmaker', 'quote_executable':True,
                    'quote_provenance':{'version':SOURCE_VERSION, 'kind':'direct_combo',
                        'source_bet_id':discovery.get('source_bet_id',184), 'discovery':discovery,
                        'source_bet_name':discovery.get('source_bet_name',bet['name']), 'source_outcome':'Yes', 'opposite_outcome':'No'}}
                old = result.get(key)
                snapshot = (key, at)
                if snapshot in ambiguous: continue
                if old and at == stamp(old['provider_updated_at']) and old['odds'] != odds:
                    ambiguous.add(snapshot); result.pop(key); continue
                if old is None or at > stamp(old['provider_updated_at']): result[key] = candidate
    return result


def synthetic_margin(offer):
    parts = (offer.get('quote_provenance') or {}).get('components') or []
    if offer.get('quote_source') != 'synthetic_combo' or len(parts) != 2: return None
    if {p.get('period') for p in parts} != {'ht', 'sh'}: return None
    if len({(p.get('fixture_id'), p.get('bookmaker_id'), p.get('provider_updated_at')) for p in parts}) != 1: return None
    if any(p.get('line') != .5 or set(p.get('odds', {})) != {'over', 'under'} for p in parts): return None
    if any(number(v) is None or number(v) <= 1 for p in parts for v in p['odds'].values()): return None
    margins = [100*(sum(1/number(v) for v in p['odds'].values())-1) for p in parts]
    product = parts[0]['odds']['over']*parts[1]['odds']['over']
    if abs((offer.get('odds') or {}).get('over', 0)-product) > 1e-9: return None
    return max(margins) if all(0 <= v <= 9+1e-9 for v in margins) else None


def synthetic_fair(offer):
    parts = (offer.get('quote_provenance') or {}).get('components') or []
    if synthetic_margin(offer) is None: return None
    value = 1.
    for part in parts:
        odds = part['odds']
        value *= (1/odds['over'])/sum(1/v for v in odds.values())
    return value
