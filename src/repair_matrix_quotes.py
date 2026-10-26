"""Repair current handicap prices from the private ledger, with zero API calls.

Statistical inputs and started forecasts stay frozen. Original source prices
remain in the ledger. Only upcoming fixtures receive corrected price readings.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from audit_storage import read_store, write_store
from lateral_matrix import MODEL_ID, matrix_rows, specification
from matrix_quotes import (migrate_handicap_history, ledger_offers, analyse_prices, expire)
from matrix_pipeline import build_board
from multimarket_model import stamp


def captured_handicaps(fixture, names):
    migrate_handicap_history(fixture)
    captured = {}
    for key, points in fixture.get('groups', {}).items():
        parts = key.split('|')
        if len(parts) != 6 or parts[0] != 'ah2' or not points: continue
        _, bid, kind, period, anchor, book = parts
        point = points[-1]
        captured[key] = {**point, 'bookmaker_id': book,
            'bookmaker': names.get(book, f'Operatore {book}'), 'bet_id': int(bid),
            'group': '|'.join(parts[1:-1]), 'specs': {
                side: specification(kind, period, side, (float(anchor)*(1 if side == 'home' else -1)) or 0.0)
                for side in point['odds']}}
    return captured


def repair_day(day, state, as_of):
    count, prices = 0, 0
    for match in day.get('matches', []):
        meta, assessment = match.get('fixture') or {}, match.get('multimarket') or {}
        kickoff = stamp(meta.get('kickoff'))
        if (assessment.get('model_id') != MODEL_ID or not assessment.get('rates') or
                not kickoff or kickoff <= stamp(as_of) or meta.get('status_short') not in ('NS', 'TBD')):
            continue
        fixture = state.get('matrix_quotes', {}).get(str(meta.get('fixture_id')))
        if not fixture: continue
        rows = assessment['matrix']['rows']
        names = {str(r['bookmaker_id']): r['bookmaker'] for r in rows if r.get('bookmaker_id') and r.get('bookmaker')}
        groups = captured_handicaps(fixture, names)
        offers = ledger_offers(state, meta['fixture_id'], groups, as_of, meta['kickoff'])
        specs = {r['id']: r['spec'] for r in rows if r.get('spec', {}).get('kind') == 'handicap'}
        specs.update({key: items[0]['spec'] for key, items in offers.items()})
        corrected = matrix_rows(assessment, specs)
        for row in corrected:
            analyse_prices(row, offers.get(row['id'], []), as_of, meta['kickoff'])
        assessment['matrix']['rows'] = [r for r in rows if r.get('spec', {}).get('kind') != 'handicap'] + corrected
        assessment['matrix']['price_adapter_repaired_at'] = as_of
        assessment['matrix']['as_of'] = as_of
        expire(assessment, meta, as_of)
        current = assessment['matrix']['rows']
        assessment['matrix']['coverage'].update(modeled_markets=len(current),
            priced_markets=sum(r.get('quote_status') == 'recent' for r in current),
            movement_markets=sum(r.get('oci') is not None for r in current),
            categories=dict(Counter(r.get('category', 'no_quote') for r in current)))
        count += 1
        prices += sum(r.get('quote_status') == 'recent' for r in corrected)
    if count:
        day['price_adapter_repaired_at'] = as_of
        day['updated_at'] = as_of
        build_board(day, as_of)
    return count, prices


def main(directory='data'):
    directory = Path(directory)
    as_of = datetime.now(timezone.utc).isoformat()
    state = read_store(directory/'arab_quote_state.json')
    repaired, counters = {}, {}
    for path in sorted(directory.glob('arab_quote_day[1-5].json')):
        day = read_store(path)
        count, prices = repair_day(day, state, as_of)
        if count:
            write_store(path, day)
            repaired[day['date']] = day
        counters[path.name] = {'fixtures': count, 'corrected_recent_handicap_prices': prices}
        print(f'{path.name}: repaired {count} future fixtures, {prices} recent handicap prices; no API calls', flush=True)
    if not repaired: raise RuntimeError('No current matrix fixtures repaired; no publication')
    dashboard_path = directory/'arab_quote_dashboard.json'
    dashboard = read_store(dashboard_path)
    dashboard['days'] = [repaired.get(day.get('date'), day) for day in dashboard.get('days', [])]
    dashboard['updated_at'] = as_of
    dashboard['price_adapter_repair'] = {'adapter_version': 2, 'at': as_of, 'api_calls': 0, 'days': counters}
    write_store(dashboard_path, dashboard)
    write_store(directory/'arab_quote_state.json', state)
    print('Handicap repair complete; original statistical capture times and started forecasts preserved', flush=True)


if __name__ == '__main__': main()
