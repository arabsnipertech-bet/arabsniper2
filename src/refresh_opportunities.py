"""Reprice existing future matrices from saved observations, with zero API calls."""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from audit_storage import read_store, write_store
from lateral_matrix import MODEL_ID, CONFIG_ID, base_specs, market_id, matrix_rows
from matrix_quotes import (CATALOG, partition, migrate_handicap_history, ledger_offers,
                           analyse_prices, expire)
from matrix_pipeline import build_board, selection_export
from multimarket_model import stamp
from opportunity_policy import POLICY_ID
from platinum_policy import POLICY_ID as PLATINUM_POLICY_ID
from gold_competitions import SCOPE_ID, enforce_day
from market_scope import MARKET_SCOPE_ID, trim_matrix, core_market
from proposal_stability import seed_stability
from copy import deepcopy


def cached_refresh_safe(paths):
    """A price/UI-only push needs no new statistical collection.

    Unknown paths fail to the normal scan. Scheduled/manual scans stay normal.
    """
    price_files = {'src/opportunity_policy.py', 'src/matrix_quotes.py', 'src/matrix_pipeline.py',
                   'src/homepage_preview.py', 'src/context_analysis.py', 'src/refresh_opportunities.py',
                   'src/statistics_integration.py', 'src/gold_policy_audit.py',
                   'src/market_scope.py', 'src/proposal_stability.py',
                   'src/platinum_policy.py', 'src/platinum_audit.py'}
    # This migration changes the computed market universe, not the Poisson
    # kernel or source statistics; the dedicated tests require identical core probabilities.
    if 'src/market_scope.py' in paths: price_files.add('src/lateral_matrix.py')
    # A competition change needs a provider schedule fetch: repricing only the
    # old snapshot cannot recover fixtures previously excluded by the whitelist.
    auxiliary = {'.github/workflows/fast_scan.yml', '.github/workflows/night_scan.yml', '.github/workflows/policy_tests.yml'}
    return bool(set(paths) & price_files) and all(
        p in price_files | auxiliary or p.startswith(('web/', 'tests/', 'docs/')) for p in paths)


def captured_prices(fixture, rows):
    migrate_handicap_history(fixture)
    names = {str(r['bookmaker_id']): r.get('bookmaker') for r in rows if r.get('bookmaker_id')}
    specs = {}
    for row in rows:
        spec = row.get('spec')
        if not core_market(spec): continue
        group, token, _ = partition(spec)
        for bid, (_, kind, period) in CATALOG.items():
            if period == spec['period'] and (kind == spec['kind'] or kind == 'total' and spec['kind'] == 'asian_total'):
                specs.setdefault(f'{bid}|{group}', {})[token] = spec
    captured = {}
    for key, points in fixture.get('groups', {}).items():
        if not points: continue
        parts = key.split('|')
        if 'handicap' in parts and parts[0] != 'ah2': continue
        normal = key.removeprefix('ah2|')
        if normal.startswith('asian'): normal = normal.split('|', 1)[1]
        group, book = normal.rsplit('|', 1)
        point = points[-1]
        if key.startswith('synthetic_combo_v1|'):
            from quote_sources import synthetic_margin
            if synthetic_margin(point) is not None and all(core_market(s) for s in point.get('specs', {}).values()):
                captured[key] = {**point, 'bookmaker_id': book, 'bookmaker': point.get('bookmaker') or names.get(book) or f'Operatore {book}'}
            continue
        if group not in specs: continue
        current_specs = {s: spec for s, spec in specs[group].items() if s in point['odds']}
        if not current_specs: continue
        captured[key] = {**point, 'bookmaker_id': book, 'bookmaker': names.get(book) or f'Operatore {book}',
            'bet_id': int(group.split('|')[0]), 'group': group, 'specs': current_specs}
    return captured


def refresh_day(day, state, as_of):
    count = 0
    scope_changed = (day.get('gold_scope') or {}).get('policy_id') != SCOPE_ID
    previous_count = len(day.get('matches', []))
    enforce_day(day)
    scope_changed = scope_changed or previous_count != len(day['matches'])
    for match in day.get('matches', []):
        meta, a = match.get('fixture') or {}, match.get('multimarket') or {}
        kickoff = stamp(meta.get('kickoff'))
        if (a.get('model_id') != MODEL_ID or not kickoff or kickoff <= stamp(as_of) or
                meta.get('status_short') not in ('NS', 'TBD') or meta.get('result')): continue
        matrix = a.get('matrix') or {}
        # Save the published choice before repricing can expire its quote.
        seed_stability(deepcopy(a), a)
        trim_matrix(matrix)
        rows = matrix.get('rows', [])
        if a.get('rates'):
            existing = {r['id'] for r in rows}
            missing = {market_id(s): s for s in base_specs() if market_id(s) not in existing}
            if missing: rows.extend(matrix_rows(a, missing))
        matrix['market_scope'] = {'id': MARKET_SCOPE_ID, 'modeled_markets': len(rows)}
        a['config_id'] = CONFIG_ID
        a.setdefault('config', {})['market_scope'] = MARKET_SCOPE_ID
        fixture = state.get('matrix_quotes', {}).get(str(meta.get('fixture_id')))
        if fixture:
            captured = captured_prices(fixture, rows)
            offers = ledger_offers(state, meta['fixture_id'], captured, as_of, meta['kickoff'])
            for row in rows:
                analyse_prices(row, offers.get(row['id'], []), as_of, meta['kickoff'], allow_cached=True)
        expire(a, meta, as_of)
        matrix['opportunity_refreshed_at'] = as_of
        matrix.setdefault('coverage', {}).update(modeled_markets=len(rows),
            priced_markets=sum(r.get('quote_status') in ('recent', 'cached_stable') for r in rows),
            movement_markets=sum(r.get('oci') is not None for r in rows),
            categories=dict(Counter(r.get('category', 'no_quote') for r in rows)))
        count += 1
    if count or scope_changed:
        day['opportunity_refreshed_at'] = as_of
        day['updated_at'] = as_of
        build_board(day, as_of)
    return count


def main(directory='data'):
    directory = Path(directory)
    as_of = datetime.now(timezone.utc).isoformat()
    state = read_store(directory/'arab_quote_state.json')
    refreshed, counters = {}, {}
    for path in sorted(directory.glob('arab_quote_day[1-5].json')):
        day = read_store(path)
        count = refresh_day(day, state, as_of)
        # Empty allowed schedules are valid. Publish the scope even when every
        # former fixture was excluded; otherwise stale minor leagues reappear.
        write_store(path, day)
        write_store(directory/path.name.replace('arab_quote_day', 'arab_quote_selections_day'),
                    selection_export(day, as_of))
        refreshed[day['date']] = day
        summary = day.get('matrix_summary') or {}
        upcoming = [m['multimarket'] for m in day.get('matches', [])
            if m.get('multimarket', {}).get('model_id') == MODEL_ID
            and m.get('fixture', {}).get('status_short') in ('NS', 'TBD')
            and not m['fixture'].get('result') and stamp(m['fixture'].get('kickoff'))
            and stamp(m['fixture']['kickoff']) > stamp(as_of)]
        policies = Counter(a.get('matrix', {}).get('opportunity_policy', {}).get('id') for a in upcoming)
        required = {market_id(s) for s in base_specs()}
        for a in upcoming:
            if a.get('matrix', {}).get('platinum_policy', {}).get('id') != PLATINUM_POLICY_ID:
                raise RuntimeError('Incomplete Platinum migration; refusing publication')
            if a.get('matrix', {}).get('opportunity_policy', {}).get('id') != POLICY_ID:
                raise RuntimeError('Incomplete Gold policy migration; refusing publication')
            if a.get('rates') and not required.issubset({r['id'] for r in a['matrix']['rows']}):
                raise RuntimeError('Incomplete Gold market migration; refusing publication')
        counters[path.name] = {'fixtures': count, 'eligible_markets': summary.get('eligible_markets', 0),
            'fixtures_with_proposal': summary.get('upcoming_fixtures_with_proposal', 0),
            'fixtures_with_platinum': summary.get('upcoming_fixtures_with_platinum', 0),
            'saved_policy_versions': dict(policies),
            'fixtures_with_combo': sum(any(r.get('spec', {}).get('kind') == 'halves_total'
                for r in a.get('matrix', {}).get('rows', [])) for a in upcoming),
            'allowed_fixtures': len(day.get('matches', [])),
            'excluded_fixtures': (day.get('gold_scope') or {}).get('excluded_fixtures', 0)}
        print(f'{path.name}: {counters[path.name]}; policy {POLICY_ID}; API calls: 0', flush=True)
    if not refreshed: raise RuntimeError('No day files restored; refusing publication')
    dashboard_path = directory/'arab_quote_dashboard.json'
    dashboard = read_store(dashboard_path)
    # Rebuild from the authoritative day files: no stale fallback day can leak
    # an excluded competition or an old recommendation into the dashboard.
    dashboard['days'] = sorted(refreshed.values(), key=lambda d: d['date'])
    dashboard['updated_at'] = as_of
    dashboard['opportunity_refresh'] = {'policy_id': POLICY_ID, 'at': as_of, 'api_calls': 0, 'days': counters}
    dashboard.setdefault('method', {}).setdefault('user_policy', {}).update(
        competition_policy=SCOPE_ID, universal_observation=False, romania_allowed=True,
        goals_only_proposals=True, drought_probability_bonus=0)
    dashboard['method'].setdefault('active_analysis', {}).update(
        competition_policy=SCOPE_ID, opportunity_policy=POLICY_ID,
        universal_observation=False, all_whitelisted_fixtures=True)
    dashboard['method'].update(
        competition_filter='Prime tre divisioni senior dei paesi in whitelist e coppe autorizzate; Romania inclusa; esclusi giovanili e livelli inferiori.',
        markers='Gold v6.1: cinque mercati gol e Combo PT+ST; ordine di valutazione fra mercati idonei.')
    write_store(dashboard_path, dashboard)
    write_store(directory/'arab_quote_state.json', state)
    print('Policy refresh complete: statistics, source timestamps and started forecasts preserved', flush=True)


if __name__ == '__main__':
    import sys
    if '--classify-push' in sys.argv:
        print('cached' if cached_refresh_safe(sys.stdin.read().splitlines()) else 'full')
    else: main()

