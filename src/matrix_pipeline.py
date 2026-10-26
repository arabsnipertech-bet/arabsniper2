"""Strict competition admission and uncapped observation within that scope."""
from collections import Counter
from copy import deepcopy
from lateral_matrix import MODEL_ID, CONFIG_ID, build_matrix, market_id, specification
from matrix_quotes import expire, safe_price_row
from multimarket_model import stamp
from opportunity_policy import POLICY_ID, goal_market
from gold_competitions import (SCOPE_ID, profile as whitelist_profile, fixture_allowed,
                               enforce_day, output_allowed)

LEGACY = {'over25_ft': specification('total', 'ft', 'over', 2.5),
          'over15_ht': specification('total', 'ht', 'over', 1.5),
          'over35_ft': specification('total', 'ft', 'over', 3.5),
          'gg_ht': specification('btts', 'ht', 'yes')}


universal_profile = whitelist_profile  # Compatibility alias; admission is strict.


def complete_day(ns, client, day, as_of):
    """Keep coverage explicit, including fixtures with failed data or no snapshot."""
    response = client.get('fixtures', {'date': day['date'], 'timezone': 'Europe/Rome'}) if client is not None else None
    rows = (response or {}).get('response', [])
    enforce_day(day)
    seen = {str(m.get('fixture', {}).get('fixture_id')) for m in day.get('matches', [])}
    for raw in rows:
        fid = str((raw.get('fixture') or {}).get('id'))
        if not fixture_allowed(raw) or fid in seen: continue
        meta = ns['fixture_meta'](raw)
        kickoff = stamp(meta.get('kickoff'))
        item = {'fixture': meta, 'statistics': {}, 'display': {'analysis_engine': MODEL_ID}}
        future = kickoff and kickoff > stamp(as_of) and meta.get('status_short') in ('NS', 'TBD')
        if future:
            from statistics_first import statistical_analysis
            a = build_matrix(statistical_analysis(item, as_of), as_of)
            a['matrix']['data_status'] = 'prior_only_after_missing_inputs'
            a['notes'].append('Raccolta dei dati della partita incompleta: valori di riferimento, non storico squadra inventato.')
        else:
            a = {'model_id': MODEL_ID, 'status': 'no_prematch_snapshot', 'captured_at': None,
                 'matrix': {'rows': [], 'data_status': 'no_prematch_snapshot', 'coverage': {'modeled_markets': 0, 'priced_markets': 0}},
                 'markets': [], 'profiles': {}, 'data_quality': {},
                 'best_candidate': None, 'selected_markets': []}
            result = ns['_result_payload'](raw)
            if result: meta['result'] = result
            item['display'].update(primary_choice=None, ranked_choices=[], market_candidates=[], markets={})
        item['multimarket'] = a
        meta['data_freshness'] = 'data_unavailable' if future else 'no_prematch_snapshot'
        day.setdefault('matches', []).append(item)
        seen.add(fid)
    day['matches'].sort(key=lambda m: ((m.get('fixture') or {}).get('kickoff') or '', str((m.get('fixture') or {}).get('fixture_id'))))
    day['universal_coverage'] = {'available_fixture_ids': len({str(r.get('fixture', {}).get('id')) for r in rows if fixture_allowed(r)}) if response is not None else None,
        'displayed_fixtures': len(day['matches']),
        'excluded_by_competition': sum(not fixture_allowed(r) for r in rows),
        'coverage_basis': 'provider_schedule' if response is not None else 'provider_unavailable',
        'competition_policy': SCOPE_ID,
        'statistics_without_prices_are_retained': True}
    enforce_day(day)


def build_board(day, as_of):
    enforce_day(day)
    board = {key: {'label': key, 'selected': [], 'eligible': 0, 'remaining_slots': None, 'started_selections': 0} for key in LEGACY}
    categories = Counter()
    modeled, priced, complete, eligible, proposals = 0, 0, 0, 0, 0
    for match in day.get('matches', []):
        a, meta = match.get('multimarket') or {}, match.get('fixture') or {}
        kickoff = stamp(meta.get('kickoff'))
        if kickoff and kickoff <= stamp(as_of):
            for row in a.get('markets', []):
                if row.get('published_selection') and row.get('market') in board:
                    board[row['market']]['started_selections'] += 1
        if a.get('model_id') != MODEL_ID: continue
        if kickoff and kickoff > stamp(as_of): expire(a, meta, as_of)
        rows = [safe_price_row(r) for r in a.get('matrix', {}).get('rows', [])]
        indexed = {r['id']: r for r in rows}
        modeled += len(rows)
        priced += sum(r.get('quote_status') == 'recent' for r in rows)
        complete += bool(rows)
        category_counts = Counter(r.get('category', 'no_quote') for r in rows)
        categories.update(category_counts)
        if kickoff and kickoff <= stamp(as_of) or meta.get('result'):
            continue  # Immutable forecast, even when it is no longer upcoming.
        a.get('matrix', {})['summary'] = {'categories': dict(category_counts), 'observed_markets': len(rows),
                                        'priced_markets': sum(r.get('quote_status') == 'recent' for r in rows)}
        policy = a.get('matrix', {}).get('opportunity_policy') or {}
        eligible += policy.get('eligible_markets', 0)
        proposals += bool(policy.get('best_market_id'))
        a['selected_markets'], a['best_candidate'] = [], None
        a['markets'] = [r for r in a.get('markets', []) if r.get('market') in LEGACY]
        # Backward-compatible four-market exports. The dashboard uses the full
        # matrix; these flags no longer restrict fixtures or the other markets.
        for old in a.get('markets', []):
            if old['market'] not in LEGACY: continue
            equivalents = [r for r in rows if goal_market(r) == old['market'] and
                           (r.get('opportunity') or {}).get('eligible') is True]
            new = min(equivalents, key=lambda r: r['opportunity']['rank']) if equivalents else indexed.get(market_id(LEGACY[old['market']]))
            if not new: continue
            chosen = (new.get('opportunity') or {}).get('eligible') is True
            old.update(statistical_pct=new['statistical_pct'], statistical_candidate=new['quality'] == 'supported',
                       primary_eligible=chosen, published_selection=chosen, published_rank=None,
                       statistical_score=new['low_pct'], selection_reasons=[], odd=new.get('odd'),
                       matrix_category=new.get('category'))
            old['probabilities']['statistics'] = new['statistical_pct']/100
            old['probabilities']['selection_reference'] = new['statistical_pct']/100
            old['quote_status'] = new['quote_status']
            old['book_probability_pct'] = new.get('market_fair_pct')
            old['quote'] = {'bookmaker': new.get('bookmaker'), 'bookmaker_id': new.get('bookmaker_id'),
                'provider_updated_at': new.get('provider_updated_at'),
                'movement': {'first_odds': {old['selection']: new.get('first_odd')}}} if new.get('odd') else None
            old['price_assessment'] = {'reference_source': 'statistics_only',
                'reference_fair_odd': new['fair_odd'], 'estimated_return_pct': new.get('ev_pct'),
                'validated_advantage': False}
            old['opportunity'] = deepcopy(new.get('opportunity') or {})
            old['matrix_market_id'] = new['id']
            old['selection_reasons'] = list(old['opportunity'].get('reasons') or [])
            old['published_rank'] = old['opportunity'].get('rank')
            old['market_confirmation'] = {'status': {'convergence': 'confirmed', 'contrarian': 'contrary'}.get(old['opportunity'].get('signal'), 'neutral'),
                                         'scope': 'same_market', 'patterns': [], 'probability_bonus': 0}
            if chosen:
                a['selected_markets'].append(old['market'])
                if kickoff and kickoff > stamp(as_of):
                    board[old['market']]['selected'].append({'fixture_id': str(meta['fixture_id']),
                        'market': old['market'], 'kickoff': meta['kickoff'], 'statistical_pct': new['statistical_pct'],
                        'statistical_score': new['low_pct'], 'confirmation': old['market_confirmation']['status']})
                if new['id'] == policy.get('best_market_id'):
                    old['matrix_ev_low_pct'] = new.get('ev_low_pct')
                    a['best_candidate'] = old
    for key, group in board.items():
        group['selected'].sort(key=lambda r: (r['kickoff'], r['fixture_id']))
        group['eligible'] = len(group['selected'])
        for rank, row in enumerate(group['selected'], 1): row['rank'] = rank
    day['selection_board'] = {'model_id': MODEL_ID, 'config_id': CONFIG_ID, 'as_of': as_of,
        'max_per_market': None, 'markets': board, 'ranking': 'all_fixtures_observation', 'book_weight_in_statistics': 0, 'book_weight_in_ranking': 0}
    day['matrix_summary'] = {'model_id': MODEL_ID, 'as_of': as_of, 'analyzed_fixtures': complete,
        'displayed_fixtures': len(day.get('matches', [])), 'modeled_markets': modeled,
        'priced_markets': priced, 'categories': dict(categories), 'selection_limit': None}
    day['matrix_summary'].update(opportunity_policy=POLICY_ID, competition_policy=SCOPE_ID, eligible_markets=eligible,
                                 upcoming_fixtures_with_proposal=proposals)
    day.setdefault('multimarket_method', {}).update(model_id=MODEL_ID,
        competition_policy=SCOPE_ID, opportunity_policy=POLICY_ID)
    refresh_exports(day, as_of)
    return day['selection_board']


def refresh_exports(day, as_of):
    """Keep legacy display/ranking projections on the exact Gold policy."""
    rankings = {key: [] for key in LEGACY}
    for match in day.get('matches', []):
        a, meta = match.get('multimarket') or {}, match.get('fixture') or {}
        kickoff = stamp(meta.get('kickoff'))
        if (a.get('model_id') != MODEL_ID or not kickoff or kickoff <= stamp(as_of)
                or meta.get('status_short') not in ('NS', 'TBD') or meta.get('result')): continue
        candidates = []
        for row in a.get('markets', []):
            if row.get('market') not in rankings: continue
            candidate = {**{k: meta.get(k) for k in ('fixture_id', 'match', 'time', 'kickoff', 'league', 'country')},
                'market': row['market'], 'selection': row['selection'],
                'probability_pct': row['statistical_pct'], 'odd': row.get('odd'),
                'gate': {'eligible': row.get('published_selection') is True, 'reasons': row.get('selection_reasons', [])},
                'ranking_score': row.get('statistical_score'), 'market_confirmation': row.get('market_confirmation'),
                'published_rank': row.get('published_rank'), 'opportunity': deepcopy(row.get('opportunity'))}
            candidates.append(candidate)
            if candidate['gate']['eligible']: rankings[row['market']].append(candidate)
        match['display'] = {'analysis_engine': MODEL_ID, 'primary_choice': a.get('best_candidate'),
            'ranked_choices': sorted((r for r in candidates if r['gate']['eligible']), key=lambda r: r['published_rank'] or 999),
            'market_candidates': candidates, 'markets': {}}
    for rows in rankings.values(): rows.sort(key=lambda r: (r['published_rank'] or 999, str(r['fixture_id'])))
    day['market_rankings'] = rankings
    day['probability_rankings'] = deepcopy(rankings)


def selection_export(day, as_of):
    return {'date': day.get('date'), 'generated_at': as_of,
        'gold_scope': deepcopy(day.get('gold_scope')), 'opportunity_policy': POLICY_ID,
        'selection_board': day.get('selection_board', {}), 'rankings': day.get('market_rankings', {}),
        'probability_rankings': day.get('probability_rankings', {})}
