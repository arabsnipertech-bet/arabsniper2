"""Statistics-first pipeline with the active scoped V5 matrix and V4 replay."""
from copy import deepcopy

from selection_policy import competition
from multimarket_model import collect_quotes, update_ledger, stamp
from statistics_first import (statistical_analysis, confirm_markets, history, numeric,
                              build_selection_board, MODEL_ID, CONFIG, VALIDATION_FROM)


def install(ns, matrix_mode=False):
    # Scans freeze new evidence only. Historical settlement/reporting is nightly.
    ns['DEFER_AUDIT'] = True
    engine_id = MODEL_ID
    if matrix_mode:
        from lateral_matrix import MODEL_ID as engine_id, build_matrix
        from matrix_quotes import attach as attach_matrix
        from matrix_pipeline import universal_profile, fixture_allowed, complete_day, build_board, selection_export
        from gold_competitions import SCOPE_ID, output_allowed, fixture_profile
        from proposal_stability import seed_stability
        ns['DEFAULT_EXCLUDED_COUNTRIES'].clear()
        ns['fixture_allowed'] = fixture_allowed
        ns['output_fixture_allowed'] = output_allowed
        ns['fixture_filter_profile'] = lambda row, excluded=(): fixture_profile(row)
        ns['competition_allowed'] = lambda league, excluded=(): universal_profile(league)['allowed']
    ns['ACTIVE_ANALYSIS_ENGINE'] = engine_id
    ns['DEFAULT_EXCLUDED_COUNTRIES'].discard('Egypt')
    review, history_context = {}, {}

    def competition_profile(league):
        result = universal_profile(league) if matrix_mode else competition(league)
        if result.get('reason') == 'unmapped_competition_review':
            review[(str(league.get('country')), str(league.get('name')))] = dict(league)
        return result
    ns['competition_profile'] = competition_profile

    old_row = ns.setdefault('_statistics_match_row_base', ns['_match_row'])
    def match_row(fixture, team_id=None):
        row = old_row(fixture, team_id)
        if not row:
            return None
        f = fixture.get('fixture') or {}
        when, cutoff = stamp(f.get('date')), stamp(history_context.get('as_of', ns['iso_now']()))
        if not when or not cutoff or when >= cutoff:
            return None
        score, status = fixture.get('score') or {}, (f.get('status') or {}).get('short')
        full = score.get('fulltime') or {}
        if status == 'FT' and any(full.get(k) is None for k in ('home', 'away')):
            full = fixture.get('goals') or {}
        if any(not numeric(full.get(k)) or full[k] < 0 for k in ('home', 'away')):
            return None
        h, a = full['home'], full['away']
        ht = score.get('halftime') or {}
        if not all(numeric(ht.get(k)) and 0 <= ht[k] <= full[k] for k in ('home', 'away')):
            row['halftime_available'] = False
            for key in ('home_ht_goals', 'away_ht_goals', 'total_ht', 'over15_ht', 'gg_ht',
                        'team_goals_ht', 'team_conceded_ht', 'team_goals_2h', 'team_conceded_2h'):
                if key in row:
                    row[key] = None
        row.update(kickoff=f['date'], season=(fixture.get('league') or {}).get('season'),
                   friendly='friendl' in str((fixture.get('league') or {}).get('name', '')).lower(),
                   home_goals=h, away_goals=a, total_ft=h+a,
                   over25_ft=h+a >= 3, over15_ft=h+a >= 2, gg_ft=h > 0 and a > 0)
        if row.get('side') in ('home', 'away'):
            gf, ga = (h, a) if row['side'] == 'home' else (a, h)
            row.update(research_ft={'scored': gf, 'conceded': ga},
                       team_goals_ft=gf, team_conceded_ft=ga)
            if row.get('halftime_available'):
                row['team_goals_2h'] = max(0, gf-row['team_goals_ht'])
                row['team_conceded_2h'] = max(0, ga-row['team_conceded_ht'])
        return row
    ns['_match_row'] = match_row

    def recent(client, team_id, last=8):
        payload = client.get('fixtures', {'team': team_id, 'last': 32, 'status': 'FT'})
        rows = [match_row(r, team_id) for r in (payload or {}).get('response', [])]
        validated = history([r for r in rows if r], history_context.get('as_of', ns['iso_now']()),
                            history_context.get('season'))
        by_id = {str(r['fixture_id']): r for r in rows if r}
        # Preserve full rows for existing API summaries; compact only inside the model.
        return [by_id[str(r['fixture_id'])] for r in validated]
    ns['get_team_recent_matches'] = recent

    def fixture(client, row, state, cache):
        if matrix_mode and not fixture_allowed(row): return None
        meta = ns['fixture_meta'](row)
        if not meta:
            return None
        cutoff = ns['iso_now']()
        kickoff = stamp(meta.get('kickoff'))
        if not kickoff or kickoff <= stamp(cutoff):
            return None
        history_context.update(as_of=cutoff, season=meta.get('season'))
        # 1. Independent statistical inputs; the quotes have not been requested.
        home = ns['enrich_team_matches_with_xg'](client, recent(client, ns['safe_int'](meta['home_id'])),
                                                  ns['safe_int'](meta['home_id']))
        away = ns['enrich_team_matches_with_xg'](client, recent(client, ns['safe_int'](meta['away_id'])),
                                                  ns['safe_int'](meta['away_id']))
        league_key = f"{meta['league_id']}::{meta['season']}"
        if league_key not in cache:
            cache[league_key] = ns['get_league_baseline'](client, ns['safe_int'](meta['league_id']),
                                                        ns['safe_int'](meta['season']))
            cache[league_key]['as_of'] = cutoff
        h2h = ns['summarize_h2h'](ns['get_h2h_matches'](client, ns['safe_int'](meta['home_id']),
                                                     ns['safe_int'](meta['away_id'])))
        output = {'fixture': meta, 'statistics': {
            'home': ns['summarize_team_matches'](home, 'home'),
            'away': ns['summarize_team_matches'](away, 'away'),
            'league': cache[league_key], 'h2h': h2h}}
        assessment = statistical_analysis(output, cutoff)
        assessment['statistics_as_of'] = cutoff
        if matrix_mode:
            assessment['markets'] = [r for r in assessment['markets'] if r['market'] in ('over15_ht', 'gg_ht', 'over25_ft', 'over35_ft')]
            assessment = build_matrix(assessment, cutoff)
        # 2. Reuse the existing API-Football recovery/catalogue and quote ledger.
        if getattr(client, 'odds_enricher', None) is not None:
            client.odds_enricher.register_fixture(meta)
        payload = client.get('odds', {'fixture': ns['safe_int'](meta['fixture_id'])})
        observed = ns['iso_now']()
        if matrix_mode:
            from near_kickoff_quotes import recover_near_kickoff
            payload = recover_near_kickoff(client, payload, state, meta, observed)
            observed = ns['iso_now']()
        if stamp(observed) >= kickoff:
            return None
        fixture_state = state.setdefault('fixtures', {}).setdefault(str(meta['fixture_id']), {'meta': meta, 'markets': {}})
        fixture_state['meta'] = meta
        book = {}
        assessment['captured_at'] = observed
        if matrix_mode:
            attach_matrix(assessment, payload or {}, state, meta, observed)
            from adaptive_pricing import freeze_pattern
            freeze_pattern(ns, output, assessment, observed)
            validation = assessment['matrix']['coverage']['adapters']
        else:
            markets, validation = ns['extract_bookmaker_markets_with_audit'](payload or {})
            for key in ns['MARKET_SELECTIONS']:
                consensus = ns['build_market_consensus'](markets, key)
                if consensus is not None:
                    market_state = fixture_state.setdefault('markets', {}).setdefault(key, {})
                    book[key] = ns['update_market_state'](market_state, consensus, observed)
            captured = collect_quotes(payload or {}, observed).get(str(meta['fixture_id']), {})
            quotes = update_ledger(state, meta['fixture_id'], captured, observed, meta['kickoff'])
            confirm_markets(assessment, quotes, meta, observed)
        output['multimarket'] = assessment
        output['book'] = {'markets': book, 'market_validation': validation}
        p = {r['market']: {r['selection']: r['probabilities']['statistics']}
             for r in assessment['markets']}
        output['model'] = {'active_engine': engine_id, 'probabilities_raw': p,
            'probabilities_calibrated': deepcopy(p), 'calibration_applied': False,
            'probabilities': deepcopy(p), 'lambdas': deepcopy(assessment['rates']),
            'reliability_pct': 100*min(v['sample']/8 for v in assessment['data_quality'].values()),
            'markers': {}}
        output['model']['lambdas'].update(
            total_ft=assessment['rates']['home_ft']+assessment['rates']['away_ft'],
            total_ht=assessment['rates']['home_ht']+assessment['rates']['away_ht'])
        output['display'] = {'analysis_engine': engine_id, 'primary_choice': None,
                             'ranked_choices': [], 'market_candidates': [], 'markets': {}}
        if matrix_mode:
            # Project the matrix into the four legacy exports; no second quote
            # parser, ledger, OCI or overround calculation is required.
            build_board({'matches': [output]}, observed)
            if assessment['matrix'].get('calibration'):
                output['model']['calibration_applied'] = True
                output['model']['calibration'] = assessment['matrix']['calibration']
                from adaptive_calibration import market_of
                reference = assessment['matrix'].get('calibration_reference')
                calibrated = output['model']['probabilities_calibrated']
                raw = output['model']['probabilities_raw']
                for leg in [*assessment['matrix']['rows'], *([reference] if reference else [])]:
                    market = market_of(leg.get('spec') or {})
                    if market and leg.get('calibration'):
                        side = leg['spec']['side']
                        raw.setdefault(market, {})[side] = (leg.get('calibration_base') or leg)['win_weight']
                        calibrated.setdefault(market, {})[side] = leg['win_weight']
                output['model']['probabilities'] = deepcopy(calibrated)
            if assessment.get('favorite_trap_pattern'):
                output['model']['favorite_trap_pattern'] = assessment['favorite_trap_pattern']
        return output
    ns['build_fixture_output'] = fixture

    old_scan = ns['_scan_dates_before_prematch_audit']
    def scan(client, dates, excluded):
        previous = {}
        for index in range(1, 6):
            saved = ns['read_json'](ns['DATA_DIR']/f'arab_quote_day{index}.json', {}) or {}
            if saved.get('date') in dates:
                previous.update({str(m.get('fixture', {}).get('fixture_id')): m
                                 for m in saved.get('matches', [])})
        enricher = getattr(client, 'odds_enricher', None)
        if matrix_mode and enricher is not None:
            enricher.reserve_priority(previous.values(), ns['iso_now']())
        output = old_scan(client, dates, excluded)
        # 3. Decide which forecasts are actually published BEFORE freezing them.
        as_of = ns['iso_now']()
        for day in output.get('days', []):
            if matrix_mode:
                complete_day(ns, client, day, as_of)
                # Schedule completion can now collect odds after a statistics
                # failure. Assess freshness/freeze boundaries after those reads.
                as_of = ns['iso_now']()
            for index, match in enumerate(day.get('matches', [])):
                meta = match.get('fixture') or {}
                kickoff = stamp(meta.get('kickoff'))
                old = previous.get(str(meta.get('fixture_id')))
                # A fixture may kick off while the remaining fixtures are collected.
                # Its already-published forecast must survive this boundary intact.
                if old and kickoff and kickoff <= stamp(as_of):
                    restored = deepcopy(old)
                    restored['fixture'].update({k: meta[k] for k in
                        ('live', 'result', 'status_short', 'status_long', 'elapsed') if k in meta})
                    day['matches'][index] = restored
                elif matrix_mode and old and kickoff and kickoff > stamp(as_of):
                    seed_stability(old.get('multimarket') or {}, match.get('multimarket') or {})
            board = build_board(day, as_of) if matrix_mode else build_selection_board(day, as_of)
            if matrix_mode:
                summary = day['matrix_summary']
                coverage = day.get('universal_coverage', {})
                ns['log'](f"V5 {day['date']}: {summary['displayed_fixtures']} partite visibili / "
                          f"{coverage.get('available_fixture_ids')} disponibili; "
                          f"{summary['analyzed_fixtures']} matrici, {summary['modeled_markets']} esiti, "
                          f"{summary['priced_markets']} quote recenti; categorie {summary['categories']}")
            if matrix_mode:
                rankings = day['market_rankings']
            else:
                rankings = {key: [] for key in board['markets']}
                for match in day.get('matches', []):
                    assessment = match.get('multimarket') or {}
                    if assessment.get('model_id') != engine_id:
                        continue
                    meta = match['fixture']
                    candidates = []
                    for row in assessment['markets']:
                        candidate = {**{k: meta.get(k) for k in ('fixture_id', 'match', 'time', 'kickoff', 'league', 'country')},
                            'market': row['market'], 'selection': row['selection'],
                            'probability_pct': row['statistical_pct'], 'odd': row['odd'],
                            'gate': {'eligible': row['published_selection'], 'reasons': row['selection_reasons']},
                            'ranking_score': row['statistical_score'],
                            'market_confirmation': row['market_confirmation'],
                            'published_rank': row['published_rank']}
                        candidates.append(candidate)
                        kickoff = stamp(meta.get('kickoff'))
                        if row['published_selection'] and kickoff and kickoff > stamp(as_of):
                            rankings[row['market']].append(candidate)
                    for rows in rankings.values():
                        rows.sort(key=lambda r: (r['published_rank'] or 999, str(r['fixture_id'])))
                    match['display'] = {'analysis_engine': engine_id,
                        'primary_choice': assessment.get('best_candidate'),
                        'ranked_choices': [r for r in candidates if r['gate']['eligible']],
                        'market_candidates': candidates, 'markets': {}}
                day['market_rankings'] = rankings
                day['probability_rankings'] = deepcopy(rankings)
            day.setdefault('multimarket_method', {}).update(model_id=engine_id, validation_from=VALIDATION_FROM,
                                         experimental=True, statistics_first=True)
            ns['atomic_write_json'](ns['DATA_DIR']/f"arab_quote_selections_day{day.get('day', 1)}.json",
                                   selection_export(day, as_of) if matrix_mode else
                                   {'date': day.get('date'), 'generated_at': as_of, 'selection_board': board,
                                    'rankings': rankings, 'probability_rankings': rankings})
        from scan_audit import update_audit
        research = update_audit(ns, output, client)
        for day in output.get('days', []):
            day['multimarket_audit'] = {'cohorts': research['cohorts'], 'status': research['status'],
                'matrix_validation': {k: v for k, v in research.get('matrix_observation', {}).items() if k != 'groups'}}
        output['competition_review'] = list(review.values())
        for key in ('first_half', 'favorite_trap', 'price_gate', 'tipsters', 'market_goal_regime',
                    'ft_bridge', 'focused_shortlist', 'pattern_audit', 'legacy_labels_removed'):
            output.get('method', {}).pop(key, None)
        for day in output.get('days', []):
            day.pop('focused_shortlist', None)
        output.setdefault('method', {})['active_analysis'] = {
            'model_id': engine_id, 'book_weight_in_statistics': 0, 'book_weight_in_ranking': 0,
            'history': CONFIG['history_ft'], 'ht_history': CONFIG['history_ht'],
            'top_per_market': None if matrix_mode else CONFIG['top_per_market'], 'allow_abstention': True,
            'snapshot_policy': 'published_selections_and_all_rejected_markets_before_kickoff'}
        output['method']['user_policy'] = {'statistics_first': True,
            'automatic_drought_bonus': False, 'market_confirmation_after_selection': True}
        output['method'].update(
            model='Potenziale gol robusto: attacco, difesa, contesto, xG e precedenti limitati.',
            calibration='Nessuna miscela con le probabilità bookmaker; modello nuovo in verifica prospettica.',
            markets='Over 2,5 FT; Over 1,5 PT; Over 3,5 FT; GG PT.',
            markers='Oro: selezione statistica. Azzurro: conferma diretta nelle quote.',
            book='Movimenti osservati sugli stessi operatori, depurati dall’aggio; nessun bonus alle probabilità.')
        if matrix_mode:
            output['method']['active_analysis'].update(universal_observation=False, all_whitelisted_fixtures=True,
                competition_policy=SCOPE_ID, all_supported_score_markets=True)
            output['method']['user_policy'].update(universal_observation=False, competition_policy=SCOPE_ID,
                romania_allowed=True, goals_only_proposals=True, drought_probability_bonus=0,
                market_confirmation_after_selection=False)
            output['method'].update(markets='Matrice 1X2, doppia chance, Over/Under, GG/NG, PT/ST, asiatici, multigol e combo.',
                competition_filter='Prime tre divisioni senior dei paesi in whitelist e coppe autorizzate; Romania inclusa; esclusi giovanili e livelli inferiori.',
                markers='Proposte su cinque mercati gol, controlli OCI/aggio e Combo PT+ST; soglie gold-goals-v6.1.',
                book='OCI e aggio per esito, linea e periodo; volumi e smart money non osservati.')
        ns['atomic_write_json'](ns['OUTPUT_FILE'], output)
        for day in output.get('days', []):
            number = ns['safe_int'](day.get('day'), 0)
            if number in ns['REMOTE_DAY_FILES']:
                ns['atomic_write_json'](ns['DATA_DIR']/ns['REMOTE_DAY_FILES'][number].split('/')[-1], day)
        return output
    ns['scan_dates'] = scan

