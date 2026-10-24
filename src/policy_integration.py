"""Integrate user policy without changing the legacy market API."""
import os
from copy import deepcopy
from selection_policy import competition, drought, movements
from first_half_quality import assess, evidence, PROFILE_ID, THRESHOLDS, VALIDATION_FROM
from multimarket_model import collect_quotes, update_ledger
from context_analysis import evaluate, MODEL_ID

TARGETS = {'over15_ht': 'over', 'gg_ht': 'yes', 'over25_ft': 'over'}


def install(ns):
    # Capture the odds response already fetched by the legacy engine.
    captured_quotes = {}
    from external_odds import Recovery
    external_recovery = Recovery()
    old_extract = ns['extract_bookmaker_markets_with_audit']
    def extract(payload):
        captured_quotes.update(collect_quotes(payload, ns['iso_now']()))
        return old_extract(payload)
    ns['extract_bookmaker_markets_with_audit'] = extract
    ns['DEFAULT_EXCLUDED_COUNTRIES'].discard('Egypt')
    drought_enabled = os.getenv('ARAB_DROUGHT_BONUS_ENABLED', '1').lower() not in {'0','false','no'}
    review = {}
    def competition_profile(league):
        result = competition(league)
        if result.get('reason') == 'unmapped_competition_review':
            review[(str(league.get('country')),str(league.get('name')))] = dict(league)
        return result
    ns['competition_profile'] = competition_profile
    old_row = ns['_match_row']
    def match_row(fixture, team_id=None):
        result = old_row(fixture, team_id)
        if result:
            halftime = (fixture.get('score') or {}).get('halftime') or {}
            result['kickoff'] = (fixture.get('fixture') or {}).get('date')
            result['season'] = (fixture.get('league') or {}).get('season')
            result['friendly'] = 'friendl' in str((fixture.get('league') or {}).get('name','')).lower()
            result['halftime_available'] = all(halftime.get(k) is not None for k in ('home','away'))
            ft = (fixture.get('score') or {}).get('fulltime') or {}
            side = result.get('side')
            if side in {'home', 'away'} and all(ft.get(k) is not None for k in ('home', 'away')):
                other = 'away' if side == 'home' else 'home'
                result['research_ft'] = {'scored': ft[side], 'conceded': ft[other]}
        return result
    ns['_match_row'] = match_row
    history_context = {}
    def recent(client, team_id, last=8):
        # Retrieve candidates, then select eight official CURRENT-season records.
        # Extra candidates only replace friendlies; never enlarge the analysis window.
        from context_analysis import select_history
        payload = client.get('fixtures', {'team':team_id, 'last':32, 'status':'FT'})
        rows = [match_row(r,team_id) for r in (payload or {}).get('response',[])]
        return select_history([r for r in rows if r], history_context.get('as_of',ns['iso_now']()), history_context.get('season'))
    ns['get_team_recent_matches'] = recent
    old_summary = ns['summarize_team_matches']
    def summary(rows, expected_side):
        result = old_summary(rows, expected_side)
        result['goal_drought'] = {'all': drought(rows), 'context': drought([r for r in rows if r.get('side') == expected_side])}
        return result
    ns['summarize_team_matches'] = summary

    old_lambdas = ns['build_lambdas']
    def lambdas(home, away, league, h2h):
        result = old_lambdas(home, away, league, h2h)
        baseline = {k: result.get(k) for k in ('home_ft','away_ft','total_ft','home_ht','away_ht','total_ht')}
        bonuses = {}
        for side, profile in [('home',home),('away',away)]:
            evidence = (profile.get('goal_drought') or {}).get('all', {})
            bonus = evidence.get('bonus_relative', 0) if drought_enabled else 0
            bonuses[side] = deepcopy(evidence)
            for period in ('ft','ht'):
                key = f'{side}_{period}'
                result[key] = ns['round3'](float(result[key])*(1+bonus))
        for period in ('ft','ht'):
            result[f'total_{period}'] = ns['round3'](result[f'home_{period}']+result[f'away_{period}'])
        result['drought_layer'] = {'baseline_lambdas': baseline, 'teams': bonuses, 'experimental': True, 'enabled': drought_enabled, 'max_relative_bonus': .04}
        return result
    ns['build_lambdas'] = lambdas

    old_movement = ns['analyze_market_state']
    def analyze(state, key):
        result = old_movement(state, key)
        result['history'] = deepcopy(state.get('history', []))
        for side in ns['MARKET_SELECTIONS'][key]:
            result.setdefault('movement_features', {})[side] = movements(result, side)
        return result
    ns['analyze_market_state'] = analyze

    old_weight = ns['market_calibration_weight']
    def weight(market, validation):
        base = old_weight(market, validation)
        if not base:
            return 0.0
        selections = (market or {}).get('selections', {})
        ratios = [s.get('bookmaker_consensus', {}).get('ratio', 0) for s in selections.values()]
        # Increase the existing blend modestly; reduce it for fragmented movements.
        multiplier = 1.5
        if ratios and max(ratios) < .5:
            multiplier = 1.15
        return round(min(base*multiplier, .18), 4)
    ns['market_calibration_weight'] = weight

    old_gate = ns['recommendation_gate']
    def gate(market, pick, raw, calibrated, analysis, validation, reliability, meta, relation, empirical):
        previous = old_gate(market,pick,raw,calibrated,analysis,validation,reliability,meta,relation,empirical)
        if TARGETS.get(market) != pick:
            return previous
        odd = ((analysis or {}).get('current', {}).get('odds', {}) or {}).get(pick, 0) or 0
        p = calibrated.get(pick, 0) or 0
        ev = p*odd-1 if odd>1 else -1
        hard = []
        if not validation.get('usable_for_signals'): hard.append('mercato_non_validato')
        if odd<=1: hard.append('quota_assente')
        if reliability<60: hard.append('affidabilita_sotto_60')
        if p<.40: hard.append('probabilita_sotto_40')
        if ev<=0: hard.append('valore_atteso_non_positivo')
        recovered = not previous.get('eligible') and not hard
        score = max(0, min(100, p*55 + reliability*.20 + min(max(ev,0),.20)*75 + (10 if relation=='confirmed' else 3 if relation=='model_only' else 0)))
        return {**previous, 'eligible': not hard, 'score': round(score,3),
                'grade': ('A' if score>=79 else 'B' if score>=69 else 'C') if not hard else 'NO',
                'reasons': hard, 'initial_reasons': previous.get('reasons', []),
                'initial_eligible': bool(previous.get('eligible')), 'recovered': recovered,
                'probability_pct': round(p*100,3), 'expected_value_pct': round(ev*100,3),
                'absolute_probability_floor_pct': 40, 'reliability_floor_pct': 60,
                'policy': 'user_40pct_60reliability_positive_ev',
                'selection_reason': 'recuperata: criteri specifici mercato superati' if recovered else 'criteri specifici mercato superati' if not hard else '; '.join(hard)}
    ns['recommendation_gate'] = gate

    old_fixture = ns['build_fixture_output']
    def fixture(client, row, state, cache):
        history_context.update(season=((row or {}).get('league') or {}).get('season'), as_of=ns['iso_now']())
        output = old_fixture(client,row,state,cache)
        if not output: return output
        model = output.get('model', {})
        book = output.get('book', {})
        raw = model.get('probabilities_raw', {})
        calibrated = model.get('probabilities_calibrated', {})
        rel = model.get('reliability_pct', 0)
        meta = output.get('fixture', {})
        candidates = []
        for market, side in TARGETS.items():
            analysis = book.get('markets', {}).get(market, {})
            validation = book.get('market_validation', {}).get(market, {})
            marker = model.get('markers', {}).get(market, {})
            relation = marker.get('relation', 'neutral')
            checked = gate(market,side,raw.get(market,{}),calibrated.get(market,{}),analysis,validation,rel,meta,relation,model.get('empirical_layer',{}))
            odd = analysis.get('current',{}).get('odds',{}).get(side)
            features = analysis.get('movement_features',{}).get(side, movements(analysis,side))
            candidates.append({'market':market,'selection':side,'fixture_id':meta.get('fixture_id'),
                'match':meta.get('match'),'time':meta.get('time'),'kickoff':meta.get('kickoff'),
                'league':meta.get('league'),'country':meta.get('country'),
                'probability_pct': calibrated.get(market,{}).get(side,0)*100,
                'probability_raw_pct': raw.get(market,{}).get(side,0)*100,
                'odd':odd,'reliability_pct':rel,'relation':relation,'gate':checked,
                'recovered':checked['recovered'],'ranking_score':checked['score'],
                'grade':checked['grade'],'expected_value_pct':checked['expected_value_pct'],
                'movement':features,'selection_reason':checked['selection_reason']})
        output.setdefault('display',{})['market_candidates'] = candidates
        first_half_market = output['display'].get('markets', {}).get('over15_ht', {})
        ht_candidate = next(c for c in candidates if c['market'] == 'over15_ht')
        assessment = assess(first_half_market, rel, ht_candidate['gate'])
        model['first_half_evidence'] = evidence(output.get('statistics', {}), model.get('lambdas', {}))
        assessment['ht_coverage_status'] = model['first_half_evidence']['coverage_status']
        if assessment['ht_coverage_status'] != 'available':
            assessment['eligible'] = False
            assessment['status'] = 'unavailable'
            assessment['reasons'].append(assessment['ht_coverage_status'])
        ht_candidate['probability_assessment'] = assessment
        output['display']['first_half_assessment'] = assessment
        # Archive every market and both odds sides, including rejected selections.
        output['quote_observation'] = {k: {'history':deepcopy(v.get('history',[])),
            'movement_features':deepcopy(v.get('movement_features',{})),
            'open':deepcopy(v.get('open',{})),'current':deepcopy(v.get('current',{})),
            'selections':deepcopy(v.get('selections',{}))} for k,v in book.get('markets',{}).items()}
        layer = model.get('lambdas',{}).get('drought_layer',{})
        if layer:
            model['probabilities_without_drought_poisson'] = ns['probabilities_from_lambdas'](layer['baseline_lambdas'])
        quotes = update_ledger(state if state is not None else {}, meta['fixture_id'], captured_quotes.pop(str(meta['fixture_id']), {}), ns['iso_now'](), meta.get('kickoff'))
        output['multimarket'] = evaluate(output, ns['iso_now'](), quotes)
        try:
            external_recovery.apply(output, ns['iso_now']())
        except Exception as exc:
            # An external provider must never stop the statistical/API scan.
            external_recovery.report['errors'] += 1
            print('EXTERNAL_ODDS_ERROR', type(exc).__name__)
        return output
    ns['build_fixture_output'] = fixture

    old_scan = ns['scan_dates']
    def scan(client, dates, excluded):
        output = old_scan(client,dates,excluded)
        output.setdefault('method', {})['external_odds'] = {'enabled': True, 'scope': 'all_countries', 'markets': ['over25_ft', 'over35_ft'], 'priority': 'API-Football', 'display_only': True, 'max_requests': 120, 'budget_seconds': 180}
        ns['atomic_write_json'](ns['DATA_DIR']/'arab_quote_external_recovery.json', external_recovery.report)
        print('EXTERNAL_ODDS_RECOVERY', __import__('json').dumps(external_recovery.report, ensure_ascii=False))
        audit_path = ns['PREMATCH_AUDIT_FILE']
        frozen = ns['read_json'](audit_path, {'fixtures':{}})
        # One request per pending date, not per fixture; never recalculate forecasts.
        pending_dates = set()
        today = ns['now_rome']().date()
        for record in [*frozen.get('fixtures',{}).values(), *frozen.get('quality_evaluations',{}).values(), *frozen.get('multimarket_evaluations',{}).values(), *frozen.get('signal_observations',{}).values(), *frozen.get('analysis_updates',{}).values()]:
            kickoff = ns['parse_iso'](record.get('fixture',{}).get('kickoff'))
            if kickoff and kickoff.date() < today and (today-kickoff.date()).days <= 14:
                outcomes = record.get('settlement',{}).get('outcomes',{})
                if not outcomes or any(v is None for v in outcomes.values()): pending_dates.add(kickoff.date().isoformat())
        extra_matches = []
        for date in sorted(pending_dates):
            payload = client.get('fixtures', {'date':date, 'timezone':'Europe/Rome'}, use_cache=True)
            for row in (payload or {}).get('response',[]):
                result = ns['_result_payload'](row)
                if result:
                    extra_matches.append({'fixture':{'fixture_id':str(row.get('fixture',{}).get('id')), 'result':result}})
        from prematch_audit import settle_predictions, build_report
        settle_predictions(frozen, {'days':[{'matches':extra_matches}]})
        ns['atomic_write_json'](audit_path,frozen)
        ns['atomic_write_json'](ns['DATA_DIR']/'arab_quote_prematch_report.json',build_report(frozen))
        from multimarket_audit import build_report as multimarket_report
        research = multimarket_report(frozen)
        ns['atomic_write_json'](ns['DATA_DIR']/'arab_quote_multimarket_report.json', research)
        observations_path = ns['DATA_DIR']/'arab_quote_observations.json'
        observations = ns['read_json'](observations_path, {'schema_version':1,'fixtures':{}})
        stamp = ns['iso_now']()
        for day in output.get('days',[]):
            day['multimarket_method'] = {'model_id':MODEL_ID,'experimental':True,'validation_from':'2026-10-20'}
            day['multimarket_audit'] = {'cohorts':research['cohorts'],'status':research['status']}
            for match in day.get('matches',[]):
                meta=match.get('fixture',{})
                kickoff=ns['parse_iso'](meta.get('kickoff'))
                if kickoff and kickoff>ns['now_rome']() and not meta.get('result'):
                    data={} if match.get('multimarket',{}).get('model_id') == MODEL_ID else match.get('quote_observation',{})
                    if data:
                        history=observations['fixtures'].setdefault(str(meta.get('fixture_id')),[])
                        if not history or history[-1]['markets'] != data:
                            history.append({'captured_at':stamp,'kickoff':meta.get('kickoff'),'model_version':ns['VERSION'],'markets':deepcopy(data)})
        observations['updated_at']=stamp
        ns['atomic_write_json'](observations_path,observations)
        for day in output.get('days',[]):
            rankings = {market: [] for market in TARGETS}
            probability_rankings = {'over15_ht': []}
            for match in day.get('matches',[]):
                meta = match.get('fixture',{})
                kickoff = ns['parse_iso'](meta.get('kickoff'))
                if not kickoff or kickoff<=ns['now_rome']() or meta.get('result'): continue
                for candidate in match.get('display',{}).get('market_candidates',[]):
                    if candidate['gate']['eligible']: rankings[candidate['market']].append(candidate)
                    if candidate['market'] == 'over15_ht' and candidate.get('probability_assessment', {}).get('eligible'):
                        probability_rankings['over15_ht'].append(candidate)
            for rows in rankings.values(): rows.sort(key=lambda c:(-c['ranking_score'],-c['probability_pct'],str(c['fixture_id'])))
            # Chronological presentation; do not manufacture a new success percentage.
            probability_rankings['over15_ht'].sort(key=lambda c:(c['kickoff'],str(c['fixture_id'])))
            day['market_rankings'] = rankings
            day['probability_rankings'] = probability_rankings
            day['first_half_quality_method'] = {'profile_id': PROFILE_ID, 'experimental': True,
                'thresholds': dict(THRESHOLDS), 'validation_from': VALIDATION_FROM}
            ns['atomic_write_json'](ns['DATA_DIR']/f"arab_quote_selections_day{day.get('day',1)}.json",{'date':day.get('date'),'generated_at':ns['iso_now'](),'rankings':rankings,'probability_rankings':probability_rankings,'first_half_quality_method':day['first_half_quality_method']})
        output['competition_review'] = list(review.values())
        output.setdefault('method',{})['active_analysis'] = {'model_id':MODEL_ID,'book_weight_in_statistics':0,'history':8,'ht_history':4,'snapshot_policy':'first_and_each_prematch_update','legacy_fields':'comparative_only'}
        output.setdefault('method',{})['user_policy'] = {'probability_floor_pct':40,'reliability_floor_pct':60,'book_max_weight':.18,'drought_bonus_max':.04,'rankings':'separate_uncapped'}
        ns['atomic_write_json'](ns['OUTPUT_FILE'],output)
        # Existing main publishes each day from output, while local consumers need updated files too.
        for day in output.get('days',[]):
            number=ns['safe_int'](day.get('day'),0)
            if number in ns['REMOTE_DAY_FILES']: ns['atomic_write_json'](ns['DATA_DIR']/ns['REMOTE_DAY_FILES'][number].split('/')[-1],day)
        return output
    ns['scan_dates'] = scan


