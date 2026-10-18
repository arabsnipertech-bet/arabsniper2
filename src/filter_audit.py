"""Predeclared filter comparisons on frozen forecasts, including rejected matches.

This module measures filters; it never selects a winner or changes live thresholds.
"""
from collections import defaultdict
from datetime import datetime
from math import isfinite, sqrt
from zoneinfo import ZoneInfo

SIDES = {'over15_ht': 'over', 'gg_ht': 'yes', 'over25_ft': 'over'}
PROFILES = ('current', 'probability_50', 'probability_60', 'reliability_75',
            'model_book_agreement', 'odds_1_5_to_3', 'model_book_gap_15')


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value)


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return parsed if parsed.tzinfo else None
    except (TypeError, ValueError):
        return None


def metrics(rows):
    n = len(rows)
    if not n:
        return {'n': 0, 'wins': 0, 'hit_rate_pct': None, 'roi_pct': None,
                'priced_n': 0, 'brier_score': None, 'wilson_95_pct': None,
                'mean_probability_pct': None, 'calibration_gap_pp': None}
    wins = sum(r['outcome'] for r in rows)
    rate = wins / n
    z = 1.96
    denom = 1 + z*z/n
    center = (rate + z*z/(2*n)) / denom
    margin = z*sqrt(rate*(1-rate)/n + z*z/(4*n*n)) / denom
    mean_p = sum(r['p'] for r in rows)/n
    priced = [r for r in rows if number(r['odd']) and r['odd'] > 1]
    return {'n': n, 'wins': wins, 'hit_rate_pct': round(rate*100, 3),
            'mean_probability_pct': round(mean_p*100, 3),
            'calibration_gap_pp': round((mean_p-rate)*100, 3),
            'brier_score': round(sum((r['p']-r['outcome'])**2 for r in rows)/n, 6),
            'wilson_95_pct': [round((center-margin)*100, 3), round((center+margin)*100, 3)],
            'priced_n': len(priced),
            'roi_pct': round(sum((r['odd'] if r['outcome'] else 0)-1 for r in priced)
                             / len(priced)*100, 3) if priced else None}


def passes(row, profile):
    if not row['current']:
        return False
    if profile == 'current':
        return True
    if profile == 'probability_50':
        return row['p'] >= .50
    if profile == 'probability_60':
        return row['p'] >= .60
    if profile == 'reliability_75':
        return number(row['reliability']) and row['reliability'] >= 75
    if profile == 'model_book_agreement':
        return row['relation'] == 'confirmed'
    if profile == 'odds_1_5_to_3':
        return number(row['odd']) and 1.5 <= row['odd'] <= 3
    if profile == 'model_book_gap_15':
        return number(row['gap']) and abs(row['gap']) <= 15
    raise ValueError(profile)


def build_filter_comparison(store):
    markets = {m: [] for m in SIDES}
    coverage = {m: {'frozen': 0, 'settled': 0, 'pending_or_missing_result': 0,
                    'missing_candidate': 0} for m in SIDES}
    invalid_evidence = 0
    for record in store.get('fixtures', {}).values():
        fixture = record.get('fixture') or {}
        kickoff, captured = timestamp(fixture.get('kickoff')), timestamp(record.get('captured_at'))
        if not kickoff or not captured or captured >= kickoff:
            invalid_evidence += 1
            continue
        candidates = record.get('market_candidates')
        if candidates is None:
            candidates = record.get('ranked_choices', [])
        for market, side in SIDES.items():
            p = (record.get('probabilities_calibrated', {}).get(market) or {}).get(side)
            if not number(p) or not 0 <= p <= 1:
                continue
            cov = coverage[market]
            cov['frozen'] += 1
            choice = next((c for c in candidates if c.get('market') == market
                           and c.get('selection') == side), None)
            cov['missing_candidate'] += choice is None
            choice = choice or {}
            gate = choice.get('gate') or {}
            outcome = (record.get('settlement') or {}).get('outcomes', {}).get(market)
            if not isinstance(outcome, bool):
                cov['pending_or_missing_result'] += 1
                continue
            cov['settled'] += 1
            markets[market].append({'p': p, 'outcome': outcome, 'odd': choice.get('odd'),
                'current': gate.get('eligible') is True,
                'reliability': record.get('reliability_pct'),
                'relation': choice.get('relation'), 'gap': gate.get('book_disagreement_pp'),
                'reasons': gate.get('reasons') or [],
                'date': kickoff.astimezone(ZoneInfo('Europe/Rome')).date().isoformat(),
                'country': fixture.get('country') or 'unknown',
                'league': (fixture.get('country') or 'unknown') + '/' + (fixture.get('league') or 'unknown'),
                'version': record.get('model_version') or 'unknown',
                'decision': 'accepted' if gate.get('eligible') is True else 'rejected' if gate.get('eligible') is False else 'unknown'})
    output = {}
    for market, rows in markets.items():
        dates = sorted({r['date'] for r in rows})
        # Entire dates stay together. No random split or optimisation on later results.
        cutoff = dates[max(1, int(len(dates)*.7))] if len(dates) >= 2 else None
        profiles = []
        for name in PROFILES:
            included = [r for r in rows if passes(r, name)]
            excluded = [r for r in rows if not passes(r, name)]
            train = [r for r in included if cutoff and r['date'] < cutoff]
            test = [r for r in included if cutoff and r['date'] >= cutoff]
            profiles.append({'profile': name, 'accepted': metrics(included),
                'rejected': metrics(excluded), 'earlier_period': metrics(train),
                'later_period': metrics(test),
                'sample_status': 'available_for_review' if len(train) >= 100 and len(test) >= 30
                                 else 'insufficient_sample'})
        cohorts = defaultdict(list)
        for row in rows:
            for dimension in ('country', 'league', 'version', 'decision'):
                cohorts[(dimension, row[dimension])].append(row)
            for reason in set(row['reasons']):
                cohorts[('rejection_reason', reason)].append(row)
        output[market] = {'coverage': coverage[market], 'all_predictions': metrics(rows),
            'later_period_start_date': cutoff, 'profiles': profiles,
            'cohorts': [{'dimension': d, 'value': v, **metrics(rs)}
                        for (d, v), rs in sorted(cohorts.items())]}
    return {'schema_version': 1, 'profiles_version': '2026-10-18-v1',
        'note': 'Exploratory fixed profiles, not validated improvements. Includes rejected forecasts. '
                'Paper ROI at first recorded prices, no staking recommendation. '
                'Later-period data must not be used to tune profiles. No automatic live filter changes.',
        'invalid_prematch_records_excluded': invalid_evidence, 'markets': output}
