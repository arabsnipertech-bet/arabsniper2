"""Offline routing comparison on a declared synthetic grid, never an ROI backtest."""
from copy import deepcopy
from itertools import product
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))
from lateral_matrix import build_matrix
from v7_decision import capture, commit_priced_slots, compatible
from v7_probability import raw_rates, score_model

AT = '2026-11-20T12:00:00+00:00'
LATER = '2026-11-20T12:10:00+00:00'
START = '2026-11-20T14:00:00+00:00'
ROLES = ('most_probable', 'recommended_alternative', 'high_odds_shot')
FIELDS = ('raw', 'statistics', 'corrected', 'calibrated', 'fair', 'odd', 'ev', 'ev_low', 'tier')
GRID = dict(ft_volume=[1.6, 2.1, 2.6, 3.1, 3.8, 4.6],
            home_share=[.30, .45, .55, .70], market_factor=[.78, .90, 1., 1.12])


def fixture(index, volume, share, factor):
    fraction = .40 if index % 2 else .48
    rates = dict(home_ft=volume*share, away_ft=volume*(1-share),
                 home_ht=volume*share*fraction, away_ht=volume*(1-share)*fraction)
    means = {side: {period: dict(sample=8,
        scored=rates[side+'_'+period] if period != 'st' else rates[side+'_ft']-rates[side+'_ht'],
        conceded=rates[other+'_'+period] if period != 'st' else rates[other+'_ft']-rates[other+'_ht'])
        for period in ('ft', 'ht', 'st')} for side, other in [('home', 'away'), ('away', 'home')]}
    profiles = {side: {direction+'_'+period: dict(sample=8,
        mean=means[side][period]['scored' if direction == 'for' else 'conceded'],
        without_last=means[side][period]['scored' if direction == 'for' else 'conceded'],
        without_peak=means[side][period]['scored' if direction == 'for' else 'conceded'])
        for period in ('ft', 'ht') for direction in ('for', 'against')} for side in ('home', 'away')}
    a = build_matrix(dict(rates=rates, profiles=profiles, statistics_as_of=AT,
        observed_goal_means=dict(captured_at=AT, teams=means), markets=[]), AT)
    book = score_model(raw_rates({'rates': {key: value*factor for key, value in rates.items()}}))
    result_odds = {side: 1/(book.estimate(dict(kind='result', period='ft', side=side))['p']*1.05)
                   for side in ('1', 'x', '2')}
    for row in a['matrix']['rows']:
        estimate = book.estimate(row['spec'])
        settled = estimate['win_weight']+estimate['loss_weight']
        fair = estimate['win_weight']/settled if settled else None
        if fair is None or fair <= 0 or fair*1.05 >= 1 or index % 19 == 0:
            continue
        odd = 1/(fair*1.05)
        row.update(odd=odd, first_odd=odd/(1.04 if index % 3 == 0 else .96 if index % 3 == 1 else 1),
            quote_status='recent', quote_source='bookmaker', quote_executable=True,
            bookmaker_id='1', bookmaker='Synthetic operator', quote_observed_at=AT, provider_updated_at=AT,
            first_observed_at='2026-11-20T11:00:00+00:00', selected_margin_pct=5.,
            selected_market_fair_pct=fair*100, market_fair_pct=min(99., fair*100+7), margin_pct=5.,
            delta_pct=4. if index % 3 == 0 else -4. if index % 3 == 1 else 0.)
        if row['spec']['kind'] == 'result':
            row['selected_market_odds'] = result_odds
    return a, dict(fixture_id=10000+index, kickoff=START, status_short='NS', country='Italy', league='Serie A')


def counters():
    return {role: dict(market_changes=0, study_to_qualified=0, qualified_displaced=0,
                      missing_before=0, missing_after=0, originals_preserved=0) for role in ROLES}


def compare(before, after, counts):
    checked = 0
    assert before['markets'].keys() == after['markets'].keys()
    for key in before['markets']:
        assert all(before['markets'][key][field] == after['markets'][key][field] for field in FIELDS), key
        checked += 1
    for role, count in counts.items():
        old, new = before['triad'][role], after['triad'][role]
        changed = (old or {}).get('market_id') != (new or {}).get('market_id')
        count['market_changes'] += changed
        count['study_to_qualified'] += bool(old and old.get('study_only') and new and not new.get('study_only'))
        count['qualified_displaced'] += bool(changed and old and not old.get('study_only'))
        count['missing_before'] += old is None
        count['missing_after'] += new is None
        saved = (after.get('triad_commitments') or {}).get(role, {})
        count['originals_preserved'] += bool(changed and saved.get('replaces'))
    picks = [pick['market_id'] for pick in after['triad'].values() if pick]
    assert len(picks) == len(set(picks)) and (not picks or compatible(picks[0], *picks[1:]))
    return checked


def run():
    fresh, inherited, checked = counters(), counters(), 0
    cases = list(product(*GRID.values()))
    for index, values in enumerate(cases):
        a, meta = fixture(index, *values)
        old = capture(deepcopy(a), meta, AT, triad_refresh={'enabled': False})
        new = capture(deepcopy(a), meta, AT)
        checked += compare(old, new, fresh)
        frozen = commit_priced_slots(old['triad'], AT, {})
        old = capture(deepcopy(a), meta, LATER, commitments=deepcopy(frozen), triad_refresh={'enabled': False})
        new = capture(deepcopy(a), meta, LATER, commitments=deepcopy(frozen))
        checked += compare(old, new, inherited)
    assert all(not count['qualified_displaced'] for counts in (fresh, inherited) for count in counts.values())
    assert all(not count['missing_after'] for counts in (fresh, inherited) for count in counts.values())
    return dict(population='synthetic fixtures; no outcome data; not a profitability backtest',
                fixtures=len(cases), grid=GRID, fresh=fresh, inherited_commitments=inherited,
                market_records_checked=checked, model_price_ev_changes=0,
                complete_compatible_triads=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = run()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))
