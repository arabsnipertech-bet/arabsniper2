"""A bounded selection preference, never a probability or EV adjustment."""
from datetime import timedelta
from statistics import mean

from gold_competitions import national_competition
from lateral_matrix import market_id, specification
from multimarket_model import stamp
from v7_probability import finite, utc

POLICY_ID = 'OVER15_HT_SINGLE_DEFICIT_V1'
POLICY = {'enabled': True}
MAX_PRIORITY_BONUS = .005  # At most 0.5 percentage points of the ranking index.
MARKET = market_id(specification('total', 'ht', 'over', 1.5))


def single_deficit(assessment, meta, as_of):
    at, start = stamp(as_of), stamp(meta.get('kickoff'))
    stats_at = stamp(assessment.get('statistics_as_of'))
    if (not POLICY['enabled'] or not at or not start or not stats_at or stats_at > at
            or at >= start or meta.get('result') or meta.get('status_short') not in ('NS', 'TBD')
            or meta.get('friendly') or national_competition(meta.get('league'))):
        return None
    inputs = assessment.get('inputs') or {}
    if not isinstance(inputs, dict):
        return None
    histories = inputs.get('histories') or {}
    if not isinstance(histories, dict):
        return None
    observations = {}
    for side in ('home', 'away'):
        unique = {}
        raw = histories.get(side, [])
        if not isinstance(raw, list):
            return None
        for row in raw:
            if not isinstance(row, dict):
                return None
            fid, when = row.get('fixture_id'), stamp(row.get('kickoff'))
            if (fid is None or str(fid) == str(meta.get('fixture_id')) or not when
                    or when+timedelta(hours=2) > stats_at or row.get('friendly')
                    or row.get('national_group')):
                return None
            previous = unique.get(str(fid))
            if previous and any(previous.get(k) != row.get(k) for k in
                                ('kickoff', 'team_goals_ht', 'team_goals_ft')):
                return None
            unique.setdefault(str(fid), row)
        rows = sorted(unique.values(), key=lambda r: stamp(r['kickoff']), reverse=True)[:8]
        def valid(row):
            h, f = row.get('team_goals_ht'), row.get('team_goals_ft')
            return (row.get('halftime_available') is not False and finite(h) and finite(f)
                    and 0 <= h <= f and int(h) == h and int(f) == f)
        if len(rows) < 6 or not all(valid(r) for r in rows[:2]):
            return None
        baseline = [r for r in rows[2:] if valid(r)]
        if len(baseline) < 4:
            return None
        recent, prior = mean(r['team_goals_ht'] for r in rows[:2]), mean(r['team_goals_ht'] for r in baseline)
        observations[side] = dict(recent_mean=recent, baseline_mean=prior,
            deficit=prior-recent, baseline_n=len(baseline),
            recent_fixture_ids=[str(r['fixture_id']) for r in rows[:2]],
            baseline_fixture_ids=[str(r['fixture_id']) for r in baseline])
    under = [side for side, value in observations.items() if value['deficit'] > 1e-9]
    if len(under) != 1:
        return None
    return dict(policy_id=POLICY_ID, market_id=MARKET, team=under[0],
                priority_bonus=MAX_PRIORITY_BONUS, scope='recommended_alternative',
                statistics_at=utc(stats_at), teams=observations)


def priority_bonus(key, role, row):
    context = row.get('selection_context') or {}
    quote = row.get('quote') or {}
    return MAX_PRIORITY_BONUS if (
        POLICY['enabled'] and key == MARKET and role == 'recommended_alternative'
        and context.get('policy_id') == POLICY_ID and context.get('market_id') == MARKET
        and row.get('supported') is True and row.get('tier') in ('BLU', 'GOLD', 'PLATINUM')
        and row.get('quote_state') in ('observed', 'moving', 'stable')
        and quote.get('bookmaker_id') is not None
        and finite(row.get('odd')) and row['odd'] > 1
        and finite(row.get('ev')) and row['ev'] > 0
        and finite(row.get('ev_low')) and row['ev_low'] >= 0
        and finite(row.get('fair')) and 0 < row['fair'] < 1
    ) else 0.


def explain(origin, key, record):
    if not priority_bonus(key, 'recommended_alternative', record):
        return origin
    context = record['selection_context']
    values = context['teams'][context['team']]
    side = 'Casa' if context['team'] == 'home' else 'Ospite'
    recent, prior = (f'{values[k]:.2f}'.replace('.', ',') for k in ('recent_mean', 'baseline_mean'))
    return dict(origin_badge=origin['origin_badge']+' · Plus PT',
        origin_tooltip=origin['origin_tooltip']+f' {side}: media gol PT {recent} nelle ultime due gare, '
        f"contro {prior} nelle precedenti {values['baseline_n']}. Una sola squadra sotto media: "
        'piccola preferenza nella selezione, con probabilità ed EV originali.')
