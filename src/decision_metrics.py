"""Fixture-clustered uncertainty for frozen, unit-stake decision comparisons."""
from collections import defaultdict
from random import Random


def uncertainty(rows, draws=400):
    groups = defaultdict(list)
    for row in rows:
        if row.get('odd') is not None and row['odd'] > 1:
            groups[str(row['fid'])].append(row['odd'] * row['y'] - 1)
    clusters = [groups[k] for k in sorted(groups)]
    out = dict(method='fixture_cluster_bootstrap_percentile', confidence=.95,
               distinct_fixtures_count=len(clusters), draws=draws, roi_pct_interval=None)
    if len(clusters) < 2:
        out['reason'] = 'fewer_than_two_independent_fixtures'
        return out
    rng = Random(5107)
    samples = []
    for _ in range(draws):
        selected = [clusters[rng.randrange(len(clusters))] for _ in clusters]
        samples.append(100 * sum(sum(c) for c in selected) / sum(map(len, selected)))
    samples.sort()
    out['roi_pct_interval'] = [samples[int(.025*(draws-1))], samples[int(.975*(draws-1))]]
    return out


def selected_summary(rows, activated, pending, changed):
    from calibration_benchmark import metrics
    return dict(metrics(rows, 'p'), first_choices_activated=activated,
                changed_vs_pure_poisson=changed, pending_choices=pending,
                net_units=sum(r['odd']*r['y']-1 for r in rows),
                uncertainty=uncertainty(rows),
                return_basis='unit_stake_at_frozen_quote; no fees, tax or execution slippage modeled')
