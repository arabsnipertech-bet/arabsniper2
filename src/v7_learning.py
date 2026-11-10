"""Nightly calibration for V7, with a chronological holdout."""
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from math import log
from pathlib import Path
from random import Random

from adaptive_calibration import HALF_LIFE, WINDOW
from audit_storage import read_store, write_store
from multimarket_model import stamp
from v7_archive import ABLATIONS, ROME, WEIGHTS, day_path
from v7_probability import BASE_WEIGHT, MAX_WEIGHT, MIN_WEIGHT, VERSION, finite, utc

ROOT = Path(__file__).resolve().parents[1]
STATE = 'data/arab_quote_v7_weights.json'
MIN_TRAIN_DAYS, MIN_TRAIN_FIXTURES = 8, 100
MIN_TEST_DAYS, MIN_TEST_FIXTURES = 5, 80
STEP, MIN_GAIN = .05, .001
BASELINES = ('raw', 'corrected', 'calibrated', 'v51', 'fair')
V7_BASELINES = tuple(key for key in BASELINES if key != 'v51')
STRESS_ROLES = ('radar_sorprese', 'colpaccio')


@lru_cache(maxsize=4)
def _state(path, mtime, size):
    return read_store(path, {})


def active_weight(as_of, root=ROOT):
    at = stamp(as_of)
    if not at:
        return BASE_WEIGHT, 'baseline'
    try:
        path = Path(root) / STATE
        stat = path.stat()
        state = _state(path, stat.st_mtime_ns, stat.st_size)
        trained = stamp(state.get('trained_at'))
        weight = state.get('weight')
        if (state.get('version') == VERSION and trained and trained <= at
                and 0 <= (at.astimezone(ROME).date() - trained.astimezone(ROME).date()).days <= 2
                and finite(weight) and MIN_WEIGHT <= weight <= MAX_WEIGHT):
            return weight, state.get('status', 'baseline')
    except (OSError, ValueError, AttributeError):
        pass
    return BASE_WEIGHT, 'baseline'


def metrics(rows, p, today):
    weighted = [(2 ** (-(today - date.fromisoformat(row['date'])).days / HALF_LIFE),
                 p(row), row['outcome']) for row in rows]
    weighted = [(w, v, y) for w, v, y in weighted if finite(v) and 0 <= v <= 1]
    mass = sum(w for w, _, _ in weighted)
    if not mass:
        return dict(n=0, brier=None, log_loss=None)
    return dict(n=len(weighted), brier=sum(w * (v-y)**2 for w, v, y in weighted) / mass,
                log_loss=-sum(w * log(max(1e-12, v if y else 1-v)) for w, v, y in weighted) / mass)


def evaluate(rows, today):
    # V7 and fair use the same population. V5.1 is a comparison when available.
    matched = [r for r in rows if all(finite(r.get(k)) for k in V7_BASELINES)]
    return {k: metrics(matched, lambda r: r.get(k), today) for k in BASELINES}


def deduplicated(rows, cutoff):
    today = cutoff.astimezone(ROME).date()
    data = {}
    for row in rows:
        observed, kickoff = stamp(row.get('observed_at')), stamp(row.get('kickoff'))
        if not (observed and kickoff and kickoff < observed <= cutoff):
            continue
        day = kickoff.astimezone(ROME).date()
        if str(day) != row.get('date') or not 0 < (today - day).days <= WINDOW:
            continue
        key = row['fixture_id'], row['market']
        if key not in data or kickoff > stamp(data[key]['kickoff']):
            data[key] = row
    return list(data.values())


def day_roi(rows):
    by_date = defaultdict(list)
    for row in rows:
        if finite(row.get('roi')):
            by_date[row['date']].append(row['roi'])
    profit = [v for values in by_date.values() for v in values]
    result = dict(n=len(profit), days=len(by_date), roi=sum(profit) / len(profit) if profit else None)
    if len(by_date) >= 5:
        rng = Random(7)
        days = [(sum(v), len(v)) for _, v in sorted(by_date.items())]
        trials = []
        for _ in range(500):
            sampled = rng.choices(days, k=len(days))
            trials.append(sum(v for v, _ in sampled) / sum(n for _, n in sampled))
        trials.sort()
        result['bootstrap_days_95'] = [trials[12], trials[487]]
    largest = sorted((v for v in profit if v > 0), reverse=True)[:3]
    result['top_wins_removed'] = len(largest)
    result['n_without_three_largest_wins'] = len(profit) - len(largest)
    result['roi_without_three_largest_wins'] = ((sum(profit) - sum(largest)) / (len(profit) - len(largest))
                                                if len(profit) > len(largest) else None)
    return result


def intermediate_persistence(row):
    value = row['features'].get('persistence')
    return finite(value) and 0 < value < 1


def signal_diagnostics(rows):
    roles = defaultdict(list, {role: [] for role in STRESS_ROLES})
    for row in rows:
        for role in row['roles']:
            roles[role].append({**row, 'roi': row.get('role_roi', {}).get(role, row['roi'])})
    signals = {role: day_roi(group) for role, group in roles.items()}
    for role in STRESS_ROLES:
        signals[role]['persistence_intermediate'] = day_roi(
            row for row in roles[role] if intermediate_persistence(row))
    return signals


def diagnostics(rows, today):
    markets, bands, ablation, observed = (defaultdict(list) for _ in range(4))
    for row in rows:
        markets[row['market']].append(row)
        band = min(9, int(10 * row['calibrated']))
        bands[str(10 * band)].append(row)
        reasons = set(row['policy_exclusions'])
        if len(reasons) == 1 and reasons & ABLATIONS:
            ablation[next(iter(reasons))].append(row)
        features = row['features']
        dimensions = {'OCI': features.get('oci_band')}
        persistence, delta, margin = (features.get(k) for k in ('persistence', 'delta_pct', 'margin_pct'))
        if finite(persistence):
            dimensions['persistenza'] = 'intermedia' if intermediate_persistence(row) else 'estrema'
        if finite(delta):
            dimensions['movimento'] = 'calo_5_plus' if delta <= -5 else 'calo' if delta < 0 else 'salita' if delta > 0 else 'stabile'
        if finite(margin):
            dimensions['aggio'] = 'fino_a_9' if margin <= 9 else 'oltre_9'
        paired, agreeing = features.get('paired_books'), features.get('agreeing_books')
        if type(paired) is int and type(agreeing) is int and paired >= 2:
            dimensions['concordanza'] = 'totale' if agreeing == paired else 'parziale'
        for dimension, band in dimensions.items():
            if band:
                observed['|'.join((row['market'], dimension, band))].append(row)
    return dict(cohorts={key: evaluate(group, today) for key, group in markets.items()},
                signals=signal_diagnostics(rows),
                calibration_bins={key: dict(n=len(group), observed=sum(r['outcome'] for r in group)/len(group),
                    predicted=sum(r['calibrated'] for r in group)/len(group)) for key, group in bands.items()},
                single_filter_observations={key: day_roi(group) for key, group in ablation.items()},
                feature_observations={key: day_roi(group) for key, group in observed.items()})


def fit(rows, cutoff, previous):
    today = cutoff.astimezone(ROME).date()
    eligible = [r for r in deduplicated(rows, cutoff) if r.get('supported', True) and r.get('benchmark', True)
                and set(r['variants']) == {str(w) for w in WEIGHTS}
                and all(finite(r.get(k)) for k in V7_BASELINES)]
    dates = sorted({r['date'] for r in eligible})
    tests = dates[-MIN_TEST_DAYS:]
    split = datetime.combine(date.fromisoformat(tests[0]), time(), ROME) if tests else cutoff
    train = [r for r in eligible if r['date'] not in tests and stamp(r['observed_at']) < split]
    holdout = [r for r in eligible if r['date'] in tests]
    grid = [w for w in WEIGHTS if abs(w - previous) <= STEP + 1e-9]
    report = dict(previous_weight=previous, weight=previous, status='baseline_retained',
                  train_dates=sorted({r['date'] for r in train}), holdout_dates=tests,
                  train_fixtures=len({r['fixture_id'] for r in train}),
                  holdout_fixtures=len({r['fixture_id'] for r in holdout}),
                  holdout=evaluate(holdout, today), candidate=None, improvement_demonstrated=False,
                  applied=False, reason='insufficient_disjoint_samples')
    if (len(report['train_dates']) < MIN_TRAIN_DAYS or len(tests) < MIN_TEST_DAYS
            or report['train_fixtures'] < MIN_TRAIN_FIXTURES or report['holdout_fixtures'] < MIN_TEST_FIXTURES
            or not grid):
        return report
    compare = lambda records, w: metrics(records, lambda r: r['variants'][str(w)], today)
    fitted = {w: compare(train, w) for w in grid}
    candidate = min(grid, key=lambda w: (fitted[w]['log_loss'], fitted[w]['brier'], abs(w-previous)))
    scores = compare(holdout, candidate)
    before = compare(holdout, previous)
    report.update(candidate=candidate, train_scores={str(w): value for w, value in fitted.items()},
                  holdout_candidate=scores, holdout_previous=before, reason='holdout_gate_not_met')
    baselines = [before, report['holdout']['fair']]
    if report['holdout']['v51']['n'] == len(holdout):
        baselines.append(report['holdout']['v51'])
    consistently_better = sum(
        compare(group, candidate)['log_loss'] <= compare(group, previous)['log_loss']
        for day in tests if (group := [r for r in holdout if r['date'] == day])) >= MIN_TEST_DAYS - 1
    if candidate != previous and consistently_better and all(
        scores[k] + MIN_GAIN <= b[k] for b in baselines for k in ('brier', 'log_loss')
    ):
        report.update(weight=candidate, status='bounded_update_applied',
                      applied=True, improvement_demonstrated=True, reason='paired_chronological_holdout')
    return report


def run(root, cutoff):
    root = Path(root)
    today = cutoff.astimezone(ROME).date()
    path = root / f'auditarchive/prematch_{today-timedelta(days=1)}_v7_learning.json'
    existing = read_store(path, {})
    if isinstance(existing, dict) and existing.get('date') == str(today) and existing.get('version') == VERSION:
        # Recovery can repeat the priority and full nightly phases. Restore the
        # same candidate after a partial write instead of applying a second step.
        if isinstance(existing.get('state'), dict):
            write_store(root/STATE, existing['state'])
        return existing
    rows, days, anomalies, previous = [], {}, {}, active_weight(cutoff, root)[0]
    for offset in range(1, WINDOW + 1):
        day = today - timedelta(days=offset)
        try:
            data = read_store(day_path(root, day), {})
        except (ValueError, OSError) as error:
            anomalies[str(day)] = type(error).__name__
            continue
        if not isinstance(data, dict):
            anomalies[str(day)] = 'invalid_v7_archive'
            continue
        if data.get('version') != VERSION or data.get('date') != str(day):
            continue
        if not isinstance(data.get('counts'), dict) or not isinstance(data.get('samples'), list):
            anomalies[str(day)] = 'invalid_v7_archive'
            continue
        days[str(day)] = {**data['counts'], 'rejected_forecasts': data.get('rejected_forecasts', 0)}
        rows.extend(data['samples'])
    settled = deduplicated(rows, cutoff)
    learned = fit(settled, cutoff, previous)
    state = dict(version=VERSION, trained_at=utc(cutoff), weight=learned['weight'],
                 status=learned['status'])
    report = dict(version=VERSION, date=str(today), window_days=WINDOW, half_life_days=HALF_LIFE,
                  state=state, checked_days=days, anomalies=anomalies, verified_fixtures=len({r['fixture_id'] for r in settled}),
                  comparison=evaluate([r for r in settled if r.get('benchmark', True)], today), update=learned,
                  diagnostics=diagnostics(settled, today),
                  manual_review=['structural_parameters', 'market_feature_weights', 'gold_promotion'])
    write_store(path, report)
    write_store(root/STATE, state)
    return report
