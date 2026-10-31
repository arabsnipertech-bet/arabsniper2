"""V5.1 cohort calibration. Only the protected Auditor writes aggregate weights."""
from zoneinfo import ZoneInfo
from collections import defaultdict
from datetime import date, timedelta
from functools import lru_cache
from hashlib import sha256
import json
from math import isfinite, log, sqrt
from pathlib import Path

from multimarket_model import stamp

VERSION = 'adaptive-calibration-v5.1'
WEIGHTS = 'data/arab_calibration_weights.json'
ROOT = Path(__file__).resolve().parents[1]
ROME_ZONE = ZoneInfo('Europe/Rome')
MIN_N, WINDOW, HALF_LIFE, PRIOR = 50, 28, 14, 50
LINES = {('ft', 2.5): 'over25_ft', ('ft', 3.5): 'over35_ft',
         ('ht', 1.5): 'over15_ht', ('ht', .5): 'over05_ht'}
TRAPS = ('strong_favorite_trap', 'possible_favorite_trap', 'multi_signal_goal_pressure',
         'odds_cross_reversal', 'favorite_steam_vulnerable', 'underdog_book_square')


def number(value):
    return type(value) in (int, float) and isfinite(value)


def bound(value, lo, hi):
    return min(hi, max(lo, value))


def market_of(spec):
    kind, period, side = (spec.get(k) for k in ('kind', 'period', 'side'))
    if kind == 'result' and period == 'ft' and side in ('1', 'x', '2'):
        return '1x2_ft'
    if kind in ('total', 'asian_total') and side == 'over':
        return LINES.get((period, spec.get('line')))
    if kind == 'btts' and period == 'ht' and side == 'yes':
        return 'gg_ht'
    if kind == 'halves_total' and period == 'ft' and side == 'over' and spec.get('line') == spec.get('extra') == .5:
        return 'combo_ht_sh'
    return None


def cohort(market, odd, tier):
    if not market or not tier or not number(odd) or odd <= 1:
        return None
    band = '1-1.79' if odd < 1.8 else '1.8-2.49' if odd < 2.5 else '2.5-3.99' if odd < 4 else '4+'
    return '|'.join((market, band, str(tier)))


def neutral():
    return {'schema_version': 1, 'version': VERSION, 'as_of': None, 'window_days': WINDOW,
            'half_life_days': HALF_LIFE, 'minimum_fixtures': MIN_N, 'prior_strength': PRIOR, 'cohorts': {}}


@lru_cache(maxsize=1)
def load_weights():
    try:
        state = json.loads((ROOT / WEIGHTS).read_bytes())
        if isinstance(state, dict) and state.get('version') == VERSION and isinstance(state.get('cohorts'), dict):
            # Keep the daytime lookup empty during cold start even when the
            # overnight report already contains many sub-threshold cohorts.
            state['cohorts'] = {k:v for k,v in state['cohorts'].items() if usable_entry(state, k)}
            return state
    except (OSError, ValueError):
        pass
    return neutral()


def current_state(state, as_of):
    if not state.get('cohorts'):
        return False
    try:
        day = stamp(as_of).astimezone(ROME_ZONE).date()
        trained_at = stamp(state.get('trained_at'))
        return (not trained_at or trained_at <= stamp(as_of)) and 0 <= (day-date.fromisoformat(state['as_of'])).days <= 2
    except (KeyError, ValueError, TypeError, AttributeError):
        return False


def usable_entry(state, key, as_of=None):
    if as_of and not current_state(state, as_of):
        return {}
    data = state.get('cohorts', {}).get(key)
    if not isinstance(data, dict) or type(data.get('n')) is not int or data['n'] < MIN_N:
        return {}
    if not isinstance(data.get('weights'), dict) or not number(data.get('threshold_delta_pp')):
        return {}
    return data


def closing_prices(offers, token, as_of, kickoff):
    """Record last executable offers, even when another book wins the display price."""
    from multimarket_model import fair
    now, start = stamp(as_of), stamp(kickoff)
    quotes = {}
    if not now or not start or not 0 < (start-now).total_seconds() <= 10800:
        return quotes
    for offer in offers:
        prices = offer.get('odds') or {}
        odd = prices.get(token)
        book = offer.get('bookmaker_id')
        source = offer.get('quote_source', 'bookmaker')
        seen, updated = stamp(offer.get('observed_at')), stamp(offer.get('provider_updated_at'))
        if (book is None or not offer.get('complete') or source not in ('bookmaker', 'asian_halfline')
                or not number(odd) or odd <= 1 or not seen or not updated or not updated <= seen <= now
                or (start-updated).total_seconds() > 10800 or (now-updated).total_seconds() > 3600
                or not 0 <= fair(prices)[1] <= 9):
            continue
        candidate = (updated.isoformat(), seen.isoformat(), odd)
        old = quotes.get(str(book))
        if old is None or (updated, seen, odd) > (stamp(old[0]), stamp(old[1]), old[2]):
            quotes[str(book)] = candidate
    return quotes


class Evidence:
    """One prospective execution and a later same-book closing observation per contract."""
    def __init__(self):
        self.first, self.last = {}, {}

    def closing(self, key, point, at, start):
        if not isinstance(point, (tuple, list)) or len(point) != 3:
            return
        updated, seen, odd = stamp(point[0]), stamp(point[1]), point[2]
        if (not number(odd) or odd <= 1 or not updated or not seen or not updated <= seen <= at < start
                or (start-updated).total_seconds() > 10800):
            return
        old = self.last.get(key)
        if old is None or (updated, seen, odd) > (stamp(old[0]), stamp(old[1]), old[2]):
            self.last[key] = (updated.isoformat(), seen.isoformat(), odd)

    def observe(self, record):
        from lateral_matrix import MODEL_ID, settle_spec
        forecast = record.get('multimarket') or {}
        meta = record.get('fixture') or {}
        at, start = stamp(record.get('captured_at')), stamp(meta.get('kickoff'))
        result = (record.get('settlement') or {}).get('result') or {}
        fid = meta.get('fixture_id')
        if (fid is None or record.get('retrospective') or forecast.get('model_id') != MODEL_ID
                or not at or not start or at >= start or start.date().isoformat() < (forecast.get('validation_from') or '2026-10-26')
                or result.get('status') not in ('FT', 'AET', 'PEN')):
            return
        matrix = forecast.get('matrix') or {}
        reference = matrix.get('calibration_reference')
        for row in [*matrix.get('rows', []), *([reference] if reference else [])]:
            spec = row.get('spec') or {}
            market = market_of(spec)
            if not market:
                continue
            contract = (str(fid), market, spec['side'])
            for provider, point in (row.get('calibration_closes') or {}).items():
                self.closing((*contract, str(provider)), point, at, start)
            odd, book = row.get('odd'), row.get('bookmaker_id')
            key = cohort(market, odd, meta.get('competition_tier'))
            seen, updated = stamp(row.get('quote_observed_at')), stamp(row.get('provider_updated_at'))
            margin, p = row.get('selected_margin_pct'), row.get('statistical_pct')
            if (not key or book is None or row.get('quote_status') != 'recent'
                    or row.get('quote_source') not in ('bookmaker', 'asian_halfline') or row.get('quote_executable') is not True
                    or not number(margin) or not 0 <= margin <= 9 or not seen or not updated
                    or not updated <= seen <= at or (at-updated).total_seconds() > (3600 if (start-at).total_seconds() <= 10800 else 21600)):
                continue
            self.closing((*contract, str(book)), (updated.isoformat(), seen.isoformat(), odd), at, start)
            settlement = settle_spec(spec, result)
            if settlement not in ((1., 0.), (0., 1.)) or not number(p) or not 0 <= p <= 100:
                continue
            # Deterministic choice among duplicate snapshots and equivalent Asian .5 rows.
            rank = (at, spec['kind'] == 'asian_total', -odd, str(book))
            old = self.first.get(contract)
            if old and old[0] <= rank:
                continue
            empirical = (row.get('empirical') or {}).get('win_stake_pct')
            fair = row.get('market_fair_pct')
            baseline = (row.get('calibration_base') or {}).get('win_weight', p/100)
            pattern = (forecast.get('favorite_trap_pattern') or {}).get('classification')
            self.first[contract] = (rank, {'fid': str(fid), 'market': market, 'side': spec['side'],
                'cohort': key, 'date': start.astimezone(ROME_ZONE).date().isoformat(), 'at': at.isoformat(),
                'updated': updated.isoformat(), 'seen': seen.isoformat(), 'book': str(book), 'odd': odd,
                'p': p/100, 'base': baseline, 'empirical': empirical/100 if number(empirical) else None,
                'fair': fair/100 if number(fair) else None, 'y': int(settlement[0]), 'pattern': pattern})

    def samples(self):
        by_fixture = {}
        for contract, (_, entry) in self.first.items():
            row = dict(entry)
            closing = self.last.get((*contract, row['book']))
            row['clv'] = (row['odd']/closing[2]-1 if closing and stamp(closing[0]) > stamp(row['updated'])
                          and stamp(closing[1]) > stamp(row['at']) else None)
            # Three correlated 1X2 legs may share a price band. Count each fixture
            # once in that cohort, picking the earliest and then highest probability.
            key = (row['fid'], row['cohort'])
            rank = (stamp(row['at']), -row['p'], row['side'])
            if key not in by_fixture or rank < by_fixture[key][0]:
                by_fixture[key] = (rank, row)
        return [by_fixture[k][1] for k in sorted(by_fixture)]


def weighted(pairs):
    mass = sum(w for w, _ in pairs)
    if not mass:
        return None, None
    avg = sum(w*x for w, x in pairs)/mass
    stderr = sqrt(sum(w*w*(x-avg)**2 for w, x in pairs))/mass
    return avg, stderr


def fit(samples, as_of, previous=None):
    result = neutral()
    result['as_of'] = as_of.isoformat()
    grouped = defaultdict(dict)
    for row in samples:
        age = (as_of-date.fromisoformat(row['date'])).days
        if 0 < age <= WINDOW:
            key = (row['cohort'], row['fid'])
            rank = (stamp(row['at']), -row['p'], row['side'])
            old = grouped[key[0]].get(key[1])
            if old is None or rank < old[0]:
                grouped[key[0]][key[1]] = (rank, 2**(-age/HALF_LIFE), row)
    for key, fixtures in sorted(grouped.items()):
        obs = [(w, r) for _, w, r in (fixtures[f] for f in sorted(fixtures))]
        n = len(obs)
        paired = [(w, r) for w, r in obs if number(r.get('clv'))]
        def avg(fn, items=obs):
            return weighted([(w, fn(r)) for w, r in items])
        brier, _ = avg(lambda r: (r['p']-r['y'])**2)
        loss, _ = avg(lambda r: -log(bound(r['p'] if r['y'] else 1-r['p'], 1e-12, 1.)))
        profit = lambda r: r['odd']*r['y']-1
        roi, _ = avg(profit)
        matched_roi, roi_se = avg(profit, paired)
        clv, clv_se = avg(lambda r: r['clv'], paired)
        empirical = fair = offset = 0.
        trap_scale = 1.
        pattern_obs = [(w, r) for w, r in obs if r.get('pattern') in TRAPS]
        signature = sha256(json.dumps([(r['fid'], r['y'], r['odd'], r['clv']) for _, r in paired],
                                      separators=(',', ':')).encode()).hexdigest()
        if n >= MIN_N:
            # Positive improvement in squared error relative to the frozen Poisson
            # prior earns a share of a single three-point budget.
            gains = []
            for field in ('empirical', 'fair'):
                pairs = [(w, r) for w, r in obs if number(r.get(field)) and 0 <= r[field] <= 1]
                gain, _ = avg(lambda r: (r['base']-r['y'])**2-(r[field]-r['y'])**2, pairs)
                mass = sum(w for w, _ in pairs)
                gains.append(max(0., gain or 0.)*mass/(mass+PRIOR) if len(pairs) >= MIN_N else 0.)
            scale = min(1., .03/sum(gains)) if sum(gains) else 0.
            empirical, fair = (v*scale for v in gains)
            if len(paired) >= MIN_N:
                old = (previous or {}).get('cohorts', {}).get(key, {})
                if matched_roi < 0 and clv > 0:
                    last = max(0., old.get('threshold_delta_pp', 0.))
                    step = 1. if matched_roi < -roi_se else .5
                    offset = last if last and signature == old.get('evidence_sha256') else min(2.5, last+step)
                elif matched_roi > max(.005, roi_se) and clv > max(.001, clv_se):
                    offset = -.5
            if len(pattern_obs) >= MIN_N:
                # Posterior hit rate against the forecast on exactly those fixtures.
                diff, _ = avg(lambda r: r['y']-r['base'], pattern_obs)
                mass = sum(w for w, _ in pattern_obs)
                trap_scale = bound(1+2*diff*mass/(mass+PRIOR), .5, 1.25)
        by_pattern = {}
        for label in sorted({r['pattern'] for _, r in pattern_obs}):
            trials = [(w, r) for w, r in pattern_obs if r['pattern'] == label]
            mass = sum(w for w, _ in trials)
            error, _ = avg(lambda r: r['y']-r['base'], trials)
            by_pattern[label] = {'n': len(trials), 'hit_rate': avg(lambda r: r['y'], trials)[0],
                                 'multiplier': bound(1+2*error*mass/(mass+PRIOR), .5, 1.25)
                                 if len(trials) >= MIN_N else 1.}
        result['cohorts'][key] = {'n': n, 'clv_n': len(paired), 'pattern_n': len(pattern_obs),
            'by_market_pattern': by_pattern,
            'pattern_hit_rate': avg(lambda r: r['y'], pattern_obs)[0],
            'brier': brier, 'log_loss': loss, 'roi_pct': 100*roi, 'clv_pct': 100*clv if clv is not None else None,
            'paired_roi_pct': 100*matched_roi if matched_roi is not None else None,
            'weights': {'poisson': 1-empirical-fair, 'empirical': empirical, 'fair': fair},
            'threshold_delta_pp': offset, 'favorite_trap_multiplier': trap_scale, 'evidence_sha256': signature}
    return result


def nightly_state(root, as_of):
    """Compact evidence is extracted during the Auditor's existing ledger traversal."""
    from audit_storage import read_store, open_verified_json
    import ijson
    rows = []
    for days in range(1, WINDOW+1):
        day = as_of-timedelta(days=days)
        path = root / f'auditarchive/prematch_{day}_calibration.json'
        rows.extend(read_store(path, []))
    # The prediction audit contains certified per-fixture by_market_pattern outcomes.
    # Never fan out an aggregate hit-rate into fictitious independent observations.
    predictions = root/'data/arab_quote_prediction_audit.json'
    pending = defaultdict(list)
    for row in rows:
        pending[row['fid']].append(row)
    if predictions.is_file() and pending:
        with open_verified_json(predictions) as stream:
            for fid, item in ijson.kvitems(stream, 'fixtures', use_float=True):
                if fid not in pending or (item.get('result') or {}).get('status') not in ('FT', 'AET', 'PEN'):
                    continue
                pattern = item.get('market_pattern') or {}
                outcomes = item.get('pattern_outcomes') or {}
                for row in pending[fid]:
                    event = 'goal_ht' if row['market'] == 'over05_ht' else row['market']
                    frozen = stamp(pattern.get('captured_at'))
                    if (row.get('pattern') is None and frozen and frozen <= stamp(row['at'])
                            and isinstance(outcomes.get(event), bool) and outcomes[event] == bool(row['y'])):
                        row['pattern'] = pattern.get('classification')
    state = fit(rows, as_of, read_store(root/WEIGHTS, {}))
    # The report is aggregate provenance. Its group counts are never training rows.
    report = root/'data/arab_quote_prematch_report.json'
    if not report.exists():
        report = root/'auditarchive/prematch_last_report.json'
    state['report_source'] = report.name if report.exists() else None
    if report.exists():
        with open_verified_json(report) as stream:
            state['report_generated_at'] = next(ijson.items(stream, 'generated_at'), None)
    return state


def main():
    import argparse
    import os
    from datetime import datetime
    from audit_storage import atomic_bytes, read_store
    from auditor_schedule import ROME, START, STOP
    from private_publication import publish
    parser = argparse.ArgumentParser()
    parser.add_argument('--as-of', type=date.fromisoformat, required=True)
    args = parser.parse_args()
    now = datetime.now(ROME)
    if (os.getenv('GOLD_PRIVATE') != 'true' or not START <= now.time() < STOP
            or args.as_of != now.date()):
        parser.error('calibration requires the active nightly Auditor window')
    state = nightly_state(ROOT, args.as_of)
    old = read_store(ROOT/WEIGHTS, {})
    trained_at = old.pop('trained_at', None)
    state['trained_at'] = trained_at if trained_at and old == state else now.isoformat()
    atomic_bytes(ROOT/WEIGHTS, (json.dumps(state, sort_keys=True, indent=2, allow_nan=False)+'\n').encode())
    publish(ROOT, {WEIGHTS})
    print(f"Calibrazione pubblicata: {len(state['cohorts'])} coorti", flush=True)


if __name__ == '__main__':
    main()
