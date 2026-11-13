"""V7 evidence in the existing private journals and dated result archive."""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import open_verified_json, read_store, write_store
from lateral_matrix import market_id, settle_spec, specification
from multimarket_model import stamp
from v7_decision import canonical, fingerprint, COMBOS, GOAL, GG, GG_SH
from v7_probability import VERSION, blend_rates, finite, score_model, utc

ROME = ZoneInfo('Europe/Rome')
FINISHED = {'FT', 'AET', 'PEN'}
VOID = {'CANC', 'ABD', 'AWD', 'WO'}
WEIGHTS = (.50, .55, .60, .65, .70, .75, .80)
BENCHMARKS = frozenset((*GOAL.values(), GG, GG_SH, *(market_id(specification('result', 'ft', side))
                                          for side in ('1', 'x', '2'))))
MONITORED = BENCHMARKS | {
    market_id(specification('btts', 'ft', 'yes')),
    *(market_id(spec) for spec in COMBOS),
}
ABLATIONS = frozenset({'sensitivity', 'odds_rising', 'price', 'suspicious_divergence',
                       'yield_floor', 'oci_negative'})


def day_path(root, day):
    return Path(root) / f'auditarchive/prematch_{day}_v7.json'


def valid_snapshot(snapshot, meta, published):
    observed, kickoff, at = (stamp(t) for t in
                             (snapshot.get('captured_at'), meta.get('kickoff'), published))
    valid = (snapshot.get('version') == VERSION and str(meta.get('fixture_id')) == snapshot.get('fixture_id')
             and observed and kickoff and at and observed <= at < kickoff
             and stamp(snapshot.get('kickoff')) == kickoff)
    if not valid:
        return False
    return fingerprint({k: v for k, v in snapshot.items() if k != 'sha256'}) == snapshot.get('sha256')


def read_quote_state(path, fixture_ids):
    """Stream the matrix ledger once. Attach only points that existed at publication."""
    if not fixture_ids or not path.is_file():
        return {}
    import ijson
    with open_verified_json(path) as stream:
        return {fid: value for fid, value in ijson.kvitems(stream, 'matrix_quotes', use_float=True)
                if fid in fixture_ids}


def quote_evidence(source, snapshot):
    cutoff = stamp(snapshot['captured_at'])
    if stamp(source.get('kickoff')) != stamp(snapshot['kickoff']):
        return None
    groups = {}
    for name, points in source.get('groups', {}).items():
        samples = []
        for point in points:
            at, updated = (stamp(point.get(k)) for k in ('observed_at', 'provider_updated_at'))
            if not at or at > cutoff or updated and updated > at:
                continue
            prices = {side: odd for side, odd in (point.get('odds') or {}).items()
                      if finite(odd) and odd > 1}
            if prices:
                samples.append(dict(at=utc(at), provider_at=utc(updated), prices=prices,
                                    complete=point.get('complete'),
                                    executable=point.get('quote_executable')))
        if samples:
            # Each scan remains available in the journal. Retain the first
            # observed price and latest observation for each source per scan.
            samples.sort(key=lambda p: p['at'])
            groups[name] = samples if len(samples) < 2 else [samples[0], samples[-1]]
    if not groups:
        return None
    evidence = dict(source='matrix_quotes', captured_at=snapshot['captured_at'], groups=groups)
    return {**evidence, 'sha256': fingerprint(evidence)}


def checked_score(result):
    score = dict(result)
    for key in ('home_ht', 'away_ht', 'home_ft', 'away_ft'):
        if type(score.get(key)) is not int or score[key] < 0:
            score[key] = None
    if any(score[s+'_ft'] is None or score[s+'_ht'] is None or score[s+'_ht'] > score[s+'_ft']
           for s in ('home', 'away')):
        score.update(home_ht=None, away_ht=None)
    return score


def merge_verified_result(previous, incoming):
    old, new = checked_score(previous or {}), checked_score(incoming or {})
    if old.get('status') in FINISHED and None not in (old['home_ft'], old['away_ft']):
        if new.get('status') not in FINISHED or None in (new['home_ft'], new['away_ft']):
            return old
        if None in (new['home_ht'], new['away_ht']) and None not in (old['home_ht'], old['away_ht']):
            return checked_score(dict(new, home_ht=old['home_ht'], away_ht=old['away_ht']))
    return new


def results_for_day(store, day, at, fetched=None, previous=None):
    """Latest valid pre-kickoff publication per fixture; outcomes kept separately."""
    latest, invalid = {}, 0
    for record in store.get('analysis_updates', {}).values():
        if not record.get('publication_snapshot') or record.get('retrospective'):
            continue
        meta = record.get('fixture') or {}
        snapshot = (((record.get('multimarket') or {}).get('matrix') or {}).get('v7'))
        if not snapshot:
            continue
        if not valid_snapshot(snapshot, meta, record.get('captured_at')):
            invalid += 1
            continue
        kickoff = stamp(snapshot['kickoff'])
        if kickoff.astimezone(ROME).date() != day or kickoff >= at:
            continue
        fid = snapshot['fixture_id']
        if fid not in latest or snapshot['captured_at'] > latest[fid][0]['captured_at']:
            latest[fid] = (snapshot, (record.get('settlement') or {}).get('result'))
    prior = (previous or {}).get('fixtures', {})
    fixtures, samples, progress = {}, [], Counter()
    for fid, (snapshot, settled) in sorted(latest.items()):
        prior_result = prior.get(fid, {}).get('result') or {}
        result = merge_verified_result(prior_result, settled) if settled else checked_score(prior_result)
        if (fetched or {}).get(fid):
            result = merge_verified_result(result, fetched[fid])
        status = result.get('status')
        observed_at = (prior.get(fid, {}).get('observed_at')
                       if result == prior_result and prior.get(fid, {}).get('observed_at') else utc(at))
        state = ('void' if status in VOID else 'postponed' if status == 'PST' else
                 'suspended' if status in ('SUSP', 'INT') else
                 'settled' if status in FINISHED and result.get('home_ft') is not None
                 and result.get('away_ft') is not None else 'pending')
        progress[state] += 1
        outcomes = {}
        rows = snapshot['markets']
        role_for_market = defaultdict(list)
        role_prices = defaultdict(dict)
        for role, signal in snapshot['signals'].items():
            if role == 'observations' or not signal:
                continue
            market = signal.get('market_id')
            if market:
                key = market_id(canonical(json.loads(market)))
                role_for_market[key].append(role)
                if finite(signal.get('odd')):
                    role_prices[key][role] = signal['odd']
        models = {}
        for key, row in rows.items():
            spec = json.loads(key)
            settled_weights = settle_spec(spec, result) if state == 'settled' else None
            if settled_weights is None:
                outcomes[key] = None
                continue
            win, loss = settled_weights
            odd = row.get('odd')
            profit = win * (odd - 1) - loss if finite(odd) and odd > 1 else None
            outcomes[key] = dict(win=win, loss=loss, roi=profit)
            binary = win + loss == 1 and win in (0, 1)
            if not binary or key not in MONITORED and not role_for_market.get(key):
                continue
            comparable = key in BENCHMARKS and row.get('fair') is not None
            if comparable and not models:
                models = {str(weight): score_model(blend_rates(snapshot['statistics'],
                          snapshot['market_projection'], weight)) for weight in WEIGHTS}
            variants = ({weight: round(model.estimate(spec)['p'], 8) for weight, model in models.items()}
                        if comparable else {})
            samples.append(dict(fixture_id=fid, market=key, date=str(day), kickoff=snapshot['kickoff'],
                observed_at=observed_at, outcome=int(win), roi=profit, roles=role_for_market.get(key, []),
                benchmark=key in BENCHMARKS,
                role_roi={role: win*(price-1)-loss for role, price in role_prices.get(key, {}).items()},
                supported=row['supported'],
                tier=row.get('tier'), horizon=row.get('horizon'), niche_id=row.get('niche_id'),
                features=row['features'], edge_exclusions=row['edge_exclusions'],
                policy_exclusions=row['policy_exclusions'], raw=row['raw'], corrected=row['corrected'],
                calibrated=row['calibrated'], v51=row['v51'], fair=row['fair'], variants=variants))
        fixtures[fid] = dict(forecast_sha256=snapshot['sha256'], kickoff=snapshot['kickoff'],
                             captured_at=snapshot['captured_at'], observed_at=observed_at,
                             status=state, result=result, outcomes=outcomes)
        if 'triad' in snapshot:
            fixtures[fid].update({key: snapshot.get(key) for key in ('triad', 'tier', 'badge', 'horizon', 'niche_id')})
    return dict(version=VERSION, date=str(day), generated_at=utc(at),
                counts=dict(progress), rejected_forecasts=invalid, fixtures=fixtures, samples=samples)


def archive_day(root, store, day, at, fetched=None):
    path = day_path(root, day)
    previous = read_store(path, {})
    by_id = {str(m.get('fixture', {}).get('fixture_id')): m.get('fixture', {}).get('result')
             for d in (fetched or {}).get('days', []) for m in d.get('matches', [])}
    output = results_for_day(store, day, at, by_id, previous)
    if output['fixtures'] or output['rejected_forecasts'] or not path.is_file():
        write_store(path, output)
    elif previous:
        return previous
    return output
