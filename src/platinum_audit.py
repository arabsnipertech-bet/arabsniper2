"""Prospective Platinum audit; frozen choices, saved results, zero provider calls."""
from collections import defaultdict
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from audit_storage import read_store, write_store
from gold_competitions import output_allowed
from gold_policy_audit import attach_result, settle_saved_archives, valid_result
from lateral_matrix import MODEL_ID, settle_spec
from multimarket_model import stamp
from platinum_policy import POLICY_ID, evaluate

FIELDS = ('id', 'label', 'spec', 'odd', 'first_odd', 'quote_status', 'bookmaker',
          'bookmaker_id', 'provider_updated_at', 'quote_observed_at', 'cache_source_at',
          'cache_expires_at', 'oci', 'delta_pct', 'selected_margin_pct', 'paired_books',
          'agreeing_books', 'selected_movement', 'movement_confirmed', 'fair_direction_coherent',
          'statistical_pct', 'ev_pct', 'ev_low_pct', 'quality', 'platinum', 'opportunity')


def capture(store, match, observed_at):
    meta, assessment = match.get('fixture') or {}, match.get('multimarket') or {}
    matrix = assessment.get('matrix') or {}; policy = matrix.get('platinum_policy') or {}
    now, kickoff, decision = stamp(observed_at), stamp(meta.get('kickoff')), stamp(policy.get('evaluated_at'))
    if (assessment.get('model_id') != MODEL_ID or policy.get('id') != POLICY_ID
            or not output_allowed(match) or not now or not kickoff or not decision
            or not decision <= now < kickoff or meta.get('result')
            or meta.get('status_short') not in ('NS', 'TBD')): return False
    primary = policy.get('best_market_id')
    chosen = next((r for r in matrix.get('rows', []) if r['id'] == primary), None)
    if primary and (not chosen or not evaluate(chosen, policy['evaluated_at'])['eligible']
                    or chosen.get('platinum', {}).get('policy_id') != POLICY_ID
                    or chosen['platinum'].get('eligible') is not True
                    or chosen['platinum'].get('rank') != 1):
        raise ValueError('Platinum audit choice mismatch; refusing publication')
    snapshot = {'policy_id': POLICY_ID, 'decision_at': policy['evaluated_at'],
                'kickoff': meta['kickoff'], 'statistics_captured_at': assessment.get('captured_at'),
                'gold_best_market_id': matrix.get('opportunity_policy', {}).get('best_market_id'),
                'choice': {k: deepcopy(chosen.get(k)) for k in FIELDS} if chosen else None}
    digest = sha256(json.dumps(snapshot, sort_keys=True, allow_nan=False).encode()).hexdigest()
    fid = str(meta['fixture_id'])
    fixture = store.setdefault('fixtures', {}).setdefault(fid, {'fixture': deepcopy(meta),
        'date': kickoff.astimezone(ZoneInfo('Europe/Rome')).date().isoformat(), 'snapshots': []})
    if any(s['sha256'] == digest for s in fixture['snapshots']): return False
    snapshot.update(sha256=digest, observed_at=observed_at)
    fixture['snapshots'].append(snapshot)
    fixture['snapshots'].sort(key=lambda s: stamp(s['decision_at']))
    return True


def report(store, observed_at):
    groups = defaultdict(lambda: {'proposals': 0, 'settled': 0, 'won': 0, 'lost': 0,
                                  'pending': 0, 'odds_sum': 0., 'net_units': 0.})
    for fixture in store.get('fixtures', {}).values():
        snapshots = fixture.get('snapshots') or []
        if not snapshots: continue
        result = fixture.get('settlement', {}).get('result') or {}
        for horizon, snapshot in (('first', snapshots[0]), ('latest', snapshots[-1])):
            row = snapshot.get('choice')
            if not row: continue
            group = groups[(fixture['date'], snapshot['policy_id'], horizon)]
            group['proposals'] += 1
            outcome = settle_spec(row['spec'], result) if valid_result(result) else None
            if outcome is None: group['pending'] += 1; continue
            win, loss = outcome
            group['settled'] += 1; group['won'] += win == 1; group['lost'] += loss == 1
            group['odds_sum'] += row['odd']; group['net_units'] += win*(row['odd']-1)-loss
    rows = []
    for (date, policy, horizon), group in sorted(groups.items()):
        n = group['settled']; total = group.pop('odds_sum')
        rows.append({'date': date, 'policy_id': policy, 'snapshot': horizon, **group,
            'strike_rate_pct': round(100*group['won']/n, 4) if n else None,
            'mean_odd': round(total/n, 4) if n else None,
            'paper_roi_pct': round(100*group['net_units']/n, 4) if n else None})
    return {'policy_id': POLICY_ID, 'generated_at': observed_at, 'api_calls': 0,
        'frozen_fixtures': len(store.get('fixtures', {})), 'groups': rows,
        'notes': ['One primary Platinum per fixture and horizon; first/latest are separate.',
                  'No retrospective classification. Gold remains a separate cohort; overlaps must not be added.',
                  'Frozen before publication; this is not a record of executed bets.',
                  'Returns use 90-minute/half-time results and one hypothetical unit at the saved bookmaker price.',
                  'Missing results remain pending. High odds and quote flows do not establish calibrated confidence.']}


def main(root, observed_at):
    root = Path(root); path = root/'data/arab_quote_platinum_audit.json'
    store = read_store(path, {'schema_version': 1, 'policy_id': POLICY_ID, 'fixtures': {}})
    if store.get('schema_version') != 1 or store.get('policy_id') != POLICY_ID:
        raise ValueError('Platinum audit identity mismatch')
    settle_saved_archives(store, root, observed_at)
    added, seen = 0, set()
    for day_path in sorted((root/'data').glob('arab_quote_day[1-5].json')):
        for match in read_store(day_path, {}).get('matches', []):
            fid = str((match.get('fixture') or {}).get('fixture_id'))
            if fid in seen: continue
            seen.add(fid)
            added += capture(store, match, observed_at)
            attach_result(store, fid, match.get('fixture', {}).get('result'), observed_at, day_path.name)
    store['updated_at'] = observed_at
    summary = report(store, observed_at)
    write_store(path, store)
    write_store(root/'data/arab_quote_platinum_report.json', summary)
    print('PLATINUM_AUDIT_SUMMARY='+json.dumps({**summary, 'snapshots_added': added}, separators=(',', ':')), flush=True)
    return summary
