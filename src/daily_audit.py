"""Read-only, reproducible ledger of frozen proposals and their outcomes."""
from collections import defaultdict
from hashlib import sha256
import json
from math import isfinite
from multimarket_model import stamp, VALIDATION_FROM
from multimarket_audit import outcome, metrics

MARKETS = {('over15_ht', 'over'), ('over25_ht', 'over'),
           ('over25_ft', 'over'), ('over35_ft', 'over'), ('gg_ht', 'yes')}


def fingerprint(record):
    """Canonical forecast hash excludes the only mutable field: settlement."""
    frozen = {k: v for k, v in record.items() if k != 'settlement'}
    return sha256(json.dumps(frozen, sort_keys=True, separators=(',', ':'),
                             ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def number(value, low, high=float('inf')):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and isfinite(value) and low <= value <= high)


def build_daily(store, date, generated_at, source_revision=None):
    snapshots = {'first': {}, 'latest': {}}
    rejected = []
    # First snapshots also provide a fallback where no update was captured.
    for collection in ('multimarket_evaluations', 'analysis_updates'):
        for source_key, record in sorted(store.get(collection, {}).items()):
            fixture = record.get('fixture') or {}
            assessment = record.get('multimarket') or {}
            captured, kickoff = stamp(record.get('captured_at')), stamp(fixture.get('kickoff'))
            fid, version = fixture.get('fixture_id'), assessment.get('model_id')
            if not captured or not kickoff or captured >= kickoff or fid is None or not version:
                rejected.append({'collection': collection, 'key': source_key, 'reason': 'invalid_prematch_identity_or_timestamp'})
                continue
            period = ('retrospective' if record.get('retrospective') else
                      'prospective' if kickoff.date().isoformat() >= VALIDATION_FROM else 'development')
            key = (version, period, str(fid))
            entry = (record, collection, source_key)
            if collection == 'multimarket_evaluations':
                old = snapshots['first'].get(key)
                if old is None or captured < stamp(old[0]['captured_at']):
                    snapshots['first'][key] = entry
            old = snapshots['latest'].get(key)
            if old is None or captured > stamp(old[0]['captured_at']):
                snapshots['latest'][key] = entry
    rows, fixtures, groups = [], [], defaultdict(list)
    for policy, entries in snapshots.items():
        for (version, period, fid), (record, collection, source_key) in sorted(entries.items()):
            assessment = record['multimarket']
            pick = assessment.get('best_candidate') or {}
            chosen = (pick.get('market'), pick.get('selection'))
            result = (record.get('settlement') or {}).get('result') or {}
            identity = {'snapshot': policy, 'model_id': version, 'period': period,
                        'fixture_id': fid, 'fixture': record['fixture'],
                        'captured_at': record['captured_at'], 'forecast_sha256': fingerprint(record),
                        'source_collection': collection, 'source_key': source_key}
            candidates = {(m.get('market'), m.get('selection')): m
                          for m in assessment.get('markets', [])}
            fixtures.append(dict(identity, primary_market='::'.join(chosen) if all(chosen) else None,
                                 result=result, data_quality=assessment.get('data_quality'),
                                 selection_diagnostic=assessment.get('selection_diagnostic')))
            for market, side in sorted(MARKETS | ({chosen} if all(chosen) else set())):
                item = candidates.get((market, side), {})
                y = outcome(market, side, result)
                probability = (item.get('probabilities') or {}).get('statistics')
                book = item.get('book_probability_pct')
                odd = item.get('odd')
                valid_odd = number(odd, 1) and odd > 1
                reason = ('not_final' if result.get('status') not in {'FT', 'AET', 'PEN'} else
                          'missing_score' if y is None else None)
                selected = chosen == (market, side)
                row = dict(identity, market=market, selection=side, primary=selected,
                           candidate_present=bool(item), odd=odd,
                           statistical_probability=probability, book_probability_pct=book,
                           outcome=y, pending_reason=reason,
                           paper_profit=(odd if y else 0)-1 if y is not None and valid_odd else None,
                           missing=item.get('missing', []), pros=item.get('pros', []),
                           cons=item.get('cons', []), price_assessment=item.get('price_assessment'))
                rows.append(row)
                if item and y is not None and number(probability, 0, 1):
                    group = (policy, version, period, market, 'primary' if selected else 'not_selected')
                    groups[(*group, 'statistics')].append((probability, y, odd if valid_odd else None))
                    if number(book, 0, 100):
                        groups[(*group, 'matched_statistics')].append((probability, y, odd if valid_odd else None))
                        groups[(*group, 'matched_book')].append((book/100, y, odd if valid_odd else None))
    return {'schema_version': 1, 'audit_date': date, 'generated_at': generated_at,
            'source_revision': source_revision,
            'note': 'Experimental. First/latest are separate cohorts, never additive. Paper ROI is not executed profit. No forecast is recalculated.',
            'excluded_records': rejected, 'fixtures': fixtures, 'markets': rows,
            'groups': [dict(zip(('snapshot', 'model_id', 'period', 'market', 'group', 'variant'), k), **metrics(v))
                       for k, v in sorted(groups.items())]}


def markdown(report):
    lines = [f"# Audit {report['audit_date']}", '', report['note'], '',
             f"Generated: {report['generated_at']} | Revision: {report['source_revision']}", '',
             f"Invalid records excluded: {len(report['excluded_records'])}", '',
             '| Snapshot | Model | Period | Fixtures | With primary | Primary settled | Primary pending |',
             '|---|---|---|---:|---:|---:|---:|']
    cohorts = defaultdict(list)
    for f in report['fixtures']:
        cohorts[(f['snapshot'], f['model_id'], f['period'])].append(f)
    for key, fs in sorted(cohorts.items()):
        picks = [r for r in report['markets'] if (r['snapshot'],r['model_id'],r['period']) == key and r['primary']]
        settled = sum(r['outcome'] is not None for r in picks)
        lines.append('| ' + ' | '.join(map(str, (*key, len(fs), sum(bool(f['primary_market']) for f in fs), settled, len(picks)-settled))) + ' |')
    lines += ['', '| Snapshot | Model | Fixture | Captured | Proposal | Quote | Statistics % | Book % | Result | Profit (paper) |',
              '|---|---|---|---|---|---:|---:|---:|---|---:|']
    for r in report['markets']:
        if not r['primary']: continue
        p = r['statistical_probability']
        values = (r['snapshot'], r['model_id'], r['fixture_id'], r['captured_at'],
                  r['market']+'::'+r['selection'], r['odd'],
                  round(p*100, 3) if number(p, 0, 1) else None, r['book_probability_pct'],
                  'won' if r['outcome'] is True else 'lost' if r['outcome'] is False else r['pending_reason'],
                  round(r['paper_profit'], 3) if r['paper_profit'] is not None else None)
        lines.append('| ' + ' | '.join(str(v).replace('|', '/') if v is not None else '—' for v in values) + ' |')
    lines += ['', 'See the adjacent JSON for every frozen source key, SHA-256, quote, probability, missing field and result.',
              'A missing result is pending, never a loss. A missing quote is excluded from paper ROI.', '']
    return '\n'.join(lines)
