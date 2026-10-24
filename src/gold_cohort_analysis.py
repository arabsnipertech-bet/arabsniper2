"""Read-only aggregate study of the existing FT Gold badge; no forecast changes."""
import argparse
from collections import defaultdict
from datetime import date, timedelta
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
from audit_storage import FORMAT

MODEL = 'context-goals-v3.4'
TARGETS = ('over25_ft', 'over15_ht')


def valid(x, low=0, high=100):
    return type(x) in (int, float) and math.isfinite(x) and low <= x <= high


def gold(row):
    p, b = row.get('statistical_probability'), row.get('book_probability_pct')
    if not valid(p, 0, 1) or not valid(b):
        return 'unknown'
    return 'gold' if p > .5 and b > 50 and abs(p*100-b) <= 10+1e-9 else 'no_gold'


def metrics(rows):
    settled = [r for r in rows if type(r.get('outcome')) is bool]
    n = len(settled)
    w = sum(r['outcome'] for r in settled)
    priced = [r for r in settled if valid(r.get('odd'), 1) and r['odd'] > 1]
    result = dict(total=len(rows), settled=n, wins=w, losses=n-w, pending=len(rows)-n,
                  hit_rate=round(100*w/n, 2) if n else None, priced=len(priced))
    if n:
        p, z = w/n, 1.96
        center = (p+z*z/(2*n))/(1+z*z/n)
        spread = z*math.sqrt(p*(1-p)/n+z*z/(4*n*n))/(1+z*z/n)
        result['wilson95_pct'] = [round(100*(center-spread), 2), round(100*(center+spread), 2)]
    for field in ('odd', 'statistical_probability', 'book_probability_pct'):
        values = [r[field] for r in settled if valid(r.get(field), 0)]
        result['mean_'+field] = round(sum(values)/len(values), 4) if values else None
    # Diagnostic paper return at archived odds, not executed bettor profit.
    result['paper_roi_pct'] = round(100*sum((r['odd'] if r['outcome'] else 0)-1 for r in priced)/len(priced), 2) if priced else None
    return result


def band(row, kind):
    if kind == 'odds':
        x = row.get('odd')
        if not valid(x, 1) or x <= 1: return 'missing'
        return '<1.70' if x < 1.7 else '1.70-1.99' if x < 2 else '2.00-2.49' if x < 2.5 else '>=2.50'
    if kind == 'statistics':
        x = row.get('statistical_probability')
        if not valid(x, 0, 1): return 'missing'
        return '<=50%' if x <= .5 else '50-60%' if x <= .6 else '>60%'
    if kind == 'league':
        f = row.get('fixture', {})
        return str(f.get('country', ''))+' / '+str(f.get('league', ''))
    raise ValueError(kind)


def analyze(reports, target):
    groups, slices, paired = defaultdict(list), defaultdict(list), defaultdict(lambda:defaultdict(int))
    filters = defaultdict(list)
    missing = []
    for day, report in sorted(reports.items()):
        if report.get('audit_date') != day: raise ValueError('Report date mismatch')
        index = {}
        for row in report.get('markets', []):
            if row.get('model_id') != MODEL or row.get('period') != 'prospective' or row.get('selection') != 'over': continue
            if row.get('snapshot') not in ('first', 'latest') or row.get('market') not in TARGETS: continue
            key = (row['snapshot'], str(row['fixture_id']), row['market'])
            if key in index: raise ValueError('Duplicate fixture/market/snapshot')
            index[key] = row
        for (snapshot, fid, market), row in index.items():
            if not row.get('candidate_present'): continue
            ft = index.get((snapshot, fid, 'over25_ft'), {})
            badge = gold(ft)
            for scope in ('all_candidates', 'primary_only') if row.get('primary') else ('all_candidates',):
                groups[(day, snapshot, market, scope, badge)].append(row)
                if snapshot == 'latest' and badge == 'no_gold' and valid(row.get('odd'), 1) and row['odd'] > 1:
                    p, b = row.get('statistical_probability'), row.get('book_probability_pct')
                    ref = (row.get('price_assessment') or {}).get('reference_probability_pct')
                    rules = {'statistics_over50': valid(p,0,1) and p>.5,
                             'book_over50': valid(b) and b>50,
                             'both_over50': valid(p,0,1) and valid(b) and p>.5 and b>50,
                             'reference_above_break_even': valid(ref) and ref>100/row['odd'],
                             'statistics_above_book': valid(p,0,1) and valid(b) and p*100>b}
                    for rule, passed in rules.items():
                        filters[(day, market, scope, rule, 'selected' if passed else 'excluded')].append(row)
                if day == target and snapshot == 'latest':
                    for kind in ('odds', 'statistics', 'league'):
                        slices[(market, scope, kind, band(row,kind), badge)].append(row)
            if day == target and snapshot == 'latest' and row.get('primary') and type(row.get('outcome')) is not bool:
                missing.append(dict(market=market, reason=row.get('pending_reason')))
        for (snapshot, fid, market), ft in index.items():
            if market != 'over25_ft' or not ft.get('candidate_present'): continue
            ht = index.get((snapshot, fid, 'over15_ht'), {})
            if not ht.get('candidate_present'): continue
            bucket = paired[(day, snapshot, gold(ft))]
            if type(ft.get('outcome')) is bool and type(ht.get('outcome')) is bool:
                bucket[f"FT_{int(ft['outcome'])}_HT_{int(ht['outcome'])}"] += 1
            else: bucket['pending'] += 1
    return dict(date=target, model=MODEL, dates=sorted(reports),
        definition='Gold is the FT Over 2.5 badge: statistics>50%, fair book>50%, gap<=10pp. PT groups inherit that FT badge. Unknown is separate. First/latest and primary/all are overlapping, never additive.',
        groups=[dict(zip(('day','snapshot','market','scope','gold'),k), **metrics(v)) for k,v in sorted(groups.items())],
        slices=[dict(zip(('market','scope','kind','band','gold'),k), **metrics(v)) for k,v in sorted(slices.items())],
        joint=[dict(zip(('day','snapshot','gold'),k), counts=dict(v)) for k,v in sorted(paired.items())],
        nongold_filters=[dict(zip(('day','market','scope','rule','membership'),k), **metrics(v)) for k,v in sorted(filters.items())],
        pending_primary=missing)


def load_checkpoint_reports(s3, bucket, days):
    manifest = json.loads(s3.get_object(Bucket=bucket, Key='state/current.json')['Body'].read())
    if manifest.get('version') != 1: raise ValueError('Invalid checkpoint')
    def get(path):
        entry = manifest['files'][path]
        digest = entry['sha256']
        if len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest): raise ValueError('Invalid hash')
        raw = s3.get_object(Bucket=bucket, Key='state/objects/'+digest)['Body'].read()
        if len(raw)!=entry['bytes'] or hashlib.sha256(raw).hexdigest()!=digest: raise ValueError('Object checksum mismatch')
        return raw
    reports = {}
    for day in days:
        path = f'auditarchive/prematch_{day}_daily.json'
        if path not in manifest['files']: continue
        value = json.loads(get(path))
        if value.get('_storage') == FORMAT:
            chunks = []
            for part in value['parts']:
                raw = gzip.decompress(get(f"auditarchive/_audit_parts/{part['sha256']}.json.gz"))
                if len(raw)!=part['bytes'] or hashlib.sha256(raw).hexdigest()!=part['sha256']: raise ValueError('Part checksum mismatch')
                chunks.append(raw)
            raw = b''.join(chunks)
            if len(raw)!=value['bytes'] or hashlib.sha256(raw).hexdigest()!=value['sha256']: raise ValueError('Manifest checksum mismatch')
            value = json.loads(raw)
        reports[day] = value
    return reports


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', required=True)
    args = parser.parse_args()
    target = date.fromisoformat(args.date)
    from gold_storage import client
    reports = load_checkpoint_reports(client(), os.environ['R2_BUCKET'], [(target-timedelta(days=i)).isoformat() for i in range(4)])
    if args.date not in reports: raise ValueError('Requested daily audit missing')
    result = analyze(reports, args.date)
    # Aggregates only. No raw forecasts, fixture identities or credentials are emitted.
    print('GOLD_COHORT_REPORT='+json.dumps(result, ensure_ascii=False, separators=(',',':')))


if __name__ == '__main__': main()
