"""Join publisher receipts with a checksum-verified Auditor checkpoint."""
from collections import Counter
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import json

from audit_storage import read_store
from auditor_daily_priority import STATUS, v7_paths
from gold_storage import previous_manifest, restore_subset
from lateral_matrix import settle_spec
from multimarket_model import stamp
from v7_probability import VERSION, finite

from .selection import rome_day


def certified(storage, day):
    manifest = previous_manifest(storage.client, storage.bucket)
    paths = v7_paths(day)
    if STATUS not in manifest or any(p not in manifest for p in paths.values()):
        return None
    with TemporaryDirectory(prefix='publisher-audit-') as directory:
        root = Path(directory)
        restore_subset(storage.client, storage.bucket, root, manifest, [STATUS])
        certificate = read_store(root / STATUS, {})
        if (certificate.get('version') != 2 or certificate.get('report_date') != str(day)
                or certificate.get('date') != str(day + timedelta(days=1))
                or any(certificate.get(key) != manifest[path]['sha256'] for key, path in paths.items())):
            return None
        path = paths['outcomes_sha256']
        restore_subset(storage.client, storage.bucket, root, manifest, [path])
        audit = read_store(root / path)
        if audit.get('version') != VERSION or audit.get('date') != str(day) or not stamp(audit.get('generated_at')):
            raise ValueError('Invalid certified V7 archive')
        return audit


def settle(pick, evidence):
    if stamp(pick['kickoff']) != stamp(evidence['kickoff']):
        raise ValueError('Auditor kickoff differs from the prematch receipt')
    if evidence['status'] == 'void':
        return 'void', 0
    if evidence['status'] != 'settled':
        return 'pending', None
    weights = settle_spec(json.loads(pick['market_id']), evidence['result'])
    if weights is None:
        return 'pending', None
    win, loss = weights
    if (win, loss) not in {(1, 0), (0, 1), (0, 0)}:
        raise ValueError('Non-binary market outside the V7 triad')
    return ('won' if win else 'lost' if loss else 'void'), win * (pick['odd'] - 1) - loss


def metrics(rows):
    counts = Counter(row['outcome'] for row in rows)
    settled = counts['won'] + counts['lost']
    priced = [row['odd'] for row in rows if finite(row.get('odd'))]
    stake = settled + counts['void']
    net = sum(row['profit'] for row in rows if row['profit'] is not None)
    return {'total': len(rows), 'verified': stake, 'won': counts['won'], 'lost': counts['lost'],
            'pending': counts['pending'], 'void': counts['void'],
            'hit_rate': round(100 * counts['won'] / settled, 1) if settled else None,
            'average_odd': round(sum(priced) / len(priced), 2) if len(priced) == len(rows) and rows else None,
            'profit': round(net, 2) if stake else None, 'roi': round(100 * net / stake, 2) if stake else None}


def build(audit, availability, public):
    """Report only evidenced Gold and free picks. Examples always came from a free feed."""
    fixtures = audit['fixtures']
    cohorts = {'public': [], 'gold': [], 'internal': []}
    public_ids = {p['fixture_id'] for p in public}
    for audience, picks in (('public', public), ('gold', availability)):
        seen = set()
        for pick in picks:
            fid = pick['fixture_id']
            if fid in seen or (audience == 'gold' and fid in public_ids):
                continue
            kickoff, observed, captured = (stamp(pick[key]) for key in ('kickoff', 'observed_at', 'captured_at'))
            if not (captured and observed and kickoff and captured <= observed < kickoff
                    and rome_day(kickoff) == audit['date'] and fid in fixtures):
                return None  # A missing receipt/outcome must never silently change the cohort.
            seen.add(fid)
            outcome, profit = settle(pick, fixtures[fid])
            cohorts[audience].append(dict(pick, outcome=outcome, profit=profit))
    for row in audit.get('samples', []):
        # An audited research market without an assigned tier or official role.
        if (row.get('tier') is None and row.get('roles') == [] and row.get('supported') is True
                and row.get('outcome') in (0, 1) and finite(row.get('roi'))):
            cohorts['internal'].append({'outcome': 'won' if row['outcome'] else 'lost', 'profit': row['roi']})

    if not any(cohorts.values()):
        return None
    free = [r for r in cohorts['public'] if r['outcome'] in ('won', 'lost', 'void')]
    examples = []
    for outcome in ('lost', 'won'):
        row = next((r for r in free if r['outcome'] == outcome), None)
        if row:
            examples.append(row)
    examples.extend(r for r in free if r not in examples)
    return {'date': audit['date'], 'certified_at': audit['generated_at'],
            'cohorts': {key: metrics(rows) for key, rows in cohorts.items()},
            'examples': [{k: r[k] for k in ('match', 'league', 'market', 'odd', 'outcome')}
                         for r in examples[:3]]}
