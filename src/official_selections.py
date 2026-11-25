"""Observed prematch populations, independent of counterfactual replays."""
from copy import deepcopy
from multimarket_model import stamp


def trace_snapshot(snapshot, sources, profiles):
    """Seal new evidence without changing any decision or enriching old choices.

    A choice points to a hash of the prematch evidence body (defined below).
    The enclosing full snapshot SHA is added to archive exports after sealing;
    keeping the two hashes distinct avoids a self-referential fingerprint.
    """
    from v7_probability import MEASUREMENT_PROTOCOL, MEASUREMENT_CONFIG, probability_origins
    from v7_movement_shadow import instrument
    from v7_decision import fingerprint
    if not stamp(snapshot.get('captured_at')) or stamp(snapshot['captured_at']) < stamp('2026-11-25T00:00:00Z'):
        return
    evidence_fields = ('fixture_id', 'kickoff', 'captured_at', 'statistics_at',
                       'raw_rates', 'statistics', 'rates', 'market_projection', 'means', 'markets')
    # Compute before any measurement additions, and record the exact body keys.
    evidence_sha = fingerprint({k: snapshot.get(k) for k in evidence_fields})
    config = dict(deepcopy(MEASUREMENT_CONFIG), control={k: deepcopy(snapshot.get(k))
        for k in ('version', 'weight', 'calibration', 'qualification_policy', 'study_selection_policy')},
        movement_config_sha256=(((snapshot.get('decision_shadow') or {}).get('movement_study') or {}).get('config_sha256')))
    trace = dict(protocol=MEASUREMENT_PROTOCOL, config=config,
                 config_sha256=fingerprint(config),
                 prematch_evidence_sha256=evidence_sha, evidence_fields=list(evidence_fields),
                 probability_origins=probability_origins(snapshot), shadow_only=True,
                 historical_backfill=False, production_selector_changed=False)
    snapshot['measurement'] = trace
    if 'triad_commitments' in snapshot:
        snapshot['triad_commitments'] = deepcopy(snapshot['triad_commitments'])
    for slot in (snapshot.get('triad_commitments') or {}).values():
        # Previously fixed slots keep their own evidence; never fill an absent
        # original P using today's P, even if the market name is unchanged.
        if slot.get('captured_at') != snapshot['captured_at']:
            continue
        row = snapshot['markets'].get(slot['market_id']) or {}
        if slot.get('odd') != row.get('odd'):
            continue
        slot.update(probability=row.get('calibrated'), fixture_id=snapshot['fixture_id'],
                    kickoff=snapshot['kickoff'], prematch_evidence_sha256=evidence_sha,
                    probability_origin='final_v7', quote=deepcopy(row.get('quote')),
                    raw_probability=row.get('raw'), statistical_probability=row.get('statistics'))
    instrument(snapshot, sources, profiles)


def archived_choice(slot, snapshot):
    """Bind an export to the actual sealed source, never a later forecast."""
    pick = deepcopy(slot)
    if (slot.get('captured_at') == snapshot.get('captured_at') and
            slot.get('prematch_evidence_sha256') == (snapshot.get('measurement') or {}).get('prematch_evidence_sha256')
            and slot.get('prematch_evidence_sha256')):
        pick['forecast_sha256'] = snapshot['sha256']
    return pick


class OfficialSelections:
    def __init__(self):
        self.first = {}; self.latest = {}; self.eligible = {}

    def observe(self, record):
        from daily_audit import fingerprint
        meta = record.get('fixture') or {}
        at, kickoff = stamp(record.get('captured_at')), stamp(meta.get('kickoff'))
        if record.get('retrospective') or not at or not kickoff or at >= kickoff or meta.get('fixture_id') is None:
            return
        fid = str(meta['fixture_id'])
        checksum = fingerprint(record)
        rank = (at, checksum)
        matrix = (record.get('multimarket') or {}).get('matrix') or {}
        selected = None
        for row in matrix.get('rows', []):
            policy = row.get('opportunity') or {}
            if policy.get('eligible') is not True: continue
            entry = dict(fixture_id=fid, captured_at=record['captured_at'], forecast_sha256=checksum,
                         kickoff=meta.get('kickoff'),
                         policy_id=(matrix.get('opportunity_policy') or {}).get('id'),
                         id=row['id'], spec=deepcopy(row.get('spec')), odd=row.get('odd'),
                         probability_pct=row.get('statistical_pct'), rank=policy.get('rank'))
            key = (fid, row['id'])
            if key not in self.eligible or rank < self.eligible[key][0]: self.eligible[key] = (rank, entry)
            if policy.get('rank') == 1: selected = entry
        if selected and (fid not in self.first or rank < self.first[fid][0]): self.first[fid] = (rank, selected)
        if fid not in self.latest or rank > self.latest[fid][0]: self.latest[fid] = (rank, selected)

    def report(self):
        def population(values):
            rows = [entry for _, entry in values if entry is not None]
            return dict(contracts_count=len(rows), distinct_fixtures_count=len({r['fixture_id'] for r in rows}),
                        selections=sorted(rows, key=lambda r:(r['fixture_id'],r['captured_at'],r['id'])))
        return dict(gold_certificate=population(self.first.values()),
                    latest_prematch_gold=population(self.latest.values()),
                    all_eligible_markets=population(self.eligible.values()),
                    definitions='First rank-1 observed; rank-1 at latest snapshot (including no proposal); first observation of each eligible fixture/market. Pending results included. Not social deliveries.')
