"""Observed prematch populations, independent of counterfactual replays."""
from copy import deepcopy
from multimarket_model import stamp


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
