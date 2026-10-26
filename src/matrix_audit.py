"""Versioned prospective evaluation of every observed matrix market.

First and latest prematch snapshots are reported separately by the caller.
Asian returns use stake fractions, not a binary win/lose approximation.
"""
from collections import defaultdict
from math import log
from lateral_matrix import MODEL_ID, settle_spec
from multimarket_model import stamp


class MatrixAccumulator:
    def __init__(self):
        self.fixtures, self.settled = set(), set()
        self.groups = defaultdict(lambda: {'n': 0, 'fixtures': set(), 'priced_n': 0,
            'return_sum': 0., 'binary_n': 0, 'brier_sum': 0., 'log_loss_sum': 0.,
            'win_stake_sum': 0., 'loss_stake_sum': 0., 'push_n': 0})

    def add(self, record):
        a, meta = record.get('multimarket') or {}, record.get('fixture') or {}
        if a.get('model_id') != MODEL_ID: return
        at, kickoff = stamp(record.get('captured_at')), stamp(meta.get('kickoff'))
        if not at or not kickoff or at >= kickoff or record.get('retrospective'): return
        if kickoff.date().isoformat() < a.get('validation_from', '2026-10-26'): return
        fid = str(meta.get('fixture_id'))
        self.fixtures.add(fid)
        result = record.get('settlement', {}).get('result') or {}
        for row in a.get('matrix', {}).get('rows', []):
            if not row.get('spec'): continue
            value = settle_spec(row['spec'], result)
            if value is None: continue
            self.settled.add(fid)
            win, loss = value
            dimensions = [('market', row['id']), ('family', row['family']),
                ('category', row.get('category', 'unknown')),
                ('behavior', row.get('market_behavior', 'unavailable')),
                ('margin_band', row.get('margin_band', 'unavailable')),
                ('margin_trend', row.get('margin_trend', 'unavailable')),
                ('quality', row.get('quality', 'unknown'))]
            for dimension, key in dimensions:
                group = self.groups[(dimension, key)]
                group['n'] += 1; group['fixtures'].add(fid)
                group['win_stake_sum'] += win; group['loss_stake_sum'] += loss
                group['push_n'] += win == 0 and loss == 0
                odd = row.get('odd')
                if isinstance(odd, (int, float)) and odd > 1 and row.get('quote_status') == 'recent':
                    group['priced_n'] += 1
                    group['return_sum'] += win*(odd-1)-loss
                if row['spec']['kind'] not in ('handicap', 'asian_total', 'dnb'):
                    p = min(1-1e-12, max(1e-12, row['statistical_pct']/100))
                    group['binary_n'] += 1
                    group['brier_sum'] += (p-win)**2
                    group['log_loss_sum'] -= log(p if win else 1-p)

    def report(self):
        groups = []
        for (dimension, value), g in sorted(self.groups.items()):
            groups.append({'dimension': dimension, 'value': value,
                'market_observations': g['n'], 'distinct_fixtures': len(g['fixtures']),
                'priced_observations': g['priced_n'], 'pushes': g['push_n'],
                'winning_stake_units': round(g['win_stake_sum'], 4),
                'losing_stake_units': round(g['loss_stake_sum'], 4),
                'paper_roi_pct': round(100*g['return_sum']/g['priced_n'], 4) if g['priced_n'] else None,
                'binary_observations': g['binary_n'],
                'binary_brier': round(g['brier_sum']/g['binary_n'], 6) if g['binary_n'] else None,
                'binary_log_loss': round(g['log_loss_sum']/g['binary_n'], 6) if g['binary_n'] else None})
        return {'model_id': MODEL_ID, 'frozen_fixtures': len(self.fixtures),
            'settled_fixtures': len(self.settled), 'groups': groups,
            'note': 'Future frozen inputs only. Markets within a fixture are correlated, not independent trials. Returns are theoretical with Asian refunds. First/latest snapshots remain separate.'}
