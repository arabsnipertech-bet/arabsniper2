"""Replay selection on frozen probabilities/prices. Never a profitability backtest.

Usage: python src/selection_replay.py INPUT.json OUTPUT.json
This isolates changes to selection from changes to collected inputs/model rates.
"""
import json
import sys
from copy import deepcopy
from pathlib import Path
from context_analysis import select_proposal, pick_goal_proposal, PRIMARY_MARKETS, MODEL_ID


def replay(payload):
    fixtures=[]
    for match in payload.get('matches',[]):
        analysis=match.get('multimarket',{})
        if not str(analysis.get('model_id','')).startswith('context-goals-v3'):
            continue
        rows=deepcopy(analysis.get('markets',[]))
        candidates,diagnostic=select_proposal(rows)
        candidates=[r for r in candidates if (r['market'],r['selection']) in PRIMARY_MARKETS]
        selected=pick_goal_proposal(candidates,1.5)
        def brief(r):
            if not r:return None
            return {k:r.get(k) for k in ('market','selection','label','odd','statistical_pct','book_probability_pct','source','price_assessment')}
        fixtures.append({'fixture':deepcopy(match['fixture']), 'input_model':analysis['model_id'],
            'input_captured_at':analysis.get('captured_at'), 'previous':brief(analysis.get('best_candidate')),
            'replayed':brief(selected),'diagnostic':diagnostic,
            'five_markets':[brief(r) for r in rows if (r['market'],r['selection']) in PRIMARY_MARKETS]})
    return {'kind':'selection_regression_not_backtest','target_model':MODEL_ID,
        'input_updated_at':payload.get('updated_at'), 'fixtures':fixtures,
        'limitation':'Same stored probabilities and quotes. No results, no fitted parameters, no demonstrated profitability.'}


if __name__=='__main__':
    data=json.loads(Path(sys.argv[1]).read_text())
    Path(sys.argv[2]).write_text(json.dumps(replay(data),ensure_ascii=False,indent=2))
