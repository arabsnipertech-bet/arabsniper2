"""Read-only comparison on authentic frozen exports. No provider/API requests.

Older V3 summaries are counted as unavailable for a full V5 replay, never filled
with future inputs or treated as zero proposals. Run outputs belong in private
research storage; only aggregate summaries may be committed to public git.
"""
import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(Path(__file__).resolve().parent)]
from baselines import gold_goals_v6 as v6, gold_scope_v2 as scope_v6
from gold_competitions import output_allowed
from lateral_matrix import base_specs, market_id, matrix_rows, settle_spec, ScoreModel
from market_scope import core_market
from matrix_quotes import expire
from opportunity_policy import goal_market, annotate
from quote_cache import remember
from multimarket_model import stamp

DATES = ('2026-10-23','2026-10-24','2026-10-25','2026-10-27')
TZ = ZoneInfo('Europe/Rome')


def records(path):
    import ijson
    with open(path, 'rb') as source:
        yield from ijson.items(source, 'records.item', use_float=True)


def metrics(picks):
    settled = [p for p in picks if p['settlement'] is not None]
    wins = sum(p['settlement'][0] == 1 for p in settled)
    losses = sum(p['settlement'][1] == 1 for p in settled)
    profit = sum(p['settlement'][0]*(p['odd']-1)-p['settlement'][1] for p in settled)
    return {'proposals':len(picks), 'wins':wins, 'losses':losses, 'pending':len(picks)-len(settled),
        'strike_rate_pct':100*wins/len(settled) if settled else None,
        'mean_odd':sum(p['odd'] for p in picks)/len(picks) if picks else None,
        'net_units':profit if settled else None, 'net_roi_pct':100*profit/len(settled) if settled else None}


def run(paths, manifest):
    expected = {str(f['fixture_id']):f for f in manifest['fixtures']}
    latest, caches, result_by_id = {}, defaultdict(dict), {}
    source_counts = Counter()
    for path in paths:
        for record in records(path):
            fid = str(record['fixture']['fixture_id'])
            if fid not in expected: continue
            captured = stamp(record['captured_at'])
            assert captured and captured < stamp(record['fixture']['kickoff'])
            result = record.get('result')
            if result and result.get('status') in ('FT','AET','PEN'): result_by_id[fid] = result
            source_counts[record['model_id']] += 1
            if record['model_id'] != expected[fid]['model']: continue
            if record['model_id'].startswith('lateral-matrix-v5'):
                a = record['assessment']
                a['matrix']['rows'] = [r for r in a['matrix']['rows'] if goal_market(r) or r.get('spec',{}).get('kind') == 'result']
                for r in a['matrix']['rows']:
                    if r.get('quote_status') != 'recent': continue
                    remember(r)
                    if not r.get('cached_quote'): continue
                    key = (r['id'], str(r.get('bookmaker_id')))
                    previous = caches[fid].get(key)
                    if not previous or captured > previous[0]: caches[fid][key] = (captured, deepcopy(r['cached_quote']))
            old = latest.get(fid)
            if not old or captured > stamp(old['captured_at']): latest[fid] = record
    assert set(latest) == set(expected), ('missing fixtures', set(expected)-set(latest))
    choices, unavailable, comparisons, combos = defaultdict(list), [], [], []
    recovered, recovered_b, recovered_proposal = set(), set(), set()
    hash_checks = probability_checks = 0
    exclusion_reasons = Counter()
    for fid, record in latest.items():
        f, source = record['fixture'], record['assessment']
        day = expected[fid]['date']; as_of = record['captured_at']
        if not record['model_id'].startswith('lateral-matrix-v5'):
            unavailable.append({'fixture_id':fid,'date':day,'model':record['model_id'],
                                'reason':'missing_original_v5_inputs_and_sensitivity_scenarios'})
            continue
        if source.get('input_sha256'):
            assert sha256(json.dumps(source['inputs'],sort_keys=True,default=str).encode()).hexdigest() == source['input_sha256']
            hash_checks += 1
        for history in source.get('inputs',{}).get('histories',{}).values():
            assert all(stamp(r['kickoff']) < stamp(as_of) for r in history)
        model = ScoreModel(source['rates'])
        for row in source['matrix']['rows']:
            assert abs(100*model.estimate(row['spec'])['p']-row['statistical_pct']) < .00011
            probability_checks += 1
        policy = source['matrix'].get('opportunity_policy') or {}
        if policy.get('id') == 'gold-goals-v6':
            saved = next((r for r in source['matrix']['rows'] if r['id'] == policy.get('best_market_id')), None)
            if saved:
                choices['v6_published'].append({'fixture_id':fid,'match':f.get('match'),'date':day,
                    'market':goal_market(saved),'odd':saved['odd'],'quote_status':saved.get('quote_status'),
                    'settlement':settle_spec(saved['spec'],result_by_id.get(fid) or {}),'captured_at':as_of})
        baseline, current = deepcopy(source), deepcopy(source)
        for a in (baseline,current): a['matrix'].pop('proposal_stability',None)
        base_best = v6.annotate(baseline['matrix'], None, baseline) if scope_v6.output_allowed({'fixture':f}) else None
        rows = current['matrix']['rows']; old_states = {r['id']:r.get('quote_status') for r in rows}
        existing = {r['id'] for r in rows}
        extra = {market_id(s):s for s in base_specs() if market_id(s) not in existing and (goal_market({'spec':s}) or s['kind']=='halves_total')}
        rows.extend(matrix_rows(current, extra))
        for row in rows:
            cached = caches[fid].get((row['id'],str(row.get('bookmaker_id'))))
            if cached and cached[0] <= stamp(as_of): row['cached_quote'] = deepcopy(cached[1])
        allowed = output_allowed({'fixture':f})
        if allowed:
            expire(current, f, as_of)
            best_id = current['matrix']['opportunity_policy']['best_market_id']
            new_best = next((r for r in rows if r['id']==best_id),None)
            changed = [r for r in rows if goal_market(r) and r.get('quote_status')=='cached_stable' and old_states.get(r['id']) != 'cached_stable']
            if changed:
                recovered.add(fid)
                if f.get('country')=='Italy' and f.get('league')=='Serie B': recovered_b.add(fid)
            if new_best and new_best.get('quote_status')=='cached_stable': recovered_proposal.add(fid)
            exclusion_reasons.update(reason for r in rows if goal_market(r) for reason in r['opportunity']['reasons'])
            combos.extend({'fixture_id':fid,'date':day,'p':r['statistical_pct'],'settlement':settle_spec(r['spec'], result_by_id.get(fid) or {})}
                          for r in rows if r.get('combo',{}).get('active'))
        else: new_best = None
        for version, best in [('v6',base_best),('v6.1',new_best)]:
            if best:
                choices[version].append({'fixture_id':fid,'match':f.get('match'),'date':day,
                    'market':goal_market(best),'odd':best['odd'],'quote_status':best.get('quote_status'),
                    'settlement':settle_spec(best['spec'],result_by_id.get(fid) or {}), 'captured_at':as_of})
        comparisons.append({'fixture_id':fid,'date':day,'baseline':goal_market(base_best) if base_best else None,
                            'upgrade':goal_market(new_best) if new_best else None,'whitelisted':allowed})
    daily = []
    for day in DATES:
        available = sum(c['date']==day for c in comparisons)
        daily.append({'date':day,'fixtures':sum(f['date']==day for f in expected.values()),'replayable_v5':available,
            'unavailable_v5':sum(r['date']==day for r in unavailable),
            **{v:metrics([p for p in choices[v] if p['date']==day]) if available else None for v in ('v6_published','v6','v6.1')}})
    return {'audit':'gold-goals-v6.1','dates':DATES,'source_fixture_count':len(expected),'daily':daily,
        'totals':{v:metrics(choices[v]) for v in ('v6_published','v6','v6.1')},
        'integrity':{'checked_input_hashes':hash_checks,'checked_probabilities':probability_checks,'mismatches':0},
        'cache':{'fixtures_recovered':len(recovered),'serie_b_recovered':len(recovered_b),
                 'proposals_from_cache':len(recovered_proposal),'fixture_ids':sorted(recovered),'serie_b_fixture_ids':sorted(recovered_b)},
        'combo':{'active':len(combos),'wins':sum(c['settlement']==(1.,0.) for c in combos),
                 'losses':sum(c['settlement']==(0.,1.) for c in combos),'roi':None,'reason':'no_observed_combo_price'},
        'exclusion_reasons':dict(exclusion_reasons),'choices':dict(choices),'comparisons':comparisons,
        'unavailable':unavailable,'notes':[
            'One latest authentic prematch snapshot per fixture. Same frozen V5 inputs for both policies; no tuning after outcomes.',
            'V3 dates are coverage-only: original V5 histories and sensitivity scenarios were not archived.',
            'Flat 1-unit paper ROI, net of lost stakes, without unobserved fees, tax or slippage. No bets placed.',
            'Missing prices for Over 2.5 HT and Combo remain missing; no implied or fair price substitutes for an offered price.',
            'Replayed v6 is a counterfactual on the same latest snapshots, not a claim every replayed pick was published.',
            'These dates were used to design v6.1: in-sample diagnostic, not independent validation.']}


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--export',action='append',required=True)
    parser.add_argument('--manifest',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();result=run(args.export,json.loads(Path(args.manifest).read_text()))
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('choices','comparisons','unavailable')},ensure_ascii=False,indent=2))
