"""Exploratory joint movements; all thresholds fixed to existing UI colors."""
import json
from pathlib import Path
from statistics import median
from datetime import datetime
from movement_review import ROOT,signal
from range_review import metrics,membership
from audit_storage import read_store

def state(s):
    return s['color']+'_'+str(s['direction'])
def main():
    rows=[]
    for day in ['2026-10-21','2026-10-22']:
        daily=read_store(ROOT/f'auditarchive/prematch_{day}_daily.json',{})
        archive=read_store(ROOT/f'auditarchive/prematch_{day}_results.json',{})
        for r in daily['markets']:
            if not(r['snapshot']=='latest' and r['model_id']=='context-goals-v3.4' and r['period']=='prospective' and r['candidate_present'] and r['market'] in ['over15_ht','over25_ft','over25_ht']):continue
            record=archive[r['source_collection']][r['source_key']]
            assert record['captured_at']==r['captured_at']
            assert datetime.fromisoformat(r['captured_at'])<datetime.fromisoformat(r['fixture']['kickoff'])
            ms={(m['market'],m['selection']):m for m in record['multimarket']['markets']}
            ss={s:signal(ms.get(('1x2_ft',s))) for s in ['1','x','2']}
            opening={}
            for s in ss:
                ops=(ms.get(('1x2_ft',s),{}).get('movement') or {}).get('operators',[])
                ps=[o.get('first_observed_fair_pct',o.get('fair_initial_pct')) for o in ops if o.get('first_observed_fair_pct',o.get('fair_initial_pct')) is not None]
                opening[s]=median(ps) if ps else None
            # Descriptive first-observed favorite; operator windows may differ.
            favorite=max(opening,key=opening.get) if all(v is not None for v in opening.values()) else None
            current={s:ms.get(('1x2_ft',s),{}).get('book_probability_pct') for s in ss}
            cf=max(current,key=current.get) if all(v is not None for v in current.values()) else None
            for market in ['over15_ht','over25_ft','over25_ht']:ss[market]=signal(ms.get((market,'over')))
            rows.append(dict(date=day,fixture_id=r['fixture_id'],match=r['fixture']['match'],market=r['market'],outcome=r['outcome'],paper_profit=r['paper_profit'],odd=r['odd'],gold=membership(r)=='gold',stat=r['statistical_probability'],book=r['book_probability_pct'],signals=ss,first_observed_favorite=favorite,first_observed_fair=opening,current_favorite=cf,source_key=r['source_key'],forecast_sha256=r['forecast_sha256']))
    groups=[]
    for target in ['over15_ht','over25_ft','over25_ht']:
        rs=[r for r in rows if r['market']==target]
        for side in ['1','x','2','first_favorite','current_favorite']:
            for d in ['up','down']:
                for market in ['over15_ht','over25_ft','over25_ht']:
                    for od in ['up','down']:
                        for colors,label in [(['yellow','red'],'yellow_red'),(['yellow'],'yellow'),(['red'],'red')]:
                            chosen=[]
                            for r in rs:
                                s=r['first_observed_favorite'] if side=='first_favorite' else r['current_favorite'] if side=='current_favorite' else side
                                if not s:continue
                                a=r['signals'][s];b=r['signals'][market]
                                if a['color'] in colors and a['direction']==d and b['color'] in ['yellow','red'] and b['direction']==od:chosen.append(r)
                            if not chosen:continue
                            groups.append(dict(target=target,side=side,side_colors=label,side_direction=d,over_signal=market,over_direction=od,combined=metrics(chosen),days={day:metrics([r for r in chosen if r['date']==day]) for day in ['2026-10-21','2026-10-22']},gold=metrics([r for r in chosen if r['gold']]),no_reversal=metrics([r for r in chosen if r['signals'][r['first_observed_favorite'] if side=='first_favorite' else r['current_favorite'] if side=='current_favorite' else side]['reversal'] is False and r['signals'][market]['reversal'] is False]),stat_book_cells={f'{lo}/{bl}':metrics([r for r in chosen if r['stat'] is not None and r['book'] is not None and lo<=r['stat']*100<lo+10 and bl<=r['book']<bl+10]) for lo in range(0,100,10) for bl in range(0,100,10) if any(r['stat'] is not None and r['book'] is not None and lo<=r['stat']*100<lo+10 and bl<=r['book']<bl+10 for r in chosen)},cases=[(r['date'],r['fixture_id'],r['match'],r['outcome']) for r in chosen]))
    out={'note':'Exploratory overlapping comparisons; no independent validation. First-observed favorite uses median operator initial fair probabilities, not certified opening and not necessarily aligned windows. Strong means existing confirmed yellow/red; no optimized cutoffs.','rows':rows,'groups':groups}
    (ROOT/'docs/research/joint-movement-review-2026-10-21-22.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
    for target in ['over15_ht','over25_ft','over25_ht']:
        print('\nTARGET',target,metrics([r for r in rows if r['market']==target]))
        gs=[g for g in groups if g['target']==target and g['side_colors']=='yellow_red' and g['over_signal']==target]
        for g in sorted(gs,key=lambda g:-(g['combined']['n'])):
            print(g['side'],g['side_direction'],g['over_direction'],g['combined'],g['days'],'gold',g['gold'])
if __name__=='__main__':main()
