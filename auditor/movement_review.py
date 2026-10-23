"""Read-only association review using the exact frozen prematch source rows."""
import json,sys,hashlib
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from range_review import metrics,membership,near
from audit_storage import read_store
ROOT=Path(__file__).resolve().parents[1]
def signal(row):
    m=(row or {}).get('movement') or {}; n=m.get('paired_books',0);a=m.get('agreeing_books',0)
    confirmed=m.get('confirmed') is True and a>=2 and a>n/2
    amp=m.get('amplitude_pct');amp=abs(amp) if isinstance(amp,(int,float)) else None
    color='unknown' if not n else 'unconfirmed' if not confirmed else 'unknown_intensity' if amp is None else 'green' if amp<3 else 'yellow' if amp<8 else 'red'
    return dict(color=color,direction=m.get('direction') if confirmed else None,amplitude_pct=amp,paired_books=n,agreeing_books=a,reversal=m.get('has_direction_reversal'),fair_coherent=m.get('fair_direction_coherent'))
def main():
    rows=[];sources={}
    for day in ['2026-10-21','2026-10-22']:
        daily=read_store(ROOT/f'auditarchive/prematch_{day}_daily.json',{});archive=read_store(ROOT/f'auditarchive/prematch_{day}_results.json',{})
        sources[day]={'source_revision':daily['source_revision'],'daily_sha256':hashlib.sha256(json.dumps(daily,sort_keys=True).encode()).hexdigest()}
        for r in daily['markets']:
            if not (r['snapshot']=='latest' and r['model_id']=='context-goals-v3.4' and r['period']=='prospective' and r['candidate_present'] and r['market'] in ['over25_ft','over15_ht']):continue
            record=archive[r['source_collection']][r['source_key']]
            assert record['captured_at']==r['captured_at']
            assert datetime.fromisoformat(r['captured_at'])<datetime.fromisoformat(r['fixture']['kickoff'])
            markets=record['multimarket']['markets']; indexed={(m['market'],m['selection']):m for m in markets}
            sides=[indexed.get(('1x2_ft',s),{}) for s in ['1','x','2']]
            favorite=max(sides,key=lambda m:m.get('book_probability_pct') or -1) if all(m.get('book_probability_pct') is not None for m in sides) else {}
            signals={'own':signal(indexed[(r['market'],r['selection'])]),'over25':signal(indexed.get(('over25_ft','over'))),'over15ht':signal(indexed.get(('over15_ht','over'))),'favorite_1x2':signal(favorite)}
            signals.update({s:signal(indexed.get(('1x2_ft',s))) for s in ['1','x','2']})
            rows.append(dict(date=day,fixture_id=r['fixture_id'],match=r['fixture']['match'],market=r['market'],captured_at=r['captured_at'],source_key=r['source_key'],forecast_sha256=r['forecast_sha256'],outcome=r['outcome'],paper_profit=r['paper_profit'],odd=r['odd'],statistical_probability=r['statistical_probability'],book_probability_pct=r['book_probability_pct'],membership=membership(r),near_gold=near(r),primary=r['primary'],signals=signals))
    groups={}
    for market in ['over25_ft','over15_ht']:
        rs=[r for r in rows if r['market']==market];selectors={'all':lambda r:True}
        for scope in ['own','over25','over15ht','favorite_1x2','1','x','2']:
            for color in ['unknown','unconfirmed','unknown_intensity','green','yellow','red']:
                selectors[f'{scope}:{color}']=lambda r,s=scope,c=color:r['signals'][s]['color']==c
            for direction in ['up','down']:
                for colors,label in [(['yellow'],'yellow'),(['red'],'red'),(['yellow','red'],'yellow_red')]:
                    selectors[f'{scope}:{label}:{direction}']=lambda r,s=scope,cs=colors,d=direction:r['signals'][s]['color'] in cs and r['signals'][s]['direction']==d
            selectors[f'{scope}:yellow_red']=lambda r,s=scope:r['signals'][s]['color'] in ['yellow','red']
        selectors['any_1x2:yellow_red']=lambda r:any(r['signals'][s]['color'] in ['yellow','red'] for s in ['1','x','2'])
        selectors['no_1x2:yellow_red']=lambda r:not any(r['signals'][s]['color'] in ['yellow','red'] for s in ['1','x','2'])
        for scope in ['own','over25','over15ht','favorite_1x2']:
            for group in ['gold','non_gold','near_gold']:
                selectors[f'{scope}:yellow_red:{group}']=lambda r,s=scope,g=group:r['signals'][s]['color'] in ['yellow','red'] and (r['membership']=='gold' if g=='gold' else r['membership']!='gold' if g=='non_gold' else r['near_gold'])
        groups[market]={name:{**{d:metrics([r for r in rs if r['date']==d and pred(r)]) for d in sources},'combined':metrics([r for r in rs if pred(r)])} for name,pred in selectors.items()}
    out={'method':'Latest frozen prospective v3.4, exact source collection/key, prematch timestamp assertions. UI thresholds: confirmed >=2 and majority; yellow 3<=abs(delta)<8; red >=8. Last directional leg, not necessarily net opening-to-close. All market rows, no outcome selection. Exploratory overlapping comparisons, no independent validation. Unknown/unconfirmed are not stable prices.','sources':sources,'groups':groups,'rows':rows}
    target=ROOT/'docs/research/movement-review-2026-10-21-22.json';target.write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
    for market,gs in groups.items():
        print('\n',market)
        for name,v in gs.items():
            if (name.startswith(('own:','favorite_1x2:','any_1x2','no_1x2','over25:yellow','over15ht:yellow')) or name=='all') and v['combined']['n']:
                print(name,' | '.join(f"{d}: {m['wins']}/{m['n']} ROI {m['paper_roi_pct']}" for d,m in v.items()))
if __name__=='__main__':main()
