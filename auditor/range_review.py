"""Read-only range review of frozen daily ledgers; never changes forecasts."""
import argparse
from pathlib import Path
import sys,json,hashlib
from collections import Counter
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import read_store

def metrics(rows):
    settled=[r for r in rows if r['outcome'] is not None]
    priced=[r for r in settled if r['paper_profit'] is not None]
    wins=sum(r['outcome'] for r in settled)
    return dict(n=len(settled),wins=wins,pending=len(rows)-len(settled),priced=len(priced),
                hit_pct=round(100*wins/len(settled),2) if settled else None,
                paper_roi_pct=round(100*sum(r['paper_profit'] for r in priced)/len(priced),2) if priced else None)

def membership(r):
    p,b=r['statistical_probability'],r['book_probability_pct']
    if p is None or b is None:return 'missing'
    p*=100
    if p>50 and b>50:return 'gold' if abs(p-b)<=10+1e-9 else 'both_above50_gap_over10'
    if p<=50 and b>50:return 'only_book_above50'
    if p>50 and b<=50:return 'only_statistics_above50'
    return 'both_at_or_below50'

def near(r):
    p,b=r['statistical_probability'],r['book_probability_pct']
    return p is not None and b is not None and membership(r)!='gold' and p*100>=47 and b>=47 and abs(p*100-b)<=13

def main():
    parser=argparse.ArgumentParser();parser.add_argument('dates',nargs='+');parser.add_argument('--output',required=True);args=parser.parse_args()
    root=Path(__file__).resolve().parents[1];days={};sources={};features={}
    for date in args.dates:
        data=read_store(root/f'auditarchive/prematch_{date}_daily.json',{})
        sources[date]={'generated_at':data['generated_at'],'source_revision':data['source_revision'],
                       'sha256':hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()}
        days[date]=[r for r in data['markets'] if r['snapshot']=='latest' and r['model_id']=='context-goals-v3.4' and r['period']=='prospective' and r['candidate_present']]
    result={'note':'Exploratory ranges inspected after BOTH days. Not held-out validation or optimized thresholds. Latest prematch only; markets/scopes overlap and must not be added. Missing quotes excluded from ROI. Gold uses unrounded statistical probability.', 'sources':sources,'markets':{}}
    allrows=[r for rs in days.values() for r in rs]
    for market in ['over25_ft','over15_ht','over35_ft']:
        groups={};example=[]
        selectors={'all':lambda r:True,'primary':lambda r:r['primary'],'not_primary':lambda r:not r['primary'],
                   'near_gold_excluded':near}
        for group in ['gold','both_above50_gap_over10','only_book_above50','only_statistics_above50','both_at_or_below50','missing']:
            selectors[group]=lambda r,g=group:membership(r)==g
        for field in ['statistical_probability','book_probability_pct']:
            for low,high in [(0,40),(40,50),(50,60),(60,70),(70,101)]:
                selectors[f'{field}_{low}_{high}']=lambda r,f=field,l=low,h=high:r[f] is not None and l<=r[f]*(100 if f=='statistical_probability' else 1)<h
        for low,high in [(0,5),(5,10),(10,20),(20,101)]:
            selectors[f'absolute_gap_{low}_{high}']=lambda r,l=low,h=high:r['statistical_probability'] is not None and r['book_probability_pct'] is not None and l<=abs(100*r['statistical_probability']-r['book_probability_pct'])<h
        # Wide two-dimensional bins, including every unsuccessful case.
        for lo in [0,40,50,60,70]:
            hi={0:40,40:50,50:60,60:70,70:101}[lo]
            for bl in [0,40,50,60,70]:
                bh={0:40,40:50,50:60,60:70,70:101}[bl]
                selectors[f'grid_stat{lo}_{hi}_book{bl}_{bh}']=lambda r,l=lo,h=hi,b=bl,c=bh:r['statistical_probability'] is not None and r['book_probability_pct'] is not None and l<=r['statistical_probability']*100<h and b<=r['book_probability_pct']<c
        flags={'peak_sensitive':'Media sensibile','declining_form':'Produzione recente in contrasto','favorite_conflict':'Forte divergenza sulla favorita','fragile_ht':'Stima PT fragile'}
        for flag,phrase in flags.items():
            selectors[flag]=lambda r,t=phrase:any(t in c for c in r['cons'])
            selectors['without_'+flag]=lambda r,t=phrase:not any(t in c for c in r['cons'])
        for name,predicate in selectors.items():
            groups[name]={date:metrics([r for r in rs if r['market']==market and predicate(r)]) for date,rs in days.items()}
            groups[name]['combined']=metrics([r for r in allrows if r['market']==market and predicate(r)])
        for date,rs in days.items():
            for r in rs:
                if r['market']==market and near(r):
                    example.append(dict(date=date,fixture_id=r['fixture_id'],match=r['fixture'].get('match'),statistics=round(100*r['statistical_probability'],3),book=r['book_probability_pct'],outcome=r['outcome'],primary=r['primary'],pros=r['pros'],cons=r['cons'],missing=r['missing']))
        result['markets'][market]={'groups':groups,'near_gold_cases_including_losses':example}
    target=root/args.output;target.parent.mkdir(exist_ok=True,parents=True);target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({m:{k:v['groups'][k] for k in ['all','gold','near_gold_excluded','peak_sensitive','without_peak_sensitive','favorite_conflict','without_favorite_conflict']} for m,v in result['markets'].items()},ensure_ascii=False))
if __name__=='__main__':main()
