"""Read-only archived quote identity diagnostic. Never calls a provider or writes R2."""
import json, sys, tempfile
from statistics import median
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from gold_storage import client, previous_manifest, restore_subset, GOLD
from audit_storage import read_store
class ReadOnly:
 def __init__(self): self.s=client();self.reads=0
 def get_object(self,**kw):
  assert kw['Bucket']=='arabsniper-gold'
  assert kw['Key']=='state/current.json' or kw['Key'].startswith('state/objects/')
  self.reads+=1
  return self.s.get_object(**kw)
def main():
 s=ReadOnly();index=previous_manifest(s,'arabsniper-gold')
 paths=[p for p in ['data/'+x for x in GOLD[:5]]+['data/arab_quote_state.json','auditarchive/prematch_2026-11-17_v7.json'] if p in index]
 result={'mode':'read-only stored data','remote_writes':0,'provider_calls':0,'files':{p:index[p] for p in paths},'matches':[],'ledger':{},'candidate_identity_conflicts':[]}
 ids=set()
 with tempfile.TemporaryDirectory() as tmp:
  root=Path(tmp);restore_subset(s,'arabsniper-gold',root,index,paths)
  for p in paths:
   if 'day' not in p:continue
   feed=read_store(root/p)
   for m in feed.get('matches',[]):
    meta=m.get('fixture') or {};names=str(meta.get('home'))+' '+str(meta.get('away'))
    if 'denmark' not in names.lower() or 'norway' not in names.lower():continue
    fid=str(meta.get('fixture_id'));ids.add(fid);matrix=(m.get('multimarket') or {}).get('matrix') or {}
    fields=['id','spec','label','odd','first_odd','opening_odd','bookmaker','bookmaker_id','bet_id','provider_updated_at','quote_observed_at','quote_status','quote_source','quote_provenance','statistical_pct','fair_odd','ev_pct','ev_low_pct','oci','selected_market_odds','cached_quote']
    rows=[{k:r.get(k) for k in fields} for r in matrix.get('rows',[]) if r.get('spec',{}).get('period')=='ft' and r.get('spec',{}).get('line') in [2.5,3.5] and r.get('spec',{}).get('kind') in ['total','asian_total']]
    result['matches'].append({'file':p,'fixture':meta,'matrix_as_of':matrix.get('as_of'),'rows':rows,'v7_captured_at':matrix.get('v7',{}).get('captured_at'),'v7_markets':{k:v for k,v in matrix.get('v7',{}).get('markets',{}).items() if v.get('source_market_id') in [r['id'] for r in rows]}})
  sp=root/'data/arab_quote_state.json'
  if sp.exists():
   state=read_store(sp);ledger=state.get('matrix_quotes',{});result['ledger_fixtures']=len(ledger)
   for fid in ids:
    entry=ledger.get(fid,{})
    result['ledger'][fid]={'kickoff':entry.get('kickoff'),'groups':{k:points for k,points in entry.get('groups',{}).items() if any(any(spec.get('period')=='ft' and spec.get('line') in [2.5,3.5] for spec in p.get('specs',{}).values()) for p in points)}}
   comparisons=[]
   for fid,entry in ledger.items():
    by_market={}
    for key,points in entry.get('groups',{}).items():
     if not points:continue
     last=points[-1]
     if not key.startswith('5|total|ft|') or last.get('quote_source','bookmaker')!='bookmaker':continue
     for token,spec in last.get('specs',{}).items():
      if spec.get('side')!='over' or spec.get('line') not in [2.5,3.5]:continue
      value=last.get('odds',{}).get(token)
      if not isinstance(value,(int,float)):continue
      mk=(spec['line'],last.get('provider_updated_at'))
      by_market.setdefault(mk,{})[key.split('|')[-1]]={'odd':value,'book':last.get('bookmaker')}
    for (line,at),books in by_market.items():
     if '3' not in books or len(books)<4:continue
     others=[v['odd'] for k,v in books.items() if k!='3'];mid=median(others)
     comparisons.append({'fixture':fid,'line':line,'provider_updated_at':at,'betfair':books['3']['odd'],'other_median':mid,'other_books':len(others),'ratio':round(books['3']['odd']/mid,4)})
   result['same_timestamp_comparisons']=len(comparisons)
   result['betfair_double_median_cases']=[c for c in comparisons if c['ratio']>=2]
   result['largest_discrepancies']=sorted(comparisons,key=lambda c:c['ratio'],reverse=True)[:15]
   # Within each retained group, specs must identify the same market over time.
   for fid,entry in ledger.items():
    for key,points in entry.get('groups',{}).items():
     specs={json.dumps(p['specs'],sort_keys=True) for p in points if p.get('specs')}
     if len(specs)>1:result['candidate_identity_conflicts'].append({'fixture':fid,'group':key,'identities':sorted(specs)})
 result['reads']=s.reads
 out=ROOT/'test-artifacts/quote-identity-audit.json';out.parent.mkdir(exist_ok=True);out.write_text(json.dumps(result,indent=2))
 print(json.dumps(result,indent=2))
if __name__=='__main__':main()
