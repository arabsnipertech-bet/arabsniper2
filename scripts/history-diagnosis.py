"""Bounded, read-only diagnosis of the complete October/November archive."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib,json,os,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from gold_storage import client, previous_manifest, read_checkpoint_object

START,END='2026-10-01','2026-11-25'

class ReadOnly:
    def __init__(self,storage):self.storage,self.gets,self.bytes=storage,0,0
    def get_object(self,**args):
        key=args['Key']
        if args['Bucket']!='arabsniper-gold' or not (key=='state/current.json' or re.fullmatch(r'state/objects/[a-f0-9]{64}',key)):
            raise ValueError('Read scope')
        self.gets+=1
        if self.gets>1800:raise ValueError('Read budget')
        result=self.storage.get_object(**args);self.bytes+=result.get('ContentLength',0)
        if self.bytes>700*1024*1024:raise ValueError('Byte budget')
        return result

def day_for(path):
    m=re.search(r'2026-(?:10|11)-\d\d',path)
    return m.group() if m and START<=m.group()<=END else None

def inventory(storage,bucket,index):
    chosen={p:i for p,i in index.items() if day_for(p) and re.fullmatch(r'(?:auditarchive|auditordata)/prematch_[^/]+\.(?:json|md)',p)}
    report=dict(version=1,cutoff=datetime.now(timezone.utc).isoformat(),start=START,end=END,
        manifest_files=len(index),manifest_bytes=sum(i['bytes'] for i in index.values()),files=chosen,
        families=dict(Counter(re.sub(r'2026-\d\d-\d\d','DATE',p) for p in chosen)),
        examples=[],remote_writes=0,provider_calls=0)
    # A small sample establishes schema and compressed size before bulk reads.
    for suffix in ('daily.json','results.json','v7.json'):
        candidates=sorted(p for p in chosen if p.endswith('_'+suffix))
        dates={day_for(p) for p in candidates}
        examples=[]
        for requested in ('2026-10-18','2026-10-21','2026-10-31','2026-11-08','2026-11-16'):
            match=next((p for p in candidates if day_for(p)==requested),None)
            if match:examples.append(match)
        if not examples and candidates:examples=[candidates[0]]
        for path in examples:
            item=chosen[path]
            if item['bytes']>1024*1024:
                report['examples'].append(dict(path=path,bytes=item['bytes'],deferred_large=True));continue
            raw=read_checkpoint_object(storage,bucket,'state/objects/'+item['sha256'])
            if len(raw)!=item['bytes'] or hashlib.sha256(raw).hexdigest()!=item['sha256']:raise ValueError('Hash mismatch')
            v=json.loads(raw)
            example=dict(path=path,keys=list(v)[:30] if isinstance(v,dict) else ['list'],bytes=item['bytes'])
            if isinstance(v,dict) and v.get('_storage'):
                example.update(storage=v['_storage'],expanded_bytes=v['bytes'],parts=len(v['parts']),
                    compressed_bytes=sum(index.get(str(Path(path).parent/'_audit_parts'/(part['sha256']+'.json.gz')),{}).get('bytes',0) for part in v['parts']))
            elif isinstance(v,dict):
                example['counts']={k:len(x) for k,x in v.items() if isinstance(x,(list,dict))}
                for key in ('markets','fixtures','multimarket_evaluations','analysis_updates'):
                    x=v.get(key)
                    sample=next(iter(x.values()),None) if isinstance(x,dict) else x[0] if isinstance(x,list) and x else None
                    if isinstance(sample,dict):example[key+'_sample_keys']=list(sample)
            report['examples'].append(example)
    return report

def main():
    p=argparse.ArgumentParser();p.add_argument('--inventory',action='store_true');args=p.parse_args()
    if os.environ.get('API_SPORTS_KEY'):raise ValueError('Provider key forbidden')
    storage=ReadOnly(client(fast_restore=True));bucket=os.environ['R2_BUCKET']
    index=previous_manifest(storage,bucket)
    result=inventory(storage,bucket,index)
    result['reads']=dict(gets=storage.gets,bytes=storage.bytes)
    output=ROOT/'test-artifacts/history-inventory.json';output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps(result,ensure_ascii=False,separators=(',',':')))
    print(json.dumps(dict(status='inventory_complete',files=len(result['files']),gets=storage.gets,bytes=storage.bytes,provider_calls=0)))

if __name__=='__main__':main()
