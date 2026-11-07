import sys,os,json
sys.path.insert(0,'src')
from gold_storage import client
s=client();m=json.loads(s.get_object(Bucket=os.environ['R2_BUCKET'],Key='state/current.json')['Body'].read());f=m['files']
docs=[(p,v) for p,v in sorted(f.items()) if '/_audit_parts/' not in p]
print('Logical document count:',len(docs))
print('Stored checkpoint bytes:',sum(v['bytes'] for v in f.values()))
for i,(p,v) in enumerate(docs,1):
 size=v['bytes']
 if p.endswith('.json') and size<1048576:
  d=json.loads(s.get_object(Bucket=os.environ['R2_BUCKET'],Key='state/objects/'+v['sha256'])['Body'].read())
  if isinstance(d,dict) and d.get('_storage')=='arabsniper-audit-parts-v1':size=d['bytes']
 if i>=95:print('Document position:',i,'Decoded bytes:',size,flush=True)
