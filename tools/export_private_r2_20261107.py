"""Exact read-only R2 checkpoint export, one copy per verified object."""
import sys,os,json,hashlib,zipfile,csv,io
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
sys.path.insert(0,'src')
from gold_storage import client,allowed
s3=client();bucket=os.environ['R2_BUCKET']
raw=s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read()
manifest=json.loads(raw);assert manifest['version']==1
files=manifest['files'];objects={}
for path,item in files.items():
 assert allowed(path)
 h=item['sha256'];assert len(h)==64 and all(c in '0123456789abcdef' for c in h)
 if h in objects:assert objects[h]==item['bytes']
 objects[h]=item['bytes']
root=Path('export-objects');root.mkdir();out=Path('export-out');out.mkdir()
print('Files:',len(files),'Unique objects:',len(objects),'Unique bytes:',sum(objects.values()),flush=True)
def fetch(pair):
 h,size=pair;body=s3.get_object(Bucket=bucket,Key='state/objects/'+h)['Body'].read()
 assert len(body)==size and hashlib.sha256(body).hexdigest()==h
 (root/h).write_bytes(body)
with ThreadPoolExecutor(max_workers=12) as pool:
 tasks=[pool.submit(fetch,x) for x in objects.items()]
 for n,t in enumerate(as_completed(tasks),1):
  t.result()
  if n%1000==0:print('Verified unique objects:',n,flush=True)
with zipfile.ZipFile(out/'ArabSniper_Archivio_R2.zip','w',allowZip64=True) as z:
 z.writestr('CHECKPOINT_MANIFEST.json',raw,compress_type=zipfile.ZIP_DEFLATED)
 z.writestr('EXPORT_METADATA.json',json.dumps({'created_at':datetime.now(timezone.utc).isoformat(),'manifest_sha256':hashlib.sha256(raw).hexdigest(),'files':len(files),'unique_objects':len(objects),'logical_documents':sum('/_audit_parts/' not in p for p in files),'scope':'Every file referenced by the fixed current private checkpoint, byte-for-byte verified. Unreferenced old R2 versions and other prefixes excluded. No results recalculated or certified.'},indent=2))
 buf=io.StringIO();w=csv.writer(buf);w.writerow(['path','stored_bytes','sha256'])
 w.writerows((p,x['bytes'],x['sha256']) for p,x in sorted(files.items()));z.writestr('INDICE_FILE.csv',buf.getvalue(),compress_type=zipfile.ZIP_DEFLATED)
 z.write('tools/read_export_20261107.py','LEGGI_ARCHIVIO.py')
 z.write('tools/export_readme_20261107.txt','LEGGIMI.txt')
 for h in sorted(objects):z.write(root/h,'objects/'+h,compress_type=zipfile.ZIP_STORED)
print('Exact export complete. Archive bytes:',(out/'ArabSniper_Archivio_R2.zip').stat().st_size,flush=True)
