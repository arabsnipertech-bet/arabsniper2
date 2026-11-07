"""One-off read-only checkpoint export. Never writes to R2."""
import sys,os,json,hashlib,zipfile,csv,shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
sys.path.insert(0,'src')
from gold_storage import client,allowed
from audit_storage import open_verified_json
s3=client(); bucket=os.environ['R2_BUCKET']
raw=s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read()
manifest=json.loads(raw); assert manifest['version']==1
files=manifest['files']; root=Path('export-work');root.mkdir()
for path,item in files.items():
 if not allowed(path):raise ValueError('Unexpected checkpoint path')
 if len(item['sha256'])!=64 or any(c not in '0123456789abcdef' for c in item['sha256']):raise ValueError('Invalid digest')
def fetch(pair):
 path,item=pair
 body=s3.get_object(Bucket=bucket,Key='state/objects/'+item['sha256'])['Body'].read()
 if len(body)!=item['bytes'] or hashlib.sha256(body).hexdigest()!=item['sha256']:raise ValueError('Integrity check failed')
 p=root/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
 return len(body)
with ThreadPoolExecutor(max_workers=12) as pool:
 for n,_ in enumerate(pool.map(fetch,files.items()),1):
  if n%1000==0:print('Verified objects:',n,flush=True)
out=Path('export-out');out.mkdir()
meta={'exported_at':datetime.now(timezone.utc).isoformat(),'source_commit':os.environ['GITHUB_SHA'],'manifest_sha256':hashlib.sha256(raw).hexdigest(),'files':len(files),'bytes':sum(x['bytes'] for x in files.values()),'scope':'All files referenced by the fixed current R2 checkpoint. Unreferenced old object versions and unrelated bucket prefixes are not included. No outcomes recalculated.'}
(root/'EXPORT_METADATA.json').write_text(json.dumps(meta,indent=2))
(root/'CHECKPOINT_MANIFEST.json').write_bytes(raw)
with open(root/'FILE_INVENTORY.csv','w') as f:
 w=csv.writer(f);w.writerow(['path','bytes','sha256']);w.writerows((p,x['bytes'],x['sha256']) for p,x in sorted(files.items()))
(root/'LEGGIMI.txt').write_text("Archivio integrale della fotografia corrente R2. Tutti i file elencati nel manifest sono verificati SHA256. I file multipart sono conservati senza perdita; la cartella documenti_leggibili contiene anche JSON ricomposti per audit e partite. Comprende dati non ancora certificati e partite future: controllare data, captured_at, kickoff e stato esito. Non attribuire una categoria mancante. Versioni orfane di oggetti R2 e altri prefissi del bucket non fanno parte di questo checkpoint. Nessuna modifica al modello o all'archivio originale.\n")
with zipfile.ZipFile(out/'ArabSniper_Archivio_R2.zip','w',zipfile.ZIP_DEFLATED,compresslevel=3,allowZip64=True) as z:
 for p in root.rglob('*'):
  if p.is_file():z.write(p,'originali/'+p.relative_to(root).as_posix())
 for path in files:
  if path.endswith('.json') and '/_audit_parts/' not in path:
   p=root/path
   if p.stat().st_size<1024*1024:
    v=json.loads(p.read_bytes())
    if isinstance(v,dict) and v.get('_storage')=='arabsniper-audit-parts-v1':
     with open_verified_json(p) as src,z.open('documenti_leggibili/'+path,'w',force_zip64=True) as dest:shutil.copyfileobj(src,dest,1024*1024)
print('Export verified. Files:',len(files),'Archive bytes:',(out/'ArabSniper_Archivio_R2.zip').stat().st_size,flush=True)
