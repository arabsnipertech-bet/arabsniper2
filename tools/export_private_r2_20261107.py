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

import sqlite3,zlib,ijson,simplejson
from ijson.common import ObjectBuilder
out=Path('export-out');out.mkdir()
db=sqlite3.connect(out/'ArabSniper_Archivio_R2.sqlite')
db.execute('pragma journal_mode=off');db.execute('pragma synchronous=off')
db.executescript('create table nodes(hash text primary key,kind text,payload blob); create table documents(path text primary key,root_hash text,source_sha256 text); create table metadata(key text primary key,value text);')
def save(kind,payload):
 encoded=payload.encode() if isinstance(payload,str) else payload
 h=hashlib.sha256(kind.encode()+b'\0'+encoded).hexdigest()
 db.execute('insert or ignore into nodes values(?,?,?)',(h,kind,zlib.compress(encoded,6)))
 return h
def build(ev,first,depth=0):
 event,value=first
 if event in ('start_map','start_array') and depth<4:
  rows=[]
  if event=='start_map':
   while True:
    e,v=next(ev)
    if e=='end_map':break
    if e!='map_key':raise ValueError('Expected key')
    rows.append([v,build(ev,next(ev),depth+1)])
   kind='object'
  else:
   while True:
    e,v=next(ev)
    if e=='end_array':break
    rows.append(build(ev,(e,v),depth+1))
   kind='array'
  return save(kind,simplejson.dumps(rows,ensure_ascii=False,separators=(',',':')))
 b=ObjectBuilder();b.event(event,value);n=int(event in ('start_map','start_array'))
 while n:
  e,v=next(ev);b.event(e,v);n+=int(e in ('start_map','start_array'))-int(e in ('end_map','end_array'))
 return save('json',simplejson.dumps(b.value,use_decimal=True,ensure_ascii=False,separators=(',',':')))
meta={'scope':'All logical JSON and Markdown documents referenced by the current private checkpoint; multipart JSON decoded and all JSON values retained with lossless structural deduplication. Old unreferenced R2 object versions and unrelated bucket prefixes excluded. Not an audit certification.','created_at':datetime.now(timezone.utc).isoformat(),'manifest_sha256':hashlib.sha256(raw).hexdigest(),'files':len(files)}
db.execute('insert into metadata values(?,?)',('export',json.dumps(meta)))
db.execute('insert into metadata values(?,?)',('checkpoint_manifest',raw.decode()))
n=0
for path,item in sorted(files.items()):
 if '/_audit_parts/' in path:continue
 p=root/path
 if path.endswith('.json'):
  with open_verified_json(p) as src:
   ev=iter(ijson.basic_parse(src));h=build(ev,next(ev))
   if next(ev,None) is not None:raise ValueError('Trailing data')
 else:h=save('text',p.read_bytes())
 db.execute('insert into documents values(?,?,?)',(path,h,item['sha256']))
 db.commit();n+=1
 print('Logical documents preserved:',n,'Database bytes:',(out/'ArabSniper_Archivio_R2.sqlite').stat().st_size,flush=True)
db.execute('insert into metadata values(?,?)',('document_count',str(n)))
db.commit()
assert db.execute('pragma integrity_check').fetchone()[0]=='ok'
print('Export complete. Documents:',n,'Unique nodes:',db.execute('select count(*) from nodes').fetchone()[0],flush=True)
db.close()
with zipfile.ZipFile(out/'ArabSniper_Archivio_R2.zip','w',zipfile.ZIP_DEFLATED,compresslevel=3,allowZip64=True) as z:
 z.write(out/'ArabSniper_Archivio_R2.sqlite','ArabSniper_Archivio_R2.sqlite')
 z.write('tools/read_export_20261107.py','LEGGI_ARCHIVIO.py')
 z.writestr('LEGGIMI.txt','Archivio completo dei documenti logici del checkpoint privato R2. Tutti i dati JSON sono preservati; ripetizioni identiche sono archiviate una sola volta e richiamate tramite hash. Per ricostruire tutti i JSON: python LEGGI_ARCHIVIO.py ArabSniper_Archivio_R2.sqlite cartella_output. Non servono API o credenziali. Include dati e risultati non certificati: verificare i rispettivi campi. La presenza nello storico non implica selezione Gold/Platinum. Dati mancanti non sono inventati. Vecchie versioni orfane R2 non incluse.')
print('Final archive bytes:',(out/'ArabSniper_Archivio_R2.zip').stat().st_size,flush=True)
