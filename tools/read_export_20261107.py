"""Read exact checkpoint files without credentials. Python 3 standard library only."""
import argparse,zipfile,json,hashlib,gzip,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('archive');p.add_argument('--list',action='store_true');p.add_argument('--document');p.add_argument('--output');p.add_argument('--restore')
a=p.parse_args();z=zipfile.ZipFile(a.archive);m=json.loads(z.read('CHECKPOINT_MANIFEST.json'))['files']
def raw(path):
 item=m[path];b=z.read('objects/'+item['sha256'])
 if len(b)!=item['bytes'] or hashlib.sha256(b).hexdigest()!=item['sha256']:raise ValueError('Checksum mismatch')
 return b
if a.list:
 for path in sorted(m):
  if '/_audit_parts/' not in path:print(path)
elif a.document:
 b=raw(a.document);v=json.loads(b) if a.document.endswith('.json') and len(b)<1048576 else None
 output=Path(a.output or Path(a.document).name)
 with output.open('wb') as f:
  if isinstance(v,dict) and v.get('_storage')=='arabsniper-audit-parts-v1':
   checksum=hashlib.sha256();size=0
   for part in v['parts']:
    chunk=(Path(a.document).parent/'_audit_parts'/(part['sha256']+'.json.gz')).as_posix()
    data=gzip.decompress(raw(chunk))
    if len(data)!=part['bytes'] or hashlib.sha256(data).hexdigest()!=part['sha256']:raise ValueError('Part checksum mismatch')
    f.write(data);checksum.update(data);size+=len(data)
   if size!=v['bytes'] or checksum.hexdigest()!=v['sha256']:raise ValueError('Document checksum mismatch')
  else:f.write(b)
 print(str(output))
elif a.restore:
 dest=Path(a.restore).resolve()
 for path in m:
  target=(dest/path).resolve()
  if not target.is_relative_to(dest):raise ValueError('Invalid path')
  target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw(path))
 print('Original files restored:',len(m))
else:p.print_help()
