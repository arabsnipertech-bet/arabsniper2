"""Restore original JSON values from the structurally deduplicated private archive."""
import sqlite3,zlib,json,sys,hashlib
from pathlib import Path
db=sqlite3.connect('file:'+str(Path(sys.argv[1]).resolve())+'?mode=ro',uri=True)
out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=True)
def write_node(h,f):
 row=db.execute('select kind,payload from nodes where hash=?',(h,)).fetchone()
 if row is None:raise ValueError('Missing node')
 kind,payload=row;raw=zlib.decompress(payload)
 assert hashlib.sha256(kind.encode()+b'\0'+raw).hexdigest()==h
 if kind in ('text','json'):f.write(raw);return
 refs=json.loads(raw);f.write(b'{' if kind=='object' else b'[')
 for i,r in enumerate(refs):
  if i:f.write(b',')
  if kind=='object':
   key,child=r;f.write(json.dumps(key,ensure_ascii=False).encode()+b':')
  else:child=r
  write_node(child,f)
 f.write(b'}' if kind=='object' else b']')
for path,h in db.execute('select path,root_hash from documents').fetchall():
 target=(out/path).resolve()
 if not target.is_relative_to(out.resolve()):raise ValueError('Invalid path')
 target.parent.mkdir(parents=True,exist_ok=True)
 with target.open('wb') as f:write_node(h,f)
 print(path)
