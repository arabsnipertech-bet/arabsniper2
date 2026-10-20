"""Lossless, checksummed audit storage below GitHub's per-file limit.

Small/legacy JSON remains readable. Large JSON is an atomic manifest referencing
immutable, compressed 4 MiB byte chunks. No forecast or old chunk is deleted.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
from copy import deepcopy

FORMAT='arabsniper-audit-parts-v1'
CHUNK_BYTES=4*1024*1024


def digest(data): return hashlib.sha256(data).hexdigest()


def atomic_bytes(path, data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_bytes(data);os.replace(temporary,path)


def read_store(path, default=None):
    path=Path(path)
    if not path.exists():return deepcopy(default)
    value=json.loads(path.read_bytes())
    if not isinstance(value,dict) or value.get('_storage')!=FORMAT:return value
    chunks=[]
    for part in value['parts']:
        name=part['sha256']
        if len(name)!=64 or any(c not in '0123456789abcdef' for c in name):
            raise ValueError('Invalid audit part identifier')
        raw=gzip.decompress((path.parent/'_audit_parts'/(name+'.json.gz')).read_bytes())
        if len(raw)!=part['bytes'] or digest(raw)!=name:raise ValueError('Audit part checksum mismatch')
        chunks.append(raw)
    raw=b''.join(chunks)
    if len(raw)!=value['bytes'] or digest(raw)!=value['sha256']:
        raise ValueError('Audit manifest checksum mismatch')
    return json.loads(raw)


def write_store(path, value, chunk_bytes=CHUNK_BYTES):
    path=Path(path)
    if chunk_bytes<1:raise ValueError('Positive chunk size required')
    raw=json.dumps(value,ensure_ascii=False,separators=(',',':')).encode('utf-8')
    if len(raw)<=chunk_bytes:
        atomic_bytes(path,raw+b'\n');return
    parts=[]
    for offset in range(0,len(raw),chunk_bytes):
        part=raw[offset:offset+chunk_bytes];sha=digest(part)
        target=path.parent/'_audit_parts'/(sha+'.json.gz')
        encoded=gzip.compress(part,compresslevel=6,mtime=0)
        if not target.exists() or target.read_bytes()!=encoded:atomic_bytes(target,encoded)
        parts.append({'sha256':sha,'bytes':len(part)})
    manifest={'_storage':FORMAT,'bytes':len(raw),'sha256':digest(raw),'parts':parts}
    # All parts exist before the index is switched. Interrupted writes keep old index valid.
    atomic_bytes(path,json.dumps(manifest,separators=(',',':')).encode()+b'\n')
