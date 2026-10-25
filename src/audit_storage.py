"""Lossless, checksummed audit storage below GitHub's per-file limit.

Small/legacy JSON remains readable. Large JSON is an atomic manifest referencing
immutable, compressed 4 MiB byte chunks. No forecast or old chunk is deleted.
"""
import gzip
import hashlib
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from copy import deepcopy

FORMAT='arabsniper-audit-parts-v1'
CHUNK_BYTES=4*1024*1024


def digest(data): return hashlib.sha256(data).hexdigest()


def atomic_bytes(path, data):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_bytes(data);os.replace(temporary,path)


@contextmanager
def open_verified_json(path):
    """Open legacy JSON or a fully checked, disk-spooled multipart document."""
    path = Path(path)
    # Manifests are small. Large legacy documents must never be read in full.
    value = None
    if path.stat().st_size <= 1024 * 1024:
        value = json.loads(path.read_bytes())
    if not isinstance(value, dict) or value.get('_storage') != FORMAT:
        with path.open('rb') as stream:
            yield stream
        return
    checksum = hashlib.sha256()
    size = 0
    with tempfile.TemporaryFile(mode='w+b') as spool:
        for part in value['parts']:
            name = part['sha256']
            if len(name) != 64 or any(c not in '0123456789abcdef' for c in name):
                raise ValueError('Invalid audit part identifier')
            raw = gzip.decompress((path.parent/'_audit_parts'/(name+'.json.gz')).read_bytes())
            if len(raw) != part['bytes'] or digest(raw) != name:
                raise ValueError('Audit part checksum mismatch')
            spool.write(raw); checksum.update(raw); size += len(raw)
        if size != value['bytes'] or checksum.hexdigest() != value['sha256']:
            raise ValueError('Audit manifest checksum mismatch')
        spool.seek(0)
        yield spool


def validate_store(path):
    """Validate all JSON tokens and checksums without building the object graph."""
    import ijson
    with open_verified_json(path) as stream:
        for _ in ijson.basic_parse(stream, use_float=True):
            pass


def publish_spool(path, spool, chunk_bytes=CHUNK_BYTES):
    """Publish a serialized JSON spool, keeping one compressed chunk in RAM."""
    path = Path(path)
    if chunk_bytes < 1:
        raise ValueError('Positive chunk size required')
    size = spool.tell(); spool.seek(0)
    if size <= chunk_bytes:
        atomic_bytes(path, spool.read()+b'\n')
        return
    parts = []; checksum = hashlib.sha256()
    while True:
        part = spool.read(chunk_bytes)
        if not part:
            break
        checksum.update(part); sha = digest(part)
        target = path.parent/'_audit_parts'/(sha+'.json.gz')
        encoded = gzip.compress(part, compresslevel=6, mtime=0)
        if not target.exists() or target.read_bytes() != encoded:
            atomic_bytes(target, encoded)
        parts.append({'sha256': sha, 'bytes': len(part)})
    manifest = {'_storage': FORMAT, 'bytes': size, 'sha256': checksum.hexdigest(), 'parts': parts}
    atomic_bytes(path, json.dumps(manifest, separators=(',', ':')).encode()+b'\n')


def read_store(path, default=None):
    path=Path(path)
    if not path.exists():return deepcopy(default)
    value=json.loads(path.read_bytes())
    if not isinstance(value,dict) or value.get('_storage')!=FORMAT:return value
    checksum=hashlib.sha256()
    size=0
    # Keep decompressed chunks on disk, not alongside the parsed object graph.
    with tempfile.TemporaryFile(mode='w+b') as spool:
        for part in value['parts']:
            name=part['sha256']
            if len(name)!=64 or any(c not in '0123456789abcdef' for c in name):
                raise ValueError('Invalid audit part identifier')
            raw=gzip.decompress((path.parent/'_audit_parts'/(name+'.json.gz')).read_bytes())
            if len(raw)!=part['bytes'] or digest(raw)!=name:
                raise ValueError('Audit part checksum mismatch')
            spool.write(raw);checksum.update(raw);size+=len(raw)
        if size!=value['bytes'] or checksum.hexdigest()!=value['sha256']:
            raise ValueError('Audit manifest checksum mismatch')
        spool.seek(0)
        # Text decoding avoids retaining a second full UTF-8 bytes copy.
        import io
        with io.TextIOWrapper(spool, encoding='utf-8') as text:
            return json.load(text)


def write_store(path, value, chunk_bytes=CHUNK_BYTES):
    path=Path(path)
    if chunk_bytes<1:raise ValueError('Positive chunk size required')
    # Serialize incrementally to disk; retain only one chunk while compressing.
    with tempfile.TemporaryFile(mode='w+b') as spool:
        import io
        text=io.TextIOWrapper(spool, encoding='utf-8', write_through=False)
        try:
            json.dump(value,text,ensure_ascii=False,separators=(',',':'))
            text.flush()
        finally:
            text.detach()
        size=spool.tell();spool.seek(0)
        if size<=chunk_bytes:
            atomic_bytes(path,spool.read()+b'\n');return
        parts=[];checksum=hashlib.sha256()
        while True:
            part=spool.read(chunk_bytes)
            if not part:break
            checksum.update(part);sha=digest(part)
            target=path.parent/'_audit_parts'/(sha+'.json.gz')
            encoded=gzip.compress(part,compresslevel=6,mtime=0)
            if not target.exists() or target.read_bytes()!=encoded:atomic_bytes(target,encoded)
            parts.append({'sha256':sha,'bytes':len(part)})
        manifest={'_storage':FORMAT,'bytes':size,'sha256':checksum.hexdigest(),'parts':parts}
    # Publish the index only after every checksummed part exists.
    atomic_bytes(path,json.dumps(manifest,separators=(',',':')).encode()+b'\n')
