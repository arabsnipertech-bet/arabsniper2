"""Private R2 checkpoint + Gold projection. Activate only after staging verification."""
from pathlib import Path
import hashlib, json, os, re, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from audit_storage import read_store, atomic_bytes, FORMAT, validate_store
PUBLIC = {'data/arab_quote_homepage.json','data/arab_quote_results_public.json'}
GOLD = [f'arab_quote_day{i}.json' for i in range(1,6)]+['arab_quote_prediction_audit.json','arab_quote_model_research.json']

def sha(data): return hashlib.sha256(data).hexdigest()
def allowed(path):
    return path not in PUBLIC and bool(re.fullmatch(r'(data/arab_quote_[a-zA-Z0-9_]+\.json|(auditordata|auditarchive)/prematch_[a-zA-Z0-9_.-]+\.(json|md)|(data|auditordata|auditarchive)/_audit_parts/[a-f0-9]{64}\.json\.gz)',path))

def collect(root, verified=None):
    root=Path(root); files={}
    verified=verified or {}
    def unchanged(key, raw):
        item=verified.get(key, {})
        return item.get('bytes')==len(raw) and item.get('sha256')==sha(raw)
    for pattern in ('data/arab_quote_*.json','auditordata/prematch_*.json','auditarchive/prematch_*'):
        for p in root.glob(pattern):
            key=p.relative_to(root).as_posix()
            if not allowed(key) or not p.is_file() or p.is_symlink(): continue
            files[key]=p.read_bytes()
            if p.suffix=='.json':
                value=json.loads(files[key])
                if isinstance(value,dict) and value.get('_storage')==FORMAT:
                    trusted=unchanged(key,files[key])
                    for part in value['parts']:
                        if not re.fullmatch('[a-f0-9]{64}',part['sha256']):
                            raise ValueError('Invalid audit part identifier')
                        chunk=p.parent/'_audit_parts'/(part['sha256']+'.json.gz')
                        chunk_key=chunk.relative_to(root).as_posix()
                        files[chunk_key]=chunk.read_bytes()
                        trusted=unchanged(chunk_key,files[chunk_key]) and trusted
                    # Unchanged pointers AND compressed parts were verified in the
                    # published checkpoint. Do not reparse historical JSON hourly.
                    if not trusted:
                        validate_store(p)
    return files

def previous_manifest(s3,bucket):
    try:
        value=json.loads(s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read())
    except Exception as error:
        code=getattr(error,'response',{}).get('Error',{}).get('Code')
        if code in ('NoSuchKey','404'):return {}
        raise
    if value.get('version')!=1 or not isinstance(value.get('files'),dict):raise ValueError('Invalid checkpoint')
    return value['files']

def previous_objects(s3,bucket):
    return {item['sha256'] for item in previous_manifest(s3,bucket).values()}

def publish(s3,bucket,root):
    root=Path(root)
    # Validate every Gold file before writing anything, avoiding partial corrupt releases.
    gold={name:json.dumps(read_store(root/'data'/name),ensure_ascii=False,separators=(',',':')).encode() for name in GOLD if (root/'data'/name).exists()}
    if len(gold)!=len(GOLD): raise ValueError('Missing Gold files; publication aborted')
    verified=previous_manifest(s3,bucket)
    files=collect(root,verified); index={}; known={item['sha256'] for item in verified.values()}
    uploads={}
    for path,data in files.items():
        digest=sha(data)
        if digest not in known: uploads[digest]=data
        index[path]={'sha256':digest,'bytes':len(data)}
    print(f'Uploading {len(uploads)} changed objects; reusing unchanged data',flush=True)
    def upload(item):
        digest,data=item
        s3.put_object(Bucket=bucket,Key='state/objects/'+digest,Body=data,Metadata={'sha256':digest})
    with ThreadPoolExecutor(max_workers=8) as pool:
        tasks=[pool.submit(upload,item) for item in uploads.items()]
        for count,task in enumerate(as_completed(tasks),1):
            task.result()
            if count%25==0 or count==len(tasks):print(f'Uploaded {count}/{len(tasks)} objects',flush=True)
    # Gold uses its existing names and requires authenticated Worker access.
    for name,data in gold.items():
        s3.put_object(Bucket=bucket,Key=name,Body=data,ContentType='application/json',CacheControl='private, no-store')
    write_manifest(s3,bucket,index)
    print(f'Private checkpoint saved: {len(index)} files; {len(gold)} Gold feeds')

def write_manifest(s3, bucket, index):
    manifest = json.dumps({'version': 1, 'files': index}, sort_keys=True, separators=(',', ':')).encode()
    s3.put_object(Bucket=bucket, Key='state/manifests/'+sha(manifest)+'.json', Body=manifest)
    # Pointer last: a failed upload leaves the previous checkpoint recoverable.
    s3.put_object(Bucket=bucket, Key='state/current.json', Body=manifest, ContentType='application/json')

def restore(s3,bucket,root):
    root=Path(root)
    manifest=json.loads(s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read())
    if manifest.get('version')!=1 or not isinstance(manifest.get('files'),dict):raise ValueError('Invalid checkpoint')
    pending={}; objects={}
    for path,item in manifest['files'].items():
        if not allowed(path) or not re.fullmatch('[a-f0-9]{64}',item['sha256']):raise ValueError('Invalid checkpoint path')
        if not isinstance(item['bytes'],int) or item['bytes']<0:raise ValueError('Invalid checkpoint size')
        digest=item['sha256']
        if digest in objects and objects[digest]!=item['bytes']:raise ValueError('Conflicting object sizes')
        objects[digest]=item['bytes']
    print(f'Restoring {len(objects)} unique objects',flush=True)
    def download(item):
        digest,size=item
        raw=s3.get_object(Bucket=bucket,Key='state/objects/'+digest)['Body'].read()
        if len(raw)!=size or sha(raw)!=digest:raise ValueError('Checkpoint checksum mismatch')
        return digest,raw
    downloaded={}
    with ThreadPoolExecutor(max_workers=8) as pool:
        tasks=[pool.submit(download,item) for item in objects.items()]
        for count,task in enumerate(as_completed(tasks),1):
            digest,raw=task.result();downloaded[digest]=raw
            if count%25==0 or count==len(tasks):print(f'Restored {count}/{len(tasks)} objects',flush=True)
    for path,item in manifest['files'].items():pending[path]=downloaded[item['sha256']]
    if not pending:raise ValueError('Empty checkpoint')
    # Validate/download all objects first. No fallback to stale public data.
    for path in collect(root):
        if path not in pending: (root/path).unlink()
    for path,raw in pending.items():atomic_bytes(root/path,raw)
    print(f'Private checkpoint restored: {len(pending)} files')

def client():
    import boto3
    from botocore.config import Config
    account=os.environ.get('R2_ACCOUNT_ID','')
    if not re.fullmatch('[a-f0-9]{32}',account):raise ValueError('R2_ACCOUNT_ID missing/invalid')
    for key in ('R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY','R2_BUCKET'):
        if not os.environ.get(key):raise ValueError(key+' missing')
    return boto3.client('s3',endpoint_url=f'https://{account}.r2.cloudflarestorage.com',region_name='auto',aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],config=Config(retries={'max_attempts':3},connect_timeout=15,read_timeout=60))

if __name__=='__main__':
    command=sys.argv[1]
    if command not in ('publish','restore'):raise SystemExit('Expected publish or restore')
    try:globals()[command](client(),os.environ['R2_BUCKET'],Path(__file__).resolve().parents[1])
    except Exception as error:
        # Never print SDK request headers/credentials.
        raise SystemExit('Private storage failed: '+type(error).__name__)

