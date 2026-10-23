"""Private R2 checkpoint + Gold projection. Activate only after staging verification."""
from pathlib import Path
import hashlib, json, os, re, sys
from audit_storage import read_store, atomic_bytes, FORMAT
PUBLIC = {'data/arab_quote_homepage.json','data/arab_quote_results_public.json'}
GOLD = [f'arab_quote_day{i}.json' for i in range(1,6)]+['arab_quote_prediction_audit.json','arab_quote_model_research.json']

def sha(data): return hashlib.sha256(data).hexdigest()
def allowed(path):
    return path not in PUBLIC and bool(re.fullmatch(r'(data/arab_quote_[a-zA-Z0-9_]+\.json|(auditordata|auditarchive)/prematch_[a-zA-Z0-9_.-]+\.(json|md)|(data|auditordata|auditarchive)/_audit_parts/[a-f0-9]{64}\.json\.gz)',path))

def collect(root):
    root=Path(root); files={}
    for pattern in ('data/arab_quote_*.json','auditordata/prematch_*.json','auditarchive/prematch_*'):
        for p in root.glob(pattern):
            key=p.relative_to(root).as_posix()
            if not allowed(key) or not p.is_file() or p.is_symlink(): continue
            files[key]=p.read_bytes()
            if p.suffix=='.json':
                value=json.loads(files[key])
                if isinstance(value,dict) and value.get('_storage')==FORMAT:
                    read_store(p) # Validate all referenced parts before publication.
                    for part in value['parts']:
                        chunk=p.parent/'_audit_parts'/(part['sha256']+'.json.gz')
                        files[chunk.relative_to(root).as_posix()]=chunk.read_bytes()
    return files

def previous_objects(s3,bucket):
    try:
        value=json.loads(s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read())
    except Exception as error:
        code=getattr(error,'response',{}).get('Error',{}).get('Code')
        if code in ('NoSuchKey','404'):return set()
        raise
    if value.get('version')!=1 or not isinstance(value.get('files'),dict):raise ValueError('Invalid checkpoint')
    return {item['sha256'] for item in value['files'].values()}

def publish(s3,bucket,root):
    root=Path(root)
    # Validate every Gold file before writing anything, avoiding partial corrupt releases.
    gold={name:json.dumps(read_store(root/'data'/name),ensure_ascii=False,separators=(',',':')).encode() for name in GOLD if (root/'data'/name).exists()}
    if len(gold)!=len(GOLD): raise ValueError('Missing Gold files; publication aborted')
    files=collect(root); index={}; known=previous_objects(s3,bucket)
    for path,data in files.items():
        digest=sha(data);key='state/objects/'+digest
        if digest not in known:
            s3.put_object(Bucket=bucket,Key=key,Body=data,Metadata={'sha256':digest})
            known.add(digest)
        index[path]={'sha256':digest,'bytes':len(data)}
    # Gold uses its existing names and requires authenticated Worker access.
    for name,data in gold.items():
        s3.put_object(Bucket=bucket,Key=name,Body=data,ContentType='application/json',CacheControl='private, no-store')
    manifest=json.dumps({'version':1,'files':index},sort_keys=True,separators=(',',':')).encode()
    s3.put_object(Bucket=bucket,Key='state/manifests/'+sha(manifest)+'.json',Body=manifest)
    # Pointer last: a failed upload leaves the previous checkpoint recoverable.
    s3.put_object(Bucket=bucket,Key='state/current.json',Body=manifest,ContentType='application/json')
    print(f'Private checkpoint saved: {len(index)} files; {len(gold)} Gold feeds')

def restore(s3,bucket,root):
    root=Path(root)
    manifest=json.loads(s3.get_object(Bucket=bucket,Key='state/current.json')['Body'].read())
    if manifest.get('version')!=1 or not isinstance(manifest.get('files'),dict):raise ValueError('Invalid checkpoint')
    pending={}
    for path,item in manifest['files'].items():
        if not allowed(path) or not re.fullmatch('[a-f0-9]{64}',item['sha256']):raise ValueError('Invalid checkpoint path')
        raw=s3.get_object(Bucket=bucket,Key='state/objects/'+item['sha256'])['Body'].read()
        if len(raw)!=item['bytes'] or sha(raw)!=item['sha256']:raise ValueError('Checkpoint checksum mismatch')
        pending[path]=raw
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
