"""Private R2 checkpoint + Gold projection. Activate only after staging verification."""
from pathlib import Path
import hashlib, json, os, re, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from audit_storage import read_store, atomic_bytes, FORMAT, validate_store
PUBLIC = {'data/arab_quote_homepage.json','data/arab_quote_results_public.json'}
GOLD = [f'arab_quote_day{i}.json' for i in range(1,6)]+['arab_quote_prediction_audit.json','arab_quote_model_research.json']

def sha(data): return hashlib.sha256(data).hexdigest()
def allowed(path):
    if path == 'data/arab_calibration_weights.json': return True
    return path not in PUBLIC and bool(re.fullmatch(r'(data/arab_quote_[a-zA-Z0-9_]+\.json|(auditordata|auditarchive)/prematch_[a-zA-Z0-9_.-]+\.(json|md)|(data|auditordata|auditarchive)/_audit_parts/[a-f0-9]{64}\.json\.gz)',path))

def collect(root, verified=None, *, patterns=None):
    root=Path(root); files={}
    verified=verified or {}
    def unchanged(key, raw):
        item=verified.get(key, {})
        return item.get('bytes')==len(raw) and item.get('sha256')==sha(raw)
    if patterns is None:
        patterns=('data/arab_calibration_weights.json','data/arab_quote_*.json','auditordata/prematch_*.json','auditarchive/prematch_*')
    for pattern in patterns:
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

def publish(s3,bucket,root, *, checkpoint_only=False, selected_paths=None):
    root=Path(root)
    # Validate every Gold file before writing anything, avoiding partial corrupt releases.
    gold={} if checkpoint_only else {name:json.dumps(read_store(root/'data'/name),ensure_ascii=False,separators=(',',':')).encode() for name in GOLD if (root/'data'/name).exists()}
    if not checkpoint_only and len(gold)!=len(GOLD): raise ValueError('Missing Gold files; publication aborted')
    verified=previous_manifest(s3,bucket)
    # Optional display archive: the scan files and decision engine stay untouched.
    if not checkpoint_only:
        try:
            from gold_history import project
            projection=project(root,json.loads(gold['arab_quote_day1.json']))
            gold['arab_quote_day1.json']=json.dumps(projection,ensure_ascii=False,separators=(',',':')).encode()
        except Exception as error:
            print('Gold yesterday archive deferred: '+type(error).__name__,flush=True)
    files = collect(root, verified, patterns=selected_paths)
    index = dict(verified) if selected_paths is not None else {}
    known = {item['sha256'] for item in verified.values()}
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
    if selected_paths is not None and previous_manifest(s3, bucket) != verified:
        raise RuntimeError('Private checkpoint advanced during incremental publication')
    if checkpoint_only:
        if not index:
            raise ValueError('Empty private checkpoint')
        write_manifest(s3,bucket,index)
        print(f'Private audit checkpoint saved: {len(index)} files; Gold feeds unchanged',flush=True)
        return
    # Gold uses its existing names and requires authenticated Worker access.
    from gold_browser_feed import project as browser_project, encoded
    def upload_browser_page(key, data):
        s3.put_object(Bucket=bucket, Key=key, Body=data,
                      ContentType='application/octet-stream', CacheControl='private, no-store')
    # Add the browser index to the already-authenticated metadata endpoint.
    # Keep every original daily feed unchanged for Telegram/Facebook readers.
    catalog = json.loads(gold['arab_quote_model_research.json'])
    catalog['_gold_browser_days'] = {
        name: browser_project(json.loads(gold[name]), upload_browser_page)
        for name in GOLD if re.fullmatch(r'arab_quote_day[1-5]\.json', name)
    }
    audit = json.loads(gold['arab_quote_prediction_audit.json'])
    catalog['_gold_audit_summary'] = {key: audit[key] for key in ('summary', 'updated_at') if key in audit}
    gold['arab_quote_model_research.json'] = encoded(catalog)
    for name, board in catalog['_gold_browser_days'].items():
        print(f"Browser feed {name}: {board.get('match_count', 0)} matches, {len(board.get('pages', []))} verified pages", flush=True)
    for name,data in gold.items():
        s3.put_object(Bucket=bucket,Key=name,Body=data,ContentType='application/json',CacheControl='private, no-store')
    write_manifest(s3,bucket,index)
    print(f'Private checkpoint saved: {len(index)} files; {len(gold)} Gold feeds')


def checkpoint_day(s3, bucket, root, day):
    """Commit one verified priority day without rebuilding Gold or old archives.

    The caller holds the normal data-writer mutex. Merge with the current
    manifest so old progress, unconsumed journals and current scans survive.
    """
    from datetime import date, datetime, timedelta
    from adaptive_calibration import VERSION, VALIDATION_CYCLE
    from auditor_daily_priority import STATUS, WEIGHTS, signature
    from auditor_history_chunks import reusable
    from auditor_schedule import ROME
    day = date.fromisoformat(str(day))
    root = Path(root)
    required = {f'auditordata/prematch_{day}.json', STATUS, WEIGHTS}
    required.update(f'auditarchive/prematch_{day}_{suffix}' for suffix in
                    ('daily.json','daily.md','results.json','calibration.json','checkpoint.json'))
    if any(not (root/path).is_file() or (root/path).is_symlink() for path in required):
        raise ValueError('Missing priority checkpoint evidence')
    status = read_store(root/STATUS, {})
    weights = read_store(root/WEIGHTS, {})
    report = root/f'auditarchive/prematch_{day}_daily.json'
    if (status.get('version') != 1 or status.get('date') != str(day+timedelta(days=1))
            or status.get('report_date') != str(day)
            or status.get('report_sha256') != signature(report)
            or status.get('weights_sha256') != signature(root/WEIGHTS)
            or weights.get('as_of') != status['date']
            or weights.get('version') != VERSION
            or weights.get('validation_cycle') != VALIDATION_CYCLE
            or read_store(report, {}).get('audit_date') != str(day)
            or not reusable(root, day, datetime.now(ROME))):
        raise ValueError('Unverified priority checkpoint evidence')
    for optional in (f'auditarchive/prematch_{day}_v7.json',
                     f'auditarchive/prematch_{day}_v7_learning.json',
                     'data/arab_quote_v7_weights.json'):
        if (root/optional).is_file():
            required.add(optional)
    verified = previous_manifest(s3,bucket)
    files = collect(root,verified,patterns=sorted(required))
    additions = {path:{'sha256':sha(raw),'bytes':len(raw)} for path,raw in files.items()}
    known = {item['sha256'] for item in verified.values()}
    uploads = {sha(raw):raw for raw in files.values() if sha(raw) not in known}
    def upload(item):
        digest, raw = item
        s3.put_object(Bucket=bucket,Key='state/objects/'+digest,Body=raw,Metadata={'sha256':digest})
    with ThreadPoolExecutor(max_workers=8) as pool:
        for _ in pool.map(upload,uploads.items()):
            pass
    if previous_manifest(s3,bucket) != verified:
        raise RuntimeError('Private checkpoint advanced during priority publication')
    write_manifest(s3,bucket,{**verified,**additions})
    print(f'Priority daily checkpoint saved: {day}; {len(additions)} files; Gold feeds unchanged',flush=True)

def write_manifest(s3, bucket, index):
    manifest = json.dumps({'version': 1, 'files': index}, sort_keys=True, separators=(',', ':')).encode()
    s3.put_object(Bucket=bucket, Key='state/manifests/'+sha(manifest)+'.json', Body=manifest)
    # Pointer last: a failed upload leaves the previous checkpoint recoverable.
    s3.put_object(Bucket=bucket, Key='state/current.json', Body=manifest, ContentType='application/json')

def restore_subset(storage, bucket, root, index, paths):
    """Download only named pointers and their verified content-addressed chunks."""
    def download(path):
        info = index[path]
        digest = info['sha256']
        if not allowed(path) or not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('Invalid private object reference')
        raw = storage.get_object(Bucket=bucket, Key='state/objects/'+digest)['Body'].read()
        if len(raw) != info['bytes'] or sha(raw) != digest:
            raise ValueError('Private object checksum mismatch')
        target = root / path
        atomic_bytes(target, raw)
        if target.suffix == '.json' and len(raw) <= 1024*1024:
            value = json.loads(raw)
            if isinstance(value, dict) and value.get('_storage') == FORMAT:
                parts = []
                for item in value['parts']:
                    name = item['sha256']
                    if not re.fullmatch('[0-9a-f]{64}', name):
                        raise ValueError('Invalid private chunk')
                    parts.append(str(Path(path).parent/'_audit_parts'/(name+'.json.gz')))
                return parts
        return []

    with ThreadPoolExecutor(max_workers=8) as pool:
        chunks = {part for parts in pool.map(download, sorted(set(paths))) for part in parts}
        for _ in pool.map(download, sorted(chunks)):
            pass


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
    # Weights are also certified in git: retain the newer certified publication
    # across a crash between the git and private-checkpoint writes.
    weights = 'data/arab_calibration_weights.json'
    local = root/weights
    if local.is_file():
        from adaptive_calibration import VERSION
        from multimarket_model import stamp
        current = json.loads(local.read_bytes())
        remote = json.loads(pending.get(weights, b'{}'))
        at, before = stamp(current.get('trained_at')), stamp(remote.get('trained_at'))
        if current.get('version') == VERSION and at and (not before or at > before):
            pending[weights] = local.read_bytes()
        elif weights not in pending:
            pending[weights] = local.read_bytes()  # migration from pre-calibration checkpoints
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
    import argparse
    from datetime import date
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=('publish','restore','checkpoint','checkpoint-day'))
    parser.add_argument('--date',type=date.fromisoformat)
    args=parser.parse_args()
    if (args.command=='checkpoint-day') != (args.date is not None):
        parser.error('--date is required only for checkpoint-day')
    try:
        storage,bucket,root=client(),os.environ['R2_BUCKET'],Path(__file__).resolve().parents[1]
        if args.command=='checkpoint':publish(storage,bucket,root,checkpoint_only=True)
        elif args.command=='checkpoint-day':checkpoint_day(storage,bucket,root,args.date)
        else:globals()[args.command](storage,bucket,root)
    except Exception as error:
        # Never print SDK request headers/credentials.
        raise SystemExit('Private storage failed: '+type(error).__name__)

