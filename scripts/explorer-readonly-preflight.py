"""Read-only production connectivity + local-only projection. No deploy/scan/remote write.
Only aggregate verification metadata may leave the ephemeral runner.
"""
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from gold_storage import client, previous_manifest, restore_subset, GOLD
from audit_storage import read_store
from gold_explorer import publish, project_match, signature
from multimarket_model import stamp

class Missing(Exception):
    response={'Error':{'Code':'NoSuchKey'}}
class Memory:
    def __init__(self):self.objects={}
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise Missing()
        return {'Body':io.BytesIO(self.objects[kw['Key']])}
    def put_object(self,**kw):
        assert kw['Key'].startswith('explorer/')
        self.objects[kw['Key']]=kw['Body']
class ReadOnly:
    def __init__(self,s3):self.s3=s3;self.reads=0;self.bytes=0
    def get_object(self,**kw):
        assert kw['Bucket']=='arabsniper-gold'
        assert kw['Key']=='state/current.json' or kw['Key'].startswith('state/objects/')
        result=self.s3.get_object(**kw);self.reads+=1;self.bytes+=result.get('ContentLength',0);return result

def main():
    result={'mode':'read-only; local projection only','remote_writes':0,'football_requests':0}
    source=ReadOnly(client());index=previous_manifest(source,'arabsniper-gold')
    assert index,'Private checkpoint missing'
    days=['data/'+name for name in GOLD[:5]]
    assert all(p in index for p in days),'Private daily feed missing'
    archives=sorted(p for p in index if p.startswith('auditarchive/prematch_') and p.endswith('_v7.json'))[-2:]
    legacy=sorted(p for p in index if p.startswith('auditordata/prematch_') and p.endswith('.json') and len(Path(p).stem)==19)[-1:]
    selected=days+archives+legacy
    with tempfile.TemporaryDirectory() as folder:
        root=Path(folder);restore_subset(source,'arabsniper-gold',root,index,selected)
        raw_before={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in selected}
        feeds=[read_store(root/p) for p in days]
        markets=Counter();published=0;valid=0;source_snapshots=0
        now=datetime.now(timezone.utc)
        for feed in feeds:
            for match in feed.get('matches',[]):
                snap=((match.get('multimarket') or {}).get('matrix') or {}).get('v7') or {}
                if snap:source_snapshots+=1
                for row in project_match(match,now):
                    original=snap['markets'][row['market']]
                    assert (row['probability'],row['odd'],row['ev'],row['ev_low'])==(original['calibrated'],original.get('odd'),original.get('ev'),original.get('ev_low'))
                    markets[row['label'] or row['market']]+=1;published+=1;valid+=bool(row['validated'])
        assert source_snapshots,'No V7 snapshots in real checkpoint'
        assert published,'No eligible original prematch snapshots; manual coverage review required'
        memory=Memory();started=time.monotonic()
        with patch.object(socket.socket,'connect',side_effect=AssertionError('Projection cannot use network')):
            publish(memory,'local-only',root,feeds,'refresh',now=now,legacy_limit=None,source_manifest=index)
        seconds=time.monotonic()-started
        assert raw_before=={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in selected},'Original archives changed'
        catalog=json.loads(memory.objects['explorer/catalog.json'])
        for day in catalog['days']:
            history=json.loads(memory.objects['explorer/days/'+day+'.json'])
            assert not history['scans'],'Refresh/backfill fabricated Explorer certificates'
        result.update(source_manifest_sha=signature(index),source_files=len(selected),source_v7_snapshots=source_snapshots,
            prematch_rows=published,validated_rows=valid,markets=dict(markets),historical_days=len(catalog['days']),
            historical_results=sum(d['results'] for d in catalog['days'].values()),legacy_rows=sum(d.get('legacy_count',0) for d in catalog['days'].values()),
            projection_seconds=round(seconds,3),projection_bytes=sum(map(len,memory.objects.values())),
            r2_gets=source.reads,r2_bytes=source.bytes,originals_unchanged=True)
    spec=importlib.util.spec_from_file_location('ops',ROOT/'scripts/deploy-telegram-campaign.py')
    ops=importlib.util.module_from_spec(spec);spec.loader.exec_module(ops)
    settings=ops.cf(ops.WORKER+'/settings',os.environ['CF_TOKEN'])
    bindings={b['name']:b for b in settings['bindings']}
    assert bindings['ACCESS_DB']['id']==ops.DB
    assert bindings['GOLD_DATA']['bucket_name']=='arabsniper-gold'
    assert bindings['AUTH_ENABLED']['text']=='true'
    tables=ops.query("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('gold_sessions','gold_codes','passes','explorer_searches','explorer_favorites')")
    assert {'gold_sessions','gold_codes','passes'} <= {r['name'] for r in tables}
    live=ops.live();base=__import__('subprocess').check_output(['git','show','FETCH_HEAD:workers/stripe-access/production-bundle.mjs'],cwd=ROOT,text=True)
    result.update(worker_read=True,d1_read=True,worker_sha=ops.digest(live),worker_matches_main=live.strip()==base.strip(),
        explorer_flag=bindings.get('GOLD_EXPLORER_ENABLED',{}).get('text','disabled'),
        personal_schema_present={'explorer_searches','explorer_favorites'} <= {r['name'] for r in tables})
    output=ROOT/'test-artifacts/explorer-real-preflight.json';output.parent.mkdir(exist_ok=True);output.write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as out:
        out.write('## Explorer: verifica reale in sola lettura\n\n```json\n'+json.dumps(result,indent=2)+'\n```\n')
if __name__=='__main__':
    try:main()
    except Exception as error:
        # Do not emit exception payloads, credentials, URLs or private records.
        raise SystemExit('Read-only preflight failed: '+type(error).__name__)
