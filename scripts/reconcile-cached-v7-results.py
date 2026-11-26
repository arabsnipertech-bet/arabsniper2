"""One-off closed-day result reconciliation; no provider, fitting or Gold writes."""
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'auditor')]
from audit_daily_evidence import evidence_paths, receipt_path, verified_evidence
from audit_official_scores import cached_feed
from audit_storage import read_store
from auditor_light import remote_receipt
from gold_storage import checkpoint_results, client, previous_manifest, restore_subset
from v7_archive import ROME

DAYS=('2026-11-25','2026-11-24','2026-11-23','2026-11-22')
MUTABLE={f'auditarchive/prematch_{day}_{suffix}' for day in DAYS
         for suffix in ('daily.json','daily.md','results.json','v7.json','v7_evidence.json')}
CHUNK=re.compile(r'auditarchive/_audit_parts/[a-f0-9]{64}\.json\.gz')


def guarded_manifest(before,after):
    """Only result-derived outputs and new immutable chunks may change."""
    changed={path for path in before.keys()|after.keys() if before.get(path)!=after.get(path)}
    for path in changed:
        if path in MUTABLE and path in after:continue
        if CHUNK.fullmatch(path) and path not in before and path in after:continue
        raise ValueError('Protected checkpoint entry changed')
    return len(changed)


def same_forecasts(before,after):
    return before.keys()<=after.keys() and all(
        all(row.get(key)==after[fid].get(key) for key in ('forecast_sha256','captured_at','kickoff'))
        for fid,row in before.items())


def cached_progress(prior,archive,cache,now):
    """Resume only when validated cached scores add information to this day.

    Check both the complete receipt and its compact V7 archive: a missing
    halftime may remain in either. Observation timestamps alone are no change.
    Reuse the production conflict, kickoff and 90-minute validation unchanged.
    """
    from unittest.mock import patch
    from audit_official_scores import read_live_scores, use_stored_scores
    fixtures={fid:row.get('kickoff') for fid,row in prior['fixtures'].items()}
    saved={fid:row['result'] for fid,row in prior['fixtures'].items() if row.get('result')}
    with patch('audit_official_scores.cached_feed',return_value=cache):
        live=read_live_scores(fixtures,now)
    merged,disputes=use_stored_scores(fixtures,saved,live)
    fields=('status','home_ft','away_ft','home_ht','away_ht','rescheduled_to','extra_time','penalties')
    def score(value):return tuple((value or {}).get(key) for key in fields)
    for fid in live.keys()-disputes:
        result=merged[fid]
        if score(result)!=score(saved.get(fid)):
            return True
        old=(archive.get('fixtures') or {}).get(fid)
        if old is not None and score(result)!=score(old.get('result')):
            return True
    return False


def save_report(report):
    output=ROOT/'test-artifacts/cached-v7-reconciliation.json'
    output.parent.mkdir(exist_ok=True);output.write_text(json.dumps(report,indent=2))


class ResultStorage:
    def __init__(self,storage,bucket):
        self.storage,self.bucket,self.baseline=storage,bucket,None

    def get_object(self,**kwargs):return self.storage.get_object(**kwargs)

    def put_object(self,**kwargs):
        if kwargs['Bucket']!=self.bucket:raise ValueError('Unexpected bucket')
        key=kwargs['Key']
        if key=='state/current.json':
            value=json.loads(kwargs['Body'])
            if self.baseline is None or value.get('version')!=1:raise ValueError('Missing base checkpoint')
            guarded_manifest(self.baseline,value['files'])
        elif not re.fullmatch(r'state/(objects|manifests)/[a-f0-9]{64}(\.json)?',key):
            raise ValueError('Only private checkpoint writes are permitted')
        return self.storage.put_object(**kwargs)


def main():
    import argparse
    from unittest.mock import patch
    from current_audit import settle_day
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day',required=True,choices=DAYS)
    args=parser.parse_args()
    days=(args.day,)
    now=datetime.now(timezone.utc)
    if (os.environ.get('GITHUB_REPOSITORY')!='arabsnipertech-bet/arabsniper2'
            or os.environ.get('GITHUB_REF')!='refs/heads/main'
            or os.environ.get('GOLD_PRIVATE')!='true'
            or now.astimezone(ROME).date()!=date(2026,11,26)):
        raise ValueError('Reconciliation requires authorized production main on 26 November')
    if os.environ.get('API_SPORTS_KEY','').strip():
        raise ValueError('Provider credentials are forbidden in cached-only reconciliation')
    # One read-only cache capture is reused for every day; its GET has no refresh.
    cache=cached_feed()
    if cache.get('schema_version')!=1 or cache.get('enabled') is not True or not isinstance(cache.get('fixtures'),dict):
        raise ValueError('No verified cached feed')
    bucket=os.environ['R2_BUCKET'];storage=ResultStorage(client(fast_restore=True),bucket)
    index=previous_manifest(storage,bucket);storage.baseline=index
    required=set().union(*(evidence_paths(day)|{receipt_path(day)} for day in days))
    if not required<=index.keys():raise ValueError('Missing date-specific evidence')
    report=dict(as_of=now.isoformat(),status='running',provider_calls=0,model_fits=0,social_sends=0,
                forecast_files_changed=0,weight_files_changed=0,gold_feed_writes=0,days={})
    with tempfile.TemporaryDirectory(prefix='v7-cached-reconcile-') as folder:
        root=Path(folder)
        originals={f'auditordata/prematch_{day}.json':index[f'auditordata/prematch_{day}.json']['sha256']
                   for day in days}
        for day in days:
            # Small, checksum-verified pointers decide whether another full
            # reconstruction is useful, before restoring gigabytes of ledgers.
            restore_subset(storage,bucket,root,index,
                {receipt_path(day),f'auditarchive/prematch_{day}_v7.json'},fast_restore=True)
            from audit_daily_evidence import read_receipt
            prior=read_receipt(root,day)
            if not remote_receipt(index,prior,day):raise ValueError('Unverified remote day')
            old=read_store(root/f'auditarchive/prematch_{day}_v7.json',{})
            before=dict(old.get('counts') or {})
            if not cached_progress(prior,old,cache,now):
                report['days'][day]=dict(before=before,after=before,unresolved=prior['pending_fixtures'],
                    frozen_forecast_sha256=originals[f'auditordata/prematch_{day}.json'],
                    original_snapshot_sha_preserved=True,skipped='no_cached_progress')
                save_report(report)
                print(json.dumps({'reconciled_day':day,**report['days'][day]}),flush=True)
                continue
            restore_subset(storage,bucket,root,index,evidence_paths(day)|{receipt_path(day)},fast_restore=True)
            if verified_evidence(root,day) is None:raise ValueError('Unverified frozen day')
            with patch('audit_official_scores.cached_feed',return_value=cache):
                queried=settle_day(root,date.fromisoformat(day),now,legacy_evidence=False,
                    max_result_ids=20,progress=lambda message:print(message,flush=True))
            if queried:raise ValueError('Provider access is forbidden')
            for path,digest in originals.items():
                if (root/path).is_file() and hashlib.sha256((root/path).read_bytes()).hexdigest()!=digest:
                    raise ValueError('Frozen forecast bytes changed')
            # The existing daily builder may prepare a promo projection. This
            # repair does not publish it or trigger any message/publication.
            (root/f'auditarchive/prematch_{day}_promo.json').unlink(missing_ok=True)
            current=previous_manifest(storage,bucket)
            if any(current.get(path)!=index[path] for path in originals):
                raise ValueError('Frozen source advanced during reconciliation')
            after=read_store(root/f'auditarchive/prematch_{day}_v7.json',{})
            if not same_forecasts(old.get('fixtures') or {},after.get('fixtures') or {}):
                raise ValueError('Sealed forecast identity changed')
            storage.baseline=current
            checkpoint_results(storage,bucket,root,day)
            updated=previous_manifest(storage,bucket)
            guarded_manifest(current,updated)
            report['days'][day]=dict(before=before,after=after.get('counts'),
                unresolved=verified_evidence(root,day)['pending_fixtures'],
                frozen_forecast_sha256=originals[f'auditordata/prematch_{day}.json'],
                original_snapshot_sha_preserved=True)
            save_report(report)
            print(json.dumps({'reconciled_day':day,**report['days'][day]}),flush=True)
    report['status']='complete';save_report(report)
    print(json.dumps(report),flush=True)


if __name__=='__main__':
    try:main()
    except Exception as error:raise SystemExit('Cached result reconciliation stopped: '+type(error).__name__) from None
