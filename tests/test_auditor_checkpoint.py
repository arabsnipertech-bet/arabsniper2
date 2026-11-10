"""Daily saves must survive retries without republishing the live Gold board."""
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import write_store, read_store
from auditor_daily_priority import STATUS, WEIGHTS, signature, ensure_priority, v7_paths
from auditor_history_chunks import seal_day
from gold_storage import checkpoint_day, publish, previous_manifest, GOLD, write_manifest, restore_v7_priority
from test_gold_storage import Fake

DAY=date(2026,11,7)
NOW=datetime.fromisoformat('2026-11-08T00:40:00+01:00')


def evidence(root):
    write_store(root/f'auditordata/prematch_{DAY}.json',
                {'audit_date':str(DAY),'fixtures':{'1':{'original':'x'*300}}},chunk_bytes=64)
    for suffix in ('daily.json','results.json','calibration.json'):
        write_store(root/f'auditarchive/prematch_{DAY}_{suffix}',
                    {'audit_date':str(DAY),'original':suffix},chunk_bytes=64)
    path=root/f'auditarchive/prematch_{DAY}_daily.md'
    path.write_text('Verified daily results')
    seal_day(root,DAY,NOW,False)
    write_store(root/WEIGHTS,{'as_of':str(DAY+timedelta(days=1)),'version':'adaptive-calibration-v5.1',
                            'validation_cycle':'validation_freeze_v5.1_2026-11-01'})
    write_store(root/STATUS,{'version':1,'date':str(NOW.date()),'report_date':str(DAY),
                            'report_sha256':signature(root/f'auditarchive/prematch_{DAY}_daily.json'),
                            'weights_sha256':signature(root/WEIGHTS)})


def v7_evidence(root):
    from v7_probability import VERSION
    evidence(root)
    paths=v7_paths(DAY)
    weights={'version':VERSION,'trained_at':NOW.isoformat(),'weight':.8,'status':'baseline_retained'}
    write_store(root/paths['weights_sha256'],weights)
    write_store(root/paths['outcomes_sha256'],{'version':VERSION,'date':str(DAY),'fixtures':{}})
    write_store(root/paths['learning_sha256'],{'version':VERSION,'date':str(NOW.date()),'state':weights})
    write_store(root/STATUS,{'version':2,'date':str(NOW.date()),'report_date':str(DAY),
                             **{key:signature(root/path) for key,path in paths.items()}})
    return paths


class AuditCheckpoint(unittest.TestCase):
    def test_daily_checkpoint_saves_parts_preserves_scans_and_does_not_need_gold(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);evidence(root);storage=Fake()
            scans={'data/arab_quote_day1.json':{'sha256':'a'*64,'bytes':12},
                   'data/arab_quote_auditor_progress.json':{'sha256':'b'*64,'bytes':8},
                   'data/arab_quote_scan_journal_20261107T120000000000_forecasts.json':{'sha256':'c'*64,'bytes':7}}
            write_manifest(storage,'test',scans)
            checkpoint_day(storage,'test',root,DAY)
            index=previous_manifest(storage,'test')
            self.assertEqual({p:index[p] for p in scans},scans)
            self.assertIn(f'auditarchive/prematch_{DAY}_checkpoint.json',index)
            parts=[p for p in index if '/_audit_parts/' in p]
            self.assertTrue(parts)
            self.assertFalse(set(GOLD)&storage.objects.keys())
            self.assertEqual(storage.calls[-1],'state/current.json')
            # No changed object is uploaded twice on retry; immutable references
            # and the current scan state are byte-identical.
            before=storage.objects['state/current.json'];storage.calls.clear()
            checkpoint_day(storage,'test',root,DAY)
            self.assertEqual(storage.objects['state/current.json'],before)
            self.assertFalse(any(p.startswith('state/objects/') for p in storage.calls))

    def test_failed_object_upload_keeps_the_previous_checkpoint_and_retry_succeeds(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);evidence(root);storage=Fake()
            write_manifest(storage,'test',{})
            original=storage.objects['state/current.json'];put=storage.put_object
            def fail(**kw):
                if kw['Key'].startswith('state/objects/'):raise RuntimeError('provider unavailable')
                put(**kw)
            with patch.object(storage,'put_object',side_effect=fail):
                with self.assertRaisesRegex(RuntimeError,'provider unavailable'):
                    checkpoint_day(storage,'test',root,DAY)
            self.assertEqual(storage.objects['state/current.json'],original)
            checkpoint_day(storage,'test',root,DAY)
            self.assertIn(STATUS,previous_manifest(storage,'test'))

    def test_unverified_or_incomplete_evidence_never_changes_the_manifest(self):
        for damage in ('report','weights','version','cycle','receipt','chunk'):
            with self.subTest(damage=damage),TemporaryDirectory() as folder:
                root=Path(folder);evidence(root);storage=Fake()
                write_manifest(storage,'test',{})
                before=storage.objects['state/current.json']
                if damage=='report':
                    write_store(root/f'auditarchive/prematch_{DAY}_daily.json',{'audit_date':'2026-11-06'})
                elif damage=='weights':write_store(root/WEIGHTS,{'as_of':'2026-11-07'})
                elif damage in ('version','cycle'):
                    weights=read_store(root/WEIGHTS)
                    weights['version' if damage=='version' else 'validation_cycle']='incorrect'
                    write_store(root/WEIGHTS,weights)
                    status=read_store(root/STATUS);status['weights_sha256']=signature(root/WEIGHTS)
                    write_store(root/STATUS,status)
                elif damage=='receipt':(root/f'auditarchive/prematch_{DAY}_checkpoint.json').unlink()
                else:
                    next((root/'auditordata/_audit_parts').glob('*.json.gz')).write_bytes(b'corrupt')
                with self.assertRaises(Exception):checkpoint_day(storage,'test',root,DAY)
                self.assertEqual(storage.objects['state/current.json'],before)

    def test_concurrent_scan_update_is_not_overwritten(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);evidence(root);storage=Fake();get=storage.get_object
            reads=0
            fresh={'data/arab_quote_day1.json':{'sha256':'f'*64,'bytes':20}}
            def concurrent(**kw):
                nonlocal reads
                if kw['Key']=='state/current.json':
                    reads+=1
                    if reads==2:write_manifest(storage,'test',fresh)
                return get(**kw)
            with patch.object(storage,'get_object',side_effect=concurrent):
                with self.assertRaisesRegex(RuntimeError,'advanced'):
                    checkpoint_day(storage,'test',root,DAY)
            self.assertEqual(previous_manifest(storage,'test'),fresh)

    def test_historical_checkpoint_never_projects_or_writes_gold_pages(self):
        with TemporaryDirectory() as folder:
            root=Path(folder);evidence(root);storage=Fake()
            with patch('gold_browser_feed.project',side_effect=AssertionError('unexpected Gold rendering')):
                publish(storage,'test',root,checkpoint_only=True)
            self.assertIn(STATUS,previous_manifest(storage,'test'))
            self.assertFalse(set(GOLD)&storage.objects.keys())
            self.assertFalse(any(p.startswith('gold-browser/') for p in storage.calls))

    def test_v7_checkpoint_survives_missing_legacy_and_rejects_corruption(self):
        with TemporaryDirectory() as folder:
            root=Path(folder); paths=v7_evidence(root); storage=Fake()
            (root/WEIGHTS).unlink()
            (root/f'auditarchive/prematch_{DAY}_calibration.json').unlink()
            checkpoint_day(storage,'test',root,DAY)
            manifest=previous_manifest(storage,'test')
            self.assertTrue(set(paths.values()) <= manifest.keys())
            self.assertNotIn(WEIGHTS,manifest)
            write_store(root/paths['learning_sha256'],{'version':'corrupt'})
            with self.assertRaisesRegex(ValueError,'Unverified V7'):
                checkpoint_day(storage,'test',root,DAY)
            self.assertEqual(previous_manifest(storage,'test'),manifest)

    def test_v7_priority_restores_and_reuses_only_the_closed_day(self):
        with TemporaryDirectory() as folder, TemporaryDirectory() as restored:
            root=Path(folder); paths=v7_evidence(root); storage=Fake()
            checkpoint_day(storage,'test',root,DAY)
            before=previous_manifest(storage,'test')
            write_manifest(storage,'test',{**before, 'auditarchive/prematch_2026-11-05_calibration.json':
                {'sha256':'a'*64, 'bytes':400}})
            destination=Path(restored)
            restore_v7_priority(storage,'test',destination,DAY)
            self.assertTrue(all((destination/path).exists() for path in paths.values()))
            calls=[]
            ensure_priority(destination,NOW.date(),lambda *a:calls.append(a))
            self.assertEqual(calls,[])


if __name__=='__main__':unittest.main()
