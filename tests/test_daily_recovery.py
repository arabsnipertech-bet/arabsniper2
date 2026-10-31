import copy
from datetime import date, datetime
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from audit_storage import read_store, write_store
from gold_storage import collect, previous_manifest, sha, write_manifest
from recover_daily import FORECASTS, install, prepare, report_path, source_set
from test_daily_audit import record
from test_gold_storage import Fake
from current_audit import archive, settle_day
from audit_journal import ingest
from nightly_auditor import PROGRESS

DAY = date(2026, 10, 30)
NOW = datetime.fromisoformat('2026-10-31T08:00:00+01:00')
JOURNAL = 'data/arab_quote_scan_journal_20261030T130000000000_forecasts.json'
LATER = 'data/arab_quote_scan_journal_20261031T060000000000_forecasts.json'


def sources(storage, root):
    model = 'lateral-matrix-v5.0'
    r = record('2026-10-30T09:00:00+01:00')
    r['fixture'].update(kickoff='2026-10-30T17:00:00+01:00', match='Casa - Ospite')
    r['model_version'] = r['multimarket']['model_id'] = model
    r['multimarket']['markets'][0]['published_selection'] = True
    scores = r.pop('settlement')['result']
    write_store(root / FORECASTS, {'fixtures':{}, 'multimarket_evaluations':{model+'::1':r}}, chunk_bytes=120)
    latest = copy.deepcopy(r)
    latest['captured_at'] = '2026-10-30T14:00:00+01:00'
    latest['multimarket']['markets'][0]['odd'] = 2.75
    update = {model+'::1::'+latest['captured_at']: latest}
    write_store(root / JOURNAL, {'journal_type':'forecasts','updated_at':latest['captured_at'],'analysis_updates':update})
    index = {}
    for path, raw in collect(root).items():
        digest = sha(raw)
        storage.put_object(Bucket='test', Key='state/objects/'+digest, Body=raw)
        index[path] = {'sha256':digest,'bytes':len(raw)}
    write_manifest(storage,'test',index)
    return {'days':[{'matches':[{'fixture':{'fixture_id':'1','result':scores}}]}]}


class DailyRecoveryTests(unittest.TestCase):
    def test_missing_day_is_recovered_from_immutable_prematch_journal_with_one_call(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            fetch = Mock(return_value=sources(storage, Path(seed)))
            before = previous_manifest(storage,'test')
            digest = prepare(storage,'test',Path(work),DAY,NOW,fetch)
            fetch.assert_called_once_with('2026-10-30', 'test')
            self.assertEqual(previous_manifest(storage,'test'),before)
            # A newer Saturday scan can commit while offline preparation is running.
            new_scan = {LATER:{'sha256':'a'*64,'bytes':2},PROGRESS:{'sha256':'b'*64,'bytes':3}}
            write_manifest(storage, 'test', {**before, **new_scan})
            public = install(storage,'test',Path(dest),DAY,digest,NOW)
            self.assertTrue(public['available'])
            self.assertEqual(public['daily']['wins'],1)
            self.assertEqual(public['all_results'][0]['odd'],2.75)
            self.assertEqual(public['all_results'][0]['captured_at'],'2026-10-30T14:00:00+01:00')
            after = previous_manifest(storage,'test')
            self.assertEqual({p:after[p] for p in new_scan}, new_scan)
            self.assertEqual(source_set(after,DAY), source_set(before,DAY))
            self.assertIn(report_path(DAY), after)
            self.assertEqual(prepare(storage,'test',Path(work),DAY,NOW,fetch),'existing')
            with TemporaryDirectory() as retry:
                self.assertEqual(install(storage,'test',Path(retry),DAY,'existing',NOW), public)
            self.assertEqual(fetch.call_count,1)

    def test_recovery_reproduces_the_normal_frozen_daily_report(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            normal = Path(seed)
            official = sources(storage,normal)
            # The legacy master fixture may contain only multimarket rows.
            fetch = lambda *args: official
            digest = prepare(storage,'test',Path(work),DAY,NOW,fetch)
            install(storage,'test',Path(dest),DAY,digest,NOW)
            ingest(normal)
            archive(normal, NOW)
            settle_day(normal, DAY, NOW, fetch)
            self.assertEqual(read_store(Path(dest)/report_path(DAY)), read_store(normal/report_path(DAY)))

    def test_changed_source_or_corrupt_preparation_leaves_official_pointer_alone(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            fetch = Mock(return_value=sources(storage,Path(seed)))
            digest = prepare(storage,'test',Path(work),DAY,NOW,fetch)
            original = previous_manifest(storage,'test')
            write_manifest(storage,'test',{**original, JOURNAL:{'sha256':'0'*64,'bytes':12}})
            pointer = storage.objects['state/current.json']
            with self.assertRaisesRegex(ValueError,'sources changed'):
                install(storage,'test',Path(dest),DAY,digest,NOW)
            self.assertEqual(storage.objects['state/current.json'],pointer)
            storage.objects['audit-recovery/'+digest+'.json.gz'] = b'corrupt'
            with self.assertRaisesRegex(ValueError,'checksum'):
                install(storage,'test',Path(dest),DAY,digest,NOW)
            self.assertEqual(storage.objects['state/current.json'],pointer)

    def test_empty_official_results_are_never_published_as_a_completed_report(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            sources(storage,Path(seed))
            before = storage.objects['state/current.json']
            with self.assertRaisesRegex(ValueError,'No verified outcomes'):
                prepare(storage,'test',Path(work),DAY,NOW,lambda *args: {'days':[]})
            self.assertEqual(storage.objects['state/current.json'],before)
