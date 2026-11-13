import copy
from datetime import date, datetime
from io import BytesIO
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'src'), str(ROOT/'auditor')]
from audit_storage import read_store, write_store
from gold_storage import collect, previous_manifest, sha, write_manifest
from recover_daily import FORECASTS, install, prepare, report_path, source_set
from test_daily_audit import record
from test_gold_storage import Fake
from current_audit import archive, fetch_results, settle_day
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
    def test_delayed_fixtures_recovered_by_id_without_regressing_completed_rows(self):
        def row(fid, status, ft=None):
            return {'fixture': {'id': fid, 'status': {'short': status}},
                    'score': {'halftime': {'home': 0, 'away': 0}, 'fulltime': ft}}

        ft = {'home': 1, 'away': 0}
        responses = [[row(1, 'FT', ft), row(2, 'NS')], [row(2, 'FT', ft), row(3, 'FT', ft), row(4, 'FT', ft)]]
        requests = []
        def provider(request, timeout):
            requests.append(parse_qs(urlparse(request.full_url).query))
            response = BytesIO(json.dumps({'response': responses.pop(0)}).encode())
            response.headers = {'x-ratelimit-requests-limit': '75000', 'x-ratelimit-requests-remaining': '10000',
                                'x-ratelimit-remaining': '20'}
            return response
        with patch('current_audit.urlopen', provider):
            output = fetch_results('2026-10-30', 'test', (1, 2, 3))
        self.assertEqual(requests[1]['ids'], ['2-3'])
        self.assertEqual(requests[1]['timezone'], ['Europe/Rome'])
        results = {r['fixture']['fixture_id']: r['fixture']['result'] for r in output['days'][0]['matches']}
        self.assertEqual(set(results), {'1', '2', '3'})
        self.assertTrue(all(r['status'] == 'FT' and r['home_ft'] == 1 for r in results.values()))

    def test_policy_journals_recover_the_last_published_analysis_and_keep_original_sources(self):
        storage=Fake()
        policy='data/arab_quote_scan_journal_20261030T150000000000_policies.json'
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            root=Path(seed);official=sources(storage,root)
            latest=copy.deepcopy(read_store(root/JOURNAL)['analysis_updates'])
            previous=next(iter(latest.values()))
            previous['captured_at']='2026-10-30T16:00:00+01:00'
            previous['multimarket']['markets'][0]['odd']=3.15
            write_store(root/policy,{'schema_version':1,'journal_type':'policies',
                'gold':{'fixtures':{}},'platinum':{'fixtures':{}},
                'analysis_updates':{'lateral-matrix-v5.0::1::'+previous['captured_at']:previous}})
            index=previous_manifest(storage,'test')
            for path,raw in collect(root).items():
                digest=sha(raw);storage.put_object(Bucket='test',Key='state/objects/'+digest,Body=raw)
                index[path]={'sha256':digest,'bytes':len(raw)}
            write_manifest(storage,'test',index)
            original=source_set(index,DAY)
            self.assertIn(policy,original)
            digest=prepare(storage,'test',Path(work),DAY,NOW,lambda *a:official)
            result=install(storage,'test',Path(dest),DAY,digest,NOW)
            self.assertEqual(result['all_results'][0]['odd'],3.15)
            self.assertEqual(result['all_results'][0]['captured_at'],previous['captured_at'])
            after=previous_manifest(storage,'test')
            self.assertEqual({p:after[p] for p in index},index)

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
            self.assertEqual({p:after[p] for p in before}, before)
            for suffix in ('results.json','calibration.json','checkpoint.json'):
                self.assertIn(f'auditarchive/prematch_{DAY}_{suffix}',after)
            self.assertIn(report_path(DAY), after)
            self.assertEqual(prepare(storage,'test',Path(work),DAY,NOW,fetch),'existing')
            with TemporaryDirectory() as retry:
                self.assertEqual(install(storage,'test',Path(retry),DAY,'existing',NOW), public)
            self.assertEqual(fetch.call_count,1)
            refreshed = prepare(storage, 'test', Path(work), DAY, NOW, fetch, refresh=True)
            self.assertNotEqual(refreshed, 'existing')
            self.assertEqual(fetch.call_count, 1)  # Final official scores are reused, not invented or fetched again.
            install(storage, 'test', Path(dest), DAY, refreshed, NOW)

    def test_existing_report_missing_learning_chain_recovers_journal_without_rewriting_predictions(self):
        from audit_ledger import AuditLedger
        from daily_audit import fingerprint
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ, {'API_SPORTS_KEY':'test'}):
            root = Path(seed)
            official = sources(storage, root)
            # Archive before journal ingestion, reproducing the November 5 gap.
            archive(root, NOW)
            settle_day(root, DAY, NOW, lambda *args: official)
            dated = f'auditordata/prematch_{DAY}.json'
            with AuditLedger(root/dated) as ledger:
                original = {(c,k): fingerprint(r) for c,k,r in ledger.records()}
            index = previous_manifest(storage, 'test')
            for path, raw in collect(root).items():
                if path.endswith(('_calibration.json', '_checkpoint.json')): continue
                digest = sha(raw)
                storage.put_object(Bucket='test', Key='state/objects/'+digest, Body=raw)
                index[path] = {'sha256': digest, 'bytes': len(raw)}
            write_manifest(storage, 'test', index)
            fetch = Mock(return_value=official)
            digest = prepare(storage, 'test', Path(work), DAY, NOW, fetch)
            self.assertNotEqual(digest, 'existing')
            install(storage, 'test', Path(dest), DAY, digest, NOW)
            with AuditLedger(Path(dest)/dated) as ledger:
                for (c,k), checksum in original.items():
                    self.assertEqual(fingerprint(ledger[c][k]), checksum)
                self.assertGreater(sum(1 for _ in ledger.records()), len(original))
            self.assertIn(f'auditarchive/prematch_{DAY}_calibration.json', previous_manifest(storage, 'test'))
            fetch.assert_called_once()

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
            recovered = read_store(Path(dest)/report_path(DAY))
            ordinary = read_store(normal/report_path(DAY))
            # Recovery and normal ingestion retain different original labels and byte hashes.
            from auditor_history_chunks import digest as file_digest
            for folder, report in ((Path(dest), recovered), (normal, ordinary)):
                hashes = report['evidence_provenance'].pop('input_files')
                self.assertTrue(hashes)
                for path, checksum in hashes.items():
                    self.assertEqual(checksum, file_digest(folder/path))
            self.assertEqual(recovered, ordinary)

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

    def test_recovers_v7_and_results_when_legacy_is_partial(self):
        from recover_daily import recovery_status
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, TemporaryDirectory() as dest, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            official = sources(storage, Path(seed))
            with patch('recover_daily.recover_legacy', return_value=('partial_v51_skip','deadline')):
                digest = prepare(storage,'test',Path(work),DAY,NOW,lambda *args:official)
            report = install(storage,'test',Path(dest),DAY,digest,NOW)
            self.assertTrue(report['available'])
            self.assertIn(f'auditarchive/prematch_{DAY}_v7.json',previous_manifest(storage,'test'))
            self.assertEqual(read_store(Path(dest)/recovery_status(DAY))['status'],'partial_v51_skip')

    def test_empty_official_results_are_never_published_as_a_completed_report(self):
        storage = Fake()
        with TemporaryDirectory() as seed, TemporaryDirectory() as work, patch.dict(os.environ,{'API_SPORTS_KEY':'test'}):
            sources(storage,Path(seed))
            before = storage.objects['state/current.json']
            with self.assertRaisesRegex(ValueError,'No verified outcomes'):
                prepare(storage,'test',Path(work),DAY,NOW,lambda *args: {'days':[]})
            self.assertEqual(storage.objects['state/current.json'],before)
