from datetime import datetime, timedelta, date
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'auditor'))
import current_audit
from auditor_history_chunks import advance, reusable, receipt_path
from audit_storage import read_store, write_store
from test_audit_ledger import forecast

NOW = datetime.fromisoformat('2026-11-10T00:40:00+01:00')


def seed(root):
    for number in range(20, 23):
        day = f'2026-10-{number}'
        row = forecast(str(number), captured=day+'T10:00:00+02:00')
        row['fixture']['kickoff'] = day+'T18:00:00+02:00'
        row['settlement'] = {'outcomes': {'over15_ht': True}, 'status': 'FT'}
        write_store(root/f'auditordata/prematch_{day}.json',
                    {'schema_version': 1, 'audit_date': day, 'fixtures': {str(number): row}})


class HistoryChunks(unittest.TestCase):
    def test_time_budget_then_resume_without_repeating_saved_days_and_same_report(self):
        with tempfile.TemporaryDirectory() as folder, tempfile.TemporaryDirectory() as expected:
            root, serial = Path(folder), Path(expected)
            seed(root); seed(serial)
            current_audit.settle(serial, NOW, prepared_date=NOW.date()-timedelta(days=1))
            calls, checkpoints = [], []
            tick = [NOW]
            def phase(*args):
                calls.append(args)
                if args == ('src/gold_storage.py', 'checkpoint'):
                    checkpoints.append({p.relative_to(root): p.read_bytes() for p in root.rglob('*') if p.is_file()})
                    tick[0] += timedelta(minutes=13)
                elif args[1] == 'daily':
                    current_audit.settle_day(root, date.fromisoformat(args[3]), datetime.fromisoformat(args[5]))
                else:
                    current_audit.settle(root, datetime.fromisoformat(args[-1]),
                                         prepared_date=date.fromisoformat(args[3]), assemble_only=True)
            end = NOW+timedelta(minutes=45)
            self.assertFalse(advance(root, NOW.date(), phase, end, lambda: tick[0]))
            self.assertEqual(len(checkpoints), 2)
            self.assertFalse((root/'data/arab_quote_prematch_report.json').exists())
            saved = receipt_path(root, '2026-10-20').read_bytes()
            calls.clear()
            end = tick[0]+timedelta(minutes=45)
            self.assertTrue(advance(root, NOW.date(), phase, end, lambda: tick[0]))
            self.assertEqual([c[3] for c in calls if c[0] == 'auditor/current_audit.py' and c[1] == 'daily'], ['2026-10-22'])
            self.assertEqual(saved, receipt_path(root, '2026-10-20').read_bytes())
            for path in ('data/arab_quote_prematch_report.json', 'data/arab_quote_multimarket_report.json'):
                self.assertEqual(read_store(root/path), read_store(serial/path))

    def test_checkpoint_survives_next_day_failure_and_rejects_corrupt_output(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); seed(root)
            saved = []
            def phase(*args):
                if args[0] == 'src/gold_storage.py':
                    saved.append(receipt_path(root, '2026-10-20').read_bytes())
                elif args[3] == '2026-10-21':
                    raise TimeoutError('interrupted next date')
                else:
                    current_audit.settle_day(root, date.fromisoformat(args[3]), NOW)
            with self.assertRaises(TimeoutError):
                advance(root, NOW.date(), phase, NOW+timedelta(minutes=45), lambda: NOW)
            self.assertEqual(len(saved), 1)
            self.assertTrue(reusable(root, date(2026,10,20), NOW))
            (root/'auditarchive/prematch_2026-10-20_daily.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'changed or missing'):
                reusable(root, date(2026,10,20), NOW)

    def test_short_pause_does_not_start_work_or_claim_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); seed(root); phase=Mock()
            self.assertFalse(advance(root, NOW.date(), phase, NOW+timedelta(minutes=19), lambda: NOW))
            phase.assert_not_called()

    def test_new_frozen_evidence_invalidates_old_receipt(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); seed(root)
            day=date(2026,10,20)
            current_audit.settle_day(root, day, NOW)
            signals = read_store(root/f'auditarchive/prematch_{day}_daily.json')['v7_signals']
            self.assertEqual(signals['radar_sorprese']['persistence_intermediate']['n'], 0)
            self.assertIsNone(signals['colpaccio']['roi_without_three_largest_wins'])
            source=root/'auditordata/prematch_2026-10-20.json'
            value=read_store(source); value['extra_evidence']=True; write_store(source,value)
            self.assertFalse(reusable(root,day,NOW))

class JournalChunks(unittest.TestCase):
    def test_bounded_ingestion_matches_full_and_leaves_unconsumed_journals(self):
        from audit_journal import ingest, PREFIX
        with tempfile.TemporaryDirectory() as one, tempfile.TemporaryDirectory() as two:
            roots=[Path(one),Path(two)]
            for root in roots:
                for n in range(3):
                    at=f'2026-10-20T{10+n}:00:00+02:00'
                    row=forecast(str(n), captured=at)
                    row['fixture']['kickoff']='2026-10-20T18:00:00+02:00'
                    write_store(root/'data'/f'{PREFIX}20261020T{10+n}0000_forecasts.json',
                                {'schema_version':1,'journal_type':'forecasts','updated_at':at,'fixtures':{str(n):row}})
            def partition(root):
                return lambda ledger: current_audit.archive_ledger(root,NOW,ledger,lambda _:None)
            ingest(roots[0],after_forecasts=partition(roots[0]))
            self.assertEqual(ingest(roots[1],after_forecasts=partition(roots[1]),limit=2,partition_delta=True),2)
            self.assertEqual(len(list((roots[1]/'data').glob(PREFIX+'*.json'))),1)
            ingest(roots[1],after_forecasts=partition(roots[1]),limit=2,partition_delta=True)
            for path in ('data/arab_quote_prematch_audit.json','auditordata/prematch_2026-10-20.json'):
                self.assertEqual(read_store(roots[0]/path),read_store(roots[1]/path))

    def test_partial_preparation_saves_and_does_not_settle_until_all_journals_ingested(self):
        from nightly_auditor import run, PROGRESS
        from audit_journal import PREFIX
        from unittest.mock import patch
        import os
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ,{'GOLD_PRIVATE':'true'}), patch('nightly_auditor.ensure_priority'):
            root=Path(folder)
            write_store(root/'data'/f'{PREFIX}20261109T100000_forecasts.json',{'pending':True})
            calls=[]
            def phase(root,cmd,deadline,clock): calls.append(cmd[1:])
            self.assertEqual(run(root,lambda:NOW,phase),'paused')
            self.assertEqual(read_store(root/PROGRESS)['stage'],'preparing')
            self.assertNotIn('daily',[c[1] for c in calls if c[0]=='auditor/current_audit.py'])
            self.assertEqual(calls[-1],['src/gold_storage.py','checkpoint'])
