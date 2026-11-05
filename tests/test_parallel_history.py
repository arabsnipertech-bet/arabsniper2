"""The bounded historical execution must produce the same reports as serial audit."""
from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'auditor'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
import current_audit
from audit_storage import read_store, write_store
from test_audit_ledger import forecast


class ParallelHistory(unittest.TestCase):
    def test_shared_content_parts_have_independent_atomic_temporaries(self):
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from audit_storage import atomic_bytes
        barrier = Barrier(8)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'shared.json.gz'
            data = b'shared audit bytes'*10000
            def writer(_):
                barrier.wait()
                atomic_bytes(path, data)
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(writer, range(32)))
            self.assertEqual(path.read_bytes(), data)
            self.assertEqual(list(Path(folder).glob('*.tmp')), [])

    def test_failed_worker_never_publishes_a_combined_report(self):
        now = datetime.fromisoformat('2026-11-10T01:00:00+01:00')
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            write_store(root/'auditordata/prematch_2026-10-20.json',
                        {'audit_date':'2026-10-19','fixtures':{}})
            report = root/'data/arab_quote_prematch_report.json'
            write_store(report, {'previous':True})
            with self.assertRaises(ValueError):
                current_audit.settle(root, now, workers=2)
            self.assertEqual(read_store(report), {'previous':True})

    def test_parallel_reports_equal_serial_and_keep_prepared_day(self):
        now = datetime.fromisoformat('2026-11-10T01:00:00+01:00')
        with tempfile.TemporaryDirectory() as left, tempfile.TemporaryDirectory() as right:
            roots = [Path(left), Path(right)]
            for root in roots:
                for day, fid in [('2026-10-20','1'), ('2026-10-21','2')]:
                    row = forecast(fid, captured=day+'T10:00:00+02:00')
                    row['fixture']['kickoff'] = day+'T18:00:00+02:00'
                    row['settlement'] = {'outcomes': {'over15_ht': True}, 'status':'FT'}
                    write_store(root/f'auditordata/prematch_{day}.json',
                                {'schema_version':1, 'audit_date':day, 'fixtures':{fid:row}})
            serial = current_audit.settle(roots[0], now)
            parallel = current_audit.settle(roots[1], now, workers=2)
            self.assertEqual(serial, parallel)
            for path in ['data/arab_quote_prematch_report.json', 'data/arab_quote_multimarket_report.json',
                         'auditarchive/prematch_2026-10-20_daily.json',
                         'auditarchive/prematch_2026-10-21_daily.json']:
                self.assertEqual(read_store(roots[0]/path), read_store(roots[1]/path))
            # A date already certified by the priority phase is consumed without rebuilding it.
            day = datetime.fromisoformat('2026-10-21').date()
            saved = (roots[1]/'auditarchive/prematch_2026-10-21_daily.json').read_bytes()
            current_audit.settle(roots[1], now, prepared_date=day, workers=2)
            self.assertEqual((roots[1]/'auditarchive/prematch_2026-10-21_daily.json').read_bytes(), saved)
