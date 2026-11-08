from datetime import datetime, timedelta, date
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import read_store, write_store
from auditor_daily_priority import ensure_priority, build_yesterday, STATUS, WEIGHTS
from nightly_auditor import run, PROGRESS
from auditor_schedule import MARKER
from audit_journal import PREFIX
from test_audit_ledger import forecast
from test_auditor_history_chunks import seed
from auditor_history_chunks import advance

NOW = datetime.fromisoformat('2026-11-06T00:40:00+01:00')

class Priority(unittest.TestCase):
    def test_yesterday_reads_forecasts_and_policy_observations_without_consuming_journals(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'API_SPORTS_KEY':'test'}):
            root=Path(folder); day=date(2026,11,5)
            first=forecast('1','2026-11-05T10:00:00+01:00')
            first['fixture']['kickoff']='2026-11-05T18:00:00+01:00'
            later=forecast('1','2026-11-05T11:00:00+01:00'); later['fixture']=first['fixture'].copy()
            key='context-goals-v3.4::1::2026-11-05T11:00:00+01:00'
            write_store(root/'data'/f'{PREFIX}20261105T090000000000_forecasts.json',
                        {'schema_version':1,'journal_type':'forecasts','fixtures':{'1':first}})
            write_store(root/'data'/f'{PREFIX}20261105T100000000000_policies.json',
                        {'schema_version':1,'journal_type':'policies','analysis_updates':{key:later}})
            frozen={p:p.read_bytes() for p in (root/'data').glob('*.json')}
            def results(*_):
                return {'days':[{'matches':[{'fixture':{'fixture_id':'1','result':{
                    'status':'FT','home_ht':1,'away_ht':1,'home_ft':2,'away_ft':1}}}]}]}
            build_yesterday(root,day,NOW,results)
            data=read_store(root/f'auditordata/prematch_{day}.json')
            self.assertEqual(data['fixtures']['1'],first)
            self.assertIn(key,data['analysis_updates'])
            self.assertTrue((root/f'auditarchive/prematch_{day}_calibration.json').exists())
            for path,raw in frozen.items(): self.assertEqual(path.read_bytes(),raw)
            original=(root/f'auditordata/prematch_{day}.json').read_bytes()
            build_yesterday(root,day,NOW,results)
            self.assertEqual((root/f'auditordata/prematch_{day}.json').read_bytes(),original)

    def test_priority_saved_before_old_history_and_never_certifies_full_history(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ,{'GOLD_PRIVATE':'true'}):
            root=Path(folder); calls=[]
            write_store(root/PROGRESS,{'version':1,'work_date':'2026-11-05','stage':'daily'})
            write_store(root/'auditarchive/prematch_2026-11-04_daily.json',{'audit_date':'2026-11-04'})
            old=(root/PROGRESS).read_bytes()
            def runner(root,command,deadline,clock):
                args=command[1:]; calls.append(args)
                if args[0]=='src/auditor_daily_priority.py':
                    write_store(root/'auditarchive/prematch_2026-11-05_daily.json',{'audit_date':'2026-11-05'})
                if args[0]=='src/adaptive_calibration.py': write_store(root/WEIGHTS,{'as_of':'2026-11-06'})
            def history(*args):
                self.assertTrue((root/STATUS).exists())
                self.assertEqual((root/PROGRESS).read_bytes(),old)
                self.assertIn(['src/gold_storage.py','checkpoint-day','--date','2026-11-05'],calls)
                self.assertEqual(read_store(root/WEIGHTS)['as_of'],'2026-11-06')
                return False
            with patch('auditor_history_chunks.advance', side_effect=history):
                self.assertEqual(run(root,lambda:NOW,runner),'paused')
            self.assertFalse((root/MARKER).exists())
            calls.clear()
            ensure_priority(root,NOW.date(),lambda *a:calls.append(a),lambda:calls.append('calibrate'))
            self.assertEqual(calls,[('src/homepage_results.py',),('src/private_publication.py','--results-only')])

    def test_stale_calibration_cannot_publish_priority_success(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); writes=[]
            write_store(root/'auditarchive/prematch_2026-11-05_daily.json',{'audit_date':'2026-11-05'})
            write_store(root/WEIGHTS,{'as_of':'2026-11-05'})
            with self.assertRaisesRegex(ValueError,'not updated'):
                ensure_priority(root,NOW.date(),lambda *a:writes.append(a),lambda:None)
            self.assertFalse((root/STATUS).exists())
            self.assertNotIn(('src/gold_storage.py','publish'),writes)

    def test_available_time_allows_more_than_two_history_dates(self):
        import current_audit
        now=datetime.fromisoformat('2026-11-10T00:40:00+01:00')
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); seed(root); days=[]
            def phase(*args):
                if args[0]=='src/gold_storage.py': return
                if args[1]=='daily':
                    days.append(args[3]); current_audit.settle_day(root,date.fromisoformat(args[3]),now)
                else:
                    current_audit.settle(root,now,prepared_date=date.fromisoformat(args[3]),assemble_only=True)
            self.assertTrue(advance(root,now.date(),phase,now+timedelta(minutes=45),lambda:now))
            self.assertEqual(len(days),3)
