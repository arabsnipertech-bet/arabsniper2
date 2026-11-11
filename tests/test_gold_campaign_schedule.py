import importlib.util
from pathlib import Path
import unittest
from datetime import datetime
spec=importlib.util.spec_from_file_location('campaign',Path(__file__).resolve().parents[1]/'scripts/gold-campaign-repeat.py')
campaign=importlib.util.module_from_spec(spec);spec.loader.exec_module(campaign)
class Schedule(unittest.TestCase):
    def test_bounded_dates_and_rome_slots(self):
        def slot(s):return campaign.selected(datetime.fromisoformat(s))
        self.assertIsNone(slot('2026-11-11T11:45:00+00:00'))
        self.assertIsNone(slot('2026-11-19T11:45:00+00:00'))
        self.assertIsNone(slot('2026-11-12T11:44:59+00:00'))
        self.assertEqual(slot('2026-11-12T11:45:00+00:00'),('2026-11-12',0,'pranzo'))
        self.assertEqual(slot('2026-11-18T19:45:00+00:00'),('2026-11-18',1,'sera'))
        self.assertEqual(slot('2026-11-12T12:15:00+00:00'),('2026-11-12',0,'pranzo'))
        self.assertIsNone(slot('2026-11-12T14:15:00+00:00'))
    def test_distinct_slots_and_consistent_terms(self):
        keys=set()
        for day in range(12,19):
            for hour in [11,19]:
                value=campaign.selected(datetime.fromisoformat(f'2026-11-{day}T{hour}:45:00+00:00'))
                keys.add((value[0],value[2]))
                text=campaign.message(value[0],value[1])
                for required in ['/prova','48 ore','10%','4,50','13,50','26,10','senza rinnovo automatico']:
                    self.assertIn(required,text)
        self.assertEqual(len(keys),14)
    def test_outside_period_never_calls_remote(self):
        campaign.ops.request=lambda *a,**k:self.fail('Unexpected remote call')
        campaign.run(datetime.fromisoformat('2026-11-19T19:45:00+00:00'))
if __name__=='__main__':unittest.main()
