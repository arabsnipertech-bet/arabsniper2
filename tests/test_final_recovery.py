import json,sys,unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from audit_storage import write_store,read_store
from recover_external_feeds import repair

class Recovery:
    report={'requests':1,'errors':0,'markets_recovered':1}
    def apply(self,m,now):
        for r in m['multimarket']['markets']:
            if r.get('odd') is None:r.update(odd=2.,quote={'indicative':True})

class Tests(unittest.TestCase):
    def test_final_feed_and_dashboard_keep_statistics_and_existing_quotes(self):
        with TemporaryDirectory() as folder:
            root=Path(folder); path=root/'data/arab_quote_day1.json'
            day={'day':1,'date':'2026-10-24','matches':[{'fixture':{'fixture_id':1},'multimarket':{'markets':[{'market':'over25_ft','selection':'over','odd':None,'statistical_pct':58.4},{'market':'over15_ht','selection':'over','odd':3.1,'statistical_pct':40}]}}]}
            write_store(path,day);write_store(root/'data/arab_quote_dashboard.json',{'days':[day]})
            changed=repair(root,Recovery());saved=read_store(path)
            self.assertEqual(saved['matches'][0]['multimarket']['markets'][0]['odd'],2.)
            self.assertEqual(saved['matches'][0]['multimarket']['markets'][0]['statistical_pct'],58.4)
            self.assertEqual(saved['matches'][0]['multimarket']['markets'][1]['odd'],3.1)
            self.assertEqual(read_store(root/'data/arab_quote_dashboard.json')['days'][0],saved)
            self.assertFalse(repair(root,Recovery()))

if __name__=='__main__':unittest.main()
