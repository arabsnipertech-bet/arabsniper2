from copy import deepcopy
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
import gold_explorer as e
from gold_browser_feed import project
from selection_display_facts import selection_facts
from test_gold_explorer import fixture, resign, Store, AT, KEY

spec = importlib.util.spec_from_file_location('compact_facts',ROOT/'scripts/explorer-compact-facts.py')
script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(script)


class SavedNumericalReferences(unittest.TestCase):
    def test_native_period_values_and_missing_data_are_never_market_estimates_or_zero_fills(self):
        snapshot={'statistics':{'home_ht':.9,'away_ht':.78,'home_st':1.1,'away_st':.8,'home_ft':2.,'away_ft':1.58},'rates':{'home_ht':8.,'away_ht':9.}}
        record={'features':{'margin_pct':0},'confirmation':{'opening_delta_pct':-3.2}}
        before=deepcopy((snapshot,record))
        facts=selection_facts({'kind':'total','period':'ht'},record,snapshot)
        self.assertEqual((facts['lambda_home'],facts['lambda_away'],facts['margin_pct']),(.9,.78,0))
        self.assertEqual(facts['opening_delta_pct'],-3.2)
        joint=selection_facts({'kind':'halves_total','period':'ft'},record,snapshot)
        self.assertAlmostEqual(joint['lambda_pt'],1.68)
        self.assertAlmostEqual(joint['lambda_st'],1.9)
        self.assertNotIn('lambda_home',selection_facts({'kind':'total','period':'ht'},record,{'rates':snapshot['rates']}))
        self.assertEqual((snapshot,record),before)

    def source(self):
        match=fixture()
        snap=match['multimarket']['matrix']['v7']
        snap['statistics']={'home_ft':1.5,'away_ft':1.2}
        snap['markets'][KEY]['features']['margin_pct']=5.2
        resign(match)
        rows=e.project_match(match,AT)
        for row in rows:row.pop('selection_facts',None)
        return match,rows

    def test_attachment_requires_the_same_frozen_snapshot_and_all_original_price_values(self):
        match,rows=self.source();support=script.saved_facts([match]);before=deepcopy(rows)
        enriched=script.attach(rows,support)
        self.assertEqual(enriched[0]['selection_facts']['lambda_home'],1.5)
        self.assertEqual(rows,before)
        for change in ({'forecast_sha256':'f'*64},{'probability':.01},{'odd':9.}):
            changed=[{**rows[0],**change}]
            self.assertNotIn('selection_facts',script.attach(changed,support)[0])

    def test_enrichment_roundtrip_preserves_gold_history_generation_and_all_original_fields(self):
        store=Store();match,rows=self.source()
        index=project({'date':'2026-11-17','matches':[match]},lambda k,v:store.data.__setitem__(k,v))
        store.data['arab_quote_model_research.json']=json.dumps({'_gold_browser_days':{'arab_quote_day1.json':index}}).encode()
        gold={k:v for k,v in store.data.items() if not k.startswith('explorer/')}
        original={'count':len(rows),'generated_at':AT.isoformat(),'phase':'fast','pages':e.pages(store,'bucket',rows)}
        e.put(store,'bucket','explorer/current.json',original)
        e.put(store,'bucket','explorer/days/2026-11-17.json',{'certificate':'unchanged'})
        history=store.data['explorer/days/2026-11-17.json']
        with redirect_stdout(io.StringIO()) as output:script.enrich(store,'bucket')
        current=store.read('current.json')
        enriched=[r for h in current['pages'] for r in store.read('objects/'+h+'.json')['rows']]
        self.assertEqual(script.core(enriched),script.core(rows))
        self.assertEqual(enriched[0]['selection_facts']['lambda_away'],1.2)
        self.assertEqual(current['generated_at'],original['generated_at'])
        self.assertEqual(store.data['explorer/days/2026-11-17.json'],history)
        self.assertEqual({k:store.data[k] for k in gold},gold)
        self.assertEqual(json.loads(output.getvalue())['original_values_changed'],0)


if __name__ == '__main__':
    unittest.main()
