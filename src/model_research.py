"""Reproducible development comparison; no outcome-based parameter search.

Run after scans. Only the separately archived, pre-kickoff inputs of 18 October
are reconstructed. Such reconstructions are explicitly retrospective, never
added to the immutable live/prospective prediction ledger.
"""
from copy import deepcopy
import json
import argparse
from pathlib import Path
from urllib.request import urlopen
from multimarket_model import evaluate, freeze_evaluations, stamp
from multimarket_audit import build_report, settle

ROOT=Path(__file__).resolve().parents[1]
SOURCES={'morning':'998d6542dfe5db02cd159dd804abd8d6d58d06d1',
         'midday':'c481abe9977c6408a8641d81ade58ad7c57b935d'}


def development_inputs(frozen, snapshots):
    """Reconstruct only from already published Git snapshots, in memory.

    No individual development archive is republished. Match exact probabilities
    to the original immutable forecast, and reject snapshots taken too late.
    """
    indexed={}
    for label,payload in snapshots.items():
        for day in payload.get('days',[]):
            for match in day.get('matches',[]):
                indexed.setdefault(str(match['fixture']['fixture_id']),[]).append((label,payload['generated_at'],match))
    records=[]
    for fid,record in frozen.get('fixtures',{}).items():
        meta=record.get('fixture',{})
        if meta.get('date')!='2026-10-18': continue
        captured,kickoff=stamp(record.get('captured_at')),stamp(meta.get('kickoff'))
        if not captured or not kickoff or captured>=kickoff: continue
        candidates=[(label,at,match) for label,at,match in indexed.get(str(fid),[])
            if stamp(at) and stamp(at)<=captured and stamp(at)<kickoff
            and match.get('model',{}).get('probabilities_raw')==record.get('probabilities_raw')]
        if not candidates: continue
        label,at,match=sorted(candidates,key=lambda item:item[1])[0]
        model={k:deepcopy(match['model'][k]) for k in ('lambdas','reliability_pct','favorite_trap_pattern') if k in match['model']}
        model.update({k:deepcopy(record.get(k,{})) for k in ('probabilities_raw','probabilities_calibrated')})
        book={'markets':{k:{'current':deepcopy(v.get('current',{})),
            'selections':{s:{'oci_as':deepcopy(value.get('oci_as',{}))} for s,value in v.get('selections',{}).items()}}
            for k,v in record.get('quote_observation',{}).items()}}
        records.append({'input':{'fixture':deepcopy(meta),'model':model,
            'statistics':deepcopy(match.get('statistics',{})),'book':book},
            'captured_at':record['captured_at'],'statistics_at':at,
            'provenance':{'public_commit':SOURCES[label],'path':'data/arab_quote_dashboard.json'},
            'result':deepcopy(record.get('settlement',{}).get('result',{}))})
    return {'records':records,'sources':SOURCES}


def research(dataset):
    store={};rejected=[]
    for record in dataset.get('records',[]):
        match=deepcopy(record['input'])
        kickoff=stamp(match['fixture'].get('kickoff'))
        captured=stamp(record.get('captured_at'))
        source=stamp(record.get('statistics_at'))
        if not kickoff or not captured or not source or source>captured or captured>=kickoff:
            rejected.append(match['fixture'].get('fixture_id'));continue
        match['multimarket']=evaluate(match,record['captured_at'])
        for market in match['multimarket'].get('markets',[]):
            for label,field in [('legacy_raw','probabilities_raw'),('legacy_calibrated','probabilities_calibrated')]:
                p=match['model'].get(field,{}).get(market['market'],{}).get(market['selection'])
                if p is not None:
                    market['probabilities'][label]=p
        temporary={}
        freeze_evaluations(temporary,{'days':[{'matches':[match]}]},record['captured_at'])
        for key,frozen in temporary.get('multimarket_evaluations',{}).items():
            frozen['retrospective']=True
            frozen['input_provenance']=record.get('provenance')
            store.setdefault('multimarket_evaluations',{})[key]=frozen
        settle(store,{str(match['fixture']['fixture_id']):record['result']})
    report=build_report(store)
    labels={'1x2_ft::1':'Esito 1 (90 minuti)','over15_ht::over':'Over 1,5 PT',
            'gg_ht::yes':'GG PT sì','over25_ft::over':'Over 2,5 finale'}
    comparisons=[]
    for key,label in labels.items():
        groups=[g for g in report['groups'] if g['market_selection']==key and g['group']=='matched_benchmark']
        row={'label':label,'market_selection':key,'n':groups[0]['n'] if groups else 0}
        row.update({g['variant']:g['brier'] for g in groups})
        comparisons.append(row)
    report['summary']={'fixtures':len(store.get('multimarket_evaluations',{})),
        'rejected_input_count':len(rejected),'benchmarks':comparisons,
        'assessment':'Retrospective development only. Do not choose weights from this table; validate frozen predictions on subsequent dates.',
        'historical_price_limitation':'Only aggregated prices archived on development day; no operator-specific ROI for newly added markets.'}
    report['sources']=dataset.get('sources',{})
    return report


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--snapshot-dir',type=Path,help='Optional cache of already public Git snapshots, named by commit SHA')
    args=parser.parse_args()
    snapshots={}
    for label,sha in SOURCES.items():
        cached=args.snapshot_dir/f'{sha}.json' if args.snapshot_dir else None
        if cached and cached.exists():
            snapshots[label]=json.loads(cached.read_text())
        else:
            url=f'https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/{sha}/data/arab_quote_dashboard.json'
            with urlopen(url,timeout=60) as response:
                snapshots[label]=json.load(response)
    from audit_ledger import AuditLedger
    with AuditLedger(ROOT/'data/arab_quote_prematch_audit.json') as frozen:
        dataset=development_inputs(frozen,snapshots)
    report=research(dataset)
    target=ROOT/'data/arab_quote_model_research.json'
    temporary=target.with_suffix('.tmp')
    temporary.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    temporary.replace(target)
    print(json.dumps(report['summary'],ensure_ascii=False))


if __name__=='__main__': main()

