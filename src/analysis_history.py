"""Append-only prematch analysis updates, separate from immutable first snapshots."""
from copy import deepcopy
from multimarket_model import stamp


def freeze_update(store, meta, assessment, as_of):
    now, kickoff = stamp(as_of), stamp(meta.get('kickoff'))
    if not now or not kickoff or now>=kickoff or meta.get('result'): return
    version=assessment['model_id']; fid=str(meta['fixture_id'])
    key=f'{version}::{fid}::{as_of}'
    previous=[r for r in store.get('analysis_updates',{}).values()
              if r.get('model_version')==version and str(r.get('fixture',{}).get('fixture_id'))==fid]
    previous.sort(key=lambda r:r['captured_at'])
    pick=assessment.get('best_candidate') or {}
    old=(previous[-1]['multimarket'].get('best_candidate') or {}) if previous else {}
    reason='prima_analisi' if not previous else 'proposta_cambiata' if (pick.get('market'),pick.get('selection'))!=(old.get('market'),old.get('selection')) else 'aggiornamento_dati_e_quote'
    store.setdefault('analysis_updates',{}).setdefault(key,deepcopy({
        'fixture':meta,'captured_at':as_of,'model_version':version,
        'update_reason':reason,'multimarket':assessment}))


def latest_store(store):
    latest={}
    for record in store.get('analysis_updates',{}).values():
        captured=stamp(record.get('captured_at')); kickoff=stamp(record.get('fixture',{}).get('kickoff'))
        if not captured or not kickoff or captured>=kickoff: continue
        key=f"{record['model_version']}::{record['fixture']['fixture_id']}"
        if key not in latest or captured>stamp(latest[key]['captured_at']): latest[key]=record
    return {'multimarket_evaluations':latest}
