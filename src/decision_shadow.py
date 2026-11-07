"""Read-only, single-hypothesis decisions on the same authentic prematch rows.

No training or network calls; no result is accepted as a decision input. Fair
probabilities use the frozen base sensitivity offsets (not invented certainty).
An absent stage excludes the whole paired snapshot rather than becoming zero.
"""
from copy import deepcopy
from hashlib import sha256
import json
from adaptive_calibration import number
from adaptive_pricing import BASE_FIELDS, reprice
from multimarket_model import stamp
from opportunity_policy import POLICY_ID, annotate, goal_market
from platinum_policy import POLICY_ID as PLATINUM_ID, evaluate as platinum_evaluate

VERSION='decision-shadow-v1'
PATHS=('market_fair','poisson_base','favorite_anchor_only','calibrated_v51')
FIELDS=('id','spec','family','period','quality','statistical_pct','low_pct','high_pct','win_weight','loss_weight',
        'fair_odd','sensitivity_weights','odd','ev_pct','ev_low_pct','edge_pp','oci','selected_margin_pct',
        'selected_margin_delta_pp','quote_status','quote_executable','quote_adapter_version','bookmaker_id',
        'selected_movement','delta_pct','first_odd','bookmaker','paired_books','agreeing_books',
        'movement_confirmed','fair_direction_coherent','margin_band','cache_source_at','cache_expires_at',
        'pure_poisson','calibration_base','calibration','goal_evidence','ht_evidence','market_fair_pct',
        'selected_market_fair_pct','value_profile','category','label')

def compact(row):
    return {k:deepcopy(row[k]) for k in FIELDS if k in row}

def stage(row, name):
    original=row.get('pure_poisson') or row.get('calibration_base') or row
    if name=='poisson_base': return original
    if name=='favorite_anchor_only': return row.get('calibration_base') or row
    if name=='calibrated_v51': return row
    p=row.get('market_fair_pct')
    if not number(p) or not 0 < p < 100: return None
    p/=100
    base=original.get('win_weight')
    if not number(base):return None
    scenarios=[min(1.,max(0.,w+p-base)) for w,l in original.get('sensitivity_weights',[[base,1-base]])]
    return dict(win_weight=p,loss_weight=1-p,statistical_pct=100*p,fair_odd=1/p,
                low_pct=100*min([p,*scenarios]),high_pct=100*max([p,*scenarios]),
                sensitivity_weights=[[q,1-q] for q in scenarios])

def decisions(matrix):
    rows=matrix['rows']
    return dict(first=next((r['id'] for r in rows if r['opportunity'].get('rank')==1),None),
        rows=[dict(id=r['id'],spec=r['spec'],odd=r.get('odd'),probability_pct=r.get('statistical_pct'),
                   ev_pct=r.get('ev_pct'),ev_low_pct=r.get('ev_low_pct'),**r['opportunity']) for r in rows])

def capture(assessment, meta, as_of, prior_stability=None):
    at,start=stamp(as_of),stamp(meta.get('kickoff'))
    if not at or not start or at>=start or meta.get('result') or meta.get('status_short') not in ('NS','TBD'):
        return None
    source=assessment.get('matrix') or {}
    if (source.get('opportunity_policy') or {}).get('id')!=POLICY_ID:
        return dict(version=VERSION,available=False,reason='unsupported_policy_version')
    rows=[compact(r) for r in source.get('rows',[]) if goal_market(r)]
    context=deepcopy(source.get('goal_context') or {})
    # Never seed a counterfactual with the winner selected at the same instant.
    prior=deepcopy(prior_stability or {})
    evidence=dict(rows=rows,goal_context=context,prior_stability=prior,as_of=as_of,fixture_id=str(meta.get('fixture_id')))
    digest=sha256(json.dumps(evidence,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    out=dict(version=VERSION,policy_id=POLICY_ID,platinum_policy_id=PLATINUM_ID,captured_at=as_of,
             evidence_sha256=digest,available=True,paths={},shadow_only=True,
             uncertainty_protocol='frozen_base_offsets_for_market_fair', prior_state_available=prior_stability is not None)
    # A missing fair quote on an unpriced row is harmless: it cannot be chosen.
    missing=[r['id'] for r in rows if r.get('quote_status') in ('recent','cached_stable')
             and number(r.get('odd')) and r['odd']>1 and any(stage(r,n) is None for n in PATHS)]
    if missing:
        out.update(available=False,reason='incomplete_paired_stages',missing=missing)
        return out
    for name in PATHS:
        matrix=dict(rows=deepcopy(rows),goal_context=deepcopy(context),proposal_stability=deepcopy(prior))
        for r in matrix['rows']:
            values=stage(r,name)
            if values is not None:
                r.update({k:deepcopy(values[k]) for k in BASE_FIELDS if k in values})
                if name!='calibrated_v51': r.pop('calibration',None)
                reprice(r)
        annotate(matrix,as_of)
        out['paths'][name]=decisions(matrix)
        if name=='calibrated_v51': baseline=matrix
    ordinary=dict(rows=deepcopy(baseline['rows']),goal_context=deepcopy(context),proposal_stability=deepcopy(prior))
    annotate(ordinary,as_of,automatic_upgrade=False)
    out['gold_no_automatic_upgrade']=decisions(ordinary)
    out['gold_first_changed']=out['paths']['calibrated_v51']['first']!=out['gold_no_automatic_upgrade']['first']
    platinum=[]
    for r in rows:
        published=platinum_evaluate(r,as_of)
        positive=all(number(r.get(k)) and r[k]>=0 for k in ('ev_pct','ev_low_pct'))
        platinum.append(dict(id=r['id'],spec=r['spec'],odd=r.get('odd'),ev_pct=r.get('ev_pct'),ev_low_pct=r.get('ev_low_pct'),
            flow_value_group='nonnegative' if number(r.get('ev_pct')) and r['ev_pct']>=0 else 'negative_or_unknown',
            baseline_eligible=published['eligible'],shadow_eligible=published['eligible'] and positive,
            shadow_reasons=published['reasons']+([] if positive else ['nonnegative_ev_and_prudent_ev_required'])))
    out['platinum_nonnegative_ev']=platinum
    return out

class DecisionAudit:
    """Bounded report: first/latest decisions plus counts over all snapshots."""
    def __init__(self):
        self.first={};self.latest={};self.seen=set();self.unavailable=0;self.changed=0
    def observe(self,record):
        a=record.get('multimarket') or {};meta=record.get('fixture') or {}
        at,start=stamp(record.get('captured_at')),stamp(meta.get('kickoff'))
        if record.get('retrospective') or not at or not start or at>=start:return
        frozen=(a.get('matrix') or {}).get('decision_shadow')
        mode='frozen_prematch'
        if not frozen:
            # Explicit retrospective replay; never rewrites official categories.
            frozen=capture(a,meta,record.get('captured_at'))
            mode='retrospective_controlled_replay_without_prior_state'
        if not frozen or not frozen.get('available'):
            self.unavailable+=1;return
        fid=str(meta.get('fixture_id'));key=(fid,record['captured_at'],frozen['evidence_sha256'])
        if key in self.seen:return
        self.seen.add(key);self.changed+=bool(frozen.get('gold_first_changed'))
        from daily_audit import fingerprint
        item=dict(fixture_id=fid,captured_at=record['captured_at'],mode=mode,forecast_sha256=fingerprint(record),
                  decisions=frozen,result=(record.get('settlement') or {}).get('result') or {})
        rank=(at,item['forecast_sha256'])
        if fid not in self.first or rank<self.first[fid][0]:self.first[fid]=(rank,item)
        if fid not in self.latest or rank>self.latest[fid][0]:self.latest[fid]=(rank,item)
    def report(self):
        from lateral_matrix import settle_spec
        from calibration_benchmark import metrics
        summaries={};snapshots=[]
        for label,items in [('first',self.first),('latest',self.latest)]:
            paths={n:[] for n in PATHS}
            for _,item in items.values():
                snapshots.append(dict(snapshot=label,**item))
                for name,choice in item['decisions']['paths'].items():
                    row=next((r for r in choice['rows'] if r['id']==choice['first']),None)
                    if not row:continue
                    payout=settle_spec(row['spec'],item['result'])
                    if payout not in ((1.,0.),(0.,1.)):continue
                    paths[name].append(dict(fid=item['fixture_id'],odd=row['odd'],y=int(payout[0]),p=row['probability_pct']/100))
            summaries[label]={n:metrics(rows,'p') for n,rows in paths.items()}
        return dict(version=VERSION,policy_id=POLICY_ID,snapshot_count=len(self.seen),unavailable_snapshots=self.unavailable,
                    gold_no_upgrade_changed_first=self.changed,summaries=summaries,snapshots=snapshots,
                    social_publication='not_inferred_from_eligibility; requires delivery receipts',
                    note='Retrospective replay is distinct from frozen official Gold certificates. Overlapping snapshots are not independent.')
