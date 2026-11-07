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

VERSION='decision-shadow-v2.2'
FAIR_PROTOCOL='fair_quote_probability_flow_v1'
PATHS=('market_fair','poisson_base','favorite_anchor_only','calibrated_v51')
FIELDS=('id','spec','family','period','quality','statistical_pct','low_pct','high_pct','win_weight','loss_weight',
        'fair_odd','sensitivity_weights','odd','ev_pct','ev_low_pct','edge_pp','oci','selected_margin_pct',
        'selected_margin_delta_pp','quote_status','quote_executable','quote_adapter_version','bookmaker_id',
        'selected_movement','delta_pct','first_odd','bookmaker','paired_books','agreeing_books',
        'movement_confirmed','fair_direction_coherent','margin_band','cache_source_at','cache_expires_at',
        'pure_poisson','calibration_base','calibration','goal_evidence','ht_evidence','market_fair_pct',
        'selected_market_fair_pct','value_profile','category','label','provider_updated_at','quote_observed_at')

def compact(row):
    return {k:deepcopy(row[k]) for k in FIELDS if k in row}

def same_book_fair(row):
    margin, odd = row.get('selected_margin_pct'), row.get('odd')
    if not number(margin) or not 0 <= margin <= 9 or not number(odd) or odd <= 1:
        return None
    value = row.get('selected_market_fair_pct')
    return value if number(value) and 0 < value < 100 else 100 / odd / (1 + margin/100)


def stage(row, name):
    original=row.get('pure_poisson') or row.get('calibration_base') or row
    if name=='poisson_base':
        return None if row.get('_anchor_applied') and not row.get('pure_poisson') else original
    if name=='favorite_anchor_only': return row.get('calibration_base') or row
    if name=='calibrated_v51': return row
    p=same_book_fair(row)
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
        rows=[dict(id=r['id'],spec=r['spec'],odd=r.get('_quoted_odd', r.get('odd')),probability_pct=r.get('statistical_pct'),
                   **({'eligibility_odd':r['odd'], 'quote_basis':FAIR_PROTOCOL,
                       'gross_quote_ev_pct':r['statistical_pct']*r['_quoted_odd']-100} if '_quoted_odd' in r else {}),
                   ev_pct=r.get('ev_pct'),ev_low_pct=r.get('ev_low_pct'),**r['opportunity']) for r in rows])

def capture(assessment, meta, as_of, prior_stability=None):
    at,start=stamp(as_of),stamp(meta.get('kickoff'))
    if not at or not start or at>=start or meta.get('result') or meta.get('status_short') not in ('NS','TBD'):
        return None
    source=assessment.get('matrix') or {}
    if (source.get('opportunity_policy') or {}).get('id')!=POLICY_ID:
        return dict(version=VERSION,available=False,reason='unsupported_policy_version')
    rows=[compact(r) for r in source.get('rows',[]) if goal_market(r)]
    if source.get('lambda_shrinkage'):
        for row in rows: row['_anchor_applied'] = True
    context=deepcopy(source.get('goal_context') or {})
    # Never seed a counterfactual with the winner selected at the same instant.
    prior=deepcopy(prior_stability or {})
    from quote_sources import fresh
    anchor_quote = next((r for r in source.get('rows', [])
        if r.get('quote_status') == 'recent' and fresh(r.get('provider_updated_at'), as_of, meta.get('kickoff'))
        and (r.get('spec') or {}).get('kind') == 'result' and (r.get('spec') or {}).get('period') == 'ft'
        and set(r.get('selected_market_odds') or {}) == {'1','x','2'}), None)
    lambda_inputs = dict(rates=assessment.get('pure_poisson_rates'),
        sensitivities=assessment.get('pure_poisson_sensitivity_rates'),
        quote={k:deepcopy(anchor_quote.get(k)) for k in ('selected_market_odds','bookmaker_id','provider_updated_at')} if anchor_quote else None,
        factor=(source.get('lambda_shrinkage') or {}).get('factor', .15))
    peak=deepcopy(assessment.get('anti_peak_most_prolific'))
    evidence=dict(lambda_inputs=lambda_inputs, rows=rows,goal_context=context,prior_stability=prior,as_of=as_of,fixture_id=str(meta.get('fixture_id')),
                  anti_peak_most_prolific=peak, market_fair_protocol=FAIR_PROTOCOL)
    digest=sha256(json.dumps(evidence,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
    out=dict(version=VERSION,policy_id=POLICY_ID,platinum_policy_id=PLATINUM_ID,captured_at=as_of,
             evidence_sha256=digest,available=True,paths={},shadow_only=True,
             uncertainty_protocol='frozen_base_offsets_for_market_fair', prior_state_available=prior_stability is not None,
             market_fair_protocol=FAIR_PROTOCOL, anti_peak_most_prolific=peak)
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
                if name=='market_fair':
                    r['_quoted_odd']=r.get('odd')
                    r['odd']=values['fair_odd']
                reprice(r)
        annotate(matrix,as_of,comparison_basis=FAIR_PROTOCOL if name=='market_fair' else None)
        out['paths'][name]=decisions(matrix)
        if name=='calibrated_v51': baseline=matrix
    ordinary=dict(rows=deepcopy(baseline['rows']),goal_context=deepcopy(context),proposal_stability=deepcopy(prior))
    annotate(ordinary,as_of,automatic_upgrade=False)
    out['gold_no_automatic_upgrade']=decisions(ordinary)
    out['experiments'] = {'gold_no_automatic_upgrade': decisions(ordinary)}
    for variant in ('divergence_10ft_8ht', 'market_prior_blend', 'over25_floor_185', 'anti_peak_most_prolific'):
        experiment=dict(rows=deepcopy(baseline['rows']), goal_context=deepcopy(context), proposal_stability=deepcopy(prior))
        if variant == 'anti_peak_most_prolific':
            # Older frozen evidence without this statistic cannot test the hypothesis.
            if any(not number(((r.get('goal_evidence') or {}).get('teams', {}).get(side, {}).get(
                    (r.get('spec') or {}).get('period'), {}) or {}).get('without_peak'))
                    for r in experiment['rows'] for side in ('home', 'away')):
                continue
            for row in experiment['rows']:
                for team in (row.get('goal_evidence') or {}).get('teams', {}).values():
                    for period in ('ft', 'ht'):
                        values = team.get(period) or {}
                        if number(values.get('without_peak')):
                            values['without_last'] = values['without_peak']
        if variant == 'market_prior_blend':
            for row in experiment['rows']:
                fair, p = same_book_fair(row), row.get('statistical_pct')
                if fair is None or not number(p): continue
                period = (row.get('spec') or {}).get('period')
                weight = (.45 if period == 'ht' and p > fair else .30
                          if period == 'ft' and abs(p-fair) > 8 and number(row.get('oci')) and row['oci'] < 25 else 0.)
                if not weight: continue
                delta = weight*(fair-p)/100
                center = min(1., max(0., p/100+delta))
                scenarios = [min(1., max(0., w+delta)) for w,l in row.get('sensitivity_weights', [[p/100,1-p/100]])]
                row.update(win_weight=center, loss_weight=1-center, statistical_pct=100*center,
                           fair_odd=1/center if center else None, low_pct=100*min([center,*scenarios]),
                           high_pct=100*max([center,*scenarios]), sensitivity_weights=[[v,1-v] for v in scenarios])
                reprice(row)
        annotate(experiment, as_of, divergence_caps={'ft':10.,'ht':8.} if variant=='divergence_10ft_8ht' else None,
                 over25_floor=1.85 if variant=='over25_floor_185' else None)
        out['experiments'][variant] = decisions(experiment)
    out['experiment_protocol'] = 'one hypothesis per variant; identical published ranking; no production promotion'

    out['gold_first_changed']=out['paths']['calibrated_v51']['first']!=out['gold_no_automatic_upgrade']['first']
    # Total-goal research compares with anchored Poisson, not with calibrated V5.1.
    rates = lambda_inputs['rates']
    prices = anchor_quote.get('selected_market_odds') if anchor_quote else None
    out['lambda_expansion_study'] = dict(available=False, reason='missing_frozen_rates_or_fresh_1x2')
    if rates and prices:
        from lambda_shadow import expanded_rates
        from lateral_matrix import matrix_rows
        factor = (source.get('lambda_shrinkage') or {}).get('factor', .15)
        expanded, details = expanded_rates(rates, prices, factor)
        if not details.get('applied'):
            out['experiments']['lambda_total_expansion'] = deepcopy(out['paths']['favorite_anchor_only'])
        else:
            probe = dict(assessment, rates=expanded, sensitivity_rates=[expanded_rates(r, prices, factor)[0]
                         for r in assessment.get('pure_poisson_sensitivity_rates', [])])
            estimates = {r['id']: r for r in matrix_rows(probe, {r['id']:r['spec'] for r in rows})}
            experiment=dict(rows=deepcopy(rows), goal_context=deepcopy(context), proposal_stability=deepcopy(prior))
            for row in experiment['rows']:
                row.update({k:deepcopy(estimates[row['id']][k]) for k in BASE_FIELDS if k in estimates[row['id']]})
                row.pop('calibration', None)
                for side, team in (row.get('goal_evidence') or {}).get('teams', {}).items():
                    for period in ('ft', 'ht'):
                        if period in team and side+'_'+period in expanded:
                            team[period]['expected_goals'] = expanded[side+'_'+period]
                reprice(row)
            annotate(experiment, as_of)
            out['experiments']['lambda_total_expansion'] = decisions(experiment)
        out['lambda_expansion_study'] = dict(available=True, baseline='favorite_anchor_only',
            production_applied=False, **details)
    platinum=[]
    for r in rows:
        published=platinum_evaluate(r,as_of)
        positive=all(number(r.get(k)) and r[k]>=0 for k in ('ev_pct','ev_low_pct'))
        platinum.append(dict(id=r['id'],spec=r['spec'],odd=r.get('odd'),ev_pct=r.get('ev_pct'),ev_low_pct=r.get('ev_low_pct'),
            flow_value_group='platinum_positive_ev' if positive else 'platinum_flow_only_negative_ev',
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
        from decision_metrics import selected_summary
        from calibration_benchmark import metrics
        aliases = dict(market_fair='market_fair', poisson_base='pure_poisson',
                       favorite_anchor_only='anchored_poisson', calibrated_v51='calibrated_v51')
        summaries, experiments, snapshots, cohorts = {}, {}, [], {}
        protocols, availability = {}, {}
        for label, items in [('first', self.first), ('latest', self.latest)]:
            paths={}; counts={}; differences={}; pending={}; platinum={}
            protocols[label] = {}; availability[label] = {'anti_peak_most_prolific': {'available_snapshots': 0, 'unavailable_snapshots': 0}}
            references={}; reference_counts={}; reference_pending={}; variant_changes={}; compared={}
            for _,item in items.values():
                snapshots.append(dict(snapshot=label, **item))
                frozen=item['decisions']; base=frozen['paths']['poisson_base']['first']
                variants = dict(frozen.get('experiments') or {})
                protocol = frozen.get('market_fair_protocol', 'legacy_gross_quote_value_gates')
                protocols[label][protocol] = protocols[label].get(protocol, 0)+1
                field = 'available_snapshots' if 'anti_peak_most_prolific' in variants else 'unavailable_snapshots'
                availability[label]['anti_peak_most_prolific'][field] += 1
                if 'gold_no_automatic_upgrade' in frozen:
                    variants.setdefault('gold_no_automatic_upgrade', frozen['gold_no_automatic_upgrade'])
                for name, choice in variants.items():
                    reference_name = 'favorite_anchor_only' if name == 'lambda_total_expansion' else 'calibrated_v51'
                    reference = frozen['paths'][reference_name]
                    references.setdefault(name, []); reference_counts.setdefault(name, 0); reference_pending.setdefault(name, 0)
                    variant_changes[name] = variant_changes.get(name, 0) + (choice['first'] != reference['first'])
                    compared.setdefault(name, set()).add(item['fixture_id'])
                    row = next((r for r in reference['rows'] if r['id'] == reference['first']), None)
                    if row:
                        reference_counts[name] += 1
                        payout = settle_spec(row['spec'], item['result'])
                        if payout in ((1.,0.),(0.,1.)):
                            references[name].append(dict(fid=item['fixture_id'], odd=row['odd'], y=int(payout[0]), p=row['probability_pct']/100))
                        else:
                            reference_pending[name] += 1
                for name,choice in {**frozen['paths'], **variants}.items():
                    paths.setdefault(name,[]);counts.setdefault(name,0);differences.setdefault(name,0);pending.setdefault(name,0)
                    differences[name] += choice['first'] != base
                    row=next((r for r in choice['rows'] if r['id']==choice['first']),None)
                    if not row: continue
                    counts[name] += 1
                    payout=settle_spec(row['spec'],item['result'])
                    if payout not in ((1.,0.),(0.,1.)):
                        pending[name] += 1;continue
                    paths[name].append(dict(fid=item['fixture_id'],odd=row['odd'],y=int(payout[0]),p=row['probability_pct']/100))
                for row in frozen.get('platinum_nonnegative_ev', []):
                    if not row.get('baseline_eligible'): continue
                    group = ('platinum_positive_ev' if row.get('shadow_eligible') else 'platinum_flow_only_negative_ev')
                    payout=settle_spec(row['spec'],item['result'])
                    if payout in ((1.,0.),(0.,1.)):
                        platinum.setdefault(group, []).append(dict(fid=item['fixture_id'], odd=row['odd'], y=int(payout[0])))
            result={n:selected_summary(v,counts[n],pending[n],differences[n]) for n,v in paths.items()}
            summaries[label]={n:result[n] for n in PATHS if n in result}
            experiments[label]={n:v for n,v in result.items() if n not in PATHS}
            for n,v in experiments[label].items():
                ref='favorite_anchor_only' if n=='lambda_total_expansion' else 'calibrated_v51'
                reference=selected_summary(references[n], reference_counts[n], reference_pending[n], 0)
                reference.pop('changed_vs_pure_poisson', None)
                v.update(baseline=ref, baseline_on_same_snapshots=reference,
                         changed_vs_baseline=variant_changes[n], compared_fixtures_count=len(compared[n]))
                v['roi_difference_pp']=(v['roi_pct']-reference['roi_pct']
                    if v['roi_pct'] is not None and reference.get('roi_pct') is not None else None)
            cohorts[label]={n:metrics(platinum.get(n, [])) for n in
                ('platinum_positive_ev','platinum_flow_only_negative_ev')}
        comparison={label:{aliases[n]:v for n,v in groups.items()} for label,groups in summaries.items()}
        return dict(version=VERSION,policy_id=POLICY_ID,snapshot_count=len(self.seen),unavailable_snapshots=self.unavailable,
                    market_fair_protocols=protocols, experiment_availability=availability,
                    gold_no_upgrade_changed_first=self.changed,summaries=summaries,decision_path_comparison=comparison,
                    experiments=experiments,platinum_value_cohorts=cohorts,snapshots=snapshots,
                    social_publication='not_inferred_from_eligibility; requires delivery receipts',
                    note='Retrospective replay is distinct from frozen official Gold certificates. First/latest are not additive; CIs cluster by fixture.')
