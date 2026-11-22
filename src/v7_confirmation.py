"""Explain an existing qualification; never manufacture probability or value."""
from math import isclose

from multimarket_model import stamp
from opportunity_policy import confirmed_flow
from v7_probability import finite, price_usable

VERSION = 'V7_CONFIRMATION_V1'

def number(value,digits=1,signed=False):
    return (f'{value:+.{digits}f}' if signed else f'{value:.{digits}f}').replace('.',',')


def oci_context(row):
    value = None if row.get('historical_oci') else row.get('oci')
    value = value if finite(value) else None
    return dict(oci=value, oci_state='missing' if value is None else 'neutral' if value == 0 else 'current',
                last_confirmed_oci=row.get('last_confirmed_oci'))


def confirmation(spec, record, source, as_of, kickoff, *, pick=None, patterns=None):
    """Keep role admission distinct from movement and from historical OCI.

    No new Poisson floor is introduced. Statistical support is the existing V7
    quality decision. The negative-EV recovery must carry its original proof;
    it is never renamed positive value or inferred from bookmaker margin.
    """
    features, quote = record.get('features') or {}, record.get('quote') or {}
    current = oci_context(source)
    first, odd = quote.get('first_odd'), record.get('odd')
    start, end, now = (stamp(v) for v in (quote.get('first_observed_at'), quote.get('quote_observed_at'), as_of))
    usable = price_usable(source, as_of, kickoff)
    if not usable:
        # The matrix already stores the missing quote and model numbers. Do not
        # repeat a full price explanation for every unquoted market in each scan.
        return dict(id=VERSION,status='observation',observation_only=True,
                    direction='missing',oci_state=current['oci_state'],label=None)
    comparable = bool(usable and all((start,end,now)) and start < end <= now
                      and finite(first) and first > 1 and finite(odd) and odd > 1
                      and source.get('bookmaker_id') is not None
                      and str(source['bookmaker_id']) == str(quote.get('bookmaker_id'))
                      and source.get('odd') == odd)
    net = 100*(odd/first-1) if comparable else None
    direction = 'missing' if net is None else 'down' if net < -1e-9 else 'up' if net > 1e-9 else 'stable'
    leg = source.get('selected_movement') or {}
    leg_start, leg_end = (stamp(leg.get(k,leg.get(old))) for k,old in
                          (('leg_observed_from','observed_from'),('leg_observed_to','observed_to')))
    leg_delta = leg.get('delta_pct') if (usable and leg_start and leg_end and leg_start < leg_end <= now) else None
    supported = record.get('supported') is True
    margin, fair = features.get('margin_pct'), record.get('fair')
    healthy = finite(margin) and 0 <= margin <= 8
    relation = (record.get('pricing_evidence') or {}).get('value_relation') or {}
    same_book = bool(usable and quote.get('bookmaker_id') is not None and finite(fair) and 0 < fair < 1
                     and str(quote['bookmaker_id'])==str(source.get('bookmaker_id')) and odd==source.get('odd'))
    if relation.get('status') == 'verified':
        same_book = same_book and str(relation.get('bookmaker_id')) == str(quote['bookmaker_id'])
    elif (record.get('pricing_evidence') or {}).get('basis') == 'binary':
        same_book = bool(same_book and finite(margin) and isclose(fair,1/odd/(1+margin/100),rel_tol=1e-9,abs_tol=1e-12))
    ev, low = record.get('ev'), record.get('ev_low')
    value = bool(supported and same_book and healthy and finite(ev) and ev >= 0
                 and finite(low) and low >= 0)
    proof = record.get('ev_recovery') or (pick or {}).get('ev_recovery') or {}
    recovery = bool(usable and supported and finite(margin) and 0<=margin<=30
                    and proof.get('policy_id') == 'EV_RECOVERY_AGGIO_V1'
                    and proof.get('original_ev') == ev and proof.get('original_ev_low') == low
                    and proof.get('probability') == record.get('calibrated')
                    and str(proof.get('bookmaker_id')) == str(quote.get('bookmaker_id')) and same_book
                    and finite(fair) and all(finite(proof.get(k)) and proof[k]>=fair
                        for k in ('statistical_probability','prudent_probability','probability'))
                    and stamp(proof.get('statistics_at')) and stamp(proof['statistics_at'])<=now)
    flow = bool(usable and current['oci_state'] == 'current' and current['oci'] > 0
                and not source.get('prematch_stable') and confirmed_flow(source,minimum_oci=1e-9)
                and finite(leg_delta) and leg_delta < 0)
    study = (pick or {}).get('study_only') is True
    qualified = bool((value or recovery) and not study)
    gg = (spec.get('kind') == 'btts' and spec.get('period') == 'ht' and spec.get('side') == 'yes')
    contrarian = bool(not qualified and gg and comparable and odd >= 5.5 and net >= 5-1e-9
                      and current['oci_state'] == 'current' and current['oci'] < -50
                      and supported and (patterns or {}).get('rebound_gg') is True)
    label = None
    if direction == 'up':
        label = ('Studio GG PT contrarian' if contrarian else 'Rialzo con supporto documentato' if qualified
                 else 'Rialzo senza conferma')
    elif direction == 'down':
        label = 'Calo con conferma statistica' if qualified else 'Quota in calo, conferma insufficiente'
    if label is None and not qualified:
        label = 'Osservazione statistica'
    detail = []
    if comparable:
        detail.append(f'Prima osservata stesso operatore {number(first,2)} → {number(odd,2)} ({number(net,signed=True)}%).')
    if finite(leg_delta): detail.append(f'Ultimo tratto separato {number(leg_delta,signed=True)}%.')
    if current['oci_state'] == 'neutral': detail.append('OCI attuale 0: neutro, nessuna conferma di flusso.')
    elif current['oci_state'] == 'missing': detail.append('OCI attuale non disponibile.')
    else: detail.append(f"OCI attuale {number(current['oci'],signed=True)}; {'flusso coerente' if flow else 'non è da solo una conferma' }.")
    if current['last_confirmed_oci']: detail.append('L’ultimo OCI confermato è un riferimento storico separato.')
    if recovery: detail.append('Recupero statistico documentato; EV originale conservato, non valore positivo.')
    elif qualified: detail.append('Qualifica di valore con supporto statistico V7 e scenario prudente non negativo.')
    elif contrarian: detail.append('Caso specifico GG PT con supporto statistico; nessuna promozione automatica Gold.')
    else: detail.append('Solo osservazione: il movimento non sostituisce una qualifica documentata.')
    return dict(id=VERSION, status='qualified' if qualified else 'specific_study' if contrarian else 'observation',
                observation_only=not qualified, direction=direction, opening_delta_pct=net,
                last_leg_delta_pct=leg_delta, flow_confirmed=flow, statistical_support=supported,
                value_qualified=value, statistical_recovery=recovery, contrarian_study=contrarian,
                oci_state=current['oci_state'], label=label, detail=' '.join(detail))
