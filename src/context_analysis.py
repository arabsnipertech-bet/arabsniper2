"""Context model v3. Statistical forecasts never consume bookmaker probabilities.

Coefficients are explicit experimental choices, not fitted or validated accuracy.
All evidence uses at most eight official matches and four valid HT observations.
"""
from statistics import mean, median
from copy import deepcopy
from multimarket_model import SPECS, COMBOS, number, stamp, fair, goal_probabilities, quote_benchmarks

MODEL_ID = 'context-goals-v3.3'
CONFIG = {'history': 8, 'form': 4, 'halftime': 4, 'correction_per_goal': .02,
          'correction_cap': .05, 'minimum_context_reference': 3,
          'max_quote_age_hours': 6, 'agreement_pp': 10,
          'favorite_guard_min_pct': 60, 'favorite_guard_gap_pp': 25,
          'selection_policy': 'probability-price-v1', 'primary_min_odd': 1.5, 'halftime_conflict_pp': 20, 'experimental': True, 'book_weight_in_statistics': 0}


def avg(values):
    values = [v for v in values if number(v) is not None]
    return mean(values) if values else None


def select_history(rows, as_of, season=None):
    now = stamp(as_of)
    selected = []
    for r in rows:
        when = stamp(r.get('kickoff'))
        # Date-only legacy records are accepted only on strictly earlier days.
        before = when < now if when and now else bool(now and str(r.get('date', '')) < now.date().isoformat() and r.get('date'))
        if not before or r.get('friendly') or 'friendl' in str(r.get('league','')).lower():
            continue
        if season is not None and str(r.get('season')) != str(season):
            continue
        ft = r.get('research_ft') or {}
        scored, conceded = ft.get('scored'), ft.get('conceded')
        if scored is None or conceded is None:
            continue
        selected.append({**r, 'team_goals_ft': scored, 'team_conceded_ft': conceded})
    selected.sort(key=lambda r: r.get('kickoff') or r.get('date',''), reverse=True)
    seen = set()
    return [r for r in selected if not (r.get('fixture_id') in seen or seen.add(r.get('fixture_id')))][:8]


def series(rows, field):
    return [float(r[field]) for r in rows if number(r.get(field)) is not None]


def trend(rows, field, width):
    if len(rows) < 2*width or any(number(r.get(field)) is None for r in rows[:2*width]):
        return None
    return mean(series(rows[:width], field))-mean(series(rows[width:2*width], field))


def opportunities(rows, direction):
    """xG takes precedence; shots are never added as another independent vote."""
    for kind in ('xg', 'shots_on_target'):
        field = f'team_{kind}_{direction}'
        values = series(rows, field)
        if values:
            return {'source': kind, 'sample': len(values), 'mean': mean(values),
                    'trend': trend(rows, field, 4), 'field': field,
                    'goal_conversion': sum(series([r for r in rows if number(r.get(field)) is not None],
                        'team_goals_ft' if direction == 'for' else 'team_conceded_ft')) / sum(values) if sum(values)>0 else None}
    return {'source': 'unavailable', 'sample': 0, 'mean': None, 'trend': None}


def profile(rows, side):
    ht = [r for r in rows if r.get('halftime_available') and all(number(r.get(k)) is not None for k in ('team_goals_ht','team_conceded_ht'))][:4]
    ctx = [r for r in rows if r.get('side') == side]
    result = {'sample':len(rows), 'context_sample':len(ctx), 'ht_sample':len(ht),
              'rows':rows, 'ht_rows':ht, 'context_side':side,
              'opponent_strength':'unavailable_before_historical_fixture',
              'opportunities':{d:opportunities(rows,d) for d in ('for','against')}}
    for period, pool in [('ft',rows),('ht',ht)]:
        for direction, field in [('for',f'team_goals_{period}'),('against',f'team_conceded_{period}')]:
            vals=series(pool,field); context=series([r for r in pool if r.get('side')==side],field)
            result[f'{direction}_{period}'] = {'mean':avg(vals),'context_mean':avg(context),
                'sample':len(vals),'context_sample':len(context),
                'frequency':avg([int(v>0) for v in vals]), 'trend':trend(pool,field,4 if period=='ft' else 2),
                'without_peak':avg(sorted(vals)[:-1]) if len(vals)>1 else None,
                'last':vals[0] if vals else None,
                'last_two':vals[:2]}
    return result


def correction(p, direction):
    field='team_goals_ft' if direction=='for' else 'team_conceded_ft'
    ctx=[r for r in p['rows'] if r.get('side')==p['context_side']]
    opposite=[r for r in p['rows'] if r.get('side')!=p['context_side']]
    out={'relative':0.,'active':False,'reason':'campione_contesto_insufficiente','reference_n':max(0,len(ctx)-1)}
    if len(ctx)<4 or not opposite: return out
    reference=mean(series(ctx[1:],field)); other=mean(series(opposite,field)); last=ctx[0][field]
    gap=reference-last; mixed=.6*reference+.4*other; distance=abs(reference-mixed)
    attenuation=.25 if distance<=.2 else .5 if distance<=.5 else 1.
    opp=p['opportunities'][direction]; t=opp['trend']
    confirmation=.5 if t is None else 1. if t*gap>=0 else 0.
    # A consistently low-conceding defence is not assigned a goal-due bonus.
    if direction=='against' and reference<.75 and gap>0:
        confirmation=0.
    relative=max(-.05,min(.05,.02*gap))*attenuation*confirmation
    return {**out,'active':True,'reason':'ipotesi_scarto_media_sperimentale',
            'reference_mean':reference,'opposite_mean':other,'reference_60_40':mixed,
            'last':last,'gap':gap,'attenuation':attenuation,'opportunity_confirmation':confirmation,
            'relative':relative}


def component(p, direction, period):
    v=p[f'{direction}_{period}']; n=v['sample']; cn=v['context_sample']
    if not n: return None,0.,{}
    # Context influence is proportional to its actual coverage within the window.
    cw=cn/n
    value=cw*v['context_mean']+(1-cw)*v['mean'] if cn else v['mean']
    xg_weight=0.
    if period=='ft':
        o=p['opportunities'][direction]
        if o['source']=='xg':
            xg_weight=.25*o['sample']/8
            value=(1-xg_weight)*value+xg_weight*o['mean']
    quality=(n/(8 if period=='ft' else 4))*(.5+.5*cw)
    return value,quality,{'context_weight':cw,'context_n':cn,'n':n,'quality_weight':quality,'xg_weight':xg_weight,'value':value}


def estimate(home, away, league):
    rates={}; details={}; raw={}
    for side, attack, defense in [('home',home,away),('away',away,home)]:
        for period in ('ft','ht'):
            a,qa,da=component(attack,'for',period); d,qd,dd=component(defense,'against',period)
            if qa+qd:
                value=((a or 0)*qa+(d or 0)*qd)/(qa+qd)
                wa=qa/(qa+qd)
            else: value=None; wa=None
            indirect=False
            if period=='ht' and value is None and league.get('ht_sample',0)>0 and league.get('avg_total_ft',0)>0:
                ratio=league.get('avg_total_ht',0)/league['avg_total_ft']
                if 0<ratio<1 and rates.get(f'{side}_ft') is not None:
                    value=raw[f'{side}_ft']*ratio; indirect=True
            key=f'{side}_{period}'; raw[key]=value
            ca=correction(attack,'for'); cd=correction(defense,'against')
            correction_value=(wa*ca['relative']+(1-wa)*cd['relative']) if wa is not None and period=='ft' else 0
            rates[key]=max(.001,value*(1+correction_value)) if value is not None else None
            details[key]={'attack':da,'defense':dd,'attack_weight':wa,
                          'defense_weight':1-wa if wa is not None else None,
                          'uncorrected_goals':value,'correction_relative':correction_value,
                          'attack_correction':ca if period=='ft' else None,
                          'defense_correction':cd if period=='ft' else None,'indirect':indirect}
    # Keep the four rates coherent without treating missing halftime as zero.
    for side in ('home','away'):
        if rates[side+'_ft'] is not None and rates[side+'_ht'] is not None:
            rates[side+'_ht']=min(rates[side+'_ht'],rates[side+'_ft'])
            raw[side+'_ht']=min(raw[side+'_ht'],raw[side+'_ft'])
    def vector(r):
        return [r['home_ht'],r['away_ht'],r['home_ft']-r['home_ht'],r['away_ft']-r['away_ht']] if all(v is not None for v in r.values()) else None
    return vector(rates),vector(raw),details


def movement(books, side):
    """Same operators over the same observation window; no aggregate mix shift."""
    paired=[]
    for b in books:
        h=b.get('trace',[])
        if len(h)<2: continue
        start,end=stamp(h[0]['observed_at']),stamp(h[-1]['observed_at'])
        if not start or not end or end<=start: continue
        paired.append((b,h,start,end))
    if not paired: return {'confirmed':False,'paired_books':0,'reason':'storico_comparabile_assente'}
    # Use one common window (one hour tolerance), not incomparable first dates.
    anchor=max(p[2] for p in paired)
    paired=[p for p in paired if abs((p[2]-anchor).total_seconds())<=3600]
    end=max(p[3] for p in paired)
    paired=[p for p in paired if abs((p[3]-end).total_seconds())<=3600]
    rows=[]
    for b,h,start,end in paired:
        points=[x['odds'][side] for x in h]; signs=[1 if y>x else -1 for x,y in zip(points,points[1:]) if y!=x]
        reversal=any(x!=y for x,y in zip(signs,signs[1:])); turn=0
        nonflat=[(i,1 if y>x else -1) for i,(x,y) in enumerate(zip(points,points[1:])) if x!=y]
        for (i,s),(j,t) in zip(nonflat,nonflat[1:]):
            if s!=t: turn=j
        delta=(points[-1]-points[turn])/points[turn]*100
        fp,margin=fair(h[-1]['odds']); op,om=fair(h[0]['odds'])
        fair_delta=(fp[side]-op[side])*100
        legp,legmargin=fair(h[turn]['odds'])
        leg_delta=(fp[side]-legp[side])*100
        fair_steps=[fair(x['odds'])[0][side] for x in h[-4:]]
        directions=[1 if y>x else -1 for x,y in zip(fair_steps,fair_steps[1:]) if abs(y-x)>=.001]
        persistence=max(directions.count(1),directions.count(-1))/len(directions) if directions and len(h)>=3 else 0.
        hours=max((end-stamp(h[turn]['observed_at'])).total_seconds()/3600,.25)
        rows.append({'leg_fair_delta_pp':leg_delta,'hours':hours,'persistence':persistence,'bookmaker':b['bookmaker'],'initial':points[0],'current':points[-1],
            'turning':points[turn] if reversal else None,'reversal':reversal,'delta_pct':delta,
            'fair_delta_pp':fair_delta,'margin_delta_pp':margin-om,'observed_from':start.isoformat(),
            'observed_to':end.isoformat(), 'sequence':points})
    direction=lambda r: 1 if r['delta_pct']>0 else -1 if r['delta_pct']<0 else 0
    counts={s:sum(direction(r)==s for r in rows) for s in (-1,1)}
    sign=max(counts,key=counts.get); agreeing=[r for r in rows if direction(r)==sign]
    confirmed=len(agreeing)>=2 and len(agreeing)>len(rows)/2
    selected=agreeing if confirmed else rows
    amplitude=median(abs(r['delta_pct']) for r in selected) if selected else 0
    oci=None
    if confirmed:
        delta=median(r['leg_fair_delta_pp'] for r in agreeing)
        hours=median(r['hours'] for r in agreeing)
        absolute=(min(abs(delta)/4,1)*40+len(agreeing)/len(rows)*25+
                  min(abs(delta)/hours/1.5,1)*10+median(r['persistence'] for r in agreeing)*15+min(len(rows)/8,1)*10)
        oci=0 if abs(delta)<.10 else absolute*(1 if delta>0 else -1)
    return {'oci':oci,'oci_method':'OCI-AS on paired operators, last leg; descriptive, not independent evidence',
        'fair_direction_coherent':bool(confirmed and all(r['leg_fair_delta_pp']*sign<0 for r in agreeing)),
        'confirmed':confirmed,'paired_books':len(rows),'agreeing_books':len(agreeing),
        'direction':'up' if sign>0 else 'down','arrow':'⇅' if confirmed and any(r['reversal'] for r in agreeing) else '↑' if sign>0 else '↓',
        'color':'red' if amplitude>=8 else 'yellow' if amplitude>=3 else 'green',
        'amplitude_pct':amplitude,'median_drop_pct':-median(r['delta_pct'] for r in selected) if selected else None,
        'median_fair_delta_pp':median(r['fair_delta_pp'] for r in selected) if selected else None,
        'median_overround_delta_pp':median(r['margin_delta_pp'] for r in selected) if selected else None,
        'operators':rows,'reason':'consenso_multi_operatore' if confirmed else 'nessuna_maggioranza_multi_operatore'}


def market_label(key, side):
    label=SPECS[key][4] if key in SPECS else COMBOS[key][0]
    if key=='1x2_ft': return {'1':'Vittoria casa','x':'Pareggio','2':'Vittoria ospite'}[side]
    if side in ('under','no'):
        return label+' — non realizzata' if key in COMBOS else label.replace('Over','Under').replace('sì','no').replace('almeno 2 gol','meno di 2 gol')
    return label



def select_proposal(rows):
    """Compare probability and price without excluding the less likely outcome.

    Guard thresholds are explicit provisional controls, not fitted probabilities.
    A major 1X2 conflict uses book references for each specific market;
    raw statistical probabilities remain visible, never rewritten as book estimates.
    """
    result=[r for r in rows if r['market']=='1x2_ft' and r.get('book_probability_pct') is not None and r.get('bookmakers_count',0)>=2]
    favorite=max(result,key=lambda r:r['book_probability_pct']) if len(result)==3 else None
    guard=bool(favorite and favorite['selection'] in ('1','2') and
        favorite['book_probability_pct']>=CONFIG['favorite_guard_min_pct'] and
        favorite.get('statistical_pct') is not None and
        favorite['book_probability_pct']-favorite['statistical_pct']>=CONFIG['favorite_guard_gap_pp'])
    diagnostic={'status':'major_conflict' if guard else 'no_major_conflict',
        'opponent_strength_adjusted':False,
        'reason':'Forza degli avversari storici non normalizzata: copertura del campione e affidabilità sono distinte.'}
    if guard:
        diagnostic.update(favorite=favorite['selection'],statistical_pct=favorite['statistical_pct'],book_probability_pct=favorite['book_probability_pct'])
    for r in rows:
        r['research_candidate']=False
        peers=[x for x in rows if x['market']==r['market'] and x is not r]
        p,b=r.get('statistical_pct'),r.get('book_probability_pct')
        ht_market='ht' in r['market'] or '_sh' in r['market']
        fragile=bool(ht_market and (r.get('halftime_sample_fragile') or p is not None and b is not None and abs(p-b)>=CONFIG['halftime_conflict_pp']))
        book_priority=bool((guard or fragile) and b is not None and r.get('bookmakers_count',0)>=2)
        stats_leads=p is not None and all(x.get('statistical_pct') is not None and p>=x['statistical_pct'] for x in peers)
        book_leads=b is not None and all(x.get('book_probability_pct') is not None and b>=x['book_probability_pct'] for x in peers)
        r['primary_eligible']=p is not None or b is not None
        r['statistical_reliability']='fragile_halftime' if fragile else 'context_conflict' if guard else 'opponent_strength_unadjusted'
        r['evidence_blocks']['goals']=1 if stats_leads else -1 if p is not None else 0
        # Proximity is descriptive; it never contributes a favorable vote by itself.
        r['evidence_blocks']['book']=1 if stats_leads and book_leads and r.get('bookmakers_count',0)>=2 else -1 if b is not None and not book_leads else 0
        r['support_score']=sum(r['evidence_blocks'].values())
        r['pros']=[v for v in r['pros'] if not v.startswith(('Book senza margine','Frequenza di riferimento'))]
        r['cons']=[v for v in r['cons'] if not v.startswith(('Book senza margine','Frequenza di riferimento'))]
        if b is not None:
            (r['pros'] if book_leads else r['cons']).append(f"Book senza margine {b:.1f}%: {'esito più probabile' if book_leads else 'preferisce un altro esito'} nello stesso mercato.")
        directional=stats_leads and book_leads and r.get('bookmakers_count',0)>=2
        if directional and not guard and not fragile:
            r['source']='statistica e bookmaker';r['source_reason']='Entrambe le fonti preferiscono questo esito nello stesso mercato. Percentuali separate, senza bonus.'
        else:
            r['source']='analisi bookmaker' if b is not None and (book_priority or p is None or r.get('coverage',0)<.375) else 'analisi statistica' if p is not None else 'non disponibile'
            r['source_reason']='Fonti non concordi sulla direzione; nessuna conferma dedotta dalla sola vicinanza delle percentuali.' if p is not None and b is not None else 'Proposta basata sulla sola fonte disponibile.'
        if guard:
            r['cons'].insert(0,'Forte divergenza sulla favorita 1X2; medie storiche non corrette per forza degli avversari. Statistica da trattare come diagnostica.')
            r['primary_eligible']=bool(book_priority)
            r['source_reason']='Prevale il bookmaker per questo mercato: il modello contraddice nettamente la favorita 1X2 e non normalizza la forza degli avversari. Nessuna previsione automatica di goleada.'
        if fragile:
            r['cons'].insert(0,'Stima PT fragile: campione breve con componente nulla o forte scarto dal book. Zero gol osservati non significa impossibilità di segnare.')
            r['source_reason']='Riferimento bookmaker sul mercato specifico; stima statistica PT mantenuta come diagnostica, senza media fra le due fonti.' if book_priority else 'Stima PT fragile senza consenso bookmaker sufficiente; non utilizzabile come proposta principale.'
            if not book_priority:r['primary_eligible']=False
        if book_priority:
            r['evidence_blocks']={'book':1 if book_leads else -1}
            r['support_score']=r['evidence_blocks']['book']
        # A vote count is not a probability, and the likelier opposite is not a veto.
        attach_price_assessment(r)
    candidates=[r for r in rows if r['primary_eligible'] and r['price_assessment']['reference_probability_pct'] is not None
                and not (r['market'] in COMBOS and r['selection']=='no')]
    candidates.sort(key=price_rank)
    selected=pick_for_price(candidates,CONFIG['primary_min_odd'])
    if selected:
        for r in candidates:r['research_candidate']=(r['market'],r['selection'])==(selected['market'],selected['selection'])
    return candidates,diagnostic


def attach_price_assessment(row):
    p,b=row.get('statistical_pct'),row.get('book_probability_pct')
    fragile=row.get('statistical_reliability') in ('fragile_halftime','context_conflict')
    book_ok=b is not None and row.get('bookmakers_count',0)>=2
    stats_ok=p is not None and not fragile and row.get('coverage',0)>=.5
    if stats_ok and book_ok:
        ref=min(p,b);source='lower_of_two_estimates'
        note='Confronto con la minore delle due stime disponibili: non è un intervallo di confidenza né una probabilità calibrata.'
    elif stats_ok:
        ref=p;source='statistics_only';note='Solo modello statistico sperimentale; vantaggio non validato.'
    elif book_ok:
        ref=b;source='book_only';note='Confronto di prezzo basato sul book: non dimostra un vantaggio indipendente sul mercato.'
    elif b is not None and p is None and not fragile:
        ref=b;source='limited_book';note='Un solo operatore: confronto di prezzo esplorativo, senza consenso multi-book.'
    elif p is not None and not fragile:
        ref=p;source='limited_statistics';note='Campione statistico limitato: ipotesi esplorativa, convenienza non verificata.'
    else:
        ref=None;source='unavailable';note='Nessuna stima utilizzabile per il confronto probabilità/quota.'
    odd=number(row.get('odd'));priced=odd is not None and odd>1
    ratio=ref/100*odd if ref is not None and priced else None
    independent=source in ('lower_of_two_estimates','statistics_only')
    row['price_assessment']={'reference_probability_pct':ref,'reference_source':source,
        'break_even_probability_pct':100/odd if priced else None,
        'reference_fair_odd':100/ref if ref is not None and ref>0 else None,
        'return_index':ratio,'estimated_return_pct':(ratio-1)*100 if ratio is not None else None,
        'status':'unvalidated_positive_estimate' if independent and ratio is not None and ratio>1 else 'market_comparison_only' if source=='book_only' else 'exploratory',
        'validated_advantage':False,'note':note}


def price_rank(row):
    a=row['price_assessment'];value=a['return_index'];p=a['reference_probability_pct'] or 0
    return (value is None,-value if value is not None else 0,-row.get('coverage',0),-p,row['market'],row['selection'])


def pick_for_price(candidates, minimum):
    qualified=[r for r in candidates if minimum==0 or number(r.get('odd')) is not None and r['odd']>=minimum]
    chosen=qualified[0] if qualified else candidates[0] if candidates else None
    if chosen is None:return None
    result=deepcopy(chosen)
    result['requested_min_odd']=minimum
    result['price_preference_met']=bool(qualified)
    result['proposal_note']='Migliore ipotesi nel confronto probabilità/quota; convenienza non dimostrata.' if qualified else 'Nessuna quota soddisfa la preferenza: mostrata la migliore ipotesi disponibile, anche sotto soglia o senza quota.'
    return result


def evaluate(match, as_of, quotes=None):
    meta=match.get('fixture',{}); stats=match.get('statistics',{})
    profiles={s:profile(select_history(stats.get(s,{}).get('last_matches',[]),as_of,meta.get('season')),s) for s in ('home','away')}
    rates,uncorrected,weights=estimate(profiles['home'],profiles['away'],stats.get('league',{}))
    probs=goal_probabilities(rates) if rates else {}
    baseline=goal_probabilities(uncorrected) if uncorrected else {}
    # Missing halves must not erase valid FT estimates, nor generate fake PT data.
    if not rates and all(weights[s+'_ft']['uncorrected_goals'] is not None for s in ('home','away')):
        ft=[weights[s+'_ft']['uncorrected_goals'] for s in ('home','away')]
        adjusted=[v*(1+weights[s+'_ft']['correction_relative']) for v,s in zip(ft,('home','away'))]
        for dest,values in [(probs,adjusted),(baseline,ft)]:
            calculated=goal_probabilities([0.,0.,*values])
            dest.update({k:v for k,v in calculated.items() if k.endswith('_ft')})
    league=stats.get('league',{})
    league_reference={}
    if league.get('sample',0)>0:
        lh,la=number(league.get('avg_home_goals')),number(league.get('avg_away_goals'))
        hh,ah=number(league.get('avg_home_ht')),number(league.get('avg_away_ht'))
        if None not in (lh,la,hh,ah) and 0<=hh<=lh and 0<=ah<=la:
            league_reference=goal_probabilities([hh,ah,lh-hh,la-ah])
            if not league.get('ht_sample'): league_reference={k:v for k,v in league_reference.items() if k.endswith('_ft')}

    now=stamp(as_of); valid={}
    for key,books in (quotes or {}).items():
        valid[key]=[b for b in books if stamp(b.get('provider_updated_at')) and now and
                    0<=(now-stamp(b['provider_updated_at'])).total_seconds()<=6*3600]
    benchmarks=quote_benchmarks(valid,as_of); rows=[]
    for key in [*SPECS,*COMBOS]:
        for side in SPECS[key][3] if key in SPECS else ('yes','no'):
            bench=benchmarks.get(key,{}); books=bench.get('books',[])
            p=probs.get(key,{}).get(side); bp=bench.get('probabilities',{}).get(side)
            priced=sorted(books,key=lambda b:b['odds'][side]); quote=deepcopy(priced[(len(priced)-1)//2]) if priced else None
            if quote: quote.pop('trace',None)
            odd=quote['odds'][side] if quote else None
            pro=[]; con=[]; missing=[]; blocks={}
            ht='ht' in key or '_sh' in key
            n=min(profiles[s]['ht_sample' if ht else 'sample'] for s in profiles)
            if p is not None:
                pro.append(f"Modello gol: {p*100:.1f}% per questo esito; campione {'PT' if ht else 'FT'} minimo {n}.")
                reference=league_reference.get(key,{}).get(side)
                blocks['goals']=1 if reference is not None and p>reference+.02 else -1 if reference is not None and p<reference-.02 else 0
                if reference is not None:
                    (pro if p>=reference else con).append(f'Frequenza di riferimento del modello campionato: {reference*100:.1f}%; confronto specifico del mercato.')
            else: missing.append('Probabilità statistica non calcolabile con i dati disponibili.')
            if not ht and n<8: missing.append(f'Campione FT: {profiles["home"]["sample"]}/8 casa, {profiles["away"]["sample"]}/8 ospite.')
            if ht and n<4: missing.append(f'Copertura PT: {profiles["home"]["ht_sample"]}/4 casa, {profiles["away"]["ht_sample"]}/4 ospite; eventuale stima indiretta indicata nei pesi.')
            if bp is not None:
                agreement=p is not None and abs(p-bp)<=.10
                (pro if agreement else con if p is not None else pro).append(f"Book senza margine {bp*100:.1f}% su {len(books)} operatori"+(': concordanza entro 10 punti.' if agreement else '.'))
                blocks['book']=1 if agreement else -1 if p is not None else 1
            else: missing.append('Quota completa e recente del mercato non disponibile.')
            if key in COMBOS:
                missing.append('Combinazione stimata assumendo indipendenza tra tempi; dipendenza non calibrata.')
            period='ht' if ht else 'ft'; positive=side in ('over','yes'); considered=['home','away']
            if key=='home15_ft': considered=['home']
            elif key=='away15_ft': considered=['away']
            elif key=='1x2_ft' and side!='x': considered=['home' if side=='1' else 'away']
            ts=[profiles[s]['for_'+period]['trend'] for s in considered]
            if all(t is not None for t in ts):
                supportive=sum(ts)>=0 if positive or key=='1x2_ft' else sum(ts)<=0
                blocks['form']=1 if supportive else -1
                (pro if supportive else con).append('Produzione recente '+('coerente' if supportive else 'in contrasto')+' con la direzione scelta; confronto '+('2 contro 2 PT.' if ht else '4 contro 4 FT.'))
            else: missing.append('Trend recente non confrontabile su due blocchi completi.')
            os=[profiles[s]['opportunities']['for'] for s in considered]
            if not ht and all(o['trend'] is not None for o in os):
                # xG and shots are unlike units: compare directions, never sum magnitudes.
                directions=[(1 if o['trend']>0 else -1 if o['trend']<0 else 0)*(1 if positive else -1) for o in os]
                supportive=all(t>=0 for t in directions)
                contrary=all(t<=0 for t in directions) and any(t<0 for t in directions)
                blocks['opportunities']=1 if supportive else -1 if contrary else 0
                (pro if supportive else con).append('Occasioni create '+('confermano' if supportive else 'non confermano' if contrary else 'danno indicazioni miste su')+' la direzione: xG prioritari, altrimenti tiri in porta.')
            elif ht: missing.append('Occasioni FT non usate come prova specifica del primo tempo.')
            else: missing.append('Trend occasioni non disponibile su otto gare complete.')
            if key in ('gg_ht','gg_ft'):
                f=[profiles[s]['for_'+period]['frequency'] for s in profiles]
                if all(v is not None for v in f):
                    (pro if min(f)>=.5 and side=='yes' else con if side=='yes' else pro).append(f"Segna almeno un gol: casa {f[0]*100:.0f}%, ospite {f[1]*100:.0f}% nel campione; GG richiede entrambe.")
            sensitivity=any(v['mean'] and v['without_peak'] is not None and abs(v['mean']-v['without_peak'])/v['mean']>.3 for s in considered for v in [profiles[s]['for_'+period]])
            if sensitivity: con.append('Media sensibile alla partita più prolifica; nessun risultato rimosso.'); blocks['form']=min(blocks.get('form',0),0)
            source='statistica e bookmaker' if p is not None and bp is not None and abs(p-bp)<=.10 else 'analisi statistica' if p is not None and n>=3 else 'analisi bookmaker' if bp is not None else 'analisi statistica' if p is not None else 'non disponibile'
            if key=='1x2_ft':
                # Direction of a result is relative to the opponent, not total goals.
                blocks.pop('opportunities',None)
                ft_home=weights['home_ft']['uncorrected_goals']; ft_away=weights['away_ft']['uncorrected_goals']
                if ft_home is not None and ft_away is not None:
                    relative=ft_home-ft_away
                    blocks['goals']=1 if (side=='1' and relative>.15 or side=='2' and relative<-.15 or side=='x' and abs(relative)<=.15) else -1
                httrend=profiles['home']['for_ft']['trend']; attrend=profiles['away']['for_ft']['trend']
                blocks.pop('form',None)
                if httrend is not None and attrend is not None and side!='x':
                    blocks['form']=1 if (httrend-attrend)*(1 if side=='1' else -1)>=0 else -1
                pro=[t for t in pro if not t.startswith(('Produzione recente','Occasioni create'))]
                con=[t for t in con if not t.startswith(('Produzione recente','Occasioni create'))]
                if ft_home is not None and ft_away is not None:
                    pro.append(f'Confronto gol attesi FT prima delle correzioni: casa {ft_home:.2f}, ospite {ft_away:.2f}.')
            score=sum(blocks.values()); quality=n/(4 if ht else 8)
            source_reason=('Le due fonti sono entro 10 punti; nessun aumento delle percentuali.' if source=='statistica e bookmaker' else f'Prevale la statistica per la disponibilità del campione ({n} osservazioni minime); la divergenza resta visibile.' if source=='analisi statistica' and bp is not None else 'Proposta basata sui dati statistici disponibili; confronto bookmaker assente.' if source=='analisi statistica' else 'Prevale il bookmaker perché il campione statistico è assente o inferiore a tre osservazioni.' if source=='analisi bookmaker' else 'Nessuna fonte utilizzabile.')
            rows.append({'market':key,'selection':side,'label':market_label(key,side),
                'probability_pct':round(p*100,3) if p is not None else None,'statistical_pct':round(p*100,3) if p is not None else None,
                'book_probability_pct':round(bp*100,3) if bp is not None else None,
                'probabilities':{'statistics':p,'without_correction':baseline[key][side]} if p is not None else {},
                'odd':odd,'quote':quote,'bookmakers_count':len(books),'source':source,'source_reason':source_reason,
                'expected_value_pct':round((p*odd-1)*100,3) if p is not None and odd else None,
                'research_candidate':False,'reasons':[],'pros':pro[:3],'cons':con[:3],'missing':missing,
                'halftime_sample_fragile':bool(ht and any(weights[s+'_ht'].get('uncorrected_goals')==0 for s in ('home','away'))),
                'evidence_blocks':blocks,'support_score':score,'coverage':quality,'movement':movement(books,side),'reference_probability_pct':round(league_reference[key][side]*100,2) if key in league_reference else None})
    trap=favorite_context(profiles,benchmarks,rows)
    candidates,diagnostic=select_proposal(rows)
    proposals={str(limit):pick_for_price(candidates,limit) for limit in (0,1.4,1.5,1.8,2)}
    for p in profiles.values():
        p['history_ids']=[r['fixture_id'] for r in p.pop('rows')]; p['ht_history_ids']=[r['fixture_id'] for r in p.pop('ht_rows')]
    return {'model_id':MODEL_ID,'status':'experimental','captured_at':as_of,'config':CONFIG,
            'rates':{'statistics':rates,'without_correction':uncorrected},'weights':weights,
            'proposals_by_min_odd':proposals,'selection_diagnostic':diagnostic,'profiles':profiles,'favorite_context':trap,'features':{},'markets':rows,'best_candidate':deepcopy(proposals[str(CONFIG['primary_min_odd'])]),
            'ranked_hypotheses':[r['market']+'::'+r['selection'] for r in candidates],
            'notes':['Coefficienti sperimentali da verificare negli audit.','Il digiuno non rende un gol dovuto.','Book e statistica restano separati.']}


def favorite_context(profiles, benchmarks, markets):
    b=benchmarks.get('1x2_ft',{})
    if b.get('bookmakers_count',0)<2: return {'status':'unavailable','reason':'Meno di due operatori 1X2 recenti.'}
    fairp=b['probabilities']; pick=max(('1','2'),key=lambda k:fairp[k])
    if fairp[pick]<.60: return {'status':'not_triggered','reason':'Nessuna favorita netta al 60% senza margine.'}
    favorite='home' if pick=='1' else 'away'; underdog='away' if pick=='1' else 'home'
    f,u=profiles[favorite],profiles[underdog]
    movement_row=next(r for r in markets if r['market']=='1x2_ft' and r['selection']==pick)
    mv=movement_row['movement']; drop=mv.get('confirmed') and mv.get('direction')=='down'
    opp=u['opportunities']['for']
    available=f['ht_sample']==4 and u['ht_sample']==4 and opp['trend'] is not None
    if not available: return {'status':'unavailable','favorite':favorite,'drop_confirmed':bool(drop),'reason':'Servono 4 PT validi per squadra e trend occasioni dello sfavorito.'}
    clean=f['against_ft']['last']==0; drought=u['for_ft']['last']==0
    supported=clean and drought and opp['trend']>=0 and u['for_ht']['frequency']>=.5 and f['against_ht']['frequency']>=.5
    return {'status':'signal' if supported else 'not_triggered','favorite':favorite,'underdog':underdog,
            'drop_confirmed':bool(drop),'favorite_fair_pct':fairp[pick]*100,
            'reason':'Possibile gol PT dello sfavorito: favorita a porta inviolata, sfavorita a digiuno e riscontri PT/occasioni.' if supported else 'Condizioni statistiche del segnale non tutte presenti.',
            'note':'Segnale contestuale: non implica GG PT né Over 1,5 PT e non aggiunge probabilità.'}
