/* Four independent statistical shortlists. Quotes confirm; never rerank. */
(function (root) {
  'use strict';
  const MODEL = 'statistics-first-v4.0';
  const LABELS = {over25_ft:'Over 2,5 finale', over15_ht:'Over 1,5 primo tempo',
    over35_ft:'Over 3,5 finale', gg_ht:'Entrambe a segno PT'};
  const SHORT = {over25_ft:'Over 2,5 FT',over15_ht:'Over 1,5 PT',over35_ft:'Over 3,5 FT',gg_ht:'GG primo tempo'};
  const state = {market:'over25_ft', scope:'selected', signal:'all', phase:'upcoming', sort:'probability', search:''};
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = (v,n=1) => typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{minimumFractionDigits:n,maximumFractionDigits:n}):'—';
  const pct = v => number(v)+'%';
  const rowFor = (match, market) => (match.multimarket?.markets||[]).find(r=>r.market===market&&['over','yes'].includes(r.selection));
  function phase(match, now=Date.now()) {
    const f=match.fixture||{}, status=f.result?.status||f.live?.status||f.status_short;
    if(['FT','AET','PEN'].includes(status))return 'finished';
    if(['PST','CANC','ABD','SUSP','WO','AWD'].includes(status))return 'other';
    return Date.parse(f.kickoff)>now?'upcoming':'live';
  }
  function selected(match, row) {
    return match.multimarket?.model_id===MODEL && row?.published_selection===true;
  }
  function selectRows(payload, options=state, now=Date.now(), lifecycle=phase) {
    const seen=new Set();
    return (payload.matches||[]).filter(m=>{
      const f=m.fixture||{}, id=String(f.fixture_id), row=rowFor(m,options.market);
      if(!row||!f.fixture_id||seen.has(id))return false;
      seen.add(id);
      if(options.scope==='selected'&&!selected(m,row))return false;
      const confirmation=row.market_confirmation?.status;
      if(options.signal==='confirmed'&&(!selected(m,row)||confirmation!=='confirmed'))return false;
      if(options.signal==='context'&&(!selected(m,row)||confirmation!=='context'))return false;
      if(options.phase!=='all'&&lifecycle(m,now)!==options.phase)return false;
      return !options.search||[f.match,f.league,f.country].join(' ').toLocaleLowerCase().includes(options.search.toLocaleLowerCase());
    }).sort((a,b)=>{
      if(options.sort==='probability'){
        const av=rowFor(a,options.market)?.statistical_score??-1,bv=rowFor(b,options.market)?.statistical_score??-1;
        if(av!==bv)return bv-av;
      }
      return Date.parse(a.fixture.kickoff)-Date.parse(b.fixture.kickoff)||String(a.fixture.fixture_id).localeCompare(String(b.fixture.fixture_id));
    });
  }
  function signal(match,row) {
    if(match.multimarket?.model_id!==MODEL)return {className:'previous',label:'Analisi precedente',description:'Previsione conservata con la sua versione originale.'};
    if(!selected(match,row))return {className:'none',label:row.statistical_candidate?'Fuori dalla lista top':'Nessuna selezione',
      description:row.statistical_candidate?'Criteri statistici superati; altre partite hanno una stima più stabile.':row.selection_reasons?.[0]||'Dati insufficienti.'};
    const c=row.market_confirmation?.status;
    if(c==='confirmed')return {className:'confirmed',label:'ORO + AZZURRO',description:'Statistica selezionata e movimento favorevole sullo stesso mercato.'};
    if(c==='context')return {className:'context',label:'ORO · SCENARIO QUOTE',description:'Statistica selezionata; favorita in calo e avversaria con potenziale inespresso. Conferma indiretta 1X2.'};
    if(c==='contrary')return {className:'contrary',label:'ORO · QUOTE IN CONTRASTO',description:'La statistica supera il filtro; il movimento del mercato è contrario.'};
    return {className:'gold',label:'ORO · STATISTICA',description:c==='unavailable'?'Statistica selezionata; quota recente non disponibile.':'Statistica selezionata; nessuna conferma sufficiente nelle quote.'};
  }
  function quoteHtml(row) {
    const q=row.quote||row.display_quote, current=row.odd??row.display_odd;
    const first=q?.movement?.first_odds?.[row.selection];
    const stale=row.quote_status==='stale';
    if(!q||current==null)return '<div class="sf-price"><span>Quota non disponibile</span><b>—</b><small>Il calcolo statistico resta indipendente.</small></div>';
    const delta=first&&current?(current/first-1)*100:null;
    return '<div class="sf-price"><div><span>Prima osservata</span><b>'+number(first,2)+'</b></div><span class="sf-price-arrow" aria-hidden="true">'+(delta<0?'↘':delta>0?'↗':'→')+'</span><div><span>'+(stale?'Ultima disponibile':'Quota attuale')+'</span><b>'+number(current,2)+'</b></div><small>'+escape(q.bookmaker||'Bookmaker')+(delta!==null?' · '+(delta>0?'+':'')+number(delta)+'%':'')+(stale?' · da aggiornare':'')+'</small></div>';
  }
  function teamTable(match,period) {
    const a=match.multimarket, profiles=a.profiles||{};
    return ['home','away'].map((s,i)=>{
      const attack=profiles[s]?.['for_'+period]||{}, defense=profiles[s]?.['against_'+period]||{};
      const lastState={above:'Ultima sopra la media',below:'Ultima sotto la media',in_line:'Ultima in linea',unavailable:'Storico non disponibile'}[attack.last_state]||'';
      return '<section class="sf-team"><h4>'+(i?'Squadra ospite':'Squadra di casa')+'</h4><dl>'+
        [['Gol medi',number(attack.mean,2)],['Gol ultima partita',number(attack.last,0)],
         ['Media senza ultima',number(attack.without_last,2)],['Potenziale offensivo',number(attack.rate,2)],
         ['Debolezza difensiva',number(defense.rate,2)],['xG nel periodo',attack.xg_sample?number(attack.xg_mean,2)+' · '+attack.xg_sample+' gare':'Non disponibili'],
         ['Gare senza segnare',number(attack.drought,0)],['Precedenti utilizzati',number(attack.sample,0)]]
        .map(([k,v])=>'<div><dt>'+escape(k)+'</dt><dd>'+escape(v)+'</dd></div>').join('')+'</dl><p>'+escape(lastState)+
        (attack.under_conversion?' · potenziale xG inespresso':attack.peak_reduction>.15?' · media ridotta per limitare la goleada':'')+'</p></section>';
    }).join('');
  }
  function detailHtml(match,row,helpers) {
    const a=match.multimarket, period=row.market.endsWith('_ht')?'ht':'ft', m=row.movement||{};
    const list=(title,values,cls)=>values?.length?'<section class="'+cls+'"><h4>'+title+'</h4><ul>'+values.map(v=>'<li>'+escape(v)+'</li>').join('')+'</ul></section>':'';
    const all=Object.entries(LABELS).map(([k,label])=>{
      const r=rowFor(match,k);if(!r)return '';
      const badge=signal(match,r);
      return '<tr><th>'+escape(label)+'</th><td>'+pct(r.statistical_pct)+'</td><td>'+number(r.odd??r.display_odd,2)+'</td><td>'+escape(badge.label)+'</td></tr>';
    }).join('');
    const patterns=row.market_confirmation?.patterns||[];
    return '<div class="sf-detail-body">'+
      '<div data-live-slot="panel">'+(helpers.livePanel?.(match)||'')+'</div>'+
      '<div class="sf-team-grid">'+teamTable(match,period)+'</div>'+
      '<div class="sf-evidence">'+list('Perché emerge',row.pros,'sf-pros')+list('Punti deboli',row.cons,'sf-cons')+'</div>'+
      list('Perché non entra nella selezione',row.selection_reasons,'sf-reasons')+
      '<section class="sf-market-detail"><h4>Conferma nelle quote</h4><p>'+escape(signal(match,row).description)+'</p>'+
      (patterns.includes('over25_from_above_2')?'<p>Over 2,5 in calo da una quota osservata sopra 2,00, con conferma di più operatori.</p>':'')+
      '<dl class="sf-movement-grid"><div><dt>Operatori concordi</dt><dd>'+escape(m.agreeing_books??0)+' / '+escape(m.paired_books??0)+'</dd></div><div><dt>Movimento</dt><dd>'+number(m.amplitude_pct)+'% '+(m.direction==='down'?'↓':m.direction==='up'?'↑':'')+'</dd></div><div><dt>OCI</dt><dd>'+number(m.oci)+'</dd></div><div><dt>Variazione aggio</dt><dd>'+number(m.median_overround_delta_pp)+' punti</dd></div></dl>'+
      '<p class="sf-note">OCI e aggio descrivono lo stesso movimento: non vengono sommati alla probabilità statistica.</p></section>'+
      '<div class="sf-table-wrap"><table><caption>I quattro mercati della partita</caption><thead><tr><th>Mercato</th><th>Stima</th><th>Quota</th><th>Stato</th></tr></thead><tbody>'+all+'</tbody></table></div>'+
      '<details class="sf-calculation"><summary>Dati e calcolo</summary><p>Riferimento del campionato: '+pct(row.league_probability_pct)+
      '. Variazione rimuovendo un precedente alla volta: '+pct(row.sensitivity?.low_pct)+' – '+pct(row.sensitivity?.high_pct)+
      '. È un controllo di stabilità del calcolo, non un intervallo di confidenza.</p><p>Precedenti diretti aggiuntivi: '+escape(a.h2h?.used??0)+
      '; già presenti nello storico ed esclusi dal doppio conteggio: '+escape(a.h2h?.overlapping_excluded??0)+'.</p>'+
      '<p>Quota di pareggio della stima: '+number(row.price_assessment?.reference_fair_odd,2)+
      '. Rendimento teorico del modello alla quota osservata: '+pct(row.price_assessment?.estimated_return_pct)+
      '. Il vantaggio non è ancora validato.</p><p>Versione '+escape(a.model_id)+' · parametri '+escape(a.config_id||'—')+
      '. Il riferimento è il campionato; la forza dei singoli avversari storici non è normalizzata.</p></details></div>';
  }
  function cardHtml(match,row,helpers) {
    const f=match.fixture, a=match.multimarket, b=signal(match,row);
    const isSelected=selected(match,row), period=row.market.endsWith('_ht')?'ht':'ft';
    const home=a.profiles?.home?.['for_'+period],away=a.profiles?.away?.['for_'+period];
    return '<article class="sf-card sf-'+b.className+'" data-live-fixture="'+escape(f.fixture_id)+'">'+
      '<div class="sf-card-top"><span class="sf-badge" title="'+escape(b.description)+'">'+escape(b.label)+'</span>'+
      (isSelected?'<span class="sf-rank">#'+escape(row.published_rank)+' · '+escape(SHORT[row.market])+'</span>':'')+'</div>'+
      '<div class="sf-main"><div class="sf-match"><div class="sf-fixture-meta"><time>'+escape(f.time||'')+'</time><span>'+escape(f.country)+' · '+escape(f.league)+'</span></div>'+
      '<h3>'+escape(f.match)+'</h3><span data-live-slot="badge">'+(helpers.liveBadge?.(match)||'')+'</span>'+
      '<small data-live-slot="state">'+escape(helpers.stateLabel?.(match)||'')+'</small></div>'+
      '<div class="sf-probability"><span>'+escape(LABELS[row.market])+'</span><strong>'+pct(row.statistical_pct)+'</strong><small>Stima statistica</small></div>'+quoteHtml(row)+'</div>'+
      '<div class="sf-insight"><span>'+escape(b.description)+'</span>'+
      (home?.under_conversion||away?.under_conversion?'<b>Occasioni create, gol ancora bassi</b>':home?.peak_reduction>.15||away?.peak_reduction>.15?'<b>Goleada recente ridimensionata nel calcolo</b>':'')+'</div>'+
      '<div class="sf-live-outcome" data-live-slot="outcome" data-market="'+escape(row.market)+'">'+escape(helpers.outcome?.(match,row.market)||'')+'</div>'+
      '<details class="sf-detail" data-sf-detail="'+escape(f.fixture_id)+'"><summary>Perché questa partita <span>Attacco, difesa e quote</span></summary>'+detailHtml(match,row,helpers)+'</details>'+
      '<footer>Analisi '+escape(helpers.timestamp?.(a.captured_at)||a.captured_at||'—')+' · '+(a.model_id===MODEL?'Modello nuovo in verifica':'Versione precedente conservata')+'</footer></article>';
  }
  let currentPayload=null,currentHost=null,currentHelpers={};
  function render(payload,host,helpers={}) {
    currentPayload=payload;currentHost=host;currentHelpers=helpers;
    const opened=new Set(Array.from(host.querySelectorAll?.('details[data-sf-detail][open]')||[]).map(x=>x.dataset.sfDetail));
    const lifecycle=helpers.phase||phase;
    const rows=selectRows(payload,state,Date.now(),lifecycle);
    const current=(payload.matches||[]).filter(m=>m.multimarket?.model_id===MODEL);
    const counts=Object.fromEntries(Object.keys(LABELS).map(k=>[k,current.filter(m=>selected(m,rowFor(m,k))&&lifecycle(m,Date.now())==='upcoming').length]));
    const tabs=Object.keys(LABELS).map(k=>'<button type="button" role="tab" aria-selected="'+(state.market===k)+'" data-sf-market="'+k+'"><span>'+escape(SHORT[k])+'</span><b>'+counts[k]+'</b></button>').join('');
    const options=(values,chosen)=>values.map(([v,l])=>'<option value="'+v+'" '+(v===chosen?'selected':'')+'>'+escape(l)+'</option>').join('');
    host.innerHTML='<section class="sf-dashboard"><header class="sf-heading"><div><span class="sf-eyebrow">ARABSNIPER · STATISTICHE PRIMA</span><h2>Le partite che emergono dai dati.</h2><p>Prima il potenziale di gol. Poi le quote come conferma.</p></div><span class="sf-version">V4 · IN VERIFICA</span></header>'+
      '<div class="sf-summary"><span><b>'+current.length+'</b> partite analizzate con V4</span><span>Fino a <b>5</b> per mercato, senza riempire la lista</span><span>Le stime non sono risultati garantiti</span></div>'+
      '<nav class="sf-tabs" role="tablist" aria-label="Mercato">'+tabs+'</nav>'+
      '<div class="sf-controls"><label>Mostra<select data-sf-filter="scope">'+options([['selected','Solo selezioni statistiche'],['all','Tutte le analisi']],state.scope)+'</select></label>'+
      '<label>Conferma quote<select data-sf-filter="signal">'+options([['all','Tutte le selezioni'],['confirmed','Oro + azzurro · stesso mercato'],['context','Scenario favorita + digiuno']],state.signal)+'</select></label>'+
      '<label>Stato<select data-sf-filter="phase">'+options([['upcoming','Da iniziare'],['live','In corso / da aggiornare'],['finished','Terminate'],['all','Tutti gli stati']],state.phase)+'</select></label>'+
      '<label>Ordine<select data-sf-filter="sort">'+options([['probability','Statistica più stabile'],['time','Orario']],state.sort)+'</select></label>'+
      '<label class="sf-search">Cerca<input type="search" data-sf-filter="search" value="'+escape(state.search)+'" placeholder="Squadra o campionato"></label></div>'+
      '<p class="sf-legend"><span class="sf-key gold"></span> Oro: selezione statistica <span class="sf-key blue"></span> Azzurro: movimento favorevole nello stesso mercato. Nessun accordo obbligatorio fra percentuali.</p>'+
      '<div class="sf-results" aria-live="polite"><p class="sf-results-count">'+rows.length+' '+(rows.length===1?'partita':'partite')+' · '+escape(LABELS[state.market])+'</p>'+
      (rows.length?rows.map(m=>cardHtml(m,rowFor(m,state.market),helpers)).join(''):
        '<div class="sf-empty"><strong>Nessuna partita supera questi criteri.</strong><p>La lista può restare vuota. Le analisi disponibili spiegano i motivi di esclusione.</p><button type="button" data-sf-action="all">Vedi tutte le analisi</button></div>')+
      '</div><p class="sf-footnote">Le partite iniziate restano consultabili scegliendo lo stato. Analisi e quote sono quelle prepartita; il punteggio si aggiorna separatamente.</p></section>';
    for(const el of Array.from(host.querySelectorAll?.('details[data-sf-detail]')||[]))el.open=opened.has(el.dataset.sfDetail);
    host.onclick=event=>{
      const tab=event.target.closest?.('[data-sf-market]');
      if(tab){state.market=tab.dataset.sfMarket;render(currentPayload,currentHost,currentHelpers);return;}
      if(event.target.closest?.('[data-sf-action="all"]')){state.scope='all';state.signal='all';render(currentPayload,currentHost,currentHelpers);}
    };
    host.onchange=event=>{
      const key=event.target.dataset?.sfFilter;
      if(key&&key in state){state[key]=event.target.value;render(currentPayload,currentHost,currentHelpers);}
    };
  }
  const api={render,selectRows,rowFor,signal,phase,MODEL,LABELS,state,escape};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  root.ArabStatistics=api;
})(typeof window!=='undefined'?window:globalThis);
