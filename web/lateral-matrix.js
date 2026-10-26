/* Universal prematch observation. Live scores never overwrite frozen estimates. */
(function(root){
 'use strict';
 const MODEL='lateral-matrix-v5.0';
 const state={category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''};
 const opened=new Set();
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=(v,n=1)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{minimumFractionDigits:n,maximumFractionDigits:n}):'—';
 const pct=v=>typeof v==='number'&&Number.isFinite(v)?num(v)+'%':'—';
 const signed=v=>typeof v==='number'&&Number.isFinite(v)?(v>0?'+':'')+num(v):'—';
 const FAMILIES={all:'Tutti i mercati', '1x2':'1X2 e doppia chance',totals:'Over / Under e gol squadra',btts:'Goal / No Goal',asian:'Linee asiatiche e rimborsi',combo:'Combo e parziali/finale',ranges:'Multigol',scores:'Risultati esatti'};
 const CATEGORIES={all:'Tutte le analisi',convergence:'Convergenza',divergence:'Contrarian',lateral:'Laterali / combo',value:'Valore teorico positivo',no_quote:'Senza quota recente'};
 const STATUS={mature:'VALORE MATURO',contrarian:'CONTRARIAN',lateral:'LATERALE',value_watch:'VALORE IN OSSERVAZIONE',limited_data:'STORICO LIMITATO',no_edge:'PREZZO SENZA VANTAGGIO',no_quote:'SOLO STATISTICA',previous_version:'VERSIONE PRECEDENTE'};
 const QUALITY={supported:'Storico presente',limited:'Storico limitato',prior_dominant:'Una squadra con dati mancanti',prior_only:'Solo valori di riferimento'};
 const phase=(m,now=Date.now())=>{
  const f=m.fixture||{},s=f.result?.status||f.live?.status||f.status_short;
  if(['FT','AET','PEN'].includes(s))return 'finished';
  if(['CANC','PST','ABD','SUSP','WO','AWD'].includes(s))return 'other';
  return Date.parse(f.kickoff)>now?'upcoming':'live';
 };
 function rowsOf(m){
  if(m.multimarket?.matrix)return (m.multimarket.matrix.rows||[]).map(r=>{
   if(r.spec?.kind!=='handicap'||r.quote_adapter_version===2)return r;
   const clean={...r};
   for(const key of ['odd','first_odd','last_odd','ev_pct','ev_low_pct','ev_high_pct','oci','oci_previous','margin_pct','margin_delta_pp','delta_pct','persistence','speed_pp_per_hour','amplitude_pct'])clean[key]=null;
   return {...clean,quote_status:'invalid_adapter',category:'no_quote',market_behavior:'unavailable',margin_band:'unavailable',margin_trend:'unavailable',paired_books:0,agreeing_books:0,category_reason:'Quota handicap della versione precedente: associazione da aggiornare'};
  });
  return (m.multimarket?.markets||[]).map(r=>({...r,id:r.market+'::'+r.selection,
    family:r.market?.includes('gg')?'btts':'totals',period:r.market?.endsWith('_ht')?'ht':'ft',
    quote_status:r.quote_status||'missing',first_odd:r.quote?.movement?.first_odds?.[r.selection],
    bookmaker:r.quote?.bookmaker,category:'previous_version',quality:'limited'}));
 }
 function matchingRows(m,options=state){
  return rowsOf(m).filter(r=>{
   if(options.family!=='all'&&r.family!==options.family)return false;
   if(options.period!=='all'&&r.period!==options.period)return false;
   if(options.category==='convergence')return r.market_behavior==='convergence';
   if(options.category==='divergence')return r.market_behavior==='divergence';
   if(options.category==='lateral')return ['combo','asian','ranges'].includes(r.family);
   if(options.category==='value')return r.ev_pct>0&&r.quote_status==='recent';
   if(options.category==='no_quote')return r.quote_status!=='recent';
   return true;
  });
 }
 function selectMatches(payload,options=state,now=Date.now(),lifecycle=phase){
  const seen=new Set();
  return (payload.matches||[]).filter(m=>{
   const f=m.fixture||{},id=String(f.fixture_id);
   if(!f.fixture_id||seen.has(id))return false;
   seen.add(id);
   if(options.phase!=='all'&&lifecycle(m,now)!==options.phase)return false;
   if(options.search&&![f.match,f.league,f.country].join(' ').toLocaleLowerCase().includes(options.search.toLocaleLowerCase()))return false;
   return matchingRows(m,options).length>0||(options.category==='all'&&options.family==='all'&&options.period==='all');
  }).sort((a,b)=>{
   if(options.sort==='value'){
    const score=m=>Math.max(-100,...matchingRows(m,options).filter(r=>r.quote_status==='recent'&&r.quality==='supported').map(r=>r.ev_low_pct??-100));
    const d=score(b)-score(a);if(d)return d;
   }
   return (Date.parse(a.fixture.kickoff)||0)-(Date.parse(b.fixture.kickoff)||0)||String(a.fixture.fixture_id).localeCompare(String(b.fixture.fixture_id));
  });
 }
 function badge(row){
  const behavior=row.market_behavior;
  return '<span class="mx-badge mx-'+esc(row.category||'no_quote')+'">'+esc(STATUS[row.category]||'IN OSSERVAZIONE')+'</span>'+
   (behavior==='convergence'&&row.category!=='mature'?'<span class="mx-behavior">↘ Convergenza</span>':behavior==='divergence'&&row.category!=='contrarian'?'<span class="mx-behavior">↗ Divergenza</span>':'');
 }
 function bestOpportunity(rows){
  // Compare actual prices across all represented markets, including Asian
  // settlements and combos. Conservative EV is already computed by backend.
  const candidates=rows.filter(r=>r.quote_status==='recent'&&r.odd>1&&r.ev_pct>0&&Number.isFinite(r.ev_low_pct));
  const supported=candidates.filter(r=>r.quality==='supported');
  return [...(supported.length?supported:candidates)].sort((a,b)=>b.ev_low_pct-a.ev_low_pct||b.ev_pct-a.ev_pct||(b.statistical_pct||0)-(a.statistical_pct||0)||String(a.id).localeCompare(String(b.id)))[0]||null;
 }
 function movementSignal(row){
  const delta=row?.quote_status==='recent'&&Number.isFinite(row.delta_pct)?row.delta_pct:null;
  const magnitude=delta==null?null:Math.abs(delta),level=magnitude==null?'unknown':magnitude>=8?'red':magnitude>=3?'yellow':'green';
  const direction=delta==null?'Storico assente':Math.abs(delta)<.005?'Invariata':delta<0?'Discesa':'Salita';
  const arrow=delta==null?'—':Math.abs(delta)<.005?'→':delta<0?'↓':'↑';
  return '<div class="mx-net mx-net-'+level+'" aria-label="'+esc(direction+(delta==null?'':' '+signed(delta)+'%'))+'"><b>'+arrow+' '+(delta==null?'—':signed(delta)+'%')+'</b><small>'+direction+' · '+(level==='red'?'forte':level==='yellow'?'media':level==='green'?'lieve':'da osservare')+'</small></div>';
 }
 function quoteCell(row,title,helpers={}){
  row=row||{};
  const recent=row.quote_status==='recent';
  const band={low:'basso',medium:'intermedio',high:'alto'}[row.margin_band]||'';
  return '<section class="mx-quote-cell"><h4>'+esc(title)+'</h4><div class="mx-quote-pair"><span>Apertura*<b>'+num(row.first_odd,2)+'</b></span><span>'+(recent?'Attuale':'Ultima quota')+'<b>'+num(recent?row.odd:row.last_odd,2)+'</b></span></div>'+movementSignal(row)+
   '<div class="mx-quote-metrics"><span>Statistica <b>'+pct(row.statistical_pct)+'</b></span><span>OCI <b>'+signed(row.oci)+'</b></span><span>Aggio <b>'+pct(row.margin_pct)+'</b> '+esc(band)+'</span><span>Δ aggio <b>'+signed(row.margin_delta_pp)+' pt</b></span></div>'+
   '<small class="mx-quote-source">'+esc(row.bookmaker||'Quota non disponibile')+(row.provider_updated_at?' · '+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at):'')+'</small>'+
   '<small class="mx-quote-consensus">OCI: '+esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi'+(row.oci_previous==null?'':' · prima '+signed(row.oci_previous))+'</small></section>';
 }
 function quoteBoard(rows,helpers){
  const find=(kind,period,side,line)=>rows.find(r=>r.spec?.kind===kind&&r.period===period&&r.spec.side===side&&(line==null||r.spec.line===line));
  return '<section class="mx-quote-board" aria-label="Quote 1X2 apertura e attuale"><h4 class="mx-section-title">QUOTE 1 · X · 2 — APERTURA E ATTUALE</h4><div class="mx-1x2">'+[['1','1 · Casa'],['x','X · Pareggio'],['2','2 · Ospite']].map(([s,l])=>quoteCell(find('result','ft',s),l,helpers)).join('')+'</div>'+
   '<div class="mx-goal-quotes">'+[['total','ht','over',1.5,'Over 1,5 PT'],['total','ft','over',2.5,'Over 2,5 FT'],['total','ft','over',3.5,'Over 3,5 FT'],['btts','ht','yes',null,'GG PT']].map(([k,p,s,l,t])=>quoteCell(find(k,p,s,l),t,helpers)).join('')+'</div>'+
   '<p class="mx-quote-legend">* Prima quota osservata dello stesso bookmaker. ↑ sale · ↓ scende. <span class="mx-key-green">Verde &lt;3%</span> · <span class="mx-key-yellow">giallo 3–8%</span> · <span class="mx-key-red">rosso ≥8%</span>: intensità del movimento, non probabilità di vincita. OCI e aggio sono calcolati sul mercato completo e sugli operatori confrontabili.</p></section>';
 }
 function opportunityPanel(rows){
  const best=bestOpportunity(rows),priced=rows.filter(r=>r.quote_status==='recent').length;
  if(!best)return '<aside class="mx-best mx-best-empty"><span class="mx-section-title">MERCATO DA VALUTARE</span><h4>'+(priced?'Nessun vantaggio di prezzo positivo':'In attesa di quote utilizzabili')+'</h4><p>'+rows.length+' esiti statistici · '+priced+' con quota recente. La partita resta visibile e viene rivalutata al prossimo Fast.</p></aside>';
  const supported=best.quality==='supported',stable=supported&&best.ev_low_pct>0;
  return '<aside class="mx-best '+(stable?'mx-best-stable':'mx-best-watch')+'"><span class="mx-section-title">'+(stable?'MIGLIOR VALORE STIMATO':supported?'MIGLIOR IPOTESI · DA CONFERMARE':'IPOTESI CON DATI LIMITATI')+'</span><h4>'+esc(best.label)+'</h4><div class="mx-best-price">'+num(best.odd,2)+'<small>QUOTA ATTUALE · '+esc(best.bookmaker||'')+'</small></div>'+badge(best)+
   '<dl><div><dt>Statistica pura</dt><dd>'+pct(best.statistical_pct)+'</dd></div><div><dt>Quota equa del modello</dt><dd>'+num(best.fair_odd,2)+'</dd></div><div><dt>Valore teorico</dt><dd>'+signed(best.ev_pct)+'%</dd></div><div><dt>Scenario prudente</dt><dd>'+signed(best.ev_low_pct)+'%</dd></div><div><dt>OCI · precedente → attuale</dt><dd>'+signed(best.oci_previous)+' → '+signed(best.oci)+'</dd></div><div><dt>Aggio · variazione</dt><dd>'+pct(best.margin_pct)+' · '+signed(best.margin_delta_pp)+' pt</dd></div></dl>'+
   '<p>'+esc(best.category_reason||'Stima statistica indipendente confrontata con il prezzo reale.')+'</p><small>Confrontati '+rows.length+' esiti, '+priced+' quotati. Priorità allo storico presente e al rendimento teorico più alto nello scenario prudente. Non è un profitto garantito.</small></aside>';
 }
 function price(row){
  if(row.quote_status!=='recent')return '<span class="mx-missing">'+(row.quote_status==='stale'?'Da aggiornare · ultima '+num(row.last_odd,2):'Non disponibile')+'</span>';
  const arrow=row.odd<row.first_odd?'↘':row.odd>row.first_odd?'↗':'→';
  return '<span class="mx-price">'+num(row.first_odd,2)+' <i>'+arrow+'</i> <b>'+num(row.odd,2)+'</b></span><small>'+esc(row.bookmaker)+' · '+signed(row.delta_pct)+'%</small>';
 }
 function marketRow(row,helpers){
  const asian=['asian_total','handicap','dnb'].includes(row.spec?.kind);
  const trend={compressing:'in compressione',expanding:'in aumento',stable:'stabile',unavailable:'storico non disponibile'}[row.margin_trend]||'';
  const marginBand={low:'Basso',medium:'Intermedio',high:'Alto',unavailable:'Non disponibile'}[row.margin_band]||'';
  return '<div class="mx-market-row" role="row"><div class="mx-market-name" role="cell"><b>'+esc(row.label)+'</b>'+badge(row)+
   '<small>'+esc(row.category_reason||'Profilo statistico indipendente dalle quote')+'</small></div>'+
   '<div role="cell" data-title="Statistica"><strong>'+pct(row.statistical_pct)+'</strong><small>'+esc(QUALITY[row.quality]||'Stima archiviata')+'</small>'+
    '<small>'+num(row.low_pct)+'–'+num(row.high_pct)+'% · sensibilità</small>'+(asian?'<small>Esito con profitto; rimborsi nel calcolo</small>':'')+'</div>'+
   '<div role="cell" data-title="Quota equa"><b>'+num(row.fair_odd,2)+'</b><small>Dal modello</small></div>'+
   '<div role="cell" data-title="Prima → attuale">'+price(row)+
    (row.provider_updated_at?'<small>Fonte '+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="OCI"><b>'+signed(row.oci)+'</b><small>'+esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi</small>'+
    (row.oci_previous!=null?'<small>Precedente '+signed(row.oci_previous)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="Aggio"><b>'+pct(row.margin_pct)+'</b><small>'+esc(marginBand)+' · Δ '+signed(row.margin_delta_pp)+' punti</small><small>'+esc(trend)+'</small>'+
    (row.margin_anomaly?'<small>Margine anomalo</small>':'')+'</div>'+
   '<div role="cell" data-title="Valore teorico"><b class="'+(row.ev_pct>0?'mx-positive':'')+'">'+(row.ev_pct==null?'—':signed(row.ev_pct)+'%')+'</b><small>Scenario prudente '+(row.ev_low_pct==null?'—':signed(row.ev_low_pct)+'%')+'</small></div>'+
   '<div role="cell" data-title="Movimento"><b>'+pct(row.amplitude_pct)+'</b><small>'+num(row.speed_pp_per_hour,2)+' punti/ora</small><small>Persistenza '+pct(row.persistence==null?null:row.persistence*100)+'</small>'+
    (row.movement_volatile?'<small>Movimento con inversioni / ampiezza elevata</small>':'')+'</div></div>';
 }
 function teamProfiles(m){
  const a=m.multimarket||{};
  return '<div class="mx-teams">'+['home','away'].map(side=>{
   const ft=a.profiles?.[side]?.for_ft||{},def=a.profiles?.[side]?.against_ft||{},ht=a.profiles?.[side]?.for_ht||{};
   return '<section><h4>'+esc(m.fixture?.[side]||(side==='home'?'Casa':'Ospite'))+'</h4><dl>'+[
    ['Storico utilizzato',num(ft.sample,0)+' FT · '+num(ht.sample,0)+' PT'],['Attacco / difesa',num(ft.rate,2)+' / '+num(def.rate,2)],
    ['Gol medi / ultima',num(ft.mean,2)+' / '+num(ft.last,0)],['Media senza ultima',num(ft.without_last,2)],
    ['xG osservati',ft.xg_sample?num(ft.xg_mean,2)+' · '+ft.xg_sample+' gare':'Non disponibili'],
    ['Gare senza segnare',num(ft.drought,0)],['Gol attesi FT / PT',num(a.rates?.[side+'_ft'],2)+' / '+num(a.rates?.[side+'_ht'],2)]
    ].map(([k,v])=>'<div><dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd></div>').join('')+'</dl>'+
    (ft.under_conversion?'<p>Potenziale xG superiore ai gol realizzati.</p>':'')+(ft.peak_reduction>.15?'<p>Goleada recente ridimensionata.</p>':'')+'</section>';
  }).join('')+'</div>';
 }
 function matrixDetail(m,rows,helpers){
  const a=m.multimarket||{},coverage=a.matrix?.coverage||{};
  return '<div class="mx-detail"><div data-live-slot="panel">'+(helpers.livePanel?.(m)||'')+'</div>'+teamProfiles(m)+
   '<p class="mx-explanation">'+rows.length+' mercati in questa vista · '+(coverage.modeled_markets??rowsOf(m).length)+' calcolati. Prezzi e segnali si riferiscono alla stessa linea e allo stesso periodo.</p>'+
   '<div class="mx-table" role="table" aria-label="Matrice delle opportunità"><div class="mx-table-head" role="row">'+['Mercato / lettura','Statistica pura','Quota equa','Prima → attuale','OCI','Aggio / variazione','Rendimento teorico','Componenti OCI'].map(t=>'<div role="columnheader">'+t+'</div>').join('')+'</div>'+rows.map(r=>marketRow(r,helpers)).join('')+'</div>'+
   '<details class="mx-method"><summary>Come leggere questi dati</summary><p>Aggio basso indica prezzi più competitivi nel campione osservato; non misura liquidità o confidenza del bookmaker. Una compressione del margine non stabilisce la direzione dell’esito.</p><p>OCI descrive ampiezza, velocità, persistenza e consenso delle variazioni di quota. Non misura denaro scambiato e non dimostra smart money. Una spinta sull’Over 2,5 non conferma automaticamente Over 3,5 o combo.</p><p>Il rendimento è teorico: include mezze vincite, mezze perdite e rimborsi asiatici. Le quote delle combo sono reali oppure risultano non disponibili; le quote delle singole gambe non vengono moltiplicate.</p><p>La sensibilità mostra quanto cambia la stima togliendo un precedente alla volta; non è un intervallo di confidenza. Le frequenze storiche sono descrittive e il modello deve ancora essere validato su nuovi risultati.</p></details></div>';
 }
 function card(m,helpers){
  const f=m.fixture||{},a=m.multimarket||{},all=rowsOf(m),rows=matchingRows(m),fid=String(f.fixture_id),open=opened.has(fid);
  const counts={convergence:all.filter(r=>r.market_behavior==='convergence').length,divergence:all.filter(r=>r.market_behavior==='divergence').length,value:all.filter(r=>r.ev_pct>0).length};
  const noSnapshot=a.status==='no_prematch_snapshot';
  return '<article class="mx-card" data-live-fixture="'+esc(fid)+'"><header><div><time>'+esc(f.time||'')+'</time><small>'+esc(f.country)+' · '+esc(f.league)+'</small><h3>'+esc(f.match)+'</h3></div><div class="mx-live"><span data-live-slot="badge">'+(helpers.liveBadge?.(m)||'')+'</span><span data-live-slot="state">'+esc(helpers.stateLabel?.(m)||'')+'</span></div></header>'+
   (noSnapshot?'<p class="mx-explanation">Partita presente nel palinsesto. Nessuna previsione salvata prima dell’inizio: gli esiti non vengono usati per ricostruirla a posteriori.</p>':'<div class="mx-match-layout">'+quoteBoard(all,helpers)+opportunityPanel(rows)+'</div>')+
   '<div class="mx-match-summary"><span>'+all.length+' mercati</span><span>'+counts.convergence+' convergenze</span><span>'+counts.divergence+' divergenze</span><span>'+all.filter(r=>r.quote_status==='recent').length+' quote recenti</span></div>'+
   (all.length?'<button class="mx-open" type="button" data-mx-open="'+esc(fid)+'" aria-expanded="'+open+'">'+(open?'Chiudi matrice':'Apri matrice completa')+' · '+rows.length+' mercati <span>'+(open?'−':'+')+'</span></button>':'')+
   (open?matrixDetail(m,rows,helpers):'')+'<footer>Analisi '+esc(helpers.timestamp?.(a.captured_at)||a.captured_at||'non disponibile')+' · '+esc(a.model_id||'')+(f.data_freshness&&f.data_freshness!=='fresh'?' · '+esc(f.data_freshness):'')+'</footer></article>';
 }
 let payload,host,helpers;
 function render(p,h,services={}){
  payload=p;host=h;helpers=services;
  const all=p.matches||[],rows=all.flatMap(rowsOf),matches=selectMatches(p,state,Date.now(),services.phase||phase);
  const count=key=>key==='all'?all.length:all.filter(m=>matchingRows(m,{...state,category:key,family:'all',period:'all'}).length).length;
  const options=(items,chosen)=>Object.entries(items).map(([v,l])=>'<option value="'+v+'" '+(v===chosen?'selected':'')+'>'+esc(l)+'</option>').join('');
  h.innerHTML='<section class="mx-dashboard"><header class="mx-heading"><div><span>ARABSNIPER · OSSERVATORIO LATERALE</span><h2>Ogni partita. Tutte le letture.</h2><p>Statistica indipendente, prezzi reali, OCI e aggio per ciascun mercato.</p></div><b>V5 · IN VERIFICA</b></header>'+
   '<div class="mx-counters"><span><b>'+all.length+'</b> partite disponibili</span><span><b>'+rows.length+'</b> mercati calcolati</span><span><b>'+rows.filter(r=>r.quote_status==='recent').length+'</b> con quota recente</span><span>Nessun limite di selezioni</span></div>'+
   '<nav class="mx-categories" aria-label="Lettura del mercato">'+Object.entries(CATEGORIES).map(([k,l])=>'<button type="button" data-mx-category="'+k+'" aria-pressed="'+(state.category===k)+'">'+esc(l)+' <b>'+count(k)+'</b></button>').join('')+'</nav>'+
   '<div class="mx-controls"><label>Mercati<select data-mx-filter="family">'+options(FAMILIES,state.family)+'</select></label><label>Periodo<select data-mx-filter="period">'+options({all:'Tutti i periodi',ft:'Finale',ht:'Primo tempo',sh:'Secondo tempo'},state.period)+'</select></label>'+
   '<label>Stato<select data-mx-filter="phase">'+options({all:'Tutte le partite',upcoming:'Da iniziare',live:'In corso',finished:'Terminate',other:'Rinviate / altri stati'},state.phase)+'</select></label><label>Ordine<select data-mx-filter="sort">'+options({time:'Orario',value:'Valore teorico più stabile'},state.sort)+'</select></label>'+
   '<label class="mx-search">Cerca<input type="search" data-mx-filter="search" value="'+esc(state.search)+'" placeholder="Squadra o campionato"></label></div>'+
   '<p class="mx-caption">Convergenza e contrarian descrivono il rapporto tra modello e movimento. Il valore resta una stima da verificare; OCI e aggio non sono volumi di denaro.</p>'+
   '<div class="mx-results" aria-live="polite"><p class="mx-results-count">'+matches.length+' di '+all.length+' partite · '+esc(CATEGORIES[state.category])+'</p>'+
   (matches.length?matches.map(m=>card(m,services)).join(''):'<div class="mx-empty">Nessuna partita con questi filtri. <button type="button" data-mx-reset>Mostra tutto il palinsesto</button></div>')+'</div>'+
   '<p class="mx-caption">Ogni Fast ricalcola le analisi prepartita. Gli aggiornamenti del fornitore possono essere meno frequenti; la data delle quote è sempre indicata. Le partite iniziate conservano la previsione prepartita, con punteggio live separato.</p></section>';
  h.onclick=e=>{
   const expand=e.target.closest?.('[data-mx-open]'),category=e.target.closest?.('[data-mx-category]');
   if(expand){const id=expand.dataset.mxOpen;opened.has(id)?opened.delete(id):opened.add(id);render(payload,host,helpers);}
   else if(category){state.category=category.dataset.mxCategory;render(payload,host,helpers);}
   else if(e.target.closest?.('[data-mx-reset]')){Object.assign(state,{category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''});render(payload,host,helpers);}
  };
  h.onchange=e=>{const key=e.target.dataset?.mxFilter;if(key&&key in state){state[key]=e.target.value;render(payload,host,helpers);}};
 }
 const api={MODEL,state,render,rowsOf,matchingRows,selectMatches,phase,marketRow,bestOpportunity,movementSignal,quoteCell,quoteBoard,opportunityPanel,esc};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.ArabMatrix=api;
})(typeof window!=='undefined'?window:globalThis);
