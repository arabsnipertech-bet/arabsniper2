/* Universal prematch observation. Live scores never overwrite frozen estimates. */
(function(root){
 'use strict';
 const MODEL='lateral-matrix-v5.0';
 const POLICY='gold-goals-v3',SCOPE='gold-senior-whitelist-v1';
 const acceptedPolicy=d=>[POLICY,'gold-goals-v2'].includes(d?.policy_id);
 const state={category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''};
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=(v,n=1)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{minimumFractionDigits:n,maximumFractionDigits:n}):'—';
 const pct=v=>typeof v==='number'&&Number.isFinite(v)?num(v)+'%':'—';
 const signed=v=>typeof v==='number'&&Number.isFinite(v)?(v>0?'+':'')+num(v):'—';
 const FAMILIES={all:'Mercati utili', '1x2':'1X2',totals:'Over / Under',btts:'Goal / No Goal',asian:'Riferimenti asiatici'};
 const CATEGORIES={all:'Tutte le analisi',convergence:'Convergenza',divergence:'Contrarian',lateral:'Riferimenti asiatici',value:'Valore teorico positivo',no_quote:'Senza quota recente'};
 const STATUS={mature:'CONVERGENZA STATISTICA',contrarian:'DIVERGENZA STATISTICA',lateral:'LATERALE',value_watch:'VALORE IN OSSERVAZIONE',limited_data:'STORICO LIMITATO',no_edge:'PREZZO SENZA VANTAGGIO',no_quote:'SOLO STATISTICA',previous_version:'VERSIONE PRECEDENTE'};
 const QUALITY={supported:'Storico presente',limited:'Storico limitato',prior_dominant:'Una squadra con dati mancanti',prior_only:'Solo valori di riferimento'};
 const REASONS={market:'fuori dai quattro mercati gol',probability:'probabilità sotto soglia',price:'quota fuori 1,40–3,50',stale:'quote da aggiornare',history:'storico insufficiente',adapter:'associazione quota da aggiornare',margin:'aggio anomalo',edge:'vantaggio insufficiente rispetto all’aggio',sensitivity:'scenario prudente insufficiente',coherence:'probabilità gol incoerenti'};
 const allowedMatch=m=>m.gold_scope?.policy_id===SCOPE&&m.gold_scope.allowed===true;
 const scopePayload=p=>({...p,matches:(p.matches||[]).filter(allowedMatch),gold_scope:{...(p.gold_scope||{}),ready:p.gold_scope?.policy_id===SCOPE}});
 function goalMarket(r){const s=r.spec||{};return s.kind==='btts'&&s.period==='ht'&&s.side==='yes'?'gg_ht':s.kind==='total'&&s.side==='over'?(s.period==='ht'&&s.line===1.5?'over15_ht':s.period==='ft'&&s.line===2.5?'over25_ft':s.period==='ft'&&s.line===3.5?'over35_ft':null):null;}
 function coreMarket(s={}){
  if(!s||s.extra!=null)return false;
  if(s.kind==='result')return s.period==='ft'&&['1','x','2'].includes(s.side)&&s.line==null;
  if(s.kind==='dnb')return s.period==='ft'&&['1','2'].includes(s.side)&&s.line==null;
  if(s.kind==='btts')return ['ft','ht'].includes(s.period)&&['yes','no'].includes(s.side)&&s.line==null;
  if(!Number.isFinite(s.line))return false;
  if(s.kind==='total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,2.5,3.5]:s.period==='ht'?[1.5]:[]).includes(s.line);
  if(s.kind==='asian_total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,1.75,2,2.25,2.5,2.75,3,3.25,3.5]:s.period==='ht'?[1,1.25,1.5,1.75,2]:[]).includes(s.line);
  return s.kind==='handicap'&&s.period==='ft'&&['home','away'].includes(s.side)&&[-1,-.75,-.5,-.25,0,.25,.5,.75,1].includes(s.line);
 }
 const phase=(m,now=Date.now())=>{
  const f=m.fixture||{},s=f.result?.status||f.live?.status||f.status_short;
  if(['FT','AET','PEN'].includes(s))return 'finished';
  if(['CANC','PST','ABD','SUSP','WO','AWD'].includes(s))return 'other';
  return Date.parse(f.kickoff)>now?'upcoming':'live';
 };
 function rowsOf(m){
  if(m.multimarket?.matrix)return (m.multimarket.matrix.rows||[]).filter(r=>coreMarket(r.spec)).map(r=>{
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
   if(options.category==='lateral')return r.family==='asian';
   if(options.category==='value')return r.ev_pct>0&&r.quote_status==='recent';
   if(options.category==='no_quote')return r.quote_status!=='recent';
   return true;
  });
 }
 function selectMatches(payload,options=state,now=Date.now(),lifecycle=phase){
  const seen=new Set();
  return (payload.matches||[]).filter(m=>{
   const f=m.fixture||{},id=String(f.fixture_id);
   if(!f.fixture_id||seen.has(id)||!allowedMatch(m))return false;
   seen.add(id);
   if(options.phase!=='all'&&lifecycle(m,now)!==options.phase)return false;
   if(options.search&&![f.match,f.league,f.country].join(' ').toLocaleLowerCase().includes(options.search.toLocaleLowerCase()))return false;
   return matchingRows(m,options).length>0||(options.category==='all'&&options.family==='all'&&options.period==='all');
  }).sort((a,b)=>{
   if(options.sort==='value'){
    const score=m=>bestOpportunity(rowsOf(m))?.ev_low_pct??-100;
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
 function bestOpportunity(rows,{now=Date.now(),archived=false}={}){
  // The backend is the single scoring authority. Old/malformed decisions fail
  // closed; no frontend fallback to longshot EV or limited-history candidates.
  return rows.filter(r=>{
   const d=r.opportunity;
   const hold=d?.stability,retained=d?.policy_id===POLICY&&hold?.version==='gold-stability-v1'&&hold.status==='retained';
   const start=Date.parse(hold?.last_strict_at),end=Date.parse(hold?.expires_at);
   const validHold=retained&&Number.isFinite(start)&&Number.isFinite(end)&&end>start&&end-start<=90*60000&&
    (archived||now>=start&&now<=end)&&d.strict_eligible===false&&
    Number.isFinite(d.entry_min_ev_pct)&&Number.isFinite(d.entry_min_ev_low_pct)&&
    d.min_ev_pct>=Math.max(.5,d.entry_min_ev_pct-1)&&d.min_ev_low_pct>=Math.max(-.5,d.entry_min_ev_low_pct-1);
   return acceptedPolicy(d)&&d.eligible===true&&(!retained||validHold)&&Number.isInteger(d.rank)&&d.rank>0&&
    r.quote_status==='recent'&&r.quality==='supported'&&Number.isFinite(r.odd)&&r.odd>=1.4&&r.odd<=3.5&&
    Number.isFinite(r.statistical_pct)&&r.statistical_pct>=Math.max(35,d.min_probability_pct)&&r.statistical_pct<=100&&
    Number.isFinite(r.ev_pct)&&r.ev_pct>=d.min_ev_pct&&Number.isFinite(r.ev_low_pct)&&(validHold||r.ev_low_pct>0)&&r.ev_low_pct>=d.min_ev_low_pct&&
    goalMarket(r)!==null;
  }).sort((a,b)=>a.opportunity.rank-b.opportunity.rank||String(a.id).localeCompare(String(b.id)))[0]||null;
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
   '<div class="mx-quote-metrics"><span>Statistica <b>'+pct(row.statistical_pct)+'</b></span><span>OCI <b>'+signed(row.oci)+'</b></span><span>Aggio mediano <b>'+pct(row.margin_pct)+'</b> '+esc(band)+'</span><span>Δ aggio <b>'+signed(row.margin_delta_pp)+' pt</b></span></div>'+
   '<small class="mx-quote-source">'+esc(row.bookmaker||'Quota non disponibile')+(row.provider_updated_at?' · '+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at):'')+'</small>'+
   '<small class="mx-quote-consensus">OCI: '+esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi'+(row.oci_previous==null?'':' · prima '+signed(row.oci_previous))+'</small></section>';
 }
 function quoteBoard(rows,helpers){
  const find=(kind,period,side,line)=>rows.find(r=>r.spec?.kind===kind&&r.period===period&&r.spec.side===side&&(line==null||r.spec.line===line));
  return '<section class="mx-quote-board" aria-label="Quote 1X2 apertura e attuale"><h4 class="mx-section-title">QUOTE 1 · X · 2 — APERTURA E ATTUALE</h4><div class="mx-1x2">'+[['1','1 · Casa'],['x','X · Pareggio'],['2','2 · Ospite']].map(([s,l])=>quoteCell(find('result','ft',s),l,helpers)).join('')+'</div>'+
   '<div class="mx-goal-quotes">'+[['total','ht','over',1.5,'Over 1,5 PT'],['total','ft','over',2.5,'Over 2,5 FT'],['total','ft','over',3.5,'Over 3,5 FT'],['btts','ht','yes',null,'GG PT']].map(([k,p,s,l,t])=>quoteCell(find(k,p,s,l),t,helpers)).join('')+'</div>'+
   '<p class="mx-quote-legend">* Prima quota osservata dello stesso bookmaker. ↑ sale · ↓ scende. <span class="mx-key-green">Verde &lt;3%</span> · <span class="mx-key-yellow">giallo 3–8%</span> · <span class="mx-key-red">rosso ≥8%</span>: intensità del movimento, non probabilità di vincita. OCI e aggio sono calcolati sul mercato completo e sugli operatori confrontabili.</p></section>';
 }
 function timingContext(context){
  if(!context)return '';
  const f=context.imbalance,active=context.pt_priority_active;
  const preference=context.preferred_pt_market==='over15_ht'?'Over 1,5 PT':context.preferred_pt_market==='gg_ht'?'Goal PT':'Over 1,5 PT / Goal PT';
  const setting=f?.regime==='balanced'?'Quote bilanciate · distanza 1–2 '+num(f.home_away_odds_gap,2):f?'Favorita '+(f.favorite_venue==='home'?'in casa':'in trasferta')+' · quota '+num(f.favorite_odd,2):'Sbilanciamento non confermato';
  const gaps=['home','away'].map(s=>{const t=context.trends?.[s],g=t?.venue?.ft;return g?.sufficient?'<small>'+(s==='home'?'Casa, in casa':'Ospite, fuori casa')+': media recente '+num(g.recent_mean,2)+' / riferimento '+num(g.reference_mean,2)+' · digiuno '+num(g.drought_matches,0)+' gare</small>':'';}).join('');
  return '<section class="mx-timing"><b>'+(active?'Priorità '+preference:'Confronto dei quattro mercati gol')+'</b><small>'+esc(setting)+'</small>'+gaps+(f?'<small>OCI 1X2 '+signed(f.oci)+' · aggio '+pct(f.margin_pct)+' · '+esc(f.bookmaker)+'</small>':'')+'<small>Medie sui precedenti disponibili nello stesso contesto di campo. Priorità senza bonus alla probabilità; il flusso 1X2 non conferma da solo i gol.</small></section>';
 }
 function opportunityPanel(rows,context,options={}){
  const best=bestOpportunity(rows,options),priced=rows.filter(r=>r.quote_status==='recent').length;
  const checked=rows.filter(r=>acceptedPolicy(r.opportunity)),counts={};
  for(const r of checked)for(const code of r.opportunity.reasons||[])counts[code]=(counts[code]||0)+1;
  const exclusions=Object.entries(counts).sort((a,b)=>b[1]-a[1]).slice(0,3).map(([k,n])=>n+' '+(REASONS[k]||k)).join(' · ');
  const rule='Solo Over 1,5 PT, Goal PT, Over 2,5 FT o Over 3,5 FT. Quote 1,40–3,50 · probabilità ≥35%.';
  if(!best)return '<aside class="mx-best mx-best-empty"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+(checked.length?'Nessuna proposta supera i criteri':'Criteri della proposta da aggiornare')+'</h4>'+timingContext(context)+'<p>'+esc(rule)+'</p><p>'+esc(exclusions||'La prossima analisi prepartita applicherà i nuovi criteri.')+'</p><small>'+rows.length+' esiti statistici · '+priced+' con quota recente. La matrice completa resta consultabile; nessuna giocata viene forzata.</small></aside>';
  const d=best.opportunity,signal=d.signal,tag=signal==='convergence'?'mature':signal==='contrarian'?'contrarian':'value_watch',displayLabel=goalMarket(best)==='gg_ht'?'Goal PT':best.label;
  const explanation=signal==='convergence'?'Statistica e movimento concordano sullo stesso mercato gol.':signal==='contrarian'?'Quota gol in salita con vantaggio statistico e margine controllato. Contrarian sano descrive i criteri superati, non un esito sicuro.':'Il prezzo supera i criteri statistici. Flusso del mercato gol non confermato.';
  const stability=d.stability?.status==='retained'?'<p class="mx-stability"><b>Proposta mantenuta in tolleranza</b><small>Piccola oscillazione dei valori. Validità massima fino alle '+esc(new Date(d.stability.expires_at).toLocaleTimeString('it-IT',{timeZone:'Europe/Rome',hour:'2-digit',minute:'2-digit'}))+' con quote aggiornate e limiti rispettati.</small></p>':'';
  return '<aside class="mx-best mx-best-stable" data-opportunity-id="'+esc(best.id)+'" data-opportunity-odd="'+best.odd+'" data-opportunity-probability="'+best.statistical_pct+'"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+esc(displayLabel)+'</h4><div class="mx-best-price">'+num(best.odd,2)+'<small>QUOTA ATTUALE · '+esc(best.bookmaker||'')+'</small></div><span class="mx-badge mx-'+tag+'">'+esc(d.label)+'</span>'+stability+
   '<dl><div><dt>Statistica pura</dt><dd>'+pct(best.statistical_pct)+'</dd></div><div><dt>Quota equa del modello</dt><dd>'+num(best.fair_odd,2)+'</dd></div><div><dt>Valore teorico / minimo richiesto</dt><dd>'+signed(best.ev_pct)+'% / '+num(d.min_ev_pct)+'%</dd></div><div><dt>Scenario prudente</dt><dd>'+signed(best.ev_low_pct)+'%</dd></div><div><dt>OCI · precedente → attuale</dt><dd>'+signed(best.oci_previous)+' → '+signed(best.oci)+'</dd></div><div><dt>Aggio del bookmaker · variazione</dt><dd>'+pct(best.selected_margin_pct)+' · '+signed(best.selected_margin_delta_pp)+' pt</dd></div></dl>'+
   timingContext(context)+'<p>'+esc(explanation)+'</p><small>'+esc(rule)+' Aggio alto richiede più vantaggio. '+checked.filter(r=>r.opportunity.eligible).length+' mercati gol ammissibili. Il valore resta una stima.</small></aside>';
 }
 function price(row){
  if(row.quote_status!=='recent')return '<span class="mx-missing">'+(row.quote_status==='stale'?'Da aggiornare · ultima '+num(row.last_odd,2):'Non disponibile')+'</span>';
  const arrow=row.odd<row.first_odd?'↘':row.odd>row.first_odd?'↗':'→';
  return '<span class="mx-price">'+num(row.first_odd,2)+' <i>'+arrow+'</i> <b>'+num(row.odd,2)+'</b></span><small>'+esc(row.bookmaker)+' · '+signed(row.delta_pct)+'%</small>';
 }
 function marketRow(row,helpers={}){
  const asian=['asian_total','handicap','dnb'].includes(row.spec?.kind);
  return '<div class="mx-market-row" role="row"><div class="mx-market-name" role="cell"><b>'+esc(row.label)+'</b>'+badge(row)+'</div>'+
   '<div role="cell" data-title="Statistica"><strong>'+pct(row.statistical_pct)+'</strong><small>Equa '+num(row.fair_odd,2)+' · '+esc(QUALITY[row.quality]||'Stima archiviata')+'</small>'+(asian?'<small>Rimborsi inclusi nel valore</small>':'')+'</div>'+
   '<div role="cell" data-title="Prima → attuale">'+price(row)+(row.provider_updated_at?'<small>'+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="OCI"><b>'+signed(row.oci)+'</b><small>'+esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi</small></div>'+
   '<div role="cell" data-title="Aggio"><b>'+pct(row.selected_margin_pct)+'</b><small>Book selezionato · Δ '+signed(row.selected_margin_delta_pp)+' pt</small><small>Mediana '+pct(row.margin_pct)+'</small></div>'+
   '<div role="cell" data-title="Valore"><b class="'+(row.ev_pct>0?'mx-positive':'')+'">'+(row.ev_pct==null?'—':signed(row.ev_pct)+'%')+'</b><small>Prudente '+(row.ev_low_pct==null?'—':signed(row.ev_low_pct)+'%')+'</small></div></div>';
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
 const MATRIX_SECTIONS={targets:'4 obiettivi gol',ft:'Gol FT',ht:'Primo tempo',result:'1 · X · 2',asian:'Asiatici'};
 let dialog=null,modalFixtureId=null,modalSection='targets',bodyOverflow='';
 function releaseModal(instance){
  if(dialog!==instance)return;
  const oldId=modalFixtureId;dialog=null;modalFixtureId=null;instance.remove();document.body.style.overflow=bodyOverflow;
  [...(host?.querySelectorAll('[data-mx-open]')||[])].find(e=>e.dataset.mxOpen===oldId)?.focus();
 }
 function closeModal(){const instance=dialog;if(!instance)return;instance.close();releaseModal(instance);}
 function sectionRows(rows,section){return rows.filter(r=>section==='targets'?goalMarket(r)!==null:section==='asian'?r.family==='asian':section==='result'?r.spec?.kind==='result':r.period===section&&['total','btts'].includes(r.spec?.kind));}
 function matrixDetail(m,rows,services){
  return '<div class="mx-detail"><p class="mx-explanation">'+rows.length+' esiti in questa vista. Quote, OCI e aggio della stessa linea e periodo.</p>'+
   '<div class="mx-table" role="table" aria-label="Matrice delle opportunità"><div class="mx-table-head" role="row">'+['Mercato','Statistica','Prima → attuale','OCI','Aggio','Valore'].map(t=>'<div role="columnheader">'+t+'</div>').join('')+'</div>'+rows.map(r=>marketRow(r,services)).join('')+'</div>'+
   '<details class="mx-method"><summary>Statistiche delle squadre</summary>'+teamProfiles(m)+'</details>'+
   '<details class="mx-method"><summary>Come leggere i valori</summary><p>Aggio: competitività dei prezzi, non misura di liquidità. OCI: movimento delle quote, non volume di denaro. La sensibilità prudente non è un intervallo di confidenza. Il valore asiatico include rimborsi e mezzi esiti.</p></details></div>';
 }
 function updateModal(){
  if(!dialog)return;
  const m=(payload?.matches||[]).find(x=>String(x.fixture?.fixture_id)===modalFixtureId);
  if(!m){closeModal();return;}
  const focused=dialog.contains(document.activeElement)?document.activeElement?.dataset?.mxSection:null;
  const closeFocused=document.activeElement?.hasAttribute('data-mx-close');
  const scroll=dialog.querySelector('.mx-modal-scroll')?.scrollTop||0;
  dialog.innerHTML='<header class="mx-modal-header"><div><span>MATRICE · MERCATI UTILI</span><h2 id="mx-modal-title">'+esc(m.fixture?.match)+'</h2><small>'+esc(m.fixture?.country)+' · '+esc(m.fixture?.league)+'</small></div><button type="button" data-mx-close aria-label="Chiudi matrice" autofocus>×</button></header>'+
   '<nav class="mx-modal-tabs" aria-label="Mercati della matrice">'+Object.entries(MATRIX_SECTIONS).map(([k,l])=>'<button type="button" data-mx-section="'+k+'" aria-pressed="'+(modalSection===k)+'">'+esc(l)+'</button>').join('')+'</nav>'+
   '<div class="mx-modal-scroll">'+matrixDetail(m,sectionRows(rowsOf(m),modalSection),helpers)+'</div>';
  dialog.querySelector('.mx-modal-scroll').scrollTop=scroll;
  if(focused)dialog.querySelector('[data-mx-section="'+focused+'"]').focus();
  else if(closeFocused)dialog.querySelector('[data-mx-close]').focus();
 }
 function openModal(id){
  if(dialog){modalFixtureId=id;modalSection='targets';updateModal();return;}
  modalFixtureId=id;modalSection='targets';
  dialog=document.createElement('dialog');dialog.className='mx-modal';dialog.setAttribute('aria-labelledby','mx-modal-title');dialog.setAttribute('aria-modal','true');
  document.body.append(dialog);bodyOverflow=document.body.style.overflow;document.body.style.overflow='hidden';
  const instance=dialog;
  dialog.addEventListener('close',()=>releaseModal(instance),{once:true});
  dialog.addEventListener('cancel',e=>{e.preventDefault();closeModal();});
  dialog.addEventListener('click',e=>{
   if(e.target.closest?.('[data-mx-close]')){closeModal();return;}
   const tab=e.target.closest?.('[data-mx-section]');
   if(tab){modalSection=tab.dataset.mxSection;updateModal();return;}
   if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)closeModal();}
  });
  dialog.addEventListener('keydown',e=>{
   if(e.key!=='Tab')return;
   const items=[...dialog.querySelectorAll('button:not([disabled]),summary,a[href],input:not([disabled]),select:not([disabled]),[tabindex="0"]')].filter(n=>n.getClientRects().length);
   const first=items[0],last=items.at(-1),active=document.activeElement;
   if(e.shiftKey&&(active===first||!dialog.contains(active))){e.preventDefault();last?.focus();}
   else if(!e.shiftKey&&(active===last||!dialog.contains(active))){e.preventDefault();first?.focus();}
  });
  updateModal();if(dialog){dialog.showModal();dialog.querySelector('[data-mx-close]').focus();}
 }
 function card(m,helpers){
  const f=m.fixture||{},a=m.multimarket||{},all=rowsOf(m),fid=String(f.fixture_id);
  const counts={convergence:all.filter(r=>r.market_behavior==='convergence').length,divergence:all.filter(r=>r.market_behavior==='divergence').length,value:all.filter(r=>r.ev_pct>0).length};
  const noSnapshot=a.status==='no_prematch_snapshot';
  return '<article class="mx-card" data-live-fixture="'+esc(fid)+'"><header><div><time>'+esc(f.time||'')+'</time><small>'+esc(f.country)+' · '+esc(f.league)+'</small><h3>'+esc(f.match)+'</h3></div><div class="mx-live"><span data-live-slot="badge">'+(helpers.liveBadge?.(m)||'')+'</span><span data-live-slot="state">'+esc(helpers.stateLabel?.(m)||'')+'</span></div></header>'+
   (noSnapshot?'<p class="mx-explanation">Partita presente nel palinsesto. Nessuna previsione salvata prima dell’inizio: gli esiti non vengono usati per ricostruirla a posteriori.</p>':'<div class="mx-match-layout">'+quoteBoard(all,helpers)+opportunityPanel(all,a.matrix?.goal_context,{archived:phase(m)!=='upcoming'})+'</div>')+
   '<div class="mx-match-summary"><span>'+all.length+' mercati</span><span>'+counts.convergence+' convergenze</span><span>'+counts.divergence+' divergenze</span><span>'+all.filter(r=>r.quote_status==='recent').length+' quote recenti</span></div>'+
   (all.length?'<button class="mx-open" type="button" data-mx-open="'+esc(fid)+'" aria-haspopup="dialog">Apri matrice · mercati utili <span>↗</span></button>':'')+'<footer>Analisi '+esc(helpers.timestamp?.(a.captured_at)||a.captured_at||'non disponibile')+' · '+esc(a.model_id||'')+(f.data_freshness&&f.data_freshness!=='fresh'?' · '+esc(f.data_freshness):'')+'</footer></article>';
 }
 let payload,host,helpers;
 function render(p,h,services={}){
  p=scopePayload(p);payload=p;host=h;helpers=services;
  const all=p.matches||[],rows=all.flatMap(rowsOf),matches=selectMatches(p,state,Date.now(),services.phase||phase);
  const count=key=>key==='all'?all.length:all.filter(m=>matchingRows(m,{...state,category:key,family:'all',period:'all'}).length).length;
  const options=(items,chosen)=>Object.entries(items).map(([v,l])=>'<option value="'+v+'" '+(v===chosen?'selected':'')+'>'+esc(l)+'</option>').join('');
  h.innerHTML='<section class="mx-dashboard"><header class="mx-heading"><div><span>ARABSNIPER · GOLD</span><h2>Campionati scelti. Focus sui gol.</h2><p>Whitelist senior con Romania inclusa · statistiche, OCI e aggio · priorità PT nel contesto casa/trasferta.</p></div><b>V5 · IN VERIFICA</b></header>'+
   '<div class="mx-counters"><span><b>'+all.length+'</b> partite in whitelist</span><span><b>'+rows.length+'</b> mercati calcolati</span><span><b>'+rows.filter(r=>r.quote_status==='recent').length+'</b> con quota recente</span><span>Proposte su 4 mercati gol</span></div>'+(!p.gold_scope.ready?'<p class="mx-caption">Dati in aggiornamento alla whitelist: i campionati della versione precedente restano nascosti.</p>':'')+
   '<nav class="mx-categories" aria-label="Lettura del mercato">'+Object.entries(CATEGORIES).map(([k,l])=>'<button type="button" data-mx-category="'+k+'" aria-pressed="'+(state.category===k)+'">'+esc(l)+' <b>'+count(k)+'</b></button>').join('')+'</nav>'+
   '<div class="mx-controls"><label>Mercati<select data-mx-filter="family">'+options(FAMILIES,state.family)+'</select></label><label>Periodo<select data-mx-filter="period">'+options({all:'Tutti i periodi',ft:'Finale',ht:'Primo tempo'},state.period)+'</select></label>'+
   '<label>Stato<select data-mx-filter="phase">'+options({all:'Tutte le partite',upcoming:'Da iniziare',live:'In corso',finished:'Terminate',other:'Rinviate / altri stati'},state.phase)+'</select></label><label>Ordine<select data-mx-filter="sort">'+options({time:'Orario',value:'Valore teorico più stabile'},state.sort)+'</select></label>'+
   '<label class="mx-search">Cerca<input type="search" data-mx-filter="search" value="'+esc(state.search)+'" placeholder="Squadra o campionato"></label></div>'+
   '<p class="mx-caption">Convergenza e contrarian descrivono il rapporto tra modello e movimento. Il valore resta una stima da verificare; OCI e aggio non sono volumi di denaro.</p>'+
   '<div class="mx-results" aria-live="polite"><p class="mx-results-count">'+matches.length+' di '+all.length+' partite · '+esc(CATEGORIES[state.category])+'</p>'+
   (matches.length?matches.map(m=>card(m,services)).join(''):'<div class="mx-empty">Nessuna partita con questi filtri. <button type="button" data-mx-reset>Mostra tutto il palinsesto</button></div>')+'</div>'+
   '<p class="mx-caption">Ogni Fast ricalcola le analisi prepartita. Gli aggiornamenti del fornitore possono essere meno frequenti; la data delle quote è sempre indicata. Le partite iniziate conservano la previsione prepartita, con punteggio live separato.</p></section>';
  h.onclick=e=>{
   const expand=e.target.closest?.('[data-mx-open]'),category=e.target.closest?.('[data-mx-category]');
   if(expand){openModal(expand.dataset.mxOpen);}
   else if(category){state.category=category.dataset.mxCategory;render(payload,host,helpers);}
   else if(e.target.closest?.('[data-mx-reset]')){Object.assign(state,{category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''});render(payload,host,helpers);}
  };
  h.onchange=e=>{const key=e.target.dataset?.mxFilter;if(key&&key in state){state[key]=e.target.value;render(payload,host,helpers);}};
  updateModal();
 }
 const api={MODEL,POLICY,SCOPE,state,render,rowsOf,matchingRows,selectMatches,phase,marketRow,bestOpportunity,movementSignal,quoteCell,quoteBoard,opportunityPanel,timingContext,scopePayload,goalMarket,coreMarket,sectionRows,esc};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.ArabMatrix=api;
})(typeof window!=='undefined'?window:globalThis);
