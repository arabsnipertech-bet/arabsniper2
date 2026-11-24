import {officialGoldRecords} from './explorer-official-records.mjs';
import './gold-explorer-qualification.js?v=b928aceb1e45';
import {archiveLoader,currentPageHashes} from './explorer-loader.mjs?v=4c5be96bbe7b';
import {categories,modes,text,numeric,romeDay,fresh,matches,rank,outcome,statistics,historyRows} from './explorer-core.mjs?v=1086029b2649';
const $=id=>document.getElementById(id),esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number=(v,d=2)=>numeric(v)?v.toLocaleString('it-IT',{minimumFractionDigits:d,maximumFractionDigits:d}):'—';
const percent=v=>numeric(v)?number(v*100,1)+'%':'—';
const date=v=>v?new Date(v).toLocaleString('it-IT',{timeZone:'Europe/Rome',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}):'—';
const state={rows:[],searches:[],limit:10,viewRows:new Map(),objects:new Map(),snapshots:new Map(),history:[],historyLimit:10,legacyLimit:10,favorites:[],next:0};
const requestedFixture=new URLSearchParams(location.search).get('fixture');
if(/^\d+$/.test(requestedFixture||'')){state.focusFixture=requestedFixture;$('filterForm').elements.period.value='all';$('mode').value='search';}
if(new URLSearchParams(location.search).has('panel'))document.body.classList.add('embed');
const message=v=>$('message').textContent=v;
async function api(path,input){
 const response=await fetch('/api/auth/explorer/'+path,{credentials:'same-origin',cache:'no-store',...(input?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(input)}:{})});
 let data;try{data=await response.json();}catch{throw Error('Risposta non valida. Riprova.');}
 if(!response.ok){if([401,403].includes(response.status)){$('app').hidden=true;$('locked').hidden=false;}const error=Error(data.error||'Errore di rete');error.status=response.status;throw error;}
 return data;
}
async function action(fn,button){if(button)button.disabled=true;try{await fn();}catch(e){message(e.message||'Connessione non disponibile. Riprova.');}finally{if(button)button.disabled=false;}}
const getObject=archiveLoader(hash=>api('objects/'+hash),12);
async function getPages(hashes,onProgress){let done=0;const parts=await Promise.all(hashes.map(async hash=>{const data=await getObject(hash);if(!Array.isArray(data.rows))throw Error('Pagina Explorer incompleta. Premi Aggiorna dati.');onProgress?.(++done,hashes.length);return {hash,data};}));return parts.flatMap(({hash,data})=>data.rows.map(r=>({...r,object:hash})));}
async function getSnapshot(hash){if(!state.snapshots.has(hash)){const data=await getObject(hash);state.snapshots.set(hash,{...data,rows:await getPages(data.pages||[])});}return state.snapshots.get(hash);}
const filters=()=>({...Object.fromEntries(new FormData($('filterForm'))),diversify:$('filterForm').elements.diversify.checked});
const empty=(node,value='Nessuna selezione corrisponde ai criteri.')=>node.innerHTML='<p class="empty">'+esc(value)+'</p>';
const roleNames={most_probable:'Più Probabile',recommended_alternative:'Consigliata',high_odds_shot:'Colpaccio'};
const originalRoles=row=>row.roles?.length?row.roles.map(r=>roleNames[r]||r).join(', '):'Fuori dalla triade ufficiale';
function quoteContext(row,{historical=false}={}){
 const prices=row.match_odds||{};
 if(!Object.values(prices).some(q=>numeric(q.first_odd)||numeric(q.opening_odd)||numeric(q.odd)))return '';
 const valid=v=>numeric(v)&&v>1?v:null;
 return '<section class="selection-quotes" aria-label="Quote 1X2"><p class="small">1X2 · Apertura / prima osservata → '+(historical?'Ultima prematch':'Attuale')+'</p><div class="selection-1x2">'+['1','x','2'].map(side=>{
  const q=prices[side]||{},first=valid(q.opening_odd)??valid(q.first_odd),last=valid(q.odd);
  const repeated=Date.parse(q.first_observed_at)<Date.parse(q.quote_observed_at);
  const delta=first!==null&&last!==null&&(first!==last||repeated)?Math.round((last/first-1)*1000)/10||0:null;
  const direction=delta===null?'unknown':delta<0?'down':delta>0?'up':'flat';
  const arrow={unknown:'—',down:'↓',up:'↑',flat:'→'}[direction];
  const change=delta===null?'—':arrow+' '+(delta>0?'+':'')+number(delta,1)+'%';
  const current=q.quote_usable&&Date.parse(q.quote_valid_until)>Date.now();
  const note=last===null?'Non disponibile':!historical&&!current?'Da aggiornare':delta===null?'Variazione non disponibile':'';
  return '<div title="'+esc((q.bookmaker||'Fonte Gold')+' · osservata '+date(q.quote_observed_at))+'"><b>'+side.toUpperCase()+'</b><span class="selection-quote-values">'+number(first)+' → <strong>'+number(last)+'</strong></span><span class="selection-movement" data-movement="'+direction+'">'+change+'</span>'+(note?'<small>'+esc(note)+'</small>':'')+'</div>';
 }).join('')+'</div></section>';
}
function proposalOrigin(row){
 if(row.official_original&&row.probability===null)return '<p class="small">Motivazione della scelta originale non documentata nel riferimento salvato.</p>';
 const detail=row.origin_tooltip||row.motivation;
 if(detail)return '<details class="selection-origin"><summary>'+esc(row.origin_badge||'Perché questa analisi')+'</summary><p>'+esc(detail)+'</p></details>';
 // Older immutable snapshots retain only their documented numerical evidence.
 const evidence=['Probabilità V7 '+percent(row.probability),numeric(row.ev)?'EV '+percent(row.ev):'',numeric(row.delta)?'Variazione quota '+number(row.delta,1)+'%':''].filter(Boolean).join(' · ');
 return '<details class="selection-origin"><summary>Motivo · dati dello snapshot</summary><p>'+esc(evidence)+'</p></details>';
}
function proposalCoherence(row){
 if(row.coherence_status==='opposed')return '<p class="small" data-coherence="opposed"><b>Scenario opposto alla triade Gold</b> · '+esc((row.coherence_opposed_to||[]).join(', '))+'. Consultabile come alternativa; escluso dalle proposte principali.</p>';
 if(row.coherence_status==='inconsistent_gold_triad')return '<p class="small" data-coherence="inconsistent">Triade da verificare: le proposte confermate non possono vincere insieme.</p>';
 return '';
}
function emptyResults(){
 empty($('results'),!state.rows.length?'Archivio corrente caricato: nessuna analisi prematch per questa vista.':$('mode').value==='recommended'?'Nessuna Consigliata ufficiale Gold registrata corrisponde ai filtri attuali. Il caricamento è riuscito.':'Dati caricati: nessuna voce corrisponde alla vista e ai filtri attuali.');
}
function cards(rows,node,options={}){
 if(!rows.length){empty(node);return;}
 node.innerHTML=rows.map(row=>{
  const key=row.id+'-'+(row.object||'current')+'-'+(options.favorite?'favorite':'view');state.viewRows.set(key,{row,...options});
  return `<article class="selection"><div><span class="badge ${esc(row.category)}">${esc(row.observation_only?'Osservazione V7':categories[row.category]||'Archivio')}</span><p class="small">${esc(text(row.league))} · ${date(row.kickoff)}</p><h3>${esc(text(row.home))} — ${esc(text(row.away))}</h3>${quoteContext(row,options)}</div><div><strong class="market">${esc(row.label)}</strong>${proposalOrigin(row)}${globalThis.ArabV7Qualification.render(row)}${proposalCoherence(row)}<p class="small">${row.roles?.length?(row.observation_only?'Osservazione · ruolo originale: ':'Triade: ')+esc(row.roles.map(r=>roleNames[r]||r).join(', ')):'Fuori dalla triade ufficiale'}</p><p class="small">${row.reliability==='supported'?'Supporto statistico V7':'Affidabilità limitata'} · ${row.coherence_status==='opposed'?'Scenario alternativo · fuori dalle proposte principali':row.validated?'Validata da Explorer':'Non validata da Explorer'}</p>${row.outcome?`<b class="${row.outcome.win===1?'won':row.outcome.loss===1?'lost':''}">${esc(row.outcome.label)}</b>`:options.historical?'<span class="small">Esito non verificato</span>':''}</div><div class="numbers"><div><strong>${percent(row.probability)}</strong><small>Probabilità V7</small></div><div><strong>${number(row.odd)}</strong><small>${row.official_original?'Quota ufficiale originale':options.historical?'Quota snapshot':fresh(row)?'Quota di riferimento':'Quota storica / scaduta'}</small></div><p class="small">EV ${percent(row.ev)} · EV_low ${percent(row.ev_low)} · OCI ${row.oci===0?'0 · neutro':numeric(row.oci)?number(row.oci,1):'non disponibile'}</p></div><div class="actions"><button data-detail="${key}">Dettaglio</button><button ${options.favorite?'data-remove':'data-favorite'}="${key}" aria-label="${options.favorite?'Rimuovi preferito':'Salva preferito'}">${options.favorite?'Rimuovi':'☆'}</button></div></article>`;
 }).join('');
}
function render(){
 $('marketIndex').replaceChildren();$('marketNavigation').hidden=true;
 $('fixtureFocus').hidden=!state.focusFixture;
 if(state.loadingView&&state.focusFixture)$('fixtureFocusText').textContent='Caricamento delle analisi della partita…';
 if(state.loadingView||state.viewError){$('more').hidden=true;empty($('results'),state.viewError||'Caricamento dei mercati per la vista selezionata…');return;}
 $('fixtureFocus').hidden=!state.focusFixture;
 if(state.focusFixture){
  const row=state.rows.find(r=>String(r.fixture_id)===state.focusFixture);
  $('fixtureFocusText').textContent=row?'Analisi della partita: '+text(row.home)+' — '+text(row.away):'Partita selezionata: nessuna analisi corrente disponibile.';
 }
 $('viewExplanation').textContent=$('mode').value==='recommended'?'Consigliate ufficiali già registrate dalla Gold, in ordine di inizio partita. Nessun nuovo punteggio Explorer. Gli indicatori sono quelli dello snapshot; la quota ufficiale originale, se documentata, è nel dettaglio.':$('mode').value==='markets'?'Gruppi in ordine alfabetico. Usa l’indice per raggiungere un mercato senza scorrere tutti i precedenti.':'I ruoli originali Gold sono riportati senza modifiche nelle schede e nel dettaglio.';
 const f=filters(),mode=$('mode').value,now=Date.now();let rows=state.rows.filter(r=>matches(r,f,now)&&(!state.focusFixture||r.fixture_id===state.focusFixture));
 if(mode!=='search')rows=rows.filter(r=>!['opposed','inconsistent_gold_triad'].includes(r.coherence_status));
 if(mode!=='search'&&mode!=='recommended')rows=rows.filter(r=>fresh(r,now));
 if(mode==='markets'){
  $('results').replaceChildren();let more=false,count=0;
  for(const label of [...new Set(rows.map(r=>r.label))].sort()){
   const list=rank(rows.filter(r=>r.label===label),'best',f.diversify);if(!list.length)continue;count+=list.length;more ||=list.length>state.limit;
   const section=document.createElement('section'),title=document.createElement('h3'),content=document.createElement('div');title.textContent=label+' · '+list.length;title.id='market-group-'+$('marketIndex').children.length;title.tabIndex=-1;
   const jump=document.createElement('a');jump.href='#'+title.id;jump.textContent=label+' ('+list.length+')';jump.className='button';jump.onclick=e=>{e.preventDefault();title.focus({preventScroll:true});title.scrollIntoView({block:'start'});};$('marketIndex').append(jump);
   section.append(title,content);cards(list.slice(0,state.limit),content);$('results').append(section);
  }
  if(!count)emptyResults();$('marketNavigation').hidden=!count;$('more').hidden=!more;
 }else{
  rows=mode==='recommended'?officialGoldRecords(rows):mode==='search'?rows.sort((a,b)=>Date.parse(a.kickoff)-Date.parse(b.kickoff)||b.probability-a.probability||a.id.localeCompare(b.id)):rank(rows,mode,f.diversify);
  if(rows.length)cards(rows.slice(0,state.limit),$('results'));else emptyResults();$('more').hidden=rows.length<=state.limit;
 }
}
function metrics(rows,node){const s=statistics(rows);node.innerHTML='<div class="metrics">'+[
 ['Selezioni nel campione',s.n],['Esiti verificati',s.verified],['Vinte / perse',s.won+' / '+s.lost],['Copertura risultati',percent(s.coverage)],['Hit rate · binari',percent(s.hitRate)],['ROI teorico · '+s.roiN+' quote',percent(s.roi)],['EV prematch medio',percent(s.ev)],['Brier · '+s.binaryN+' esiti',number(s.brier,4)]
 ].map(([label,value])=>`<div class="metric"><strong>${esc(value)}</strong><span>${esc(label)}</span></div>`).join('')+'</div>';return s;}
function table(headers,rows){return '<div class="table-wrap"><table><thead><tr>'+headers.map(h=>'<th>'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+r.map(c=>'<td>'+esc(c)+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';}
function dashboard(){const now=Date.now(),f=filters(),rows=state.rows.filter(r=>matches(r,f,now)&&(!state.focusFixture||r.fixture_id===state.focusFixture)),valid=rows.filter(r=>r.validated&&fresh(r,now));$('metrics').innerHTML=[['Partite nella vista',new Set(rows.map(r=>r.fixture_id)).size],['Mercati nella vista',rows.length],['Opportunità validate',valid.length],['Campionati nella vista',new Set(rows.map(r=>text(r.league))).size]].map(([label,value])=>`<div class="metric"><strong>${value}</strong><span>${label}</span></div>`).join('');}
function scheduleExpiry(){clearTimeout(state.timer);const now=Date.now(),today=romeDay(now);let low=now,midnight=now+27*3600000;while(midnight-low>1000){const mid=Math.floor((low+midnight)/2);if(romeDay(mid)===today)low=mid;else midnight=mid;}const next=Math.min(midnight,...state.rows.flatMap(r=>[Date.parse(r.kickoff),Date.parse(r.quote_valid_until)]).filter(t=>t>now));state.timer=setTimeout(()=>action(loadCurrentView),Math.min(2147483647,next-now+25));}
async function loadSearches(){const data=await api('searches');state.searches=data.items||[];const selected=$('savedSearches').value;$('savedSearches').replaceChildren(new Option('Le tue ricerche',''),...state.searches.map(s=>new Option(s.name,s.id)));$('savedSearches').value=state.searches.some(s=>s.id===selected)?selected:'';}
function viewPages(){
 const f=filters(),now=Date.now(),today=romeDay(now),noon=Date.parse(today+'T12:00:00Z');
 let dates=Array.from({length:5},(_,i)=>romeDay(noon+i*86400000));
 if(f.period==='today')dates=[today];
 if(f.period==='tomorrow')dates=[dates[1]];
 if(['3','6'].includes(f.period))dates=dates.filter(d=>d<=romeDay(now+Number(f.period)*3600000));
 if(f.period==='custom')dates=dates.filter(d=>(!f.from||d>=f.from)&&(!f.to||d<=f.to));
 return currentPageHashes(state.index||{},{dates,fixture:state.focusFixture});
}
async function loadCurrentView(){
 if(!state.index)return;
 const sequence=state.pageSequence=(state.pageSequence||0)+1;
 const hashes=viewPages(),key=JSON.stringify([state.index.generated_at,hashes]);
 if(state.loadedKey===key){state.loadingView=false;state.viewError=null;render();dashboard();scheduleExpiry();$('dataState').textContent='';message(state.rows.length?'Dati caricati.':'Nessuna analisi prematch disponibile per questa vista. Puoi consultare lo storico.');return;}
 state.loadingView=true;state.viewError=null;render();
 try{
  const rows=await getPages(hashes,(done,total)=>{if(sequence===state.pageSequence)$('dataState').textContent='Caricamento mercati · '+Math.round(done/total*100)+'%';});
  if(sequence!==state.pageSequence)return;
  if(state.index.page_index_version){const selected=new Set(hashes),expected=state.index.page_index.filter(p=>selected.has(p.sha256)).reduce((n,p)=>n+p.count,0);if(rows.length!==expected)throw Error('Conteggio mercati incompleto. Premi Aggiorna dati.');}
  state.rows=rows;state.loadedKey=key;state.loadingView=false;state.limit=10;
  for(const [name,values] of [['league',state.index.facets?.leagues||rows.map(r=>text(r.league))],['market',state.index.facets?.markets||rows.map(r=>r.label)]]){const select=$('filterForm').elements[name],previous=select.value;select.replaceChildren(new Option('Tutti',''),...[...new Set(values.filter(Boolean))].sort().map(v=>new Option(v,v)));select.value=previous;}
  render();dashboard();scheduleExpiry();$('dataState').textContent='';message(rows.length?'Dati caricati.':'Nessuna analisi prematch disponibile per questa vista. Puoi consultare lo storico.');
 }catch(e){if(sequence===state.pageSequence){state.loadingView=false;state.viewError='Caricamento non riuscito. Premi Aggiorna dati per riprovare.';render();}throw e;}
}
async function refreshData(){
 message('Lettura degli archivi Gold…');let data;try{data=await api('current');}catch(e){if(e.status!==404)throw e;data={pages:[],count:0};}
 currentPageHashes(data);state.index=data;$('app').hidden=false;$('locked').hidden=true;$('updated').textContent='Ultima pubblicazione: '+date(data.generated_at);
 const today=romeDay(Date.now()),last=romeDay(Date.parse(today+'T12:00:00Z')+4*86400000);for(const key of ['from','to']){const input=$('filterForm').elements[key];input.min=today;input.max=last;}
 await loadCurrentView();await loadSearches();
}
function refresh(){return state.refreshPromise ||=(async()=>{
 $('dataState').textContent='Caricamento in corso…';
 try{await refreshData();$('dataState').textContent='';}
 catch(e){$('dataState').textContent=[401,403].includes(e.status)?'Accesso richiesto: rientra dalla Gold.':state.rows.length?'Aggiornamento non riuscito: restano visibili i dati dell’ultimo caricamento. Premi Aggiorna dati per riprovare.':'Caricamento non riuscito: non è possibile stabilire se ci siano risultati. Premi Aggiorna dati per riprovare.';throw e;}
 })().finally(()=>state.refreshPromise=null);}
async function favorites(reset=true){
 if(reset){state.favorites=[];state.next=0;}
 const data=await api('favorites?offset='+(state.next||0));state.favorites.push(...data.items);state.next=data.next;
 const rows=state.favorites.map(f=>({...f.snapshot,saved_at:f.saved_at,latest_fast:f.references?.fast,result:f.result,outcome:outcome(f.snapshot,f.result)}));
 metrics(rows,$('favoriteMetrics'));cards(rows,$('favoriteRows'),{favorite:true,historical:true});$('moreFavorites').hidden=state.next===null;
}
function renderLegacy(){const rows=state.legacyRows||[];cards(rows.slice(0,state.legacyLimit),$('legacyRows'),{historical:true});$('moreLegacy').hidden=rows.length<=state.legacyLimit;}
function renderHistory(){
 const f=filters(),reference=$('historyReference').value,cohort=$('historyCohort').value;
 const all=(ref,group)=>state.history.flatMap(h=>historyRows(h,state.snapshots,ref,group)).filter(r=>matches(r,f,Date.now(),true));
 const certified=state.history.some(h=>(h.scans||[]).length);$('certificateState').hidden=certified;$('certificateState').textContent='Le certificazioni Explorer iniziano dalla sua attivazione. In questo periodo non sono ancora presenti; i dati V7 precedenti sono disponibili nella sezione Archivio V7 qui sotto.';if(!certified)$('legacyArchive').open=true;
 const rows=all(reference,cohort),stats=metrics(rows,$('historyMetrics')),comparison=[];
 for(const ref of ['night','final'])for(const group of ['official','alternative']){const s=statistics(all(ref,group));comparison.push([ref==='night'?'Night':'Finale',group==='official'?'Triade':'Alternative',s.n,s.verified,percent(s.hitRate),percent(s.roi)]);}
 $('comparisons').innerHTML='<h3>Campioni separati · stessi filtri</h3>'+table(['Riferimento','Campione','N','Verificati','Hit rate','ROI'],comparison)+'<h3>Calibrazione probabilistica</h3>'+table(['Fascia','N','Probabilità media','Frequenza osservata'],stats.bins.filter(b=>b.n).map(b=>[b.range,b.n,percent(b.predicted),percent(b.observed)]));
 for(const field of ['label','league']){$('comparisons').innerHTML+='<h3>'+ (field==='label'?'Per mercato':'Per campionato')+'</h3>'+table(['Gruppo','N','Verificati','Hit rate','ROI'],[...new Set(rows.map(r=>text(r[field])))].map(g=>{const s=statistics(rows.filter(r=>text(r[field])===g));return[g,s.n,s.verified,percent(s.hitRate),percent(s.roi)];}));}
 cards(rows.slice(0,state.historyLimit),$('historyRows'),{historical:true});$('moreHistory').hidden=rows.length<=state.historyLimit;
 state.legacyRows=state.history.flatMap(h=>(h.legacyRows||[]).map(r=>({...r,result:h.results?.[r.fixture_id],outcome:outcome(r,h.results?.[r.fixture_id])}))).filter(r=>matches(r,f,Date.now(),true));
 metrics(state.legacyRows,$('legacyMetrics'));renderLegacy();$('legacyState').textContent=`Ultima pubblicazione V7 verificabile per mercato prima del margine prematch. Nessuna classifica Explorer ricostruita. Giornate ancora da importare: ${state.legacyPending||0}.`;
}
async function loadHistory(){
 message('Caricamento dello storico certificato…');const catalog=await api('catalog'),today=romeDay(Date.now()),period=$('historyPeriod').value;
 state.legacyPending=catalog.legacy_import_pending;const since=period==='all'?'0000-00-00':romeDay(Date.parse(today+'T12:00:00Z')-(Number(period)-1)*86400000);
 const dates=Object.keys(catalog.days||{}).filter(d=>d>=since&&d<=today).sort().reverse();state.history=[];$('certificates').replaceChildren();
 for(const day of dates){
  const h=await api('days/'+day),refs=[...new Set(Object.values(h.references||{}).flatMap(r=>[r.night,r.final]).filter(Boolean))];
  for(let i=0;i<refs.length;i+=4)await Promise.all(refs.slice(i,i+4).map(getSnapshot));h.legacyRows=await getPages(h.legacy_pages||[]);state.history.push(h);
  for(const scan of h.scans||[]){const button=document.createElement('button');button.textContent=day+' · '+scan.phase+' · '+date(scan.published_at);button.onclick=()=>action(()=>certificate(scan,h),button);$('certificates').append(button);}
  message(`Storico: ${state.history.length} di ${dates.length} giornate caricate.`);
 }
 state.historyLimit=state.legacyLimit=10;renderHistory();message(dates.length?'Storico caricato. I giorni senza evidenza non sono inventati.':'Nessun archivio disponibile nel periodo.');
}
async function certificate(scan,h){
 const snap=await getSnapshot(scan.sha256),map=new Map(snap.rows.map(r=>[r.id,r]));
 $('detailBody').innerHTML='<h2>Classifica certificata</h2><p>'+esc(scan.phase)+' · '+date(scan.published_at)+'</p><select id="certificateMode" aria-label="Classifica certificata"></select><div id="certificateRows"></div>';
 const select=$('certificateMode');for(const key of ['best','probable','recommended','shots'])select.add(new Option(modes[key],key));for(const label of Object.keys(snap.boards.markets||{}))select.add(new Option(label,'market:'+label));
 const draw=()=>{const ids=select.value.startsWith('market:')?snap.boards.markets[select.value.slice(7)]:snap.boards[select.value];cards((ids||[]).slice(0,10).map(id=>({...map.get(id),snapshot:scan.sha256,result:h.results?.[map.get(id).fixture_id],outcome:outcome(map.get(id),h.results?.[map.get(id).fixture_id])})),$('certificateRows'),{historical:true});};select.onchange=draw;draw();$('detail').showModal();
}
function detail(row){
 const fast=row.latest_fast||row.fast_reference;
 $('detailBody').innerHTML='<h2>'+esc(row.label)+'</h2><p>'+esc(text(row.home)+' — '+text(row.away))+'</p>'+quoteContext(row,{historical:Date.parse(row.kickoff)<=Date.now()})+proposalOrigin(row)+globalThis.ArabV7Qualification.render(row)+'<dl>'+[
 ['Categoria Explorer',categories[row.category]],['Ruolo originale Gold',originalRoles(row)],['Quota originale Consigliata Gold',row.roles?.includes('recommended_alternative')?number(row.official_references?.recommended_alternative?.odd):'—'],['Registrazione Consigliata Gold',row.roles?.includes('recommended_alternative')?date(row.official_references?.recommended_alternative?.captured_at):'—'],['Probabilità V7',percent(row.probability)],['Apertura',number(row.opening)],['Prima quota osservata',number(row.first_odd)],['Quota del riferimento',number(row.odd)],['Quota Night',row.night_reference?number(row.night_reference.odd)+' · '+date(row.night_reference.captured_at):'Non disponibile'],['Ultima quota Fast valida',fast?number(fast.odd)+' · '+date(fast.captured_at)+(Date.parse(fast.quote_valid_until)>Date.now()?'':' · storica / scaduta'):'Nessun aggiornamento Fast documentato'],['EV / EV_low',percent(row.ev)+' / '+percent(row.ev_low)],['OCI',number(row.oci)],['Affidabilità',row.reliability==='supported'?'Supporto statistico V7':'Limitata'],['Fonte',text(row.quote?.bookmaker)||row.quote?.bookmaker_id||'Non disponibile'],['Quota osservata',date(row.quote?.quote_observed_at)],['Aggiornata dalla fonte',date(row.quote?.provider_updated_at)],['Utilizzabile fino a',date(row.quote_valid_until)],['Snapshot V7',date(row.captured_at)],['Registrazione Explorer',row.legacy_v7?'Archivio V7 precedente':date(row.published_at)],['Salvata tra i preferiti',row.saved_at?date(row.saved_at*1000):'—'],['Riferimento finale',row.fallback_night?'Night: Fast successivo assente':row.reference||'Corrente'],['Esito',row.outcome?.label||'Non verificato'],['Risultato FT',row.outcome&&numeric(row.result?.result?.home_ft)&&numeric(row.result?.result?.away_ft)?row.result.result.home_ft+' – '+row.result.result.away_ft:'Non disponibile'],['Risultato PT',row.outcome&&numeric(row.result?.result?.home_ht)&&numeric(row.result?.result?.away_ht)?row.result.result.home_ht+' – '+row.result.result.away_ht:'Non disponibile'],['Motivi di attenzione',(row.exclusions||[]).join(', ')||'—'],['Impronta snapshot',row.forecast_sha256]
 ].map(([k,v])=>'<dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd>').join('')+'</dl><p class="note">Dati relativi al riferimento indicato. Explorer non ricalcola le probabilità V7.</p><button id="allAlternatives">Altre analisi della partita</button>';
 $('allAlternatives').onclick=()=>action(async()=>{
  if(row.snapshot){const snap=await getSnapshot(row.snapshot);$('detailBody').innerHTML='<h2>Mercati nello stesso snapshot</h2><div id="alternateRows"></div>';cards(snap.rows.filter(r=>r.fixture_id===row.fixture_id),$('alternateRows'),{historical:true});}
  else if(row.legacy_v7){$('detailBody').innerHTML='<h2>Archivio V7 della partita</h2><div id="alternateRows"></div>';cards((state.legacyRows||[]).filter(r=>r.episode===row.episode),$('alternateRows'),{historical:true});}
  else{$('detail').close();document.querySelector('[data-tab=explore]').click();resetFilters();state.focusFixture=row.fixture_id;$('filterForm').elements.team.value=text(row.home);$('filterForm').elements.period.value='all';$('mode').value='search';await loadCurrentView();}
 });$('detail').showModal();
}
document.addEventListener('click',e=>{const button=e.target.closest('button');if(!button)return;
 if(button.dataset.detail)detail(state.viewRows.get(button.dataset.detail).row);
 if(button.dataset.favorite)action(async()=>{const row=state.viewRows.get(button.dataset.favorite).row;await api('favorites',{id:row.id,object:row.object});message('Selezione salvata nei tuoi preferiti.');},button);
 if(button.dataset.remove)action(async()=>{await api('delete',{type:'favorite',id:state.viewRows.get(button.dataset.remove).row.id});await favorites();message('Preferito rimosso.');},button);
 if(button.dataset.tab){for(const id of ['explore','favorites','history'])$(id).hidden=id!==button.dataset.tab;document.querySelectorAll('[data-tab]').forEach(b=>b.setAttribute('aria-current',b===button?'page':'false'));if(button.dataset.tab==='favorites')action(()=>favorites(),button);if(button.dataset.tab==='history')renderHistory();}
});
$('closeDetail').onclick=()=>$('detail').close();$('refresh').onclick=()=>action(refresh,$('refresh'));
function resetFilters(){state.silentReset=true;$('filterForm').reset();state.silentReset=false;}
$('clearFixtureFocus').onclick=()=>action(async()=>{state.focusFixture=null;resetFilters();$('mode').value='best';state.limit=10;await loadCurrentView();});
$('filterForm').onsubmit=e=>e.preventDefault();$('filterForm').oninput=()=>action(async()=>{state.focusFixture=null;state.limit=10;await loadCurrentView();});$('filterForm').onreset=()=>{if(state.silentReset)return;state.focusFixture=null;queueMicrotask(()=>action(async()=>{state.limit=10;await loadCurrentView();}));};$('mode').onchange=()=>{state.limit=10;render();};
$('more').onclick=()=>{state.limit+=10;render();};$('moreFavorites').onclick=()=>action(()=>favorites(false),$('moreFavorites'));$('moreHistory').onclick=()=>{state.historyLimit+=10;renderHistory();};$('moreLegacy').onclick=()=>{state.legacyLimit+=10;renderLegacy();};$('loadHistory').onclick=()=>action(loadHistory,$('loadHistory'));
for(const id of ['historyReference','historyCohort'])$(id).onchange=renderHistory;
$('saveSearch').onclick=()=>action(async()=>{await api('searches',{name:$('searchName').value,filters:filters()});await loadSearches();message('Ricerca salvata nel tuo account.');},$('saveSearch'));
$('updateSearch').onclick=()=>action(async()=>{const id=$('savedSearches').value;if(!id)throw Error('Seleziona prima una ricerca salvata.');await api('searches',{id,name:$('searchName').value,filters:filters()});await loadSearches();message('Ricerca aggiornata.');},$('updateSearch'));
$('deleteSearch').onclick=()=>action(async()=>{const id=$('savedSearches').value;if(!id)throw Error('Seleziona prima una ricerca salvata.');await api('delete',{type:'search',id});await loadSearches();message('Ricerca eliminata.');},$('deleteSearch'));
$('savedSearches').onchange=()=>action(async()=>{const s=state.searches.find(s=>s.id===$('savedSearches').value);if(!s)return;await refresh();resetFilters();for(const [k,v] of Object.entries(s.filters)){const el=$('filterForm').elements[k];if(el){if(el.type==='checkbox')el.checked=v===true;else el.value=v;}}$('searchName').value=s.name;$('mode').value='search';await loadCurrentView();message('Ricerca applicata ai dati Gold appena aggiornati.');});
window.addEventListener('focus',()=>{if(state.index)action(loadCurrentView);});
action(refresh);
