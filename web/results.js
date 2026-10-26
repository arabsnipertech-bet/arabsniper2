/* Only settled public results; no current Gold payload. */
(() => {
 const endpoint='https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/arab_quote_results_public.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

 const date=v=>new Intl.DateTimeFormat('it-IT',{day:'numeric',month:'long',year:'numeric',timeZone:'Europe/Rome'}).format(new Date(v+'T12:00:00Z'));
 const cards=rows=>rows.map(r=>`<article class="result-card ${r.outcome==='won'?'won':'lost'}"><span class="result-outcome">${r.outcome==='won'?'✓ CENTRATA':'✕ NON CENTRATA'}</span><span class="league">${esc(r.league)}</span><h3>${esc(r.match)}</h3><strong>${esc(r.label)}</strong>${typeof r.odd==='number'&&Number.isFinite(r.odd)&&r.odd>1?`<span class="result-odd">${r.odd.toLocaleString('it-IT',{minimumFractionDigits:2,maximumFractionDigits:2})} <small>QUOTA PREMATCH</small></span>`:''}</article>`).join('');
 async function refresh(){
  const panel=document.getElementById('results-content');
  try{
   const response=await fetch(endpoint+'?v='+Date.now(),{cache:'no-store',signal:AbortSignal.timeout(15000)});
   if(!response.ok)throw Error('feed');
   const p=await response.json();
   if(!p.available||!Array.isArray(p.all_results)||!p.daily?.settled){panel.innerHTML=p.model_id==='statistics-first-v4.0'?'<div class="empty">Il nuovo modello V4 è in verifica prospettica. Per ieri non sono ancora disponibili selezioni concluse: i risultati del modello precedente restano separati.</div>':'<div class="empty">I risultati di ieri sono in verifica. Pubblicheremo gli esiti quando disponibili.</div>';return;}
   const today=new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Rome',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
   const expected=new Date(today+'T12:00:00Z');expected.setUTCDate(expected.getUTCDate()-1);
   if(p.date!==expected.toISOString().slice(0,10)){panel.innerHTML='<div class="empty">I risultati di ieri sono in aggiornamento.</div>';return;}
   const d=p.daily;
   const examples=(p.examples||[]).filter(r=>r.outcome==='won'&&String(r.market).startsWith('over')&&typeof r.odd==='number'&&Number.isFinite(r.odd)&&r.odd>1).sort((a,b)=>b.odd-a.odd).slice(0,10);
   panel.innerHTML=`<p class="note">${esc(date(p.date))} · Quote registrate prima del calcio d’inizio</p><p>Selezione dei soli pronostici vincenti: questo estratto non rappresenta il rendimento complessivo. Tutti gli esiti, comprese le perdite, sono consultabili qui sotto.</p>${examples.length?`<div class="results-grid">${cards(examples)}</div>`:'<div class="empty">Nessun Over vinto con quota prematch verificata disponibile per questa giornata.</div>'}<p class="note">Fino a 10 Over vinti, ordinati per quota prematch decrescente. I risultati passati non garantiscono quelli futuri.</p><details><summary>Tutti gli esiti del giorno: ${d.wins} vinti, ${d.losses} persi, ${d.pending} in attesa</summary><p class="note">${esc(p.cohort)}</p><div class="results-grid">${cards(p.all_results.filter(r=>r.outcome!=='pending'))}</div></details>`;
  }catch(_){panel.innerHTML='<div class="empty">Risultati temporaneamente non disponibili.</div>';}
 }
 refresh();setInterval(refresh,300000);
})();
