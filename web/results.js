/* Only settled public results; no current Gold payload. */
(() => {
 const endpoint='https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/arab_quote_results_public.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const pct=v=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{maximumFractionDigits:1})+'%':'—';
 const date=v=>new Intl.DateTimeFormat('it-IT',{day:'numeric',month:'long',year:'numeric',timeZone:'Europe/Rome'}).format(new Date(v+'T12:00:00Z'));
 const cards=rows=>rows.map(r=>`<article class="result-card ${r.outcome==='won'?'won':'lost'}"><span class="result-outcome">${r.outcome==='won'?'✓ CENTRATA':'✕ NON CENTRATA'}</span><span class="league">${esc(r.league)}</span><h3>${esc(r.match)}</h3><strong>${esc(r.label)}</strong></article>`).join('');
 async function refresh(){
  const panel=document.getElementById('results-content');
  try{
   const response=await fetch(endpoint+'?v='+Date.now(),{cache:'no-store',signal:AbortSignal.timeout(15000)});
   if(!response.ok)throw Error('feed');
   const p=await response.json();
   if(!p.available||!Array.isArray(p.all_results)||!p.daily?.settled){panel.innerHTML='<div class="empty">I risultati di ieri sono in verifica. Pubblicheremo gli esiti quando disponibili.</div>';return;}
   const today=new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Rome',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
   const expected=new Date(today+'T12:00:00Z');expected.setUTCDate(expected.getUTCDate()-1);
   if(p.date!==expected.toISOString().slice(0,10)){panel.innerHTML='<div class="empty">I risultati di ieri sono in aggiornamento.</div>';return;}
   const d=p.daily,h=p.history;
   panel.innerHTML=`<div class="results-summary"><div class="result-rate"><strong>${pct(d.hit_rate)}</strong><span>ESITI CENTRATI · ${esc(date(p.date))}</span></div><div class="result-counts"><span><b>${d.wins}</b> vinte</span><span><b>${d.losses}</b> perse</span><span><b>${d.pending}</b> in attesa</span></div><div class="history-rate"><strong>${pct(h.hit_rate)}</strong><span>Media ponderata su ${h.settled} esiti<br>${esc(date(h.from_date))} – ${esc(date(h.to_date))}<br>${h.days_with_data} giorni con dati</span></div></div><p class="note">${esc(p.cohort)} Percentuali su tutte le proposte concluse del gruppo, esclusi gli esiti in attesa. I risultati passati non garantiscono quelli futuri.</p><div class="results-grid">${cards(p.examples)}</div><p class="note">Vetrina: fino a 10 vinte e 3 perse, in ordine di orario per ciascun esito. È un estratto; le percentuali includono anche le partite non mostrate.</p><details><summary>Tutti gli esiti del giorno (${d.settled})</summary><div class="results-grid">${cards(p.all_results.filter(r=>r.outcome!=='pending'))}</div></details>`;
  }catch(_){panel.innerHTML='<div class="empty">Risultati temporaneamente non disponibili.</div>';}
 }
 refresh();setInterval(refresh,300000);
})();
