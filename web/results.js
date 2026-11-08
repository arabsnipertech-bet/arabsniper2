/* Only settled public results; no current Gold payload. */
(() => {
 const endpoint='https://raw.githubusercontent.com/arabsnipertech-bet/arabsniper2/main/data/arab_quote_results_public.json';
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

 const date=v=>new Intl.DateTimeFormat('it-IT',{day:'numeric',month:'long',year:'numeric',timeZone:'Europe/Rome'}).format(new Date(v+'T12:00:00Z'));
 const cards=rows=>rows.map(r=>`<article class="result-card ${r.outcome==='won'?'won':'lost'}"><span class="result-outcome">${r.outcome==='won'?'✓ CENTRATA':'✕ NON CENTRATA'}</span><span class="league">${esc(r.league)}</span><h3>${esc(r.match)}</h3><strong>${esc(r.label)}</strong>${typeof r.odd==='number'&&Number.isFinite(r.odd)&&r.odd>1?`<span class="result-odd">${r.odd.toLocaleString('it-IT',{minimumFractionDigits:2,maximumFractionDigits:2})} <small>QUOTA PREMATCH</small></span>`:''}</article>`).join('');
 async function refresh(){
  const panel=document.getElementById('results-content');
    const controller=new AbortController(), timeout=setTimeout(()=>controller.abort(),15000);
  try{
   const response=await fetch(endpoint+'?v='+Date.now(),{cache:'no-store',signal:controller.signal});
   if(!response.ok)throw Error('feed');
   const p=await response.json();
   const today=new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Rome',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
   const expected=new Date(today+'T12:00:00Z');expected.setUTCDate(expected.getUTCDate()-1);
   const yesterday=expected.toISOString().slice(0,10);
   const eligible=v=>v&&/^\d{4}-\d{2}-\d{2}$/.test(v.date)&&v.date<=yesterday&&Array.isArray(v.all_results)&&v.all_results.some(r=>['won','lost'].includes(r.outcome));
   const current=eligible(p)&&p.available&&p.date===yesterday;
   const report=current?p:eligible(p.latest_verified)?p.latest_verified:eligible(p)?p:null;
   const h=p.history;
   const history=h&&h.settled>0?`<details><summary>Riepilogo storico verificato · ${esc(h.settled)} esiti conclusi</summary><p>${esc(date(h.from_date))} – ${esc(date(h.to_date))}: ${esc(h.wins)} analisi centrate, ${esc(h.losses)} non centrate, ${esc(h.pending)} in attesa.</p><p class="note">${esc(p.cohort)} Modello: ${esc(p.model_id)}. I risultati di versioni diverse restano separati.</p></details>`:'';
   const notice=current?'':`<p class="note">Gli esiti del ${esc(date(yesterday))} sono in aggiornamento. ${report?'Di seguito l’ultima giornata verificata disponibile.':history?'È disponibile il riepilogo storico verificato qui sotto.':''}</p>`;
   if(!report){panel.innerHTML=notice+history+(!history?'<p>Non sono ancora disponibili esiti verificati da mostrare.</p>':'');return;}
   const settled=report.all_results.filter(r=>['won','lost'].includes(r.outcome));
   const wins=settled.filter(r=>r.outcome==='won').length,losses=settled.length-wins;
   panel.innerHTML=notice+`<p class="note">${esc(date(report.date))} · Verifica delle analisi prematch</p><p>${wins} esiti centrati · ${losses} non centrati. Le quote sono quelle registrate prima della partita.</p><div class="results-grid">${cards(settled.slice(0,10))}</div>${settled.length>10?`<details><summary>Altri ${settled.length-10} esiti della giornata</summary><div class="results-grid">${cards(settled.slice(10))}</div></details>`:''}<p class="note">Esiti ordinati per partita, senza ricostruire le analisi dopo il risultato. I risultati passati non garantiscono quelli futuri.</p>`+history;
  }catch(_){panel.innerHTML='<div class="empty">Risultati temporaneamente non disponibili.</div>';}finally{clearTimeout(timeout);}
 }
 refresh();setInterval(refresh,300000);
})();
