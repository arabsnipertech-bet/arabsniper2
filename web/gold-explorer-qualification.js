/* Presentation of saved V7 qualifications, shared by Gold and Explorer.
   Never recalculates probabilities, admission, tiers or historical labels. */
(function(root){
 'use strict';
 const labels={documented_value:'Valore documentato',statistical_recovery:'Recupero statistico',
  contrarian_study:'Studio GG PT contrarian',study:'Studio',observation:'In osservazione'};
 const reasons={quote_unusable:'quota utilizzabile non documentata',statistics_unsupported:'supporto statistico insufficiente',
  same_book_reference_missing:'riferimento dello stesso operatore non verificato',margin_missing:'aggio non disponibile',
  margin_out_of_range:'aggio fuori dai limiti della qualifica',ev_missing:'EV non disponibile',ev_negative:'EV centrale negativo',
  ev_low_missing:'scenario prudente non disponibile',ev_low_negative:'scenario prudente negativo',study_only:'selezione in studio'};
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const finite=v=>typeof v==='number'&&Number.isFinite(v);
 const percent=v=>(v>0?'+':'')+(100*v).toLocaleString('it-IT',{maximumFractionDigits:1,minimumFractionDigits:1})+'%';
 function describe(value){
  const q=value?.confirmation;
  if(!q||typeof q!=='object')return null;
  // Older snapshots can expose explicit original flags, never inferred gates.
  let type=q.qualification_type||(q.observation_only===false&&q.statistical_recovery===true?'statistical_recovery':
   q.observation_only===false&&q.value_qualified===true?'documented_value':q.contrarian_study===true?'contrarian_study':
   q.observation_only===true?'observation':null);
  if(q.observation_only===true&&['documented_value','statistical_recovery'].includes(type))type='observation';
  if(!labels[type])return null;
  const attention=(Array.isArray(q.qualification_reasons)?q.qualification_reasons:[]).map(key=>reasons[key]).filter(Boolean);
  const ev=value.ev;
  const summary=type==='statistical_recovery'?(finite(ev)?'EV originale '+percent(ev)+'. ':'')+'Supporto statistico; valore economico positivo non confermato.':
   type==='documented_value'?'Statistica e scenario prudente sostengono il prezzo dello snapshot. Il livello Gold applica requisiti separati.':
   type==='contrarian_study'?'Ipotesi specifica in studio, senza promozione automatica.':
   attention.length?attention.slice(0,2).join(' · ')+'.':type==='study'?'Ipotesi in studio, distinta dalle proposte qualificate.':'Qualifica non confermata al prezzo osservato.';
  return {type,label:labels[type],summary,reasons:attention};
 }
 function render(value){
  const q=describe(value);if(!q)return '';
  return '<div class="v7-qualification" data-qualification="'+esc(q.type)+'"><b class="v7-qualification-label">'+
   esc(q.label)+'</b><span class="v7-qualification-text">'+esc(q.summary)+'</span></div>';
 }
 const api=Object.freeze({describe,render});
 if(typeof module==='object'&&module.exports)module.exports=api;
 root.ArabV7Qualification=api;
})(typeof globalThis==='object'?globalThis:this);
