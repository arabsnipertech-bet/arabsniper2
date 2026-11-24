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
 const probability=v=>(100*v).toLocaleString('it-IT',{maximumFractionDigits:1,minimumFractionDigits:1})+'%';
 const number=v=>v.toLocaleString('it-IT',{maximumFractionDigits:2,minimumFractionDigits:2});
 const roles={most_probable:'Base della triade per probabilità stimata',recommended_alternative:'Alternativa della triade per probabilità e quota',high_odds_shot:'Scelta a quota alta della triade'};
 const niches={fav_home_pt_vulnerable:'Spinta precoce della casa e vulnerabilità difensiva registrate.',
  balanced_1x2_inversion:'Contesto registrato: equilibrio 1X2 e movimento incrociato delle quote.',
  bookmaker_shift_rebound:'Contesto registrato: ritmo del primo tempo e confronto con il finale.',
  pt_st_symmetry_11_22:'Proiezione dei gol distribuita tra primo e secondo tempo.'};
 function describe(value,context={}){
  const q=value?.confirmation;
  if(!q||typeof q!=='object')return null;
  // Older snapshots can expose explicit original flags, never inferred gates.
  let type=q.qualification_type||(q.observation_only===false&&q.statistical_recovery===true?'statistical_recovery':
   q.observation_only===false&&q.value_qualified===true?'documented_value':q.contrarian_study===true?'contrarian_study':
   q.observation_only===true?'observation':null);
  if(q.observation_only===true&&['documented_value','statistical_recovery'].includes(type))type='observation';
  if(!labels[type])return null;
  const attention=(Array.isArray(q.qualification_reasons)?q.qualification_reasons:[]).map(key=>reasons[key]).filter(Boolean);
  const ev=value.ev,p=value.probability??value.calibrated;
  const role=context.role||(value.roles?.length===1?value.roles[0]:null);
  const parts=[];
  if(roles[role])parts.push(roles[role]+'.');
  if(finite(p)&&p>=0&&p<=1)parts.push('Il modello V7 stima '+probability(p)+' per questo esito.');
  const niche=value.niche_id||context.signal?.niche_id;
  if(niches[niche])parts.push(niches[niche]);
  if(type==='documented_value'){
   const fair=value.model_fair_odd;
   parts.push(finite(value.odd)&&finite(fair)&&value.odd>fair?
    'Quota '+number(value.odd)+' superiore alla quota equa V7 '+number(fair)+'.':
    finite(ev)&&ev>0?'Valore stimato alla quota offerta: EV '+percent(ev)+'.':
    'Supporto di valore registrato nella fotografia prematch.');
  }else if(type==='statistical_recovery')parts.push('Supporto statistico e stima prudente almeno in linea con il fair dello stesso operatore: recupero statistico registrato.');
  else if(type==='contrarian_study')parts.push('Contesto GG PT contrarian registrato dal motore, da seguire in studio.');
  if(!parts.length)parts.push('Osservazione del modello; motivazione originale non documentata in questo riferimento.');
  const detail=[labels[type]+'.',finite(ev)?'EV originale '+percent(ev)+'.':'',
   ...attention.map(v=>v+'.'),type==='statistical_recovery'?'Il recupero statistico non certifica valore economico positivo.':'',
   ['study','observation','contrarian_study'].includes(type)?'Spunto in osservazione, distinto da una proposta qualificata.':'',
   type==='documented_value'?'Il livello Gold applica requisiti separati.':''].filter(Boolean).join(' ');
  return {type,label:['study','observation','contrarian_study'].includes(type)?'Perché questo spunto':
   type==='statistical_recovery'?'Perché questo recupero':'Perché questa scelta',summary:parts.join(' '),reasons:attention,detail};
 }
 function render(value,context){
  const q=describe(value,context);if(!q)return '';
  return '<div class="v7-qualification" data-qualification="'+esc(q.type)+'"><b class="v7-qualification-label">'+
   esc(q.label)+'</b><span class="v7-qualification-text">'+esc(q.summary)+'</span></div>'+
   '<details class="v7-qualification-details"><summary>Dati e limiti</summary><p>'+esc(q.detail)+'</p></details>';
 }
 const api=Object.freeze({describe,render});
 if(typeof module==='object'&&module.exports)module.exports=api;
 root.ArabV7Qualification=api;
})(typeof globalThis==='object'?globalThis:this);
