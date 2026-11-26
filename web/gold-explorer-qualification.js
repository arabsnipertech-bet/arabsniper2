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
 function savedEvidence(value,context){
  const f=value.selection_facts||{},a=context.evidence||{};
  if((f.evidence_protocol||context.evidence_protocol)!=='V7_RESEARCH_TRACE_20261125_V1')return [];
  const kind=f.kind||context.spec?.kind||value.kind||value.spec_kind;
  const period=kind==='pt_btts_ft_total'?'ht':f.period||context.spec?.period||value.period;
  const tag=({ht:'PT',sh:'ST',ft:'FT'})[period];
  const count=side=>f['sample_'+side]??(period==='sh'?a.observed_goal_samples?.[side]?.st:a.sample_sizes?.[side]?.['for_'+period]);
  const valid=v=>Number.isInteger(v)&&v>=0;
  const home=count('home'),away=count('away'),lines=[];
  if(tag&&(valid(home)||valid(away)))lines.push('Campioni '+tag+': casa '+(valid(home)?home:'non registrato')+', ospite '+(valid(away)?away:'non registrato')+'.');
  const peak=f.without_peak??a.without_peak;
  lines.push('Tenuta senza picco: '+(peak===true?'verificata':peak===false?'non verificata':'non registrata')+'.');
  const paired=f.paired_books??a.flow_fields?.paired_books,agreeing=f.agreeing_books??a.flow_fields?.agreeing_books;
  lines.push('Bookmaker confrontabili: '+(valid(paired)?paired:'non registrato')+'; concordi: '+(valid(agreeing)?agreeing:'non registrato')+'.');
  return lines;
 }
 function numericalReferences(value,context,type){
  const f=value.selection_facts||{},s=context.spec||{},stats=context.statistics||{};
  const kind=f.kind||s.kind||value.kind||value.spec_kind;
  const period=kind==='pt_btts_ft_total'?'ht':f.period||s.period||value.period||({PT:'ht',ST:'sh',FT:'ft'})[value.horizon];
  const suffix=({ht:'ht',sh:'st',ft:'ft'})[period],tag=({ht:'PT',sh:'ST',ft:'FT'})[period];
  const home=f.lambda_home??stats['home_'+suffix],away=f.lambda_away??stats['away_'+suffix];
  let statistical=null;
  if(['halves_total','both_halves','pt_ft_total'].includes(kind)){
   const first=f.lambda_pt??(finite(stats.home_ht)&&finite(stats.away_ht)?stats.home_ht+stats.away_ht:null);
   const tail=kind==='pt_ft_total'?'ft':'st';
   const second=f['lambda_'+tail]??(finite(stats['home_'+tail])&&finite(stats['away_'+tail])?stats['home_'+tail]+stats['away_'+tail]:null);
   if(finite(first)&&finite(second))statistical='λ stat PT/'+(tail==='ft'?'FT':'ST')+' '+number(first)+'/'+number(second);
  }else if(tag&&finite(home)&&finite(away)&&home>=0&&away>=0)statistical=['result','btts','dnb','handicap','pt_btts_ft_total'].includes(kind)?
   'λ stat '+tag+' C/O '+number(home)+'/'+number(away):'λ stat '+tag+' '+number(home+away);
  else if(finite(f.lambda_pt)&&finite(f.lambda_st))statistical='λ stat PT/ST '+number(f.lambda_pt)+'/'+number(f.lambda_st);
  const p=value.probability??value.calibrated,ev=value.ev,q=value.confirmation||{};
  const chance=finite(p)&&p>=0&&p<=1?'P V7 '+probability(p):null;
  const margin=f.margin_pct??value.features?.margin_pct;
  const cost=finite(margin)&&margin>=0&&margin<=8?'Aggio '+margin.toLocaleString('it-IT',{minimumFractionDigits:1,maximumFractionDigits:1})+'%':null;
  const delta=q.opening_delta_pct??f.opening_delta_pct??value.features?.opening_delta_pct;
  const movement=q.flow_confirmed===true&&finite(delta)&&delta<=-2?'Quota ↓ '+probability(Math.abs(delta)/100):null;
  const valueRef=finite(ev)?'EV '+percent(ev):null;
  return [statistical,chance,valueRef||cost].filter(Boolean).slice(0,3);
 }
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
  const ev=value.ev,facts=numericalReferences(value,context,type);
  const detail=[labels[type]+'.',finite(ev)?'EV originale '+percent(ev)+'.':'',
   ...savedEvidence(value,context),
   ...attention.map(v=>v+'.'),type==='statistical_recovery'?'Il recupero statistico non certifica valore economico positivo.':'',
   ['study','observation','contrarian_study'].includes(type)?'Spunto in osservazione, distinto da una proposta qualificata.':'',
   type==='documented_value'?'Il livello Gold applica requisiti separati.':''].filter(Boolean).join(' ');
  const label=['study','observation','contrarian_study'].includes(type)?'Osservazione':labels[type];
  return {type,label,facts,summary:facts.join(' · '),reasons:attention,detail};
 }
 function render(value,context){
  const q=describe(value,context);if(!q)return '';
  return (q.facts.length?'<div class="v7-qualification" data-qualification="'+esc(q.type)+'" aria-label="'+esc(q.label)+'">'+
   '<strong class="v7-qualification-label">Perché questa scelta</strong><span class="v7-qualification-text">'+
   q.facts.map(v=>'<span>'+esc(v)+'</span>').join('')+'</span></div>':'')+
   '';
 }
 function help(){return '<details class="v7-numbers-help"><summary>Come leggere λ, probabilità ed EV</summary><p><b>λ</b>: gol attesi nel periodo indicato. <b>P V7</b>: probabilità stimata della scelta.</p><p><b>EV</b>: rapporto tra probabilità e quota. Per esempio, P 50% a quota 2,10 dà EV +5%; a quota 1,90 dà EV −5%. <b>Aggio</b>: margine del bookmaker incluso nella quota.</p></details>';}
 const api=Object.freeze({describe,render,help});
 if(typeof module==='object'&&module.exports)module.exports=api;
 root.ArabV7Qualification=api;
})(typeof globalThis==='object'?globalThis:this);
