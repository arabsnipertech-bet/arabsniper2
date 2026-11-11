/* Universal prematch observation. Live scores never overwrite frozen estimates. */
(function(root){
 'use strict';
 const MODEL='lateral-matrix-v5.0',V7='arabsniper-v7.0';
 const competition=typeof module==='object'&&module.exports?require('./competition-scope.js'):globalThis.ArabCompetitionScope;
 const POLICY='gold-goals-v6.1',SCOPE=competition.id;
 const RULES_REVISION='v7-prudence-v1';
 const acceptedScope=competition.accepts;
 const acceptedPolicy=d=>[POLICY,'gold-goals-v6','gold-goals-v5','gold-goals-v4','gold-goals-v3','gold-goals-v2'].includes(d?.policy_id);
 const MIN_P={over15_ht:40,over25_ht:18,gg_ht:26,over25_ft:48,over35_ft:35};
 const V6_MIN_P={over15_ht:35,gg_ht:30,over25_ft:30,over35_ft:30};
 const ODDS_MIN={over15_ht:2.1,over25_ht:4.5,gg_ht:3.5,over25_ft:1.70,over35_ft:2.1};
 const PREVIOUS_MIN_P={over15_ht:45,gg_ht:40,over25_ft:35,over35_ft:35};
 const dedicatedPolicy=id=>[POLICY,'gold-goals-v6','gold-goals-v5'].includes(id);
 const ODDS_MAX={over15_ht:3.5,over25_ht:7.5,over25_ft:2.6,gg_ht:5.5,over35_ft:4.5};
 // Display-only monitoring. These limits never enter the Gold scoring policy.
 const MARKET_SIGNAL=Object.freeze({min_oci:30,max_delta_pct:-2,max_margin_pct:9,min_persistence:.5,min_agreeing_books:2});
 const LEGACY_PLATINUM_POLICY='platinum-flow-v1',PLATINUM_POLICY='platinum-flow-v1.1';
 const PLATINUM=Object.freeze({...MARKET_SIGNAL,max_delta_pct:-3,min_ev_low_pct:2});
 const LEGACY_PLATINUM_VALUE={min_odd:2.5,min_ev_pct:10,min_high_odds_ev_pct:-15};
 const MARKET_SIGNAL_TEXT='Radar di Mercato · Statistica debole o assente · Solo studio';
 const LEGACY_MAX={over15_ht:3.5,over25_ft:3.5,gg_ht:5.5,over35_ft:5.5};
 const priced=r=>['recent','cached_stable'].includes(r?.quote_status);
 const isCombo=r=>r.spec?.kind==='halves_total'&&r.spec.line===.5&&r.spec.extra===.5;
 // A model-only combo is an estimate, not a failed bookmaker quote download.
 const isJoint=s=>['halves_total','pt_ft_total','pt_btts_ft_total','both_halves'].includes(s?.kind);
 const missingQuote=r=>!priced(r)&&(!isJoint(r.spec)||r.combo?.price_available===true);
 const ST_GOLD={over15_sh:[45,1.55,2.15],gg_sh:[32,2.15,3.5],over25_sh:[16,3.5,7.5]};
 const state={category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''};
 const INACTIVE=['PST','CANC','ABD'];
 const PREMATCH_WINDOW_MS=120*3600000;
 const observedFixtures=new Set();
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=(v,n=1)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{minimumFractionDigits:n,maximumFractionDigits:n}):'—';
 const pct=v=>typeof v==='number'&&Number.isFinite(v)?num(v)+'%':'—';
 const signed=v=>typeof v==='number'&&Number.isFinite(v)?(v>0?'+':'')+num(v):'—';
 const FAMILIES={all:'Mercati utili', '1x2':'1X2',totals:'Over / Under',btts:'Goal / No Goal',combo:'Combo',asian:'Riferimenti asiatici'};
 const CATEGORIES={all:'Tutte',gold:'Gold',platinum:'Platinum',studio:'Studio',saved:'Osservate',market_signal:'Radar di Mercato',combo:'Combo',convergence:'Convergenza',divergence:'Contrarian',lateral:'Riferimenti asiatici',value:'Criteri di valore',no_quote:'Quote da aggiornare'};
 const STATUS={mature:'CONVERGENZA STATISTICA',contrarian:'DIVERGENZA STATISTICA',lateral:'LATERALE',value_watch:'VALORE IN OSSERVAZIONE',limited_data:'STORICO LIMITATO',no_edge:'PREZZO SENZA VANTAGGIO',no_quote:'SOLO STATISTICA',previous_version:'VERSIONE PRECEDENTE'};
 const QUALITY={supported:'Storico presente',limited:'Storico limitato',prior_dominant:'Una squadra con dati mancanti',prior_only:'Solo valori di riferimento'};
 const REASONS={market:'mercato escluso',probability:'stima bassa',price:'quota fuori range',stale:'quota da aggiornare',history:'storico insufficiente',ht_history:'meno di 4 PT per squadra',adapter:'quota da verificare',margin:'aggio oltre limite',edge:'criterio di valore non raggiunto',sensitivity:'criterio prudente non raggiunto',coherence:'stime incoerenti'};
 Object.assign(REASONS,{yield_floor:'quota sotto il minimo previsto',oci_negative:'OCI < −20',odds_rising:'salita quota >2%',suspicious_divergence:'divergenza non confermata',margin_missing:'aggio assente',ht_oci:'OCI PT < +15',structural_missing:'statistiche mancanti',isolated_peak:'media senza ultima <80%',ht_symmetry:'PT asimmetrici',ht_expected:'gol attesi PT bassi',ht_frequency:'frequenza PT <75%',v7_quote_unusable:'quota prematch non verificabile',v7_unavailable:'stima V7 non disponibile'});
 const allowedMatch=m=>m.gold_scope?.allowed===true&&competition.allows(m.gold_scope.policy_id,m.fixture?.country,m.fixture?.league);
 const scopePayload=p=>({...p,matches:(p.matches||[]).filter(allowedMatch),gold_scope:{...(p.gold_scope||{}),ready:acceptedScope(p.gold_scope?.policy_id)}});
 function goalMarket(r){
  const s=r.spec||{};
  if(s.extra!=null)return null;
  if(s.period==='sh'){
   if(s.kind==='btts'&&s.side==='yes'&&s.line==null)return 'gg_sh';
   if(['total','asian_total'].includes(s.kind)&&s.side==='over'){
    if(s.line===1.5)return 'over15_sh';
    if(s.line===2.5)return 'over25_sh';
   }
   return null;
  }
  return s.kind==='btts'&&s.period==='ht'&&s.side==='yes'&&s.line==null?'gg_ht':['total','asian_total'].includes(s.kind)&&s.side==='over'?(s.period==='ht'&&s.line===1.5?'over15_ht':s.period==='ht'&&s.line===2.5?'over25_ht':s.period==='ft'&&s.line===2.5?'over25_ft':s.period==='ft'&&s.line===3.5?'over35_ft':null):null;}
 const minProbability=r=>r.opportunity?.policy_id===POLICY?(ST_GOLD[goalMarket(r)]?.[0]??MIN_P[goalMarket(r)]??30):r.opportunity?.policy_id==='gold-goals-v6'?(V6_MIN_P[goalMarket(r)]??30):r.opportunity?.policy_id==='gold-goals-v5'?(PREVIOUS_MIN_P[goalMarket(r)]??35):35;
 const maxOdd=r=>r.opportunity?.policy_id===POLICY?(ST_GOLD[goalMarket(r)]?.[2]??ODDS_MAX[goalMarket(r)]??7.5):['gold-goals-v6','gold-goals-v5','gold-goals-v4'].includes(r.opportunity?.policy_id)?(LEGACY_MAX[goalMarket(r)]??3.5):3.5;
 const minOdd=r=>r.opportunity?.policy_id===POLICY?Math.max(ST_GOLD[goalMarket(r)]?.[1]??ODDS_MIN[goalMarket(r)]??1.70,Number.isFinite(r.opportunity.odds_min)?r.opportunity.odds_min:0):1.4;
 function coreMarket(s={}){
  if(isJoint(s)){
   if(s.period!=='ft')return false;
   if(s.kind==='halves_total')return s.side==='over'&&s.line===.5&&s.extra===.5;
   if(s.kind==='pt_ft_total')return s.side==='over'&&s.line===1.5&&s.extra===2.5;
   if(s.kind==='pt_btts_ft_total')return s.side==='yes'&&s.line===2.5&&s.extra==null;
   return s.side==='both'&&s.line==null&&s.extra==null;
  }
  if(!s||s.extra!=null)return false;
  if(s.kind==='result')return s.period==='ft'&&['1','x','2'].includes(s.side)&&s.line==null;
  if(s.kind==='dnb')return s.period==='ft'&&['1','2'].includes(s.side)&&s.line==null;
  if(s.kind==='btts')return ['ft','ht','sh'].includes(s.period)&&['yes','no'].includes(s.side)&&s.line==null;
  if(!Number.isFinite(s.line))return false;
  if(s.kind==='total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,2.5,3.5]:s.period==='ht'?[.5,1.5,2.5,3.5]:s.period==='sh'?[.5,1.5,2.5]:[]).includes(s.line);
  if(s.kind==='asian_total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,1.75,2,2.25,2.5,2.75,3,3.25,3.5]:s.period==='ht'?[.5,1,1.25,1.5,1.75,2,2.5,3.5]:s.period==='sh'?[.5,1.5,2.5]:[]).includes(s.line);
  return s.kind==='handicap'&&s.period==='ft'&&['home','away'].includes(s.side)&&[-1,-.75,-.5,-.25,0,.25,.5,.75,1].includes(s.line);
 }
 const phase=(m,now=Date.now())=>{
  const f=m.fixture||{},s=f.result?.status||f.live?.status||f.status_short;
  if(['FT','AET','PEN'].includes(s))return 'finished';
  if(['CANC','PST','ABD','SUSP','WO','AWD'].includes(s))return 'other';
  return Date.parse(f.kickoff)>now?'upcoming':'live';
 };
 function inactiveWithoutAnalysis(m){
  const f=m.fixture||{},a=m.multimarket||{},matrix=a.matrix||{};
  if(!INACTIVE.includes(f.status_short||f.result?.status||f.live?.status))return false;
  return !a.markets?.length&&!matrix.rows?.length&&matrix.v7?.status!=='published'&&
   !m._gold_history?.verified_prematch&&!Object.values(m.triad||{}).some(p=>['GOLD','PLATINUM'].includes(p?.tier));
 }
 function rowsOf(m){
  if(m.multimarket?.matrix)return (m.multimarket.matrix.rows||[]).filter(r=>coreMarket(r.spec)).map(r=>{
   if(r.spec?.kind!=='handicap'||r.quote_adapter_version===2)return r;
   const clean={...r};
   for(const key of ['odd','first_odd','last_odd','ev_pct','ev_low_pct','ev_high_pct','oci','oci_previous','margin_pct','margin_delta_pp','delta_pct','persistence','speed_pp_per_hour','amplitude_pct'])clean[key]=null;
   return {...clean,quote_status:'invalid_adapter',category:'no_quote',market_behavior:'unavailable',margin_band:'unavailable',margin_trend:'unavailable',paired_books:0,agreeing_books:0,category_reason:'Quota handicap della versione precedente: associazione da aggiornare'};
  });
  return (m.multimarket?.markets||[]).filter(r=>['over25_ft','over35_ft','over15_ht','over25_ht','gg_ht'].includes(r.market)).map(r=>({...r,id:r.market+'::'+r.selection,
    family:r.market?.includes('gg')?'btts':'totals',period:r.market?.endsWith('_ht')?'ht':'ft',
    quote_status:r.quote_status||'missing',first_odd:r.quote?.movement?.first_odds?.[r.selection],
    bookmaker:r.quote?.bookmaker,category:'previous_version',quality:'limited'}));
 }
 function matchingRows(m,options=state,now=Date.now(),lifecycle=phase){
  const all=rowsOf(m),tierOptions={now,archived:lifecycle(m,now)!=='upcoming',kickoff:m.fixture?.kickoff,monitoring:m.multimarket?.matrix?.quote_monitoring};
  if(options.category==='saved'&&!observedFixtures.has(String(m.fixture?.fixture_id)))return [];
  const v7=publishedView(m,now);
  if(v7&&['gold','platinum','studio','market_signal'].includes(options.category)){
   const wanted=options.category==='market_signal'?(v7.triad?'blu':'radar'):options.category;
   if(wanted!==v7.tier)return [];
   const relevant=v7.triad?Object.values(v7.triad).filter(Boolean).map(p=>p.source):v7.primary?[v7.primary.source]:all;
   return (relevant.length?relevant:all).filter(r=>(options.family==='all'||options.family===r.family)&&
    (options.period==='all'||options.period===r.period));
  }
  if(['gold','studio'].includes(options.category)){
   const tone=cockpitTier(m,{phase:match=>lifecycle(match,now)}).tone;
   if(options.category==='gold'&&tone!=='gold'||options.category==='studio'&&tone!=='studio')return [];
  }
  const platinum=allowedMatch(m)?platinumSignals(all,tierOptions):[];
  const studyRows=options.category==='market_signal'&&!tierOptions.archived&&Array.isArray(tierOptions.monitoring?.studio_ideas)
   ?all.filter(row=>tierOptions.monitoring.studio_ideas.some(idea=>idea.spec?.kind===row.spec?.kind&&idea.spec?.side===row.spec?.side&&idea.spec?.period===row.spec?.period&&idea.spec?.line===row.spec?.line)):[];
  const frozen=m._gold_history?.version==='gold-history-v1'?m._gold_history:null;
  const historicalIds=frozen&&(options.category==='platinum'?frozen.platinum_ids:options.category==='market_signal'?frozen.radar_ids:null);
  const source=Array.isArray(historicalIds)?all.filter(r=>historicalIds.includes(r.id)):options.category==='platinum'?platinum:options.category==='market_signal'?(allowedMatch(m)&&!platinum.length?[...marketSignals(all,tierOptions),...monitoringRows(all,tierOptions),...studyRows]:[]):all;
  return source.filter(r=>{
   if(options.family!=='all'&&r.family!==options.family)return false;
   if(options.period!=='all'&&r.period!==options.period)return false;
   if(options.category==='combo')return v7?.triad?isJoint(r.spec)&&priced(r)&&r.quote_executable===true:isCombo(r)&&comboActive(r);
   if(options.category==='convergence')return r.market_behavior==='convergence';
   if(options.category==='divergence')return r.market_behavior==='divergence';
   if(options.category==='lateral')return r.family==='asian';
   if(options.category==='value')return r.ev_pct>0&&priced(r);
   if(options.category==='no_quote')return missingQuote(r);
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
   return matchingRows(m,options,now,lifecycle).length>0||(options.category==='all'&&options.family==='all'&&options.period==='all');
  }).sort((a,b)=>{
   if(['oci','odd'].includes(options.sort)){
    const key=options.sort==='oci'?'oci':'odd';
    const score=m=>Math.max(-Infinity,...rowsOf(m).map(r=>Number.isFinite(r[key])?r[key]:-Infinity));
    const d=score(b)-score(a);if(d)return d;
   }
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
 function calibrationDelta(r){
  const c=r.calibration;
  return c?.version==='adaptive-calibration-v5.1'&&Number.isInteger(c.n)&&c.n>=50&&Number.isFinite(c.threshold_delta_pp)
   ?Math.max(-.5,Math.min(2.5,c.threshold_delta_pp)):0;
 }
 function adaptiveValue(r,floor){
  const delta=calibrationDelta(r);
  return Number.isFinite(r.ev_pct)&&r.ev_pct>=Math.max(0,floor+delta)&&(!delta||
    (Number.isFinite(r.edge_pp)&&r.edge_pp>=delta&&(delta>=0||r.ev_pct>0)));
 }
 function v61Valid(r,now,archived){
  const d=r.opportunity;if(d?.policy_id!==POLICY)return true;
  const m=goalMarket(r),margin=r.selected_margin_pct,delta=Number.isFinite(r.delta_pct)?r.delta_pct:r.selected_movement?.delta_pct;
  if(!d.structural_supported||!d.market_checks_passed||!Number.isFinite(margin)||margin<0||margin>8||r.oci< -20||delta>2+1e-9)return false;
  if(d.rules_revision===RULES_REVISION){
   if(!Number.isFinite(r.ev_low_pct)||!Number.isFinite(d.min_ev_low_pct)||r.ev_low_pct+1e-9<d.min_ev_low_pct)return false;
  }else{
   const book=Number.isFinite(r.selected_market_fair_pct)?r.selected_market_fair_pct:Number.isFinite(margin)&&margin> -100&&r.odd>1?100/r.odd/(1+margin/100):r.market_fair_pct;
   const floors={over15_ht:2,over25_ht:0,over25_ft:2,over35_ft:1,gg_ht:-1};
   if(m==='over25_ht'&&(!Number.isFinite(r.oci)||r.oci<15))return false;
   if(Number.isFinite(book)&&r.statistical_pct-book>15+1e-9&&!(delta<0&&r.oci>0))return false;
   if(r.ev_low_pct<floors[m]+(margin>=6.5?1.5:0))return false;
  }
  if(!adaptiveValue(r,m==='gg_ht'?6:0)||d.stability?.status==='retained')return false;
  if(r.quote_status==='cached_stable'){
   const at=Date.parse(r.cache_source_at),end=Date.parse(r.cache_expires_at);
   if(!Number.isFinite(at)||!Number.isFinite(end)||end-at!==12*3600000||(!archived&&(now<at||now>end)))return false;
  }
  return true;
 }
 function comboReasons(row){
  const c=row?.combo;
  if(c?.policy_id!==POLICY)return 'Valutazione assente';
  const teams=row.goal_evidence?.teams||{};
  const rates=[['PT',c.lambda_ht,1.15],['ST',c.lambda_sh,1.40]].filter(([,value,floor])=>!Number.isFinite(value)||value<floor)
   .map(([period,value,floor])=>Number.isFinite(value)?period+' '+num(value,2)+' < '+num(floor,2):'Gol '+period+' assenti').join(' · ');
  const descriptions={
   combo_probability:'stima <56%',
   combo_rates:rates||'Gol attesi insufficienti',
   combo_goalless:'gare a zero FT casa/ospite '+num(teams.home?.ft?.goalless_matches,0)+'/'+num(teams.away?.ft?.goalless_matches,0)+' (max 1)',
   combo_oci:Number.isFinite(c.over25_ft_oci)?'OCI '+signed(c.over25_ft_oci)+' < −10':'OCI assente'
  };
  return (c.reasons||[]).map(code=>descriptions[code]||'Controllo non superato').join(' · ')||'Criteri non superati';
 }
 const comboActive=row=>row?.combo?.policy_id===POLICY&&row.combo.active===true&&row.statistical_pct>=56;
 function comboTone(row){
  if(comboActive(row))return 'active';
  const c=row?.combo,teams=row?.goal_evidence?.teams;
  if(c?.policy_id!==POLICY||!teams||!Number.isFinite(row.statistical_pct)||row.statistical_pct>100)return 'inactive';
  // Display-only proximity: never relax eligibility, ranking or backend flags.
  const controls=[[row.statistical_pct,56,2],[c.lambda_ht,1.15,.05],[c.lambda_sh,1.40,.05],[c.over25_ft_oci,-10,2]];
  const close=controls.every(([value,floor,tolerance])=>Number.isFinite(value)&&value+1e-9>=floor-tolerance);
  const below=controls.some(([value,floor])=>value+1e-9<floor);
  const scoring=['home','away'].every(side=>{const n=teams[side]?.ft?.goalless_matches;return Number.isInteger(n)&&n>=0&&n<=1;});
  const known=(c.reasons||[]).every(reason=>['combo_probability','combo_rates','combo_oci'].includes(reason));
  return close&&below&&scoring&&known?'near':'inactive';
 }
 function comboText(rows){
  const row=rows.find(isCombo);
  if(!row)return '<br><span class="mx-combo-inactive">Combo PT+ST · Dati assenti</span>';
  const tone=comboTone(row),active=tone==='active',tag=active?'strong':'span';
  const name=active?'★ Combo Consigliata':tone==='near'?'Combo vicina · non attiva':'Combo non attiva';
  const title=tone==='near'?'Vicino alle soglie: massimo 2 punti % di probabilità, 0,05 gol PT/ST e 2 punti OCI. Gare a zero già nei limiti.':active?'Tutti i criteri Combo superati. La quota equa è calcolata dal modello.':comboReasons(row);
  return '<br><'+tag+' class="mx-combo-'+tone+'" title="'+esc(title)+'">'+name+': Over 0,5 PT + Over 0,5 ST · Stima '+pct(row.statistical_pct)+' · <b>Equa '+num(row.fair_odd,2)+'</b> · '+
   (priced(row)?'<b>'+(row.quote_source==='synthetic_combo'?'Quota sintetica PT × ST ':'Quota book ')+num(row.odd,2)+'</b>':'Quota book n.d.')+(active?'':'<br>'+esc(comboReasons(row)))+'</'+tag+'>';
 }
 function extraGoalText(rows){
  const row=rows.find(r=>r.spec?.kind==='total'&&goalMarket(r)==='over25_ht');
  const extra=row?'<br>Over 2,5 PT · Stima '+pct(row.statistical_pct)+' · Equa '+num(row.fair_odd,2)+' · '+
   (priced(row)?(row.quote_status==='cached_stable'?'Quota cache ':'Quota book ')+num(row.odd,2):'Quota n.d.')+
   ' · '+(!priced(row)?'Non valutabile':row.opportunity?.eligible?'Idoneo':esc((row.opportunity?.reasons||[]).slice(0,2).map(code=>REASONS[code]||code).join(' · ')||'Valutazione assente')):'';
  return (extra+comboText(rows)).replace(/^<br>/,'');
 }
 function quoteAvailability(row){
  if(row.quote_status==='invalid_adapter')return 'Associazione quota da aggiornare';
  if(row.quote_status==='stale')return 'Ultima quota '+num(row.last_odd,2)+' · non valida per la proposta';
  return 'Quota del mercato non ricevuta';
 }
 function bestOpportunity(rows,{now=Date.now(),archived=false}={}){
  // The backend is the single scoring authority. Old/malformed decisions fail
  // closed; no frontend fallback to longshot EV or limited-history candidates.
  return rows.filter(r=>{
   const d=r.opportunity;
   const hold=d?.stability,retained=acceptedPolicy(d)&&d.policy_id!=='gold-goals-v2'&&hold?.version==='gold-stability-v1'&&hold.status==='retained';
   const start=Date.parse(hold?.last_strict_at),end=Date.parse(hold?.expires_at);
   const validHold=retained&&Number.isFinite(start)&&Number.isFinite(end)&&end>start&&end-start<=90*60000&&
    (archived||now>=start&&now<=end)&&d.strict_eligible===false&&
    Number.isFinite(d.entry_min_ev_pct)&&Number.isFinite(d.entry_min_ev_low_pct)&&
    d.min_ev_pct>=Math.max(.5,d.entry_min_ev_pct-1)&&d.min_ev_low_pct>=Math.max(-.5,d.entry_min_ev_low_pct-1);
   return acceptedPolicy(d)&&(d.policy_id===POLICY||archived)&&v61Valid(r,now,archived)&&d.eligible===true&&(!retained||validHold)&&Number.isInteger(d.rank)&&d.rank>0&&
    priced(r)&&r.quality==='supported'&&Number.isFinite(r.odd)&&r.odd>=minOdd(r)&&r.odd<=maxOdd(r)&&
    Number.isFinite(d.min_probability_pct)&&Number.isFinite(r.statistical_pct)&&r.statistical_pct>=Math.max(minProbability(r),d.min_probability_pct)&&r.statistical_pct<=100&&
    Number.isFinite(r.ev_pct)&&r.ev_pct>=d.min_ev_pct&&Number.isFinite(r.ev_low_pct)&&(d.policy_id===POLICY||validHold||r.ev_low_pct>0)&&r.ev_low_pct>=d.min_ev_low_pct&&
    goalMarket(r)!==null&&(!dedicatedPolicy(d.policy_id)||!['over15_ht','over25_ht','gg_ht'].includes(goalMarket(r))||d.ht_history_supported===true);
  }).sort((a,b)=>a.opportunity.rank-b.opportunity.rank||String(a.id).localeCompare(String(b.id)))[0]||null;
 }
 function marketSignals(rows,{now=Date.now(),archived=false,monitoringClosed=false}={}){
  // Never replace an official proposal or reinterpret a started fixture.
  if(archived||monitoringClosed||bestOpportunity(rows,{now,archived}))return [];
  const signals=rows.filter(r=>flowSignalValid(r,MARKET_SIGNAL,now)).sort((a,b)=>b.oci-a.oci||a.delta_pct-b.delta_pct||String(a.id).localeCompare(String(b.id)));
  // Equivalent Asian half-lines remain a single monitored goal market.
  const seen=new Set();return signals.filter(r=>{const key=goalMarket(r);if(seen.has(key))return false;seen.add(key);return true;});
 }
 function monitoringRows(rows,options={}){
  const m=options.monitoring;
  if(!m||m.version!=='quote-monitoring-v1'||m.study_only!==true||options.archived||options.monitoringClosed||bestOpportunity(rows,options)||platinumSignals(rows,options).length)return [];
  const confirmed=new Set(marketSignals(rows,options).map(r=>r.id)),ids=new Set(m.row_ids||[]);
  return rows.filter(r=>ids.has(r.id)&&!confirmed.has(r.id));
 }
 function flowSignalValid(r,limits,now){
   const move=r.selected_movement;
   // Shared quote checks only; statistical quality never enters this predicate.
   if(!goalMarket(r)||!priced(r)||!Number.isFinite(r.odd)||r.odd<=1)return false;
   if(r.quote_status==='cached_stable'){
    const at=Date.parse(r.cache_source_at),end=Date.parse(r.cache_expires_at);
    if(!Number.isFinite(at)||!Number.isFinite(end)||end-at!==12*3600000||now<at||now>end)return false;
   }
   if(!Number.isFinite(r.oci)||r.oci<limits.min_oci||r.oci>100||
      !Number.isFinite(r.selected_margin_pct)||r.selected_margin_pct<0||r.selected_margin_pct>limits.max_margin_pct||
      !Number.isFinite(r.first_odd)||r.first_odd<=1||typeof r.bookmaker!=='string'||!r.bookmaker.trim()||
      !Number.isFinite(r.delta_pct)||r.delta_pct>limits.max_delta_pct+1e-9||
      Math.abs(r.delta_pct-100*(r.odd/r.first_odd-1))>.01)return false;
   return r.movement_confirmed===true&&r.fair_direction_coherent===true&&
    Number.isInteger(r.paired_books)&&Number.isInteger(r.agreeing_books)&&r.paired_books>=limits.min_agreeing_books&&
    r.agreeing_books>=limits.min_agreeing_books&&r.agreeing_books<=r.paired_books&&r.agreeing_books>r.paired_books/2&&
    Number.isFinite(move?.delta_pct)&&move.delta_pct<=limits.max_delta_pct+1e-9&&
    Number.isFinite(move.fair_delta_pp)&&move.fair_delta_pp>0&&
    Number.isFinite(move.persistence)&&move.persistence>=limits.min_persistence&&move.persistence<=1&&move.reversal===false;

 }
 function platinumSignals(rows,{now=Date.now(),archived=false,monitoringClosed=false,kickoff=null}={}){
  const start=Date.parse(kickoff);
  const candidates=rows.filter(r=>{
   const d=r.platinum,at=Date.parse(d?.evaluated_at);
   const legacy=(archived||monitoringClosed)&&d?.policy_id===LEGACY_PLATINUM_POLICY;
   if((d?.policy_id!==PLATINUM_POLICY&&!legacy)||d.eligible!==true||!Number.isInteger(d.rank)||d.rank<1||
      !Number.isFinite(at)||at>now||(Number.isFinite(start)&&at>=start))return false;
   if(!flowSignalValid(r,PLATINUM,archived||monitoringClosed?at:now))return false;
   if(d.rules_revision===RULES_REVISION){
    return r.ev_pct>0&&adaptiveValue(r,0)&&Number.isFinite(r.ev_low_pct)&&r.ev_low_pct>=PLATINUM.min_ev_low_pct;
   }
   const prior=LEGACY_PLATINUM_VALUE;
   return adaptiveValue(r,prior.min_ev_pct)||
    (r.odd>=prior.min_odd&&(legacy||
     (Number.isFinite(r.ev_pct)&&r.ev_pct>=prior.min_high_odds_ev_pct+Math.max(0,calibrationDelta(r))&&(calibrationDelta(r)<=0||r.edge_pp>=calibrationDelta(r)))||
     (r.ev_pct==null&&r.statistical_pct==null)));
  }).sort((a,b)=>a.platinum.rank-b.platinum.rank||String(a.id).localeCompare(String(b.id)));
  const seen=new Set();return candidates.filter(r=>{const key=goalMarket(r);if(seen.has(key))return false;seen.add(key);return true;});
 }
 const MONITORED_GOALS={
  over15_ht:'Over 1,5 PT',over25_ht:'Over 2,5 PT',over25_ft:'Over 2,5 FT',
  over35_ft:'Over 3,5 FT',gg_ht:'GG PT',combo_ht_sh:'Combo Over 0,5 PT + Over 0,5 ST'
 };
 function formingPressure(r,{now=Date.now(),kickoff,archived=false,monitoringClosed=false}={}){
  const end=Date.parse(kickoff),at=Date.parse(r?.provider_updated_at),seen=Date.parse(r?.quote_observed_at);
  const s=r?.spec||{};
  if(!r||archived||monitoringClosed||!Number.isFinite(end)||now>=end||!priced(r)||!Number.isFinite(r.odd)||r.odd<=1||
     r.quote_executable===false||r.quality!=='supported'||!(goalMarket(r)||isJoint(r.spec)||(s.kind==='result'&&s.period==='ft')))return false;
  const cache=Date.parse(r.cache_source_at),expiry=Date.parse(r.cache_expires_at);
  const fresh=r.quote_status==='cached_stable'?expiry-cache===12*3600000&&cache<=now&&now<=expiry:
   Number.isFinite(at)&&at<=now&&now-at<=(end-now<=3*3600000?1:6)*3600000;
  return fresh&&(!Number.isFinite(seen)||seen<=now)&&Number.isFinite(r.ev_pct)&&r.ev_pct>0&&
   Number.isInteger(r.agreeing_books)&&Number.isInteger(r.paired_books)&&r.paired_books>=2&&r.agreeing_books<=r.paired_books&&
   r.agreeing_books/r.paired_books>=.6&&Number.isFinite(r.speed_pp_per_hour)&&r.speed_pp_per_hour>=.8&&
   ((Number.isFinite(r.oci)&&Math.abs(r.oci)>=25&&Math.abs(r.oci)<40)||(Number.isFinite(r.fair_delta_pp)&&Math.abs(r.fair_delta_pp)>=1.2));
 }
 function visibleStudyIdeas(ideas){
  ideas=ideas.filter(i=>!i.suppressed&&(!i.market?.startsWith('1x2_')||
   Number.isFinite(i.odd)&&i.odd>1&&i.odd<=4.5&&Number.isFinite(i.poisson_pct)&&
   i.poisson_pct>=(i.market==='1x2_x'||i.odd>3.6?35:32)&&
   (Number.isFinite(i.market_fair_pct)&&i.market_fair_pct>=18&&(i.poisson_pct-i.market_fair_pct<=18||Number.isFinite(i.delta_pct)&&i.delta_pct<=-3))&&
   (!(i.market==='1x2_x'||i.odd>4)||Number.isFinite(i.delta_pct)&&i.delta_pct<=-2.5)));
  const alternatives=ideas.some(i=>i.type!=='studio_combo_1x2_over25'||Number.isFinite(i.odd)&&i.odd>1);
  ideas=ideas.filter(i=>i.type!=='studio_combo_1x2_over25'||Number.isFinite(i.odd)&&i.odd>1||
   !alternatives&&Number.isFinite(i.blended_pct)&&i.blended_pct>48);

  return ideas;
 }
 function bestStudyIdea(ideas){
  return visibleStudyIdeas(ideas).find(i=>i.best_study===true&&Number.isFinite(i.poisson_pct)&&i.poisson_pct>=32&&i.poisson_pct<=100&&Number.isFinite(i.odd)&&i.odd>1
   &&i.quality==='supported'&&i.quote_executable===true&&['recent','cached_stable'].includes(i.quote_status)
   &&Number.isFinite(i.ev_low_pct)&&i.ev_low_pct>=0&&i.odd*i.poisson_pct>100
   &&(!i.star_gate||i.star_gate.eligible===true));
 }
 function starredStudyRow(m){
  const ideas=m.multimarket?.matrix?.quote_monitoring?.studio_ideas;
  const best=Array.isArray(ideas)?bestStudyIdea(ideas):null;
  if(!best?.spec?.kind||!best.spec.period||!best.spec.side)return null;
  return rowsOf(m).find(row=>row.spec&&Object.keys({...row.spec,...best.spec}).every(key=>row.spec[key]===best.spec[key]))||null;
 }
 function studyDefaultRow(m,now=Date.now()){
  if(m.multimarket?.status==='no_prematch_snapshot'||m._gold_history&&!m._gold_history.verified_prematch)return null;
  const starred=starredStudyRow(m);
  if(starred)return starred;
  // Display-only default for upcoming matches. Never backfill historical decisions.
  if(m._gold_history||phase(m,now)!=='upcoming')return null;
  const kickoff=Date.parse(m.fixture?.kickoff);
  const candidates=rowsOf(m).filter(r=>{
   if(!goalMarket(r)||r.quality!=='supported'||r.quote_executable!==true||!priced(r)||
      !Number.isFinite(r.odd)||r.odd<=1||!Number.isFinite(r.statistical_pct)||r.statistical_pct<=0||r.statistical_pct>100)return false;
   const at=Date.parse(r.provider_updated_at),seen=Date.parse(r.quote_observed_at);
   const cache=Date.parse(r.cache_source_at),expiry=Date.parse(r.cache_expires_at);
   const fresh=r.quote_status==='cached_stable'?expiry-cache===12*3600000&&cache<=now&&now<=expiry:
    Number.isFinite(at)&&at<=now&&now-at<=(kickoff-now<=3*3600000?1:6)*3600000;
   return fresh&&(!Number.isFinite(seen)||seen<=now);
  });
  const cautious=r=>Number.isFinite(r.low_pct)&&r.low_pct>=0&&r.low_pct<=r.statistical_pct?r.low_pct:0;
  return candidates.sort((a,b)=>cautious(b)-cautious(a)||b.statistical_pct-a.statistical_pct||String(a.id).localeCompare(String(b.id)))[0]||null;
 }
 function studyIdeaPanel(ideas){
  ideas=visibleStudyIdeas(ideas);
  const best=bestStudyIdea(ideas);
  const badge=best?'<strong class="mx-badge mx-market-signal" data-best-study>⭐ Miglior Spunto: '+esc(best.label)+' a '+num(best.odd,2)+'</strong>':'';
  const secondary=new Map();
  for(const idea of ideas){
   const key=idea.market||idea.label;
   if(best&&key===(best.market||best.label))continue;
   if(!secondary.has(key)||idea.type==='studio_ht_flow_forcing')secondary.set(key,idea);
  }
  const rows=Array.from(secondary.values(),i=>{
   const note=i.type==='studio_ht_flow_forcing'?'pressione book, statistica debole':
    i.type==='studio_combo_1x2_over25'?'combo correlata':i.type==='studio_1x2_value_flow'?(i.real_flow===true&&Number.isFinite(i.delta_pct)&&i.delta_pct<0&&(i.delta_pct<=-2||i.fair_delta_pp>=1.5)?'valore e flusso':'valore statistico'):
    i.study_note==='goal_context'?'scenario gol':
    i.historical_oci?'flusso osservato in precedenza':'flusso gol';
   const quote=Number.isFinite(i.odd)&&i.odd>1?'Quota '+num(i.odd,2):'Quota non disponibile';
   return '<li>'+esc(i.label)+' · '+quote+' ('+note+')</li>';
  });
  return '<section class="mx-timing" data-study-ideas>'+badge+(rows.length?'<ul>'+rows.join('')+'</ul>':'')+'</section>';
 }
 function monitoringPanel(rows,monitor,history,context,options={}){
  const key=r=>isCombo(r)?'combo_ht_sh':goalMarket(r);
  const quoted=rows.filter(r=>key(r)&&priced(r)&&Number.isFinite(r.odd)&&r.odd>1);
  const waiting=new Map((monitor.waiting_markets||[]).filter(m=>Object.hasOwn(MONITORED_GOALS,m.market)).map(m=>[m.market,m.reason]));
  const ready=new Set(quoted.filter(r=>!waiting.has(key(r))).map(key));
  const pressure=monitor.radar_state==='radar_watch'&&rows.find(r=>r.id===monitor.watch_id&&formingPressure(r,options));
  if(pressure){
   const s=pressure.spec,market=s.kind==='result'?({'1':'1 · Casa','x':'X · Pareggio','2':'2 · Ospite'}[s.side]):pressure.label;
   const movement=Number.isFinite(pressure.oci)?'OCI '+signed(displayOci(pressure).value):'Variazione fair '+signed(pressure.fair_delta_pp)+' pp';
   return '<aside class="mx-best mx-best-market" data-radar-state="radar_watch"><span class="mx-badge mx-market-signal">RADAR WATCH · FLUSSO IN FORMAZIONE</span><h4>Pressione in formazione su '+esc(market)+'</h4><p>'+movement+' · '+pressure.agreeing_books+'/'+pressure.paired_books+' operatori concordi · '+num(pressure.speed_pp_per_hour,1)+' pp/ora.</p><p>Solo studio · nessuna proposta confermata.</p>'+history+timingContext(context)+'</aside>';
  }
  const fallback=monitor.radar_state==='radar_watch'?(monitor.priced_core?.some(k=>k.endsWith('_ht'))&&monitor.priced_core?.some(k=>k.endsWith('_ft'))&&monitor.priced_markets>=20?'balanced':'waiting_quotes'):monitor.radar_state;
  const titles={balanced:'Mercato in equilibrio · Monitoraggio attivo',waiting_quotes:'In attesa di quote',observing_flow:'Movimento di mercato · Monitoraggio attivo'};
  const title=titles[fallback]||(ready.size?'Quote disponibili · flusso da confermare':quoted.length?'Quote presenti · nessun mercato gol idoneo al Radar':'In attesa di quote sui mercati gol');
  const state=ready.size&&waiting.size?'partial_quotes':monitor.state;
  const issues=Array.from(waiting,([market,reason])=>{
   const offers=quoted.filter(r=>key(r)===market);
   const detail=offers.length&&offers.every(r=>Number.isFinite(r.selected_margin_pct)&&r.selected_margin_pct>9+1e-9)?'aggio oltre il 9%':
    offers.length?'aggio o coppia da verificare':reason==='stale'?'quota da aggiornare':reason==='incomplete_or_invalid_pair'?'coppia da verificare':'quota mancante';
   return esc(MONITORED_GOALS[market])+' · '+detail;
  });
  return '<aside class="mx-best mx-best-market" data-market-monitoring="'+esc(state)+'" data-radar-state="'+esc(fallback||state)+'"><span class="mx-section-title">RADAR BLU · MONITORAGGIO</span><h4>'+title+'</h4><span class="mx-badge mx-market-signal">SOLO STUDIO</span><p>'+ready.size+' dei 6 mercati gol hanno una quota idonea. Il flusso richiesto dal Radar è ancora da confermare.</p>'+
   (issues.length?'<p>'+issues.join('<br>')+'</p>':'')+history+timingContext(context)+'</aside>';
 }
 function poissonEstimate(row={}){
  // Frozen V5 rows predate blending, or retain the untouched statistical base.
  const base=row.calibration_base||row;
  const p=base.statistical_pct;
  const probability=Number.isFinite(p)&&p>=0&&p<=100?p:null;
  const spec=row.spec||{};
  const binary=spec.kind==='result'||spec.kind==='btts'||isCombo(row)||
   (['total','asian_total'].includes(spec.kind)&&Number.isFinite(spec.line)&&spec.line%1===.5);
  const fair=Number.isFinite(base.fair_odd)&&base.fair_odd>=1?base.fair_odd:
   binary&&probability>0?100/probability:null;
  return {probability,fair};
 }
 function studyObservation(row,study,ociOnly=false){
  if(!study||!['recent','cached_stable','stale','missing'].includes(row.quote_status)||(!ociOnly&&priced(row)))return null;
  const start=Date.parse(study.kickoff),now=study.now??Date.now();
  const snapshots=[row.cached_quote,row.study_flow];
  if(!priced(row)&&Number.isFinite(row.last_odd))snapshots.push({...row,odd:row.last_odd});
  let last=null,time=-Infinity;
  for(const source of snapshots){
   if(!source||!Number.isFinite(source.odd)||source.odd<=1||!(source.bookmaker_id||source.bookmaker)||(ociOnly&&!Number.isFinite(source.oci)))continue;
   const seen=Date.parse(source.quote_observed_at),provider=Date.parse(source.provider_updated_at);
   if(!Number.isFinite(seen)||!Number.isFinite(provider)||!Number.isFinite(start)||provider>seen||seen>=start||seen>now||seen<=time)continue;
   last=source;time=seen;
  }
  if(!last)return null;
  const first=last.first_odd,delta=Number.isFinite(first)&&first>1?100*(last.odd/first-1):null;
  return {...last,delta_pct:Number.isFinite(last.delta_pct)?last.delta_pct:delta};
 }
 function studyHeader(study){
  const teams=['home','away'].map(side=>{
   const fields=[];
   for(const [period,tag] of [['ft','FT'],['ht','PT']]){
    const p=study.profiles?.[side]?.['for_'+period];
    if(Number.isFinite(p?.xg_mean)&&p.xg_mean>=0&&p.xg_sample>0)fields.push('xG medio '+tag+' '+num(p.xg_mean,2)+' ('+num(p.xg_sample,0)+' gare)');
    if(Number.isFinite(p?.shots_mean)&&p.shots_mean>=0)fields.push('tiri in porta medi '+tag+' '+num(p.shots_mean,1));
   }
   return fields.length?'<p><b>'+esc(study[side]||(side==='home'?'Casa':'Ospite'))+'</b> · '+esc(fields.join(' · '))+'</p>':'';
  }).join('');
  return '<section class="mx-study-base" aria-label="Analisi Statistica di Base"><h4>Analisi Statistica di Base</h4><span>In osservazione · stime Poisson prematch</span>'+
   (teams||'<p>xG e tiri in porta non disponibili nello storico osservato.</p>')+'</section>';
 }
 // Descriptive display only. Never overwrite the OCI used by policy/selection.
 function displayOci(row={}){
  if(row.prematch_stable===true)return {value:0,label:'Quota stabile · nessun movimento confermato'};
  if(Number.isFinite(row.oci))return {value:row.oci,label:'Movimento delle quote · '+(row.paired_books??'—')+' bookmaker'};
  const m=row.selected_movement||{},start=Date.parse(m.observed_from),end=Date.parse(m.observed_to);
  if(row.paired_books!==1)return {value:null,label:row.paired_books>1?'Consenso multi-book insufficiente':'Servono due rilevazioni confrontabili'};
  const hours=Number.isFinite(row.movement_hours)?row.movement_hours:
   Number.isFinite(row.speed_pp_per_hour)&&row.speed_pp_per_hour>0?Math.abs(m.fair_delta_pp)/row.speed_pp_per_hour:
   m.reversal===false?Math.max((end-start)/3600000,.25):m.fair_delta_pp===0?.25:null;
  if(row.quote_source==='synthetic_combo'||!Number.isFinite(start)||!Number.isFinite(end)||end<=start||
     ![m.fair_delta_pp,m.persistence,hours].every(Number.isFinite)||hours<=0||m.persistence<0||m.persistence>1)
   return {value:null,label:'Storico completo del singolo bookmaker non disponibile'};
  const delta=m.fair_delta_pp;
  const persistenceWeight=row.oci_method==='oci-as-intermediate-persistence-v1'?
   (m.persistence>0&&m.persistence<1?15:0):m.persistence*15;
  const amplitude=Math.min(Math.abs(delta)/4,1)*40+25+Math.min(Math.abs(delta)/hours/1.5,1)*10+persistenceWeight+10/8;
  return {value:Math.abs(delta)<.10?0:amplitude*Math.sign(delta),label:'Singolo bookmaker · consenso multi-book non confermato'};
 }
 function ociHistory(row,helpers={},v7){
  const last=row.last_confirmed_oci;
  if(!Number.isFinite(last?.value)||!Number.isFinite(Date.parse(last.observed_at)))return '';
  const reasons={storico_comparabile_assente:'Rilevazioni confrontabili insufficienti',
   nessuna_maggioranza_multi_operatore:'Consenso tra bookmaker insufficiente'};
  const reason=row.quote_status==='stale'?'Quota scaduta':row.quote_status==='missing'?'Quota non disponibile':reasons[row.oci_unavailable_reason];
  const rebound=v7?.niches?.bookmaker_shift_rebound||[];
  const signal=goalMarket(row)==='gg_ht'&&v7?.status==='published'?
   '<small>Effetto molla · '+(rebound.some(id=>v7.markets?.[id]?.source_market_id===row.id)?'confermato nello snapshot':'assente nello snapshot')+'</small>':'';
  return (!Number.isFinite(row.oci)?'<small>OCI corrente non confermato'+(reason?' · '+esc(reason):'')+'</small>':'')+
   '<small>Ultimo OCI confermato <b>'+signed(last.value)+'</b> · '+
   esc(helpers.timestamp?.(last.observed_at)||last.observed_at)+'</small>'+signal;
 }
 function movementSignal(row,saved=false){
  const delta=(priced(row)||saved)&&Number.isFinite(row.delta_pct)?row.delta_pct:null;
  const first=Date.parse(row.first_observed_at),last=Date.parse(row.quote_observed_at);
  if(delta===0&&Number.isFinite(first)&&first===last&&row.first_odd===row.odd){
   return '<div class="mx-net mx-net-unknown"><b>Prima rilevazione</b><small>In attesa di una seconda osservazione dello stesso bookmaker.</small></div>';
  }
  const magnitude=delta==null?null:Math.abs(delta),level=magnitude==null?'unknown':magnitude>=8?'red':magnitude>=3?'yellow':'green';
  const direction=delta==null?'Storico assente':Math.abs(delta)<.005?'Invariata':delta<0?'Discesa':'Salita';
  const arrow=delta==null?'—':Math.abs(delta)<.005?'→':delta<0?'↓':'↑';
  return '<div class="mx-net mx-net-'+level+'" aria-label="'+esc(direction+(delta==null?'':' '+signed(delta)+'%'))+'"><b>'+arrow+' '+(delta==null?'—':signed(delta)+'%')+'</b><small>'+direction+' · '+(level==='red'?'forte':level==='yellow'?'media':level==='green'?'lieve':'da osservare')+'</small></div>';
 }
 function quoteCell(row,title,helpers={},study=null){
  row=row||{};
  const saved=studyObservation(row,study),flow=saved||row,recent=priced(row);
  const older=study&&!Number.isFinite(flow.oci)?studyObservation(row,study,true):null;
  const pressure=older||flow;
  const band={low:'basso',medium:'intermedio',high:'alto'}[flow.margin_band]||'';
  const base=study?poissonEstimate(row):null;
  const statistics=base?'<span>Poisson prematch <b>'+pct(base.probability)+'</b></span><span>Quota equa Poisson <b>'+num(base.fair,2)+'</b></span>':
   '<span>Statistica <b>'+pct(row.statistical_pct)+'</b></span>';
  const priceLabel=saved?'Ultima osservata':row.quote_status==='cached_stable'?'Quota stabile da cache':recent?'Attuale':Number.isFinite(row.last_odd)?'Ultima quota · da confermare':'Quota non ricevuta';
  const observed=saved?flow.quote_observed_at:flow.provider_updated_at;
  return '<section class="mx-quote-cell"><h4>'+esc(title)+'</h4><div class="mx-quote-pair"><span title="Prima quota reale osservata presso lo stesso operatore; non apertura ufficiale">Prima osservata<b>'+num(flow.first_odd,2)+'</b></span><span>'+priceLabel+'<b>'+num(saved||recent?flow.odd:row.last_odd,2)+'</b></span></div>'+movementSignal(flow,!!saved)+
   '<div class="mx-quote-metrics">'+statistics+'<span>'+(saved||older?'Ultimo OCI osservato':'OCI')+' <b>'+signed(displayOci(pressure).value)+'</b></span><span>Aggio mediano <b>'+pct(flow.margin_pct)+'</b> '+esc(band)+'</span><span>Δ aggio <b>'+signed(flow.margin_delta_pp)+' pt</b></span>'+
   (study&&row.quality&&QUALITY[row.quality]?'<span>'+esc(QUALITY[row.quality])+'</span>':'')+
   (study&&Number.isFinite(flow.speed_pp_per_hour)?'<span>Velocità <b>'+num(flow.speed_pp_per_hour,1)+' pp/ora</b></span>':'')+
   (study&&Number.isFinite(flow.fair_delta_pp)?'<span>Δ fair <b>'+signed(flow.fair_delta_pp)+' pp</b></span>':'')+'</div>'+
   '<small class="mx-quote-source">'+(saved?'Flusso storico · ':!recent?esc(quoteAvailability(row))+' · ':'')+(study&&flow.quote_source==='synthetic_combo'?'Combo stimata · ':'')+esc(flow.bookmaker||'Fonte non disponibile')+(observed?' · '+esc(helpers.timestamp?.(observed)||observed):'')+'</small>'+
   '<small class="mx-quote-consensus">'+esc(displayOci(pressure).label)+' · '+esc(pressure.agreeing_books??0)+'/'+esc(pressure.paired_books??0)+' operatori concordi'+(pressure.oci_previous==null?'':' · prima '+signed(pressure.oci_previous))+(older?' · '+esc(older.bookmaker||older.bookmaker_id)+' · '+esc(helpers.timestamp?.(older.quote_observed_at)||older.quote_observed_at):'')+'</small></section>';
 }
 function quoteBoard(rows,helpers,study=null){
  const find=(kind,period,side,line)=>{
   const exact=r=>r.period===period&&r.spec?.side===side&&(line==null||r.spec.line===line)&&r.spec.extra==null;
   const direct=rows.find(r=>r.spec?.kind===kind&&exact(r));
   if(kind!=='total'||priced(direct))return direct;
   return rows.find(r=>r.spec?.kind==='asian_total'&&exact(r)&&priced(r))||direct;
  };
  const goals=[['total','ht','over',1.5,'Over 1,5 PT'],['total','ft','over',2.5,'Over 2,5 FT'],['total','ft','over',3.5,'Over 3,5 FT'],['btts','ht','yes',null,'GG PT']];
  if(study)goals.splice(1,0,['total','ht','over',2.5,'Over 2,5 PT']);
  const combo=study&&rows.find(isCombo);
  return '<section class="mx-quote-board" aria-label="Quote 1X2 prima osservata e attuale">'+(study?studyHeader(study):'')+'<h4 class="mx-section-title">QUOTE 1 · X · 2 — PRIMA OSSERVATA E ATTUALE</h4><div class="mx-1x2">'+[['1','1 · Casa'],['x','X · Pareggio'],['2','2 · Ospite']].map(([s,l])=>quoteCell(find('result','ft',s),l,helpers,study)).join('')+'</div>'+
   '<details class="mx-more-quotes"><summary>Quote gol e altri indicatori</summary><div class="mx-goal-quotes">'+goals.map(([k,p,s,l,t])=>quoteCell(find(k,p,s,l),t,helpers,study)).join('')+(combo?quoteCell(combo,'Combo Over 0,5 PT + Over 0,5 ST',helpers,study):'')+'</div></details></section>';
 }
 function timingContext(context){
  if(!context)return '';
  const f=context.imbalance,active=context.pt_priority_active;
  const preference=context.preferred_pt_market==='over15_ht'?'Over 1,5 PT':context.preferred_pt_market==='gg_ht'?'Goal PT':'Over 1,5 PT / Goal PT';
  const setting=f?.regime==='balanced'?'Quote bilanciate · distanza 1–2 '+num(f.home_away_odds_gap,2):f?'Favorita '+(f.favorite_venue==='home'?'in casa':'in trasferta')+' · quota '+num(f.favorite_odd,2):'';
  const gaps=['home','away'].map(s=>{const t=context.trends?.[s],g=t?.venue?.ft;return g?.sufficient?'<small>'+(s==='home'?'Casa, in casa':'Ospite, fuori casa')+': media recente '+num(g.recent_mean,2)+' / riferimento '+num(g.reference_mean,2)+' · digiuno '+num(g.drought_matches,0)+' gare</small>':'';}).join('');
  return '<section class="mx-timing"><b>'+(active?'Ordine di valutazione: '+preference:'Confronto dei cinque mercati gol')+'</b>'+(setting?'<small>'+esc(setting)+'</small>':'')+gaps+(f?'<small>OCI 1X2 '+signed(f.oci)+' · aggio '+pct(f.margin_pct)+' · '+esc(f.bookmaker)+'</small>':'')+(active?'<small>Precedenza tra idonei · probabilità invariata.</small>':'')+'</section>';
 }
 function platinumPanel(rows,context,options,elite,best){
  const gold=best?'<details class="mx-method mx-platinum-gold"><summary>Giocata ufficiale Gold v6.1 · '+esc(best.label)+' · '+num(best.odd,2)+'</summary>'+opportunityPanel(rows,context,{...options,suppressPlatinum:true})+'</details>':'<p>Categoria Platinum · distinta dalle giocate Gold ufficiali.</p>';
  const caption=elite.platinum.rules_revision===RULES_REVISION?'Valore e flusso':'Quota &amp; Flusso Dominante';
  return '<aside class="mx-best mx-best-platinum" data-platinum-id="'+esc(elite.id)+'"><span class="mx-section-title">Top Pick · <strong>PLATINUM</strong> · '+caption+'</span><h4>'+esc(elite.label)+'</h4><span class="mx-badge mx-platinum"><strong>PLATINUM</strong></span><div class="mx-best-price">'+num(elite.odd,2)+'<small>'+(elite.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA OSSERVATA')+' · '+esc(elite.bookmaker)+'</small></div><dl>'+[
   ['OCI / variazione quota',signed(elite.oci)+' / '+signed(elite.delta_pct)+'%'],
   ['Aggio / operatori concordi',pct(elite.selected_margin_pct)+' / '+elite.agreeing_books+'/'+elite.paired_books],
   ['Persistenza del movimento',pct(100*elite.selected_movement.persistence)],
   ['Probabilità fair mercato',pct(operatorFair(elite))],
   ['Statistica',Number.isFinite(elite.statistical_pct)?pct(elite.statistical_pct):'Non disponibile']
  ].map(([label,value])=>'<div><dt>'+esc(label)+'</dt><dd>'+esc(value)+'</dd></div>').join('')+'</dl>'+gold+'<p>'+extraGoalText(rows)+'</p></aside>';
 }
 function opportunityPanel(rows,context,options={}){
  const best=bestOpportunity(rows,options),pricedCount=rows.filter(r=>priced(r)).length;
  const elite=!options.suppressPlatinum&&platinumSignals(rows,options)[0];
  if(elite&&(!best||!elite.platinum.secondary_to_gold))return platinumPanel(rows,context,options,elite,best);
  const checked=rows.filter(r=>goalMarket(r)!==null&&acceptedPolicy(r.opportunity)),counts={};
  for(const r of checked)for(const code of (!priced(r)?['stale']:r.opportunity.reasons||[]))counts[code]=(counts[code]||0)+1;
  const blockers=['oci_negative','odds_rising','suspicious_divergence','margin','margin_missing','stale'];
  const exclusions=Object.entries(counts).sort((a,b)=>{const priority=k=>blockers.includes(k)?blockers.indexOf(k):blockers.length;return priority(a[0])-priority(b[0])||b[1]-a[1]}).slice(0,3).map(([k,n])=>n+' '+(REASONS[k]||k)).join(' · ');
  const rulePolicy=best?.opportunity.policy_id||(checked.length&&checked.every(r=>r.opportunity.policy_id===checked[0].opportunity.policy_id)?checked[0].opportunity.policy_id:POLICY);
  const archive=rulePolicy===POLICY?'':'Proposta archiviata · '+esc(rulePolicy)+'<br>';
  const last=options.lastProposal;
  const history=!best&&last&&Number.isFinite(last.odd)?'<section class="mx-timing"><b>Ultima proposta pubblicata · '+esc(last.label||last.market_id)+'</b><strong>'+num(last.odd,2)+'</strong><small>'+esc(last.bookmaker||'')+' · '+esc(last.provider_updated_at||last.published_at||'')+'</small><small>'+(last.status==='withdrawn'?'Proposta ritirata: i dati aggiornati non superano più i criteri.':'In attesa di aggiornamento: conservata come riferimento, quota e validità attuali da confermare.')+'</small></section>':'';
  const ideas=options.monitoring?.studio_ideas;
  if(!best&&!options.archived&&!options.monitoringClosed&&Array.isArray(ideas)&&ideas.length&&
     (options.now??Date.now())<Date.parse(options.kickoff)){
   return '<aside class="mx-best mx-best-market"><span class="mx-section-title">RADAR BLU · MONITORAGGIO</span><span class="mx-badge mx-market-signal">SOLO STUDIO</span>'+studyIdeaPanel(ideas)+history+'</aside>';
  }
  const signals=!best&&!Array.isArray(ideas)?marketSignals(rows,options):[],watch=signals[0];
  if(watch)return '<aside class="mx-best mx-best-market" data-market-signal-id="'+esc(watch.id)+'"><span class="mx-section-title">RADAR DI MERCATO</span><h4>'+esc(watch.label)+'</h4><span class="mx-badge mx-market-signal">STUDIO / ALTA ATTENZIONE</span><p>'+MARKET_SIGNAL_TEXT+'</p><div class="mx-best-price">'+num(watch.odd,2)+'<small>'+(watch.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA OSSERVATA')+' · '+esc(watch.bookmaker)+'</small></div><dl>'+[
   ['Persistenza del movimento',pct(100*watch.selected_movement.persistence)],
   ['OCI / variazione quota',signed(watch.oci)+' / '+signed(watch.delta_pct)+'%'],
   ['Aggio / operatori concordi',pct(watch.selected_margin_pct)+' / '+watch.agreeing_books+'/'+watch.paired_books]
  ].map(([label,value])=>'<div><dt>'+esc(label)+'</dt><dd>'+esc(value)+'</dd></div>').join('')+'</dl><p><b>Solo studio · esclusa dalle giocate Gold ufficiali.</b></p>'+history+timingContext(context)+'<p>'+extraGoalText(rows)+'</p><small>'+signals.length+' '+(signals.length===1?'mercato in monitoraggio':'mercati in monitoraggio')+' · 0 proposte Gold</small></aside>';
  const waiting=!best&&monitoringRows(rows,options);
  if(waiting?.length||(!best&&options.monitoring?.radar_state&&!options.archived&&!options.monitoringClosed))return monitoringPanel(rows,options.monitoring,history,context,options);
  if(!best)return '<aside class="mx-best mx-best-empty"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+(checked.length&&checked.every(r=>!priced(r))?'Quote da aggiornare':checked.length?'Nessuna proposta supera i criteri':'Valutazione da aggiornare')+'</h4>'+history+timingContext(context)+'<p>'+archive+extraGoalText(rows)+'</p><p>'+esc(exclusions||'Dati insufficienti')+'</p><small>'+rows.length+' esiti · '+pricedCount+' quote utilizzabili</small></aside>';
  const d=best.opportunity,signal=d.signal,tag=signal==='convergence'?'mature':signal==='contrarian'?'contrarian':'value_watch',displayLabel=goalMarket(best)==='gg_ht'?'Goal PT':best.label;
  const stability=d.stability?.status==='retained'?'<p class="mx-stability"><b>Proposta mantenuta in tolleranza</b><small>Piccola oscillazione dei valori. Validità massima fino alle '+esc(new Date(d.stability.expires_at).toLocaleTimeString('it-IT',{timeZone:'Europe/Rome',hour:'2-digit',minute:'2-digit'}))+' con quote aggiornate e limiti rispettati.</small></p>':'';
  return '<aside class="mx-best mx-best-stable" data-opportunity-id="'+esc(best.id)+'" data-opportunity-odd="'+best.odd+'" data-opportunity-max-odd="'+maxOdd(best)+'" data-opportunity-probability="'+best.statistical_pct+'"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+esc(displayLabel)+'</h4><div class="mx-best-price">'+num(best.odd,2)+'<small>'+(best.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA ATTUALE')+' · '+esc(best.bookmaker||'')+'</small></div><span class="mx-badge mx-'+tag+'">'+esc(d.label)+'</span>'+stability+
   '<dl><div><dt>Statistica pura</dt><dd>'+pct(best.statistical_pct)+' · minimo '+pct(d.min_probability_pct)+'</dd></div><div><dt>Quota equa del modello</dt><dd>'+num(best.fair_odd,2)+'</dd></div><div><dt>OCI · precedente → attuale</dt><dd>'+signed(best.oci_previous)+' → '+signed(best.oci)+'</dd></div><div><dt>Aggio del bookmaker · variazione</dt><dd>'+pct(best.selected_margin_pct)+' · '+signed(best.selected_margin_delta_pp)+' pt</dd></div></dl>'+
   firstHalfEvidence(best)+timingContext(context)+'<p>'+archive+extraGoalText(rows)+'</p><small>'+new Set(checked.filter(r=>r.opportunity.eligible).map(goalMarket)).size+' mercati idonei</small></aside>';
 }
 function firstHalfEvidence(row){
  const e=row.ht_evidence;if(!e||!['over15_ht','over25_ht','gg_ht'].includes(goalMarket(row)))return '';
  return '<p class="mx-timing mx-first-half"><b>Riscontri del primo tempo</b>'+['home','away'].map(side=>{const t=e.teams?.[side]||{};return '<small>'+(side==='home'?'Casa':'Ospite')+': '+num(t.sample,0)+' PT · gol fatti/subiti '+num(t.scored_mean,2)+' / '+num(t.conceded_mean,2)+' · xG PT '+(t.xg_for_sample?num(t.xg_for_mean,2)+' su '+num(t.xg_for_sample,0)+' gare':'non disponibili')+'</small>';}).join('')+'</p>';
 }
 function price(row){
  if(!priced(row))return '<span class="mx-missing">'+(isCombo(row)?'Quota bookmaker non fornita':esc(quoteAvailability(row)))+'</span>'+(isCombo(row)?'<small>Equa calcolata dal modello</small>':'');
  if(row.quote_source==='synthetic_combo')return '<span class="mx-price"><b>'+num(row.odd,2)+'</b></span><small>Quota sintetica PT × ST · '+esc(row.bookmaker)+'</small><small>Stima; quota Combo non offerta dal book</small>';
  const arrow=row.odd<row.first_odd?'↓':row.odd>row.first_odd?'↑':'→';
  return (row.quote_status==='cached_stable'?'<small>Quota stabile da cache</small>':'')+'<span class="mx-price">'+num(row.first_odd,2)+' <i>'+arrow+'</i> <b>'+num(row.odd,2)+'</b></span><small>'+esc(row.bookmaker)+' · '+signed(row.delta_pct)+'%</small>';
 }
 function marketRow(row,helpers={},options={}){
  const asian=['asian_total','handicap','dnb'].includes(row.spec?.kind);
  const d=row.opportunity,range=goalMarket(row)&&acceptedPolicy(d)?'<small title="'+esc('Quote '+num(minOdd(row),2)+'–'+num(maxOdd(row),2)+' · soglia '+pct(d.min_probability_pct))+'">'+(options.platinum?'Gold: ':'')+(d.eligible?'Idoneo':'Scartato · '+esc((d.reasons||[]).map(code=>REASONS[code]||code).join(' · ')||'criteri non superati'))+'</small>':'';
  const tone=isCombo(row)?comboTone(row):null;
  const base=options.study?poissonEstimate(row):null;
  const observed=studyObservation(row,options.study);
  const stats=base?'<strong>'+pct(base.probability)+'</strong><small>Poisson prematch · Quota equa '+num(base.fair,2)+' · '+esc(QUALITY[row.quality]||'Stima archiviata')+'</small>':
   '<strong>'+pct(row.statistical_pct)+'</strong><small>Equa '+num(row.fair_odd,2)+' · '+esc(QUALITY[row.quality]||'Stima archiviata')+'</small>';
  const prices=observed?'<strong>'+num(observed.odd,2)+'</strong><small>Ultima osservata · prima osservata '+num(observed.first_odd,2)+' · '+signed(observed.delta_pct)+'%</small>':price(row);
  const priceTime=observed?.quote_observed_at||row.provider_updated_at;
  const reading=options.study&&!Number.isFinite((observed||row).oci)?studyObservation(row,options.study,true):null;
  const flow=reading||observed||row;
  const comboReference=isCombo(row)&&row.probability_model!==V7;
  const studyFlowLabel=reading||observed?'Ultimo OCI osservato · '+esc(flow.bookmaker||flow.bookmaker_id)+' · '+esc(helpers.timestamp?.(flow.quote_observed_at)||flow.quote_observed_at):'OCI del mercato';
  return '<div class="mx-market-row" role="row"><div class="mx-market-name" role="cell"><b>'+esc(row.label)+'</b>'+(isCombo(row)?'<span class="mx-badge mx-combo-'+tone+'">'+(tone==='active'?'Combo Attiva':tone==='near'?'Sotto soglia · Vicina':row.combo?.policy_id===POLICY?'Sotto soglia':'Dati assenti')+'</span>'+(!comboActive(row)?'<small>'+esc(comboReasons(row))+'</small>':''):options.platinum?'<span class="mx-badge mx-platinum"><strong>PLATINUM</strong></span>':options.marketSignal?'<span class="mx-badge mx-market-signal" title="'+MARKET_SIGNAL_TEXT+'">Radar di Mercato · Solo studio</span>':badge(row))+range+'</div>'+
   '<div role="cell" data-title="Statistica">'+stats+(asian?'<small>Rimborsi inclusi nel valore</small>':'')+'</div>'+
   '<div role="cell" data-title="Prima → attuale">'+prices+(priceTime?'<small>'+esc(helpers.timestamp?.(priceTime)||priceTime)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="OCI"><b>'+signed(comboReference&&!options.study?row.combo?.over25_ft_oci:displayOci(flow).value)+'</b><small>'+esc(displayOci(flow).label)+' · '+(options.study?studyFlowLabel+' · '+esc(flow.agreeing_books??0)+'/'+esc(flow.paired_books??0)+' operatori concordi'+(comboReference?' · Rif. Over 2,5 FT '+signed(row.combo?.over25_ft_oci):''):comboReference?'Riferimento Over 2,5 FT':esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi')+'</small>'+ociHistory(row,helpers,options.v7)+'</div>'+
   '<div role="cell" data-title="Aggio"><b>'+pct(row.selected_margin_pct)+'</b><small>'+(row.quote_source==='synthetic_combo'?'Massimo sui singoli PT/ST · '+(row.quote_provenance?.component_margins_pct||[]).map(pct).join(' / '):isCombo(row)&&!priced(row)?'Richiede una quota bookmaker':'Book selezionato · Δ '+signed(row.selected_margin_delta_pp)+' pt')+'</small>'+(!isCombo(row)?'<small>Mediana '+pct(row.margin_pct)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="Fair mercato"><b>'+pct(operatorFair(row))+'</b><small>Probabilità senza aggio dello stesso operatore</small></div></div>';
 }
 function teamProfiles(m){
  const a=m.multimarket||{},rates=publishedView(m)?.v7?.rates||a.rates;
  return '<div class="mx-teams">'+['home','away'].map(side=>{
   const ft=a.profiles?.[side]?.for_ft||{},def=a.profiles?.[side]?.against_ft||{},ht=a.profiles?.[side]?.for_ht||{};
   return '<section><h4>'+esc(m.fixture?.[side]||(side==='home'?'Casa':'Ospite'))+'</h4><dl>'+[
    ['Storico utilizzato',num(ft.sample,0)+' FT · '+num(ht.sample,0)+' PT'],['Attacco / difesa',num(ft.rate,2)+' / '+num(def.rate,2)],
    ['Gol medi / ultima',num(ft.mean,2)+' / '+num(ft.last,0)],['Media senza ultima',num(ft.without_last,2)],
    ['xG osservati',ft.xg_sample?num(ft.xg_mean,2)+' · '+ft.xg_sample+' gare':'Non disponibili'],
    ['xG primo tempo',ht.xg_sample?num(ht.xg_mean,2)+' · '+ht.xg_sample+' gare':'Non disponibili'],['Gare senza segnare',num(ft.drought,0)],['Gol attesi FT / PT',num(rates?.[side+'_ft'],2)+' / '+num(rates?.[side+'_ht'],2)]
    ].map(([k,v])=>'<div><dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd></div>').join('')+'</dl>'+
    (ft.under_conversion?'<p>Potenziale xG superiore ai gol realizzati.</p>':'')+(ft.peak_reduction>.15?'<p>Goleada recente ridimensionata.</p>':'')+'</section>';
  }).join('')+'</div>';
 }
 function operatorFair(row={}){
  if(Number.isFinite(row.selected_market_fair_pct))return row.selected_market_fair_pct;
  const margin=row.selected_margin_pct;
  return Number.isFinite(row.odd)&&row.odd>1&&Number.isFinite(margin)&&margin>=0?100/row.odd/(1+margin/100):null;
 }
 function observedMeans(a){
  if(a.observed_goal_means?.version==='observed-goal-means-v1')return a.observed_goal_means.teams;
  const teams={};
  for(const side of ['home','away']){
   const profiles=a.profiles?.[side]||{},history=a.inputs?.histories?.[side]||[];
   const paired=history.filter(r=>r.halftime_available===true&&['goals','conceded'].every(k=>['ft','ht'].every(p=>Number.isFinite(r['team_'+k+'_'+p]))&&r['team_'+k+'_ft']>=r['team_'+k+'_ht'])).slice(0,4);
   const mean=k=>paired.length?paired.reduce((n,r)=>n+r['team_'+k+'_ft']-r['team_'+k+'_ht'],0)/paired.length:null;
   teams[side]={ht:{scored:profiles.for_ht?.mean,conceded:profiles.against_ht?.mean,sample:profiles.for_ht?.sample},st:{scored:mean('goals'),conceded:mean('conceded'),sample:paired.length}};
  }
  return teams;
 }
 function goalPanels(m){
  const a=m.multimarket||{},r=a.rates||{},means=observedMeans(a);
  const rate=(s,p)=>p==='ht'?r[s+'_ht']:Number.isFinite(r[s+'_ft'])&&Number.isFinite(r[s+'_ht'])&&r[s+'_ft']>=r[s+'_ht']?r[s+'_ft']-r[s+'_ht']:null;
  const head='<thead><tr><th>Tempo</th><th>'+esc(m.fixture?.home||'Casa')+'</th><th>'+esc(m.fixture?.away||'Ospite')+'</th><th>Totale</th></tr></thead>';
  return '<div class="mx-goal-panels"><section class="mx-goal-panel"><h3>Gol attesi <small>λ · stima del modello</small></h3><table>'+head+'<tbody>'+['ht','st'].map(p=>{const home=rate('home',p),away=rate('away',p);return '<tr><th>'+(p==='ht'?'PT':'ST')+'</th><td>'+num(home,2)+'</td><td>'+num(away,2)+'</td><td>'+num(Number.isFinite(home)&&Number.isFinite(away)?home+away:null,2)+'</td></tr>';}).join('')+'</tbody></table><p>Stima media dei gol nel periodo. Può essere sbagliata; non è il risultato esatto previsto.</p></section>'+
   '<section class="mx-goal-panel"><h3>Medie gol <small>Fatti / subiti per gara</small></h3><table><thead><tr><th>Tempo</th><th>'+esc(m.fixture?.home||'Casa')+'</th><th>'+esc(m.fixture?.away||'Ospite')+'</th></tr></thead><tbody>'+['ht','st'].map(p=>'<tr><th>'+(p==='ht'?'PT':'ST')+'</th>'+['home','away'].map(s=>{const v=means?.[s]?.[p]||{};return '<td>'+num(v.scored,2)+' / '+num(v.conceded,2)+'<small>'+num(v.sample,0)+' gare osservate</small></td>';}).join('')+'</tr>').join('')+'</tbody></table><p>PT = primo tempo; ST = secondo tempo. Medie ST da punteggi PT e FT della stessa gara, a 90 minuti. Un dato mancante resta —.</p></section></div>';
 }
 const MATRIX_SECTIONS={targets:'Obiettivi gol',ft:'Gol FT',ht:'Primo tempo',sh:'Secondo tempo',result:'1 · X · 2',asian:'Asiatici'};
 let dialog=null,modalFixtureId=null,modalSection='targets',bodyOverflow='';
 function releaseModal(instance){
  if(dialog!==instance)return;
  const oldId=modalFixtureId;dialog=null;modalFixtureId=null;instance.remove();document.body.style.overflow=bodyOverflow;
  [...(host?.querySelectorAll('[data-mx-open]')||[])].find(e=>e.dataset.mxOpen===oldId)?.focus();
 }
 function closeModal(){const instance=dialog;if(!instance)return;instance.close();releaseModal(instance);}
 function sectionRows(rows,section){return rows.filter(r=>section==='targets'?goalMarket(r)!==null||isJoint(r.spec):section==='asian'?r.family==='asian':section==='result'?r.spec?.kind==='result':r.period===section&&['total','btts'].includes(r.spec?.kind));}
 // Presentation-only instruments: no scoring, filtering or payload mutation.
 function cockpitTier(m,services={}){
  const history=m._gold_history;
  const v7=publishedView(m);
  if(v7){
   return {name:V7_TIERS[v7.tier],tone:['gold','platinum'].includes(v7.tier)?v7.tier:'studio',
    category:v7.tier,badge:v7.badge,row:v7.primary?.source||null};
  }
  if(history?.version==='gold-history-v1'){
   const row=rowsOf(m).find(r=>r.id===history.proposal_id)||null;
   return history.verified_prematch?{name:history.name||'STUDIO',tone:history.tone||'studio',row}:
    {name:'ARCHIVIO',tone:'studio',row:null};
  }
  const a=m.multimarket||{},rows=rowsOf(m);
  const options={archived:phase(m)!=='upcoming',monitoringClosed:(services.phase||phase)(m)!=='upcoming',kickoff:m.fixture?.kickoff,monitoring:a.matrix?.quote_monitoring};
  if(a.status==='no_prematch_snapshot')return {name:'STUDIO',tone:'studio',row:null};
  const elite=platinumSignals(rows,options)[0],gold=bestOpportunity(rows,options);
  if(elite&&(!gold||!elite.platinum.secondary_to_gold))return {name:'PLATINUM',tone:'platinum',row:elite};
  return gold?{name:'GOLD',tone:'gold',row:gold}:{name:'STUDIO',tone:'studio',row:null};
 }
 function historyProposal(m,services,options){
  const h=m._gold_history;
  if(h?.version!=='gold-history-v1')return opportunityPanel(rowsOf(m),m.multimarket?.matrix?.goal_context,options);
  const tier=cockpitTier(m,services),row=tier.row,market=row&&goalMarket(row);
  const f=root.ArabLive?.fixture?.(m)||m.fixture||{},status=f.live?.status||f.result?.status||f.status_short;
  const outcome=['CANC'].includes(status)?'Annullata': ['PST','ABD','AWD','WO','SUSP','INT'].includes(status)?'Da verificare · '+status:
    market?(services.outcome?.(m,market)||'Esito in attesa'):'Nessuna proposta ufficiale da valutare';
  return '<section class="mx-opportunity"><h3>Proposta prematch conservata</h3>'+
   (row?'<p><b>'+esc(row.label)+'</b> · quota '+num(row.odd,2)+' · '+esc(tier.name)+'</p><p data-live-slot="outcome" data-market="'+esc(market||'')+'">'+esc(outcome)+'</p>':
    '<p>'+(!h.verified_prematch?'Snapshot prematch non verificabile: nessun pronostico ricostruito dopo il risultato.':h.category==='radar'?'Radar Blu: osservazione di mercato, esclusa dalle proposte Gold.':'Solo Studio: nessuna proposta ufficiale salvata.')+'</p>')+
   '<small>Analisi del '+esc(services.timestamp?.(h.captured_at)||h.captured_at||'non disponibile')+'. Quote e probabilità originali, senza ricalcolo.</small></section>';
 }
 function instrument(label,value,min,max,suffix,note){
  const valid=Number.isFinite(value),ratio=valid?Math.max(0,Math.min(1,(value-min)/(max-min))):0;
  const ticks=Array.from({length:21},(_,i)=>'<line x1="24" y1="110" x2="'+(i%5===0?35:30)+'" y2="110" transform="rotate('+i*9+' 110 110)"/>').join('');
  return '<div class="mx-instrument'+(!valid?' mx-instrument-empty':'')+'"><svg viewBox="0 0 220 150" aria-hidden="true"><path class="mx-dial-track" d="M24 110 A86 86 0 0 1 196 110"/><path class="mx-dial-fill" pathLength="100" stroke-dasharray="'+ratio*100+' 100" d="M24 110 A86 86 0 0 1 196 110"/><g class="mx-dial-ticks">'+ticks+'</g>'+(valid?'<g class="mx-needle" transform="rotate('+ratio*180+' 110 110)"><path d="M38 110 L110 107 L110 113 Z"/><circle cx="110" cy="110" r="5"/></g>':'')+'<text x="24" y="132">'+min+'</text><text x="196" y="132" text-anchor="end">'+max+'</text></svg><strong>'+esc(valid?num(value)+(suffix||''):'—')+'</strong><b>'+esc(label)+'</b><small>'+esc(valid||label==='OCI'?note:'Dato non disponibile')+'</small></div>';
 }
 let instrumentId=null;
 const inlineInstruments=new Map();
 function cockpit(m,services={},inline=false){
  const tier=cockpitTier(m,services),rows=rowsOf(m);
  const active=publishedView(m);
  const fid=String(m.fixture?.fixture_id),selectedId=inline?inlineInstruments.get(fid):instrumentId;
  const caption=active?.triad?'Più probabile':'Proposta '+tier.name;
  const studyDefault=!active&&!tier.row&&tier.tone==='studio'?studyDefaultRow(m):null;
  const row=rows.find(r=>r.id===selectedId)||tier.row||studyDefault;
  const statisticalDefault=studyDefault&&studyDefault!==starredStudyRow(m);
  const study=!active&&tier.tone==='studio'?{kickoff:m.fixture?.kickoff}:null;
  const saved=row&&studyObservation(row,study),flow=saved||row||{};
  const probability=row?(study&&!(statisticalDefault&&row===studyDefault)?poissonEstimate(row).probability:row.statistical_pct):null;
  const reading=(priced(row)||saved)?displayOci(flow):{value:null,label:'Quote e storico confrontabile non disponibili'};
  const oci=reading.value,margin=(priced(row)||saved)?flow.selected_margin_pct??flow.margin_pct:null;
  return '<section class="mx-cockpit mx-tone-'+tier.tone+'"><div class="mx-cockpit-heading"><span class="mx-category">'+tier.name+'</span><span>ANALISI PREPARTITA</span></div>'+
   (!inline&&tier.row?'<div class="mx-official-reference" data-official-proposal><strong>'+esc(caption)+'</strong><span>'+esc(tier.row.label)+' · '+num(tier.row.odd,2)+'</span><small>Resta la proposta iniziale mentre esplori altri mercati.</small></div>':'')+'<label class="mx-instrument-select">Mercato visualizzato<select '+(inline?'data-mx-inline-instrument="'+esc(fid)+'"':'data-mx-instrument')+'><option value="">'+(tier.row?esc(caption)+' · '+esc(tier.row.label):studyDefault?(statisticalDefault?'Studio statistico · ':'Miglior spunto · ')+esc(studyDefault.label):'Scegli un mercato da osservare')+'</option>'+rows.map(r=>'<option value="'+esc(r.id)+'" '+(r.id===selectedId?'selected':'')+'>'+esc(r.label)+'</option>').join('')+'</select></label>'+
   (row?'<div class="mx-cockpit-pick"><div><small>'+(row===tier.row?esc(caption.toUpperCase()):row===studyDefault?(statisticalDefault?'STUDIO STATISTICO · VALORE NON CONFERMATO':'MIGLIOR SPUNTO · SOLO STUDIO'):'MERCATO IN OSSERVAZIONE')+'</small><h3>'+esc(row.label)+'</h3></div><div class="mx-cockpit-price"><small>'+(saved?'Prima osservata → ultima osservata':'Prima osservata → quota attuale')+'</small>'+(saved?'<span class="mx-price">'+num(flow.first_odd,2)+' → <b>'+num(flow.odd,2)+'</b></span><small>Storico · da confermare</small>':price(row))+'</div></div>':'<p class="mx-cockpit-empty">Le analisi di studio restano consultabili: scegli il mercato per leggere i suoi indicatori.</p>')+
   (statisticalDefault&&row===studyDefault?'<p class="mx-cockpit-note" data-study-default>Mercato gol con la maggiore probabilità prudente disponibile; a parità, probabilità centrale. Stima disponibile per lo studio. Nessuna stella Radar o proposta Gold.</p>':'')+
   (!inline&&!active?goalPanels(m):'')+
   '<div class="mx-instruments">'+instrument('OCI',oci,-100,100,'',reading.label)+instrument('Probabilità modello',probability,0,100,'%','Stima statistica prematch')+instrument('Aggio',margin,0,20,'%','Più basso = minor margine')+'</div>'+
   (!inline?'<div class="mx-fair-reading"><b>Fair mercato '+pct(operatorFair(row||{}))+'</b><span>Probabilità ricavata dalle quote dello stesso operatore, tolto il suo margine.</span><b>OCI '+signed(oci)+'</b><span>'+esc(reading.label)+'. '+(oci==null?'Movimento non disponibile.':oci>0?'Movimento concorde con questo mercato.':oci<0?'Movimento contrario a questo mercato.':'Nessuna direzione netta.')+' Scala −100 / +100, non probabilità di vittoria.</span><details><summary>Poisson, gol attesi e medie</summary><p>Le medie descrivono le gare osservate. Lambda stima i gol attesi nella partita. Poisson trasforma quei gol attesi in probabilità: se lambda è stimato male, anche le probabilità possono esserlo.</p></details></div>':'')+
   (row?'<div class="mx-cockpit-evidence">'+movementSignal(flow,!!saved)+'<div><small>OPERATORI CONCORDI</small><b>'+esc(flow.agreeing_books??'—')+' / '+esc(flow.paired_books??'—')+'</b></div><div><small>FONTE</small><b>'+esc(flow.bookmaker||'Non disponibile')+'</b><small>'+esc(services.timestamp?.(flow.quote_observed_at)||flow.quote_observed_at||'')+'</small></div></div>':'')+
   '<p class="mx-cockpit-note">OCI e aggio descrivono le quote, non la certezza dell’esito. I quadranti non modificano la probabilità. Aggio oltre 20% e OCI fuori scala conservano il valore numerico.</p></section>';
 }
 const V7_ROLES={lineare:'Lineare',alternativa:'Alternativa',colpaccio:'Colpaccio',studio:'Studio',
  radar_conversione:'Radar Conversione',radar_sorprese:'Radar Sorprese',radar_blu:'Radar Blu',gold:'Gold',platinum:'Platinum'};
 const V7_REASONS={conversione_gg_pt:'GG PT in salita e Over 2,5 FT in calo',
  alternativa_goal_1x2:'Mercato gol valutato dopo le esclusioni 1X2',
  probabilita_e_prezzo:'Probabilità e quota sostengono la selezione',prezzo_e_probabilita_propri:'Quota e probabilità valutate sul singolo mercato',
  quota_alta_e_riscontro_statistico:'Quota alta sostenuta dalla stima statistica',
  stima_prematch:'Stima statistica prima del calcio d’inizio',
  gg_pt_sale_over25_ft_scende:'Movimenti opposti su GG PT e Over 2,5 FT',
  quota_alta_in_salita:'Quota alta in salita, in osservazione'};
 const V7_TIERS={gold:'GOLD',platinum:'PLATINUM',blu:'BLU',studio:'STUDIO',radar:'RADAR BLU'};
 const BADGES={blu:'◆ BLU',gold:'● GOLD',platinum:'★ PLATINUM'};
 const TRIAD={most_probable:'Più probabile',recommended_alternative:'Consigliata',high_odds_shot:'Colpaccio'};
 const MODEL_ESTIMATE='Stima V7 · in attesa di quote bookmaker';
 const quoteStateLabel=(signal,source)=>signal?.quote_state==='model_estimate'?MODEL_ESTIMATE:
  signal?.quote_state==='stable'?(source.first_odd===source.odd?'Stabile da apertura':'Ultima quota osservata · stabile'):
  signal?.quote_state==='moving'?'In movimento':'Quota osservata';
 const V7_HEADLINE='Miglior proposta rispetto alla quota';
 const v7Estimate=v=>pct(Number.isFinite(v)?100*v:null);
 function v7Snapshot(m,now){
  const v7=m.multimarket?.matrix?.v7;
  const start=Date.parse(m.fixture?.kickoff),observed=Date.parse(v7?.captured_at);
  return v7?.version===V7&&Number.isFinite(observed)&&observed<=now&&observed<start&&
   Date.parse(v7.kickoff)===start&&String(m.fixture?.fixture_id)===v7.fixture_id?v7:null;
 }
 function publishedView(m,now=Date.now()){
  const matrix=m.multimarket?.matrix;
  if(matrix?.publication_model!==V7&&matrix?.v7?.status!=='published')return null;
  const v7=v7Snapshot(m,now);
  if(v7?.status!=='published'||m._gold_history?.verified_prematch===false){
   return {tier:'studio',triad:m.triad?Object.fromEntries(Object.keys(TRIAD).map(role=>[role,null])):null};
  }
  const sources=new Map(rowsOf(m).map(row=>[row.id,row]));
  const kickoff=Date.parse(v7.kickoff),archived=kickoff<=now;
  const pick=signal=>{
   const record=v7.markets?.[signal?.market_id];
   const source=sources.get(signal?.source_market_id);
   if(!record||!source||source.probability_model!==V7||record.source_market_id!==source.id||
      !Number.isFinite(record.calibrated)||!(record.calibrated>=0&&record.calibrated<=1))return null;
   if(signal.quote_state==='model_estimate'){
    if(signal.tier!=='BLU'||signal.study_only!==true||signal.odd!==null||record.odd!==null||
       !Number.isFinite(record.model_fair_odd)||record.model_fair_odd<=1||
       record.model_fair_odd!==signal.model_fair_odd||signal.probability!==record.calibrated)return null;
    return {record,source,signal,reasons:signal.reasons||[]};
   }
   if(!priced(source)||source.quote_executable!==true||record.odd!==source.odd||signal.odd!==source.odd||
      !Number.isFinite(source.odd)||source.odd<=1||source.prematch_stable&&signal.tier!=='BLU')return null;
   const at=Date.parse(source.provider_updated_at),seen=Date.parse(source.quote_observed_at);
   if(!(at<=seen&&seen<=Date.parse(v7.captured_at)))return null;
   if(!archived){
    if(now-at>PREMATCH_WINDOW_MS)return null;
    const cached=source.quote_status==='cached_stable',expiry=Date.parse(source.cache_expires_at);
    const current=cached?now<=expiry:now-at<=(kickoff-now<=10800000?3600000:21600000);
    if(!current){
     if(signal.tier!=='BLU')return null;
     return {record,source,signal:{...signal,quote_state:'stable'},reasons:signal.reasons||[]};
    }
   }
   return {record,source,signal,reasons:signal.reasons||[]};
  };
  if(v7.triad){
   const triad=Object.fromEntries(Object.keys(TRIAD).map(role=>[role,pick(v7.triad[role])]));
   const selected=Object.values(triad).filter(Boolean);
   const tier=['platinum','gold','blu'].find(level=>selected.some(p=>p.signal.tier===level.toUpperCase()))
    ||(v7.tier==='BLU'?'blu':'studio');
   return {v7,triad,tier,badge:BADGES[tier],primary:triad.most_probable};
  }
  const primary=pick(v7.signals?.primary),recommended=pick(v7.signals?.consigliata),longshot=pick(v7.signals?.colpaccio);
  const tier=(primary||v7.tier==='radar')&&Object.hasOwn(V7_TIERS,v7.tier)?v7.tier:'studio';
  return {v7,tier,primary,recommended:primary?recommended:null,longshot};
 }
 function levelBadge(tier,badge){
  return badge&&Object.hasOwn(BADGES,tier)?'<span class="mx-level" data-tier="'+tier+'">'+esc(badge)+'</span>':'';
 }
 function matchBadge(tier){
  return levelBadge(tier.category,tier.badge)||'<span class="mx-category'+(tier.tone==='platinum'?' mx-tier-label':'')+'">'+esc(tier.name)+'</span>';
 }
 function triadPanel(view,compact=false){
  const saved=view.v7?.triad;
  if(!Object.values(view.triad).some(Boolean)){
   const message=Object.values(saved||{}).some(Boolean)?'Quota da aggiornare':'Nessuna proposta prematch disponibile.';
   return '<p class="mx-v7-empty">'+message+'</p>';
  }
  return '<ul class="mx-triad'+(compact?' mx-triad-compact':'')+'" aria-label="Triade prematch V7">'+
   Object.entries(TRIAD).map(([role,title])=>{
    const p=view.triad[role],s=p?.signal;
    const estimate=s?.quote_state==='model_estimate';
    const badge=s&&levelBadge(String(s.tier).toLowerCase(),s.badge);
    return '<li data-v7-slot="'+role+'"><div class="mx-triad-heading"><span>'+title+'</span>'+(badge||'')+'</div>'+
     (p?'<div class="mx-triad-market"><b>'+esc(p.record.label)+'</b>'+(estimate?'':'<strong>'+num(p.record.odd,2)+'</strong>')+'</div>'+
      '<div class="mx-triad-meta"><span>'+esc(s.horizon_label)+'</span><span>'+v7Estimate(p.record.calibrated)+' V7</span>'+
      (Number.isFinite(p.record.model_fair_odd)?'<span>Quota equa '+num(p.record.model_fair_odd,2)+'</span>':'')+
      (s.quote_state?'<span>'+esc(quoteStateLabel(s,p.source))+'</span>':'')+
      (Number.isFinite(s.ev)?'<span>EV '+signed(100*s.ev)+'%</span>':'')+
      (s.niche_label?'<span class="mx-niche">'+esc(s.niche_label)+'</span>':'')+'</div>':
      '<small>'+(saved?.[role]?'Quota da aggiornare':'Nessun mercato idoneo')+'</small>')+'</li>';
   }).join('')+'</ul>';
 }
 function modelReferences(v7){
  const references=Object.values(v7?.markets||{}).filter(r=>r.odd==null&&Array.isArray(r.reference_legs)&&
   r.reference_legs.length===2&&Number.isFinite(r.model_fair_odd));
  if(!references.length)return '';
  return '<section class="mx-model-references"><h3>Quote eque delle combo</h3>'+references.map(r=>
   '<p><b>'+esc(r.label)+'</b> · quota equa del modello '+num(r.model_fair_odd,2)+
   ' <small>Quote rilevate '+r.reference_legs.map(leg=>esc(leg.label)+' '+num(leg.odd,2)).join(' · ')+'</small></p>').join('')+'</section>';
 }
 function publishedPanel(m,view,services,detailsOpen){
  const {v7,primary,recommended,longshot}=view;
  const item=(pick,title,reason,role)=>pick?'<li data-v7-role="'+role+'"><span>'+title+'</span><b>'+esc(pick.record.label)+'</b>'+
   '<strong>'+num(pick.record.odd,2)+' <small>quota</small></strong><small>'+v7Estimate(pick.record.calibrated)+' · '+reason+'</small></li>':'';
  const rows=item(primary,V7_HEADLINE,primary?.reasons.includes('platinum_flusso')?'Platinum su criterio di flusso':
   view.tier==='gold'?'Supera i criteri Gold':view.tier==='platinum'?'Supera i criteri Platinum':'Stima V7 · '+V7_TIERS[view.tier], 'primary')+
   item(recommended,'Consigliata','Alternativa in studio a quota superiore','consigliata')+
   item(longshot,'Colpaccio','Quota alta · esito meno probabile','colpaccio');
  const record=primary?.record,source=primary?.source;
  const metrics=record&&Number.isFinite(record.odd)?'<div class="mx-v7-odds"><span>Prima → ultima quota <b>'+num(record.quote?.first_odd,2)+' → '+num(record.odd,2)+'</b></span>'+
   '<span>Movimento quota <b>'+pct(source.delta_pct)+'</b></span><span>OCI <b>'+signed(record.features?.oci)+'</b></span><span>Aggio <b>'+pct(record.features?.margin_pct)+'</b></span>'+
   '<span>Poisson originale <b>'+v7Estimate(record.raw)+'</b></span><span>Calibrata V7 <b>'+v7Estimate(record.calibrated)+'</b></span>'+
   '<span>Fair mercato <b>'+v7Estimate(record.fair)+'</b></span><span>Quota '+(source.quote_status==='cached_stable'?'da cache':'rilevata')+
   '<b>'+esc(record.quote?.bookmaker||'—')+'</b></span></div>':'';
  const calibration=v7?.calibration==='bounded_update_applied'?'Correzione verificata nel ciclo V7':'Peso di riferimento V7';
  const technical=v7?'<details data-v7-details'+(detailsOpen?' open':'')+'><summary>Quote, probabilità e gol attesi</summary>'+metrics+
   '<p>'+calibration+' · '+(v7.data_status==='team_history'?'Storico squadre disponibile':'Dati limitati')+'</p>'+
   goalPanels({fixture:m.fixture,multimarket:{rates:v7.rates,observed_goal_means:{version:'observed-goal-means-v1',teams:v7.means}}})+
   modelReferences(v7)+'</details>':'';
  return '<section class="mx-v7-panel mx-v7-published" aria-label="Selezioni prematch V7"><header><small>MOTORE V7 · PREMATCH</small><small>'+
   esc(services.timestamp?.(v7?.captured_at)||'')+'</small></header>'+
   (view.triad?triadPanel(view):
    (!primary?'<p class="mx-v7-empty">Nessuna proposta principale con quota verificata e criteri soddisfatti.</p>':'')+
    (rows?'<ul>'+rows+'</ul>':''))+technical+'</section>';
 }
 function v7Panel(m,services={},compact=false){
  const v7=m.multimarket?.matrix?.v7;
  const start=Date.parse(m.fixture?.kickoff), observed=Date.parse(v7?.captured_at);
  if(v7?.version!==V7||v7.status!=='shadow'||start>Date.now()||!Number.isFinite(observed)||!(observed<start)||
     Date.parse(v7.kickoff)!==start||String(m.fixture?.fixture_id)!==v7.fixture_id)return '';
  const sources=new Map(rowsOf(m).map(r=>[r.id,r]));
  const estimate=v=>pct(Number.isFinite(v)?100*v:null);
  const model=(key,signal)=>{
   if(!signal?.market_id)return null;
   if(['gold','platinum','radar_blu'].includes(key)){
    const r=sources.get(signal.market_id);
    return r?{label:r.label,odd:r.odd,calibrated:Number.isFinite(r.statistical_pct)?r.statistical_pct/100:null}:null;
   }
   return v7.markets?.[signal.market_id];
  };
  const signal=(key)=>v7.signals?.[key];
  const line=model('lineare',signal('lineare'));
  if(compact)return line?'<small class="mx-v7-compact"><b>Archivio V7 · Lineare</b> '+esc(line.label)+' · '+estimate(line.calibrated)+'</small>':'';
  const rows=Object.entries(V7_ROLES).map(([key,name])=>{
   const choice=signal(key),row=model(key,choice);
   if(!row)return '';
   const reasons=(choice.reasons||[]).map(reason=>V7_REASONS[reason]).filter(Boolean);
   return '<li><span>'+name+'</span><b>'+esc(row.label)+'</b><strong>'+estimate(row.calibrated)+'</strong>'+
    (Number.isFinite(row.odd)?'<small>Quota osservata '+num(row.odd,2)+'</small>':'')+
    (reasons.length?'<small>'+esc(reasons.join(' · '))+'</small>':'')+'</li>';
  }).join('');
  const main=line||model('studio',signal('studio'));
  const quote=main?.quote;
  const movement=main?.features;
  const calibration=v7.calibration==='bounded_update_applied'?'Correzione verificata nel ciclo V7':'Peso di riferimento V7';
  const details=main?'<div class="mx-v7-odds"><span>Prima → ultima quota <b>'+num(quote?.first_odd,2)+' → '+num(main.odd,2)+'</b></span><span>Movimento <b>'+signed(movement?.delta_pct)+'</b></span><span>OCI <b>'+signed(movement?.oci)+'</b></span><span>Aggio <b>'+pct(movement?.margin_pct)+'</b></span></div>'+
   '<div class="mx-v7-odds"><span>Poisson originale <b>'+estimate(main.raw)+'</b></span><span>Calibrata V7 <b>'+estimate(main.calibrated)+'</b></span><span>Fair mercato <b>'+estimate(main.fair)+'</b></span><span>Quota <b>'+(!quote?'Da acquisire':quote.quote_status==='cached_stable'?'Da cache':'Rilevata prematch')+'</b></span></div>':'';
  return '<section class="mx-v7-panel" aria-label="Archivio V7"><header><div><small>ARABSNIPER V7 · ARCHIVIO</small><h3>Proposte prematch</h3></div><small>'+esc(services.timestamp?.(v7.captured_at)||'Rilevazione prematch')+'</small></header>'+
   (rows?'<ul>'+rows+'</ul>':'<p>Dati insufficienti per una proposta V7.</p>')+
   '<details><summary>Probabilità, quote e gol attesi V7</summary>'+details+
   '<p>'+calibration+' · '+(v7.market_projection?'Mercati fair disponibili':'Stima da statistiche squadra')+'</p>'+
   goalPanels({...m,multimarket:{rates:v7.rates,observed_goal_means:{teams:v7.means}}})+'</details></section>';
 }
 function matrixDetail(m,rows,services){
  const tierOptions={archived:(services.phase||phase)(m)!=='upcoming',kickoff:m.fixture?.kickoff,monitoring:m.multimarket?.matrix?.quote_monitoring};
  const signals=new Set(marketSignals(rowsOf(m),tierOptions).map(r=>r.id));
  const platinum=new Set(platinumSignals(rowsOf(m),tierOptions).map(r=>r.id));
  const active=publishedView(m),study=!active&&!platinum.size&&!bestOpportunity(rowsOf(m),tierOptions)?{kickoff:m.fixture?.kickoff}:null;
  const teams=active?.v7?{...m,multimarket:{...m.multimarket,rates:active.v7.rates}}:m;
  return '<div class="mx-detail"><p class="mx-explanation">'+rows.length+' esiti in questa vista. Quote, OCI e aggio della stessa linea e periodo.</p>'+
   '<div class="mx-table" role="table" aria-label="Matrice delle opportunità"><div class="mx-table-head" role="row">'+['Mercato','Statistica','Prima → attuale','OCI','Aggio','Fair mercato'].map(t=>'<div role="columnheader">'+t+'</div>').join('')+'</div>'+rows.map(r=>marketRow(r,services,{study,v7:active?.v7,marketSignal:signals.has(r.id),platinum:platinum.has(r.id)})).join('')+'</div>'+
   '<details class="mx-method"><summary>Statistiche delle squadre</summary>'+teamProfiles(teams)+'</details>'+
   '<details class="mx-method"><summary>Come leggere i valori</summary><p>Aggio: competitività dei prezzi, non misura di liquidità. OCI: movimento delle quote, non volume di denaro. La sensibilità prudente non è un intervallo di confidenza. Il valore asiatico include rimborsi e mezzi esiti.</p></details></div>';
 }
 function updateModal(resetScroll=false){
  if(!dialog)return;
  const m=(payload?.matches||[]).find(x=>String(x.fixture?.fixture_id)===modalFixtureId);
  if(!m){closeModal();return;}
  const focused=dialog.contains(document.activeElement)?document.activeElement?.dataset?.mxSection:null;
  const closeFocused=document.activeElement?.hasAttribute('data-mx-close');
  const instrumentFocused=document.activeElement?.hasAttribute('data-mx-instrument');
  const active=publishedView(m);
  const advancedOpen=!!dialog.querySelector('[data-v7-advanced]')?.open;
  const detailsOpen=!!dialog.querySelector('[data-v7-details]')?.open;
  const scroll=resetScroll?0:dialog.querySelector('.mx-modal-scroll')?.scrollTop||0;
  const tier=cockpitTier(m,helpers);dialog.className='mx-modal mx-tone-'+tier.tone;
  const tabs='<nav class="mx-modal-tabs" aria-label="Mercati della matrice">'+Object.entries(MATRIX_SECTIONS).map(([k,l])=>'<button type="button" data-mx-section="'+k+'" aria-pressed="'+(modalSection===k)+'">'+esc(l)+'</button>').join('')+'</nav>';
  const live=m._gold_history?'<div data-live-slot="panel">'+(helpers.livePanel?.(m)||'')+'</div>':'';
  const reference=!active||m._gold_history?historyProposal(m,helpers,{archived:phase(m)!=='upcoming',monitoringClosed:(helpers.phase||phase)(m)!=='upcoming',kickoff:m.fixture?.kickoff,monitoring:m.multimarket?.matrix?.quote_monitoring,lastProposal:m.multimarket?.matrix?.last_proposal}):'';
  const all=rowsOf(m),detailRows=active&&modalSection==='targets'?all.filter(r=>
   goalMarket(r)||isCombo(r)||r.spec?.kind==='btts'&&r.spec.side==='yes'||r.spec?.kind==='total'&&r.period==='ht'&&r.spec.line===3.5&&r.spec.side==='over'):sectionRows(all,modalSection);
  const detail=quoteBoard(all,helpers,!active&&tier.tone==='studio'?{profiles:m.multimarket?.profiles,home:m.fixture?.home,away:m.fixture?.away,kickoff:m.fixture?.kickoff}:null)+
   matrixDetail(m,detailRows,helpers);
  dialog.innerHTML='<header class="mx-modal-header"><div>'+matchBadge(tier)+'<h2 id="mx-modal-title">'+esc(m.fixture?.match)+'</h2><small>'+esc(m.fixture?.country)+' · '+esc(m.fixture?.league)+'</small></div><button type="button" data-mx-close aria-label="Chiudi matrice" autofocus>×</button></header>'+
   (active?'':tabs)+'<div class="mx-modal-scroll">'+(active?publishedPanel(m,active,helpers,detailsOpen)+live+
    '<details class="mx-v7-advanced" data-v7-advanced'+(advancedOpen?' open':'')+'><summary>Analisi completa dei mercati</summary>'+
    tabs+cockpit(m,helpers)+reference+detail+'</details>':
    v7Panel(m,helpers)+cockpit(m,helpers)+live+reference+detail)+
   '<p class="mx-caption">Analisi '+esc(helpers.timestamp?.(active?.v7?.captured_at||m.multimarket?.captured_at)||m.multimarket?.captured_at||'non disponibile')+' · Modello '+esc(active?V7:m.multimarket?.model_id||'non disponibile')+'</p></div>';
  dialog.querySelector('.mx-modal-scroll').scrollTop=scroll;
  if(focused)dialog.querySelector('[data-mx-section="'+focused+'"]').focus();
  else if(closeFocused)dialog.querySelector('[data-mx-close]').focus();
  else if(instrumentFocused)dialog.querySelector('[data-mx-instrument]')?.focus();
 }
 function openModal(id){
  instrumentId=null;
  if(dialog){
   dialog.querySelectorAll('details[open]').forEach(el=>{el.open=false;});
   modalFixtureId=id;modalSection='targets';updateModal(true);return;
  }
  modalFixtureId=id;modalSection='targets';
  dialog=document.createElement('dialog');dialog.className='mx-modal';dialog.setAttribute('aria-labelledby','mx-modal-title');dialog.setAttribute('aria-modal','true');
  document.body.append(dialog);bodyOverflow=document.body.style.overflow;document.body.style.overflow='hidden';
  const instance=dialog;
  dialog.addEventListener('close',()=>releaseModal(instance),{once:true});
  dialog.addEventListener('cancel',e=>{e.preventDefault();closeModal();});
  dialog.addEventListener('click',e=>{
   if(e.target.closest?.('[data-mx-close]')){closeModal();return;}
   const tab=e.target.closest?.('[data-mx-section]');
   if(tab){modalSection=tab.dataset.mxSection;updateModal(!dialog.querySelector('[data-v7-advanced]'));return;}
   if(e.target===dialog){const r=dialog.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)closeModal();}
  });
  dialog.addEventListener('change',e=>{if(e.target.hasAttribute('data-mx-instrument')){instrumentId=e.target.value||null;updateModal();dialog.querySelector('[data-mx-instrument]')?.focus();}});
  dialog.addEventListener('keydown',e=>{
   if(e.key!=='Tab')return;
   const items=[...dialog.querySelectorAll('button:not([disabled]),summary,a[href],input:not([disabled]),select:not([disabled]),[tabindex="0"]')].filter(n=>n.getClientRects().length);
   const first=items[0],last=items.at(-1),active=document.activeElement;
   if(e.shiftKey&&(active===first||!dialog.contains(active))){e.preventDefault();last?.focus();}
   else if(!e.shiftKey&&(active===last||!dialog.contains(active))){e.preventDefault();first?.focus();}
  });
  updateModal();if(dialog){dialog.showModal();dialog.querySelector('[data-mx-close]').focus();}
 }
 function proposalData(m,helpers={}){
  const active=publishedView(m);
  if(active){
   const pick=active.primary;
   if(!pick)return null;
   return {title:active.triad?TRIAD.most_probable:V7_HEADLINE,label:pick.record.label,odd:pick.record.odd,first:pick.record.quote?.first_odd,
    note:quoteStateLabel(pick.signal,pick.source),
    probability:100*pick.record.calibrated,v7:true};
  }
  const tier=cockpitTier(m,helpers),star=!m._gold_history?starredStudyRow(m):null;
  const radar=!m._gold_history?marketSignals(rowsOf(m),{archived:(helpers.phase||phase)(m)!=='upcoming',kickoff:m.fixture?.kickoff})[0]:null;
  const row=tier.row||star||radar||(!m._gold_history?studyDefaultRow(m):null);
  if(!row)return null;
  const starred=tier.tone==='studio'&&(row===star||row===radar);
  const title=tier.row?'Proposta '+tier.name:starred?'Miglior spunto Blu · solo studio':'Miglior proposta Studio · statistica';
  const saved=tier.tone==='studio'?studyObservation(row,{kickoff:m.fixture?.kickoff}):null;
  const odd=saved?.odd??(priced(row)?row.odd:null);
  const note=saved?'Ultima osservata · da confermare':row.quote_source==='synthetic_combo'?'Quota sintetica PT × ST':row.quote_status==='cached_stable'?'Quota da cache':priced(row)?'Quota osservata':'Quota non disponibile';
  return {title,label:row.label,odd,note,probability:row.statistical_pct};
 }
 function compactProposal(m,helpers={}){
  const view=publishedView(m);
  if(view?.triad)return triadPanel(view,true);
  const proposal=proposalData(m,helpers);
  if(!proposal)return '<div class="mx-compact-proposal"><small>Nessuna proposta principale disponibile</small></div>';
  const {title,label,odd,first,note,probability,v7}=proposal;
  return '<section class="mx-compact-proposal" aria-label="Migliore proposta"><div><small>'+esc(title)+'</small><strong>'+esc(label)+'</strong>'+
   (v7?'<small>'+pct(probability)+' · probabilità V7</small>':'')+'</div><div class="mx-compact-proposal-price"><b>'+num(odd,2)+'</b><small>'+esc(note)+'</small>'+
   (v7&&Number.isFinite(first)?'<small>Prima osservata '+num(first,2)+'</small>':'')+'</div></section>';
 }
 function compactQuotes(rows,{kickoff,closed=false}={}){
  const validQuote=value=>Number.isFinite(value)&&value>1?value:null;
  const quotes=[['1','Casa'],['x','Pareggio'],['2','Ospite']].map(([side,label])=>{
   const row=rows.find(r=>r.spec?.kind==='result'&&r.spec.period==='ft'&&r.spec.side===side&&r.spec.line==null&&r.spec.extra==null)||{};
   const saved=studyObservation(row,{kickoff}),source=saved||row,recent=priced(row);
   const first=validQuote(source.first_odd),last=validQuote(saved||recent?source.odd:row.last_odd);
   const firstSeen=Date.parse(source.first_observed_at);
   const single=first===last&&Number.isFinite(firstSeen)&&firstSeen===Date.parse(source.quote_observed_at);
   const delta=(saved||recent)&&first!=null&&last!=null&&!single?Math.round((last/first-1)*1000)/10:null;
   const status=last==null?'':saved?'Ultima':row.quote_status==='cached_stable'?'Cache':recent?'':'Da confermare';
   return {side:side.toUpperCase(),label,first,last,single,delta,status};
  });
  if(quotes.every(({first,last})=>first==null&&last==null))return '';
  return '<div class="mx-compact-quotes" role="group" aria-label="Quote 1X2">'+
   '<div class="mx-compact-quotes-label"><b>1X2</b><span title="Prima quota reale osservata dello stesso bookmaker; non apertura ufficiale">Prima osservata → '+(closed?'Ultima prematch':'Attuale')+'</span></div>'+
   quotes.map(({side,label,first,last,single,delta,status})=>{
    const movement=delta==null?'unknown':delta<0?'down':delta>0?'up':'flat';
    const [arrow,description]={
     unknown:['—',single?'Prima rilevazione':'Variazione non disponibile'],
     down:['↓','Calo'],up:['↑','Rialzo'],flat:['→','Invariata']
    }[movement];
    const change=delta==null?arrow:arrow+' '+signed(delta||0)+'%';
    return '<section aria-label="'+side+' · '+label+'"><div class="mx-compact-quote-line"><b class="mx-compact-side">'+side+'</b><span>'+
     num(first,2)+' → <strong class="mx-compact-price">'+num(last,2)+'</strong></span></div>'+
     '<div class="mx-compact-quote-move"><b data-movement="'+movement+'" title="'+description+'">'+change+'</b>'+
     (status?'<small>'+status+'</small>':'')+'</div></section>';
   }).join('')+'</div>';
 }
 function card(m,helpers){
  const f=m.fixture||{},a=m.multimarket||{},all=rowsOf(m),fid=String(f.fixture_id);
  const noSnapshot=a.status==='no_prematch_snapshot';
  const tier=cockpitTier(m,helpers);
  const elite=tier.tone==='platinum';
  return '<article class="mx-card mx-compact mx-tone-'+tier.tone+(elite?' mx-card-platinum':'')+'" data-live-fixture="'+esc(fid)+'"><header><div>'+matchBadge(tier)+'<time>'+esc(f.time||'')+'</time><small>'+esc(f.country)+' · '+esc(f.league)+'</small>'+'<h3>'+esc(f.match)+'</h3></div><div class="mx-live"><button type="button" class="mx-save" data-mx-save="'+esc(fid)+'" aria-label="Osserva '+esc(f.match)+'" aria-pressed="'+observedFixtures.has(fid)+'">'+(observedFixtures.has(fid)?'★':'☆')+'</button><span data-live-slot="badge">'+(helpers.liveBadge?.(m)||'')+'</span><span data-live-slot="state">'+esc(helpers.stateLabel?.(m)||'')+'</span><span data-live-slot="halftime">'+halfTimeText(helpers.firstHalf?.(m))+'</span></div></header>'+
   compactQuotes(all,{kickoff:f.kickoff,closed:m._gold_history||(helpers.phase||phase)(m)!=='upcoming'})+
   (m._gold_history?'<div data-live-slot="panel">'+(helpers.livePanel?.(m)||'')+'</div>':'')+
   (noSnapshot?'<p class="mx-explanation">Nessuna analisi prematch salvata per questa partita.</p>':compactProposal(m,helpers))+
   (all.length?'<button class="mx-open" type="button" data-mx-open="'+esc(fid)+'" aria-haspopup="dialog">Apri analisi <span>↗</span></button>':'')+'</article>';
 }
 let payload,host,helpers;
 function halfTimeText(half){
  if(!half||half.status==='pending')return '';
  if(!half.score)return 'PT · dato non disponibile';
  return '<b>PT '+esc(half.score)+'</b> · '+(half.status==='complete'?'concluso':half.stale?'ultimo rilevato':'in corso');
 }
 function render(p,h,services={}){
  p=scopePayload(p);payload=p;host=h;helpers=services;
  const all=p.matches||[],rows=all.flatMap(rowsOf),matches=selectMatches(p,state,Date.now(),services.phase||phase);
  const activeMatches=[],inactiveMatches=[];
  for(const m of matches)(inactiveWithoutAnalysis(m)?inactiveMatches:activeMatches).push(m);
  const active=all.some(m=>m.multimarket?.matrix?.publication_model===V7);
  const count=key=>key==='all'?all.length:all.filter(m=>matchingRows(m,{...state,category:key,family:'all',period:'all'},Date.now(),services.phase||phase).length).length;
  const options=(items,chosen)=>Object.entries(items).map(([v,l])=>'<option value="'+v+'" '+(v===chosen?'selected':'')+'>'+esc(l)+'</option>').join('');
  h.innerHTML=(p._gold_loading?'<p class="mx-loading-notice" role="status">Elenco parziale · caricamento in corso. Conteggi e confronti riguardano solo le partite già ricevute.</p>':'')+'<section class="mx-dashboard"><header class="mx-heading"><div><span>ARABSNIPER · GOLD</span><h2>Analisi <em>prematch</em></h2><p>Una scelta principale, un’alternativa e un colpaccio quando i dati lo consentono.</p></div><b>'+(active?'GOLD V7':'GOLD v6.1')+'</b></header>'+
   '<div class="mx-counters"><span><b>'+all.length+'</b> partite in whitelist</span><span><b>'+rows.length+'</b> mercati calcolati</span><span><b>'+rows.filter(r=>priced(r)).length+'</b> quote utilizzabili · '+rows.filter(r=>r.quote_status==='cached_stable').length+' da cache</span><span><b>'+all.filter(m=>['gold','platinum'].includes(cockpitTier(m,services).tone)).length+'</b>Gold + Platinum<small>'+(active?'Selezioni del motore V7':'Proposte su 5 mercati gol · Combo PT+ST')+'</small></span></div>'+(!p.gold_scope.ready?'<p class="mx-caption">Dati in aggiornamento alla whitelist: i campionati della versione precedente restano nascosti.</p>':'')+
   '<nav class="mx-categories" aria-label="Lettura del mercato">'+Object.entries(CATEGORIES).map(([k,l])=>'<button type="button" data-mx-category="'+k+'" aria-pressed="'+(state.category===k)+'">'+esc(l)+' <b>'+count(k)+'</b></button>').join('')+'</nav>'+
   '<div class="mx-controls"><label>Mercati<select data-mx-filter="family">'+options(FAMILIES,state.family)+'</select></label><label>Periodo<select data-mx-filter="period">'+options({all:'Tutti i periodi',ft:'Finale',ht:'Primo tempo',sh:'Secondo tempo'},state.period)+'</select></label>'+
   '<label>Stato<select data-mx-filter="phase">'+options({all:'Tutte le partite',upcoming:'Da iniziare',live:'In corso',finished:'Terminate',other:'Rinviate / altri stati'},state.phase)+'</select></label><label>Ordine<select data-mx-filter="sort">'+options({time:'Orario partita',oci:'OCI più alto',odd:'Quota più alta',value:'Criteri interni di valore'},state.sort)+'</select></label>'+
   '<label class="mx-search">Cerca<input type="search" data-mx-filter="search" value="'+esc(state.search)+'" placeholder="Squadra o campionato"></label></div>'+
   '<details class="mx-reading-help"><summary>Come leggere categorie e indicatori</summary><p class="mx-caption">Gold e Platinum applicano i criteri dei rispettivi livelli alle stime prematch. Il Radar Blu raccoglie osservazioni sui movimenti delle quote. Consigliata e Colpaccio sono ipotesi a quota superiore valutate a parte. La quota equa deriva dalla probabilità stimata. OCI e aggio descrivono il mercato delle quote.</p></details>'+
   '<div class="mx-results" aria-live="polite"><p class="mx-results-count">'+matches.length+' di '+all.length+' partite · '+esc(CATEGORIES[state.category])+'</p>'+
   (matches.length?activeMatches.map(m=>card(m,services)).join(''):'<div class="mx-empty">Nessuna partita con questi filtri. <button type="button" data-mx-reset>Mostra tutto il palinsesto</button></div>')+
   (inactiveMatches.length?'<details class="mx-reading-help mx-inactive"><summary>Rinviate, annullate o sospese · '+inactiveMatches.length+'</summary>'+inactiveMatches.map(m=>card(m,services)).join('')+'</details>':'')+'</div>'+
   '<p class="mx-caption">'+(p.history_view?'Archivio di ieri: analisi, quote e categorie prematch conservate. Si aggiornano solo i risultati disponibili; le partite senza risultato restano in attesa.':'Ogni Fast ricalcola le analisi prepartita. Gli aggiornamenti del fornitore possono essere meno frequenti; la data delle quote è sempre indicata. Le partite iniziate conservano la previsione prepartita, con punteggio live separato.')+'</p></section>';
  h.onclick=e=>{
   const expand=e.target.closest?.('[data-mx-open]'),category=e.target.closest?.('[data-mx-category]'),save=e.target.closest?.('[data-mx-save]');
   if(expand){openModal(expand.dataset.mxOpen);}
   else if(save){const id=save.dataset.mxSave;observedFixtures.has(id)?observedFixtures.delete(id):observedFixtures.add(id);render(payload,host,helpers);}
   else if(category){state.category=category.dataset.mxCategory;if(['combo','platinum'].includes(state.category)){state.family='all';state.period='all';}render(payload,host,helpers);}
   else if(e.target.closest?.('[data-mx-reset]')){Object.assign(state,{category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''});render(payload,host,helpers);}
  };
  h.onchange=e=>{const fid=e.target.dataset?.mxInlineInstrument;if(fid!==undefined){inlineInstruments.set(fid,e.target.value||null);const m=(payload.matches||[]).find(x=>String(x.fixture?.fixture_id)===fid);if(m){e.target.closest('.mx-cockpit').outerHTML=cockpit(m,helpers,true);[...h.querySelectorAll('[data-mx-inline-instrument]')].find(x=>x.dataset.mxInlineInstrument===fid)?.focus();}return;}const key=e.target.dataset?.mxFilter;if(key&&key in state){state[key]=e.target.value;render(payload,host,helpers);}};
  updateModal();
 }
 const api={publishedView,triadPanel,v7Panel,displayOci,proposalData,compactProposal,compactQuotes,card,operatorFair,observedMeans,goalPanels,studyDefaultRow,historyProposal,cockpitTier,cockpit,instrument,poissonEstimate,studyObservation,monitoringRows,PLATINUM,PLATINUM_POLICY,platinumSignals,flowSignalValid,halfTimeText,MARKET_SIGNAL,marketSignals,comboText,comboTone,comboReasons,extraGoalText,missingQuote,v61Valid,minOdd,MODEL,POLICY,SCOPE,state,render,rowsOf,matchingRows,selectMatches,phase,marketRow,bestOpportunity,movementSignal,quoteCell,quoteBoard,opportunityPanel,timingContext,scopePayload,goalMarket,maxOdd,minProbability,firstHalfEvidence,coreMarket,sectionRows,esc};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.ArabMatrix=api;
})(typeof window!=='undefined'?window:globalThis);

