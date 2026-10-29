/* Universal prematch observation. Live scores never overwrite frozen estimates. */
(function(root){
 'use strict';
 const MODEL='lateral-matrix-v5.0';
 const POLICY='gold-goals-v6.1',SCOPE='gold-senior-whitelist-v2.1';
 // v1 is a strict subset of v2: keep existing data visible during rollout.
 const acceptedScope=id=>[SCOPE,'gold-senior-whitelist-v2','gold-senior-whitelist-v1'].includes(id);
 const acceptedPolicy=d=>[POLICY,'gold-goals-v6','gold-goals-v5','gold-goals-v4','gold-goals-v3','gold-goals-v2'].includes(d?.policy_id);
 const MIN_P={over15_ht:40,over25_ht:18,gg_ht:26,over25_ft:48,over35_ft:35};
 const V6_MIN_P={over15_ht:35,gg_ht:30,over25_ft:30,over35_ft:30};
 const ODDS_MIN={over15_ht:2.1,over25_ht:4.5,gg_ht:3.5,over25_ft:1.85,over35_ft:2.1};
 const PREVIOUS_MIN_P={over15_ht:45,gg_ht:40,over25_ft:35,over35_ft:35};
 const dedicatedPolicy=id=>[POLICY,'gold-goals-v6','gold-goals-v5'].includes(id);
 const ODDS_MAX={over15_ht:3.5,over25_ht:7.5,over25_ft:2.6,gg_ht:5.5,over35_ft:4.5};
 // Display-only monitoring. These limits never enter the Gold scoring policy.
 const MARKET_SIGNAL=Object.freeze({min_oci:30,max_delta_pct:-2,max_margin_pct:9,min_persistence:.5,min_agreeing_books:2});
 const LEGACY_PLATINUM_POLICY='platinum-flow-v1',PLATINUM_POLICY='platinum-flow-v1.1';
 const PLATINUM=Object.freeze({...MARKET_SIGNAL,max_delta_pct:-3,min_odd:2.5,min_ev_pct:10,min_high_odds_ev_pct:-15});
 const MARKET_SIGNAL_TEXT='Radar di Mercato · Statistica debole o assente · Solo studio';
 const LEGACY_MAX={over15_ht:3.5,over25_ft:3.5,gg_ht:5.5,over35_ft:5.5};
 const priced=r=>['recent','cached_stable'].includes(r?.quote_status);
 const isCombo=r=>r.spec?.kind==='halves_total'&&r.spec.line===.5&&r.spec.extra===.5;
 // A model-only combo is an estimate, not a failed bookmaker quote download.
 const missingQuote=r=>!priced(r)&&(!isCombo(r)||r.combo?.price_available===true);
 const state={category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''};
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=(v,n=1)=>typeof v==='number'&&Number.isFinite(v)?v.toLocaleString('it-IT',{minimumFractionDigits:n,maximumFractionDigits:n}):'—';
 const pct=v=>typeof v==='number'&&Number.isFinite(v)?num(v)+'%':'—';
 const signed=v=>typeof v==='number'&&Number.isFinite(v)?(v>0?'+':'')+num(v):'—';
 const FAMILIES={all:'Mercati utili', '1x2':'1X2',totals:'Over / Under',btts:'Goal / No Goal',asian:'Riferimenti asiatici'};
 const CATEGORIES={all:'Tutte le analisi',platinum:'PLATINUM',market_signal:'Radar di Mercato',combo:'Combo',convergence:'Convergenza',divergence:'Contrarian',lateral:'Riferimenti asiatici',value:'Valore teorico positivo',no_quote:'Quote mancanti o scadute'};
 const STATUS={mature:'CONVERGENZA STATISTICA',contrarian:'DIVERGENZA STATISTICA',lateral:'LATERALE',value_watch:'VALORE IN OSSERVAZIONE',limited_data:'STORICO LIMITATO',no_edge:'PREZZO SENZA VANTAGGIO',no_quote:'SOLO STATISTICA',previous_version:'VERSIONE PRECEDENTE'};
 const QUALITY={supported:'Storico presente',limited:'Storico limitato',prior_dominant:'Una squadra con dati mancanti',prior_only:'Solo valori di riferimento'};
 const REASONS={market:'mercato escluso',probability:'stima bassa',price:'quota fuori range',stale:'quota da aggiornare',history:'storico insufficiente',ht_history:'meno di 4 PT per squadra',adapter:'quota da verificare',margin:'aggio oltre limite',edge:'EV insufficiente',sensitivity:'EV prudente basso',coherence:'stime incoerenti'};
 Object.assign(REASONS,{yield_floor:'quota sotto 1,85',oci_negative:'OCI < −20',odds_rising:'salita quota >2%',suspicious_divergence:'divergenza non confermata',margin_missing:'aggio assente',ht_oci:'OCI PT < +15',structural_missing:'statistiche mancanti',isolated_peak:'media senza ultima <80%',ht_symmetry:'PT asimmetrici',ht_expected:'gol attesi PT bassi',ht_frequency:'frequenza PT <75%'});
 const allowedMatch=m=>acceptedScope(m.gold_scope?.policy_id)&&m.gold_scope.allowed===true;
 const scopePayload=p=>({...p,matches:(p.matches||[]).filter(allowedMatch),gold_scope:{...(p.gold_scope||{}),ready:acceptedScope(p.gold_scope?.policy_id)}});
 function goalMarket(r){const s=r.spec||{};if(s.extra!=null)return null;return s.kind==='btts'&&s.period==='ht'&&s.side==='yes'&&s.line==null?'gg_ht':['total','asian_total'].includes(s.kind)&&s.side==='over'?(s.period==='ht'&&s.line===1.5?'over15_ht':s.period==='ht'&&s.line===2.5?'over25_ht':s.period==='ft'&&s.line===2.5?'over25_ft':s.period==='ft'&&s.line===3.5?'over35_ft':null):null;}
 const minProbability=r=>r.opportunity?.policy_id===POLICY?(MIN_P[goalMarket(r)]??30):r.opportunity?.policy_id==='gold-goals-v6'?(V6_MIN_P[goalMarket(r)]??30):r.opportunity?.policy_id==='gold-goals-v5'?(PREVIOUS_MIN_P[goalMarket(r)]??35):35;
 const maxOdd=r=>r.opportunity?.policy_id===POLICY?(ODDS_MAX[goalMarket(r)]??7.5):['gold-goals-v6','gold-goals-v5','gold-goals-v4'].includes(r.opportunity?.policy_id)?(LEGACY_MAX[goalMarket(r)]??3.5):3.5;
 const minOdd=r=>r.opportunity?.policy_id===POLICY?(ODDS_MIN[goalMarket(r)]??1.85):1.4;
 function coreMarket(s={}){
  if(s?.kind==='halves_total')return s.period==='ft'&&s.side==='over'&&s.line===.5&&s.extra===.5;
  if(!s||s.extra!=null)return false;
  if(s.kind==='result')return s.period==='ft'&&['1','x','2'].includes(s.side)&&s.line==null;
  if(s.kind==='dnb')return s.period==='ft'&&['1','2'].includes(s.side)&&s.line==null;
  if(s.kind==='btts')return ['ft','ht'].includes(s.period)&&['yes','no'].includes(s.side)&&s.line==null;
  if(!Number.isFinite(s.line))return false;
  if(s.kind==='total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,2.5,3.5]:s.period==='ht'?[1.5,2.5]:[]).includes(s.line);
  if(s.kind==='asian_total')return ['over','under'].includes(s.side)&&(s.period==='ft'?[1.5,1.75,2,2.25,2.5,2.75,3,3.25,3.5]:s.period==='ht'?[1,1.25,1.5,1.75,2,2.5]:[]).includes(s.line);
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
  return (m.multimarket?.markets||[]).filter(r=>['over25_ft','over35_ft','over15_ht','over25_ht','gg_ht'].includes(r.market)).map(r=>({...r,id:r.market+'::'+r.selection,
    family:r.market?.includes('gg')?'btts':'totals',period:r.market?.endsWith('_ht')?'ht':'ft',
    quote_status:r.quote_status||'missing',first_odd:r.quote?.movement?.first_odds?.[r.selection],
    bookmaker:r.quote?.bookmaker,category:'previous_version',quality:'limited'}));
 }
 function matchingRows(m,options=state,now=Date.now(),lifecycle=phase){
  const all=rowsOf(m),tierOptions={now,archived:lifecycle(m,now)!=='upcoming',kickoff:m.fixture?.kickoff};
  const platinum=allowedMatch(m)?platinumSignals(all,tierOptions):[];
  const source=options.category==='platinum'?platinum:options.category==='market_signal'?(allowedMatch(m)&&!platinum.length?marketSignals(all,tierOptions):[]):all;
  return source.filter(r=>{
   if(options.family!=='all'&&r.family!==options.family)return false;
   if(options.period!=='all'&&r.period!==options.period)return false;
   if(options.category==='combo')return isCombo(r)&&comboActive(r);
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
 function v61Valid(r,now,archived){
  const d=r.opportunity;if(d?.policy_id!==POLICY)return true;
  const m=goalMarket(r),margin=r.selected_margin_pct,delta=Number.isFinite(r.delta_pct)?r.delta_pct:r.selected_movement?.delta_pct;
  const book=Number.isFinite(r.selected_market_fair_pct)?r.selected_market_fair_pct:Number.isFinite(margin)&&margin> -100&&r.odd>1?100/r.odd/(1+margin/100):r.market_fair_pct;
  const floors={over15_ht:2,over25_ht:0,over25_ft:2,over35_ft:1,gg_ht:-1};
  const minimum=floors[m]+(margin>=6.5?1.5:0);
  if(!d.structural_supported||!d.market_checks_passed||!Number.isFinite(margin)||margin<0||margin>8||r.oci< -20||delta>2+1e-9)return false;
  if(m==='over25_ht'&&(!Number.isFinite(r.oci)||r.oci<15))return false;
  if(Number.isFinite(book)&&r.statistical_pct-book>15+1e-9&&!(delta<0&&r.oci>0))return false;
  if(r.ev_low_pct<minimum||r.ev_pct<(m==='gg_ht'?6:0)||d.stability?.status==='retained')return false;
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
   (priced(row)?'<b>Quota book '+num(row.odd,2)+'</b>':'Quota book n.d.')+(active?'':'<br>'+esc(comboReasons(row)))+'</'+tag+'>';
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
   return flowSignalValid(r,PLATINUM,archived||monitoringClosed?at:now)&&
    ((Number.isFinite(r.ev_pct)&&r.ev_pct>=PLATINUM.min_ev_pct)||
     (r.odd>=PLATINUM.min_odd&&(legacy||
      (Number.isFinite(r.ev_pct)&&r.ev_pct>=PLATINUM.min_high_odds_ev_pct)||
      (r.ev_pct==null&&r.statistical_pct==null))));
  }).sort((a,b)=>a.platinum.rank-b.platinum.rank||String(a.id).localeCompare(String(b.id)));
  const seen=new Set();return candidates.filter(r=>{const key=goalMarket(r);if(seen.has(key))return false;seen.add(key);return true;});
 }
 function movementSignal(row){
  const delta=priced(row)&&Number.isFinite(row.delta_pct)?row.delta_pct:null;
  const magnitude=delta==null?null:Math.abs(delta),level=magnitude==null?'unknown':magnitude>=8?'red':magnitude>=3?'yellow':'green';
  const direction=delta==null?'Storico assente':Math.abs(delta)<.005?'Invariata':delta<0?'Discesa':'Salita';
  const arrow=delta==null?'—':Math.abs(delta)<.005?'→':delta<0?'↓':'↑';
  return '<div class="mx-net mx-net-'+level+'" aria-label="'+esc(direction+(delta==null?'':' '+signed(delta)+'%'))+'"><b>'+arrow+' '+(delta==null?'—':signed(delta)+'%')+'</b><small>'+direction+' · '+(level==='red'?'forte':level==='yellow'?'media':level==='green'?'lieve':'da osservare')+'</small></div>';
 }
 function quoteCell(row,title,helpers={}){
  row=row||{};
  const recent=priced(row);
  const band={low:'basso',medium:'intermedio',high:'alto'}[row.margin_band]||'';
  return '<section class="mx-quote-cell"><h4>'+esc(title)+'</h4><div class="mx-quote-pair"><span title="Quota iniziale rilevata presso lo stesso operatore">Apertura<b>'+num(row.first_odd,2)+'</b></span><span>'+(row.quote_status==='cached_stable'?'Quota stabile da cache':recent?'Attuale':Number.isFinite(row.last_odd)?'Ultima quota · da confermare':'Quota non ricevuta')+'<b>'+num(recent?row.odd:row.last_odd,2)+'</b></span></div>'+movementSignal(row)+
   '<div class="mx-quote-metrics"><span>Statistica <b>'+pct(row.statistical_pct)+'</b></span><span>OCI <b>'+signed(row.oci)+'</b></span><span>Aggio mediano <b>'+pct(row.margin_pct)+'</b> '+esc(band)+'</span><span>Δ aggio <b>'+signed(row.margin_delta_pp)+' pt</b></span></div>'+
   '<small class="mx-quote-source">'+(!recent?esc(quoteAvailability(row))+' · ':'')+esc(row.bookmaker||'Fonte non disponibile')+(row.provider_updated_at?' · '+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at):'')+'</small>'+
   '<small class="mx-quote-consensus">'+(Number.isFinite(row.oci)?'OCI: ':'OCI non disponibile · ')+esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi'+(row.oci_previous==null?'':' · prima '+signed(row.oci_previous))+'</small></section>';
 }
 function quoteBoard(rows,helpers){
  const find=(kind,period,side,line)=>rows.find(r=>r.spec?.kind===kind&&r.period===period&&r.spec.side===side&&(line==null||r.spec.line===line));
  return '<section class="mx-quote-board" aria-label="Quote 1X2 apertura e attuale"><h4 class="mx-section-title">QUOTE 1 · X · 2 — APERTURA E ATTUALE</h4><div class="mx-1x2">'+[['1','1 · Casa'],['x','X · Pareggio'],['2','2 · Ospite']].map(([s,l])=>quoteCell(find('result','ft',s),l,helpers)).join('')+'</div>'+
   '<div class="mx-goal-quotes">'+[['total','ht','over',1.5,'Over 1,5 PT'],['total','ft','over',2.5,'Over 2,5 FT'],['total','ft','over',3.5,'Over 3,5 FT'],['btts','ht','yes',null,'GG PT']].map(([k,p,s,l,t])=>quoteCell(find(k,p,s,l),t,helpers)).join('')+'</div></section>';
 }
 function timingContext(context){
  if(!context)return '';
  const f=context.imbalance,active=context.pt_priority_active;
  const preference=context.preferred_pt_market==='over15_ht'?'Over 1,5 PT':context.preferred_pt_market==='gg_ht'?'Goal PT':'Over 1,5 PT / Goal PT';
  const setting=f?.regime==='balanced'?'Quote bilanciate · distanza 1–2 '+num(f.home_away_odds_gap,2):f?'Favorita '+(f.favorite_venue==='home'?'in casa':'in trasferta')+' · quota '+num(f.favorite_odd,2):'Sbilanciamento non confermato';
  const gaps=['home','away'].map(s=>{const t=context.trends?.[s],g=t?.venue?.ft;return g?.sufficient?'<small>'+(s==='home'?'Casa, in casa':'Ospite, fuori casa')+': media recente '+num(g.recent_mean,2)+' / riferimento '+num(g.reference_mean,2)+' · digiuno '+num(g.drought_matches,0)+' gare</small>':'';}).join('');
  return '<section class="mx-timing"><b>'+(active?'Ordine di valutazione: '+preference:'Confronto dei cinque mercati gol')+'</b><small>'+esc(setting)+'</small>'+gaps+(f?'<small>OCI 1X2 '+signed(f.oci)+' · aggio '+pct(f.margin_pct)+' · '+esc(f.bookmaker)+'</small>':'')+(active?'<small>Precedenza tra idonei · probabilità invariata.</small>':'')+'</section>';
 }
 function platinumPanel(rows,context,options,elite,best){
  const gold=best?'<details class="mx-method mx-platinum-gold"><summary>Giocata ufficiale Gold v6.1 · '+esc(best.label)+' · '+num(best.odd,2)+'</summary>'+opportunityPanel(rows,context,{...options,suppressPlatinum:true})+'</details>':'<p>Categoria Platinum · distinta dalle giocate Gold ufficiali.</p>';
  return '<aside class="mx-best mx-best-platinum" data-platinum-id="'+esc(elite.id)+'"><span class="mx-section-title">Top Pick · <strong>PLATINUM</strong> · Quota &amp; Flusso Dominante</span><h4>'+esc(elite.label)+'</h4><span class="mx-badge mx-platinum"><strong>PLATINUM</strong></span><div class="mx-best-price">'+num(elite.odd,2)+'<small>'+(elite.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA OSSERVATA')+' · '+esc(elite.bookmaker)+'</small></div><dl>'+[
   ['OCI / variazione quota',signed(elite.oci)+' / '+signed(elite.delta_pct)+'%'],
   ['Aggio / operatori concordi',pct(elite.selected_margin_pct)+' / '+elite.agreeing_books+'/'+elite.paired_books],
   ['Persistenza del movimento',pct(100*elite.selected_movement.persistence)],
   ['EV stimato',Number.isFinite(elite.ev_pct)?signed(elite.ev_pct)+'%':'Non disponibile'],
   ['Statistica',Number.isFinite(elite.statistical_pct)?pct(elite.statistical_pct):'Non disponibile']
  ].map(([label,value])=>'<div><dt>'+esc(label)+'</dt><dd>'+esc(value)+'</dd></div>').join('')+'</dl>'+gold+'<p>'+extraGoalText(rows)+'</p></aside>';
 }
 function opportunityPanel(rows,context,options={}){
  const best=bestOpportunity(rows,options),pricedCount=rows.filter(r=>priced(r)).length;
  const elite=!options.suppressPlatinum&&platinumSignals(rows,options)[0];
  if(elite)return platinumPanel(rows,context,options,elite,best);
  const checked=rows.filter(r=>goalMarket(r)!==null&&acceptedPolicy(r.opportunity)),counts={};
  for(const r of checked)for(const code of (!priced(r)?['stale']:r.opportunity.reasons||[]))counts[code]=(counts[code]||0)+1;
  const blockers=['oci_negative','odds_rising','suspicious_divergence','margin','margin_missing','stale'];
  const exclusions=Object.entries(counts).sort((a,b)=>{const priority=k=>blockers.includes(k)?blockers.indexOf(k):blockers.length;return priority(a[0])-priority(b[0])||b[1]-a[1]}).slice(0,3).map(([k,n])=>n+' '+(REASONS[k]||k)).join(' · ');
  const rulePolicy=best?.opportunity.policy_id||(checked.length&&checked.every(r=>r.opportunity.policy_id===checked[0].opportunity.policy_id)?checked[0].opportunity.policy_id:POLICY);
  const archive=rulePolicy===POLICY?'':'Proposta archiviata · '+esc(rulePolicy)+'<br>';
  const last=options.lastProposal;
  const history=!best&&last&&Number.isFinite(last.odd)?'<section class="mx-timing"><b>Ultima proposta pubblicata · '+esc(last.label||last.market_id)+'</b><strong>'+num(last.odd,2)+'</strong><small>'+esc(last.bookmaker||'')+' · '+esc(last.provider_updated_at||last.published_at||'')+'</small><small>'+(last.status==='withdrawn'?'Proposta ritirata: i dati aggiornati non superano più i criteri.':'In attesa di aggiornamento: conservata come riferimento, quota e validità attuali da confermare.')+'</small></section>':'';
  const signals=!best?marketSignals(rows,options):[],watch=signals[0];
  if(watch)return '<aside class="mx-best mx-best-market" data-market-signal-id="'+esc(watch.id)+'"><span class="mx-section-title">RADAR DI MERCATO</span><h4>'+esc(watch.label)+'</h4><span class="mx-badge mx-market-signal">STUDIO / ALTA ATTENZIONE</span><p>'+MARKET_SIGNAL_TEXT+'</p><div class="mx-best-price">'+num(watch.odd,2)+'<small>'+(watch.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA OSSERVATA')+' · '+esc(watch.bookmaker)+'</small></div><dl>'+[
   ['Persistenza del movimento',pct(100*watch.selected_movement.persistence)],
   ['OCI / variazione quota',signed(watch.oci)+' / '+signed(watch.delta_pct)+'%'],
   ['Aggio / operatori concordi',pct(watch.selected_margin_pct)+' / '+watch.agreeing_books+'/'+watch.paired_books]
  ].map(([label,value])=>'<div><dt>'+esc(label)+'</dt><dd>'+esc(value)+'</dd></div>').join('')+'</dl><p><b>Solo studio · esclusa dalle giocate Gold ufficiali.</b></p>'+history+timingContext(context)+'<p>'+extraGoalText(rows)+'</p><small>'+signals.length+' '+(signals.length===1?'mercato in monitoraggio':'mercati in monitoraggio')+' · 0 proposte Gold</small></aside>';
  if(!best)return '<aside class="mx-best mx-best-empty"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+(checked.length&&checked.every(r=>!priced(r))?'Quote da aggiornare':checked.length?'Nessuna proposta supera i criteri':'Valutazione da aggiornare')+'</h4>'+history+timingContext(context)+'<p>'+archive+extraGoalText(rows)+'</p><p>'+esc(exclusions||'Dati insufficienti')+'</p><small>'+rows.length+' esiti · '+pricedCount+' quote utilizzabili</small></aside>';
  const d=best.opportunity,signal=d.signal,tag=signal==='convergence'?'mature':signal==='contrarian'?'contrarian':'value_watch',displayLabel=goalMarket(best)==='gg_ht'?'Goal PT':best.label;
  const stability=d.stability?.status==='retained'?'<p class="mx-stability"><b>Proposta mantenuta in tolleranza</b><small>Piccola oscillazione dei valori. Validità massima fino alle '+esc(new Date(d.stability.expires_at).toLocaleTimeString('it-IT',{timeZone:'Europe/Rome',hour:'2-digit',minute:'2-digit'}))+' con quote aggiornate e limiti rispettati.</small></p>':'';
  return '<aside class="mx-best mx-best-stable" data-opportunity-id="'+esc(best.id)+'" data-opportunity-odd="'+best.odd+'" data-opportunity-max-odd="'+maxOdd(best)+'" data-opportunity-probability="'+best.statistical_pct+'"><span class="mx-section-title">MIGLIOR VALORE STIMATO</span><h4>'+esc(displayLabel)+'</h4><div class="mx-best-price">'+num(best.odd,2)+'<small>'+(best.quote_status==='cached_stable'?'Quota stabile da cache':'QUOTA ATTUALE')+' · '+esc(best.bookmaker||'')+'</small></div><span class="mx-badge mx-'+tag+'">'+esc(d.label)+'</span>'+stability+
   '<dl><div><dt>Statistica pura</dt><dd>'+pct(best.statistical_pct)+' · minimo '+pct(d.min_probability_pct)+'</dd></div><div><dt>Quota equa del modello</dt><dd>'+num(best.fair_odd,2)+'</dd></div><div><dt>Valore teorico / minimo richiesto</dt><dd>'+signed(best.ev_pct)+'% / '+num(d.min_ev_pct)+'%</dd></div><div><dt>Scenario prudente / minimo</dt><dd>'+signed(best.ev_low_pct)+'% / '+num(d.min_ev_low_pct)+'%</dd></div><div><dt>OCI · precedente → attuale</dt><dd>'+signed(best.oci_previous)+' → '+signed(best.oci)+'</dd></div><div><dt>Aggio del bookmaker · variazione</dt><dd>'+pct(best.selected_margin_pct)+' · '+signed(best.selected_margin_delta_pp)+' pt</dd></div></dl>'+
   firstHalfEvidence(best)+timingContext(context)+'<p>'+archive+extraGoalText(rows)+'</p><small>'+new Set(checked.filter(r=>r.opportunity.eligible).map(goalMarket)).size+' mercati idonei</small></aside>';
 }
 function firstHalfEvidence(row){
  const e=row.ht_evidence;if(!e||!['over15_ht','over25_ht','gg_ht'].includes(goalMarket(row)))return '';
  return '<p class="mx-timing mx-first-half"><b>Riscontri del primo tempo</b>'+['home','away'].map(side=>{const t=e.teams?.[side]||{};return '<small>'+(side==='home'?'Casa':'Ospite')+': '+num(t.sample,0)+' PT · gol fatti/subiti '+num(t.scored_mean,2)+' / '+num(t.conceded_mean,2)+' · xG PT '+(t.xg_for_sample?num(t.xg_for_mean,2)+' su '+num(t.xg_for_sample,0)+' gare':'non disponibili')+'</small>';}).join('')+'</p>';
 }
 function price(row){
  if(!priced(row))return '<span class="mx-missing">'+(isCombo(row)?'Quota bookmaker non fornita':esc(quoteAvailability(row)))+'</span>'+(isCombo(row)?'<small>Equa calcolata dal modello</small>':'');
  const arrow=row.odd<row.first_odd?'↘':row.odd>row.first_odd?'↗':'→';
  return (row.quote_status==='cached_stable'?'<small>Quota stabile da cache</small>':'')+'<span class="mx-price">'+num(row.first_odd,2)+' <i>'+arrow+'</i> <b>'+num(row.odd,2)+'</b></span><small>'+esc(row.bookmaker)+' · '+signed(row.delta_pct)+'%</small>';
 }
 function marketRow(row,helpers={},options={}){
  const asian=['asian_total','handicap','dnb'].includes(row.spec?.kind);
  const d=row.opportunity,range=goalMarket(row)&&acceptedPolicy(d)?'<small title="'+esc('Quote '+num(minOdd(row),2)+'–'+num(maxOdd(row),2)+' · soglia '+pct(d.min_probability_pct))+'">'+(options.platinum?'Gold: ':'')+(d.eligible?'Idoneo':'Scartato · '+esc((d.reasons||[]).map(code=>REASONS[code]||code).join(' · ')||'criteri non superati'))+'</small>':'';
  const tone=isCombo(row)?comboTone(row):null;
  return '<div class="mx-market-row" role="row"><div class="mx-market-name" role="cell"><b>'+esc(row.label)+'</b>'+(isCombo(row)?'<span class="mx-badge mx-combo-'+tone+'">'+(tone==='active'?'Combo Attiva':tone==='near'?'Sotto soglia · Vicina':row.combo?.policy_id===POLICY?'Sotto soglia':'Dati assenti')+'</span>'+(!comboActive(row)?'<small>'+esc(comboReasons(row))+'</small>':''):options.platinum?'<span class="mx-badge mx-platinum"><strong>PLATINUM</strong></span>':options.marketSignal?'<span class="mx-badge mx-market-signal" title="'+MARKET_SIGNAL_TEXT+'">Radar di Mercato · Solo studio</span>':badge(row))+range+'</div>'+
   '<div role="cell" data-title="Statistica"><strong>'+pct(row.statistical_pct)+'</strong><small>Equa '+num(row.fair_odd,2)+' · '+esc(QUALITY[row.quality]||'Stima archiviata')+'</small>'+(asian?'<small>Rimborsi inclusi nel valore</small>':'')+'</div>'+
   '<div role="cell" data-title="Prima → attuale">'+price(row)+(row.provider_updated_at?'<small>'+esc(helpers.timestamp?.(row.provider_updated_at)||row.provider_updated_at)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="OCI"><b>'+signed(isCombo(row)?row.combo?.over25_ft_oci:row.oci)+'</b><small>'+(isCombo(row)?'Riferimento Over 2,5 FT':esc(row.agreeing_books??0)+'/'+esc(row.paired_books??0)+' operatori concordi')+'</small></div>'+
   '<div role="cell" data-title="Aggio"><b>'+pct(row.selected_margin_pct)+'</b><small>'+(isCombo(row)&&!priced(row)?'Richiede una quota bookmaker':'Book selezionato · Δ '+signed(row.selected_margin_delta_pp)+' pt')+'</small>'+(!isCombo(row)?'<small>Mediana '+pct(row.margin_pct)+'</small>':'')+'</div>'+
   '<div role="cell" data-title="Valore"><b class="'+(row.ev_pct>0?'mx-positive':'')+'">'+(row.ev_pct==null?'—':signed(row.ev_pct)+'%')+'</b><small>'+(isCombo(row)&&!priced(row)?'EV non calcolabile senza quota reale':'Prudente '+(row.ev_low_pct==null?'—':signed(row.ev_low_pct)+'%'))+'</small></div></div>';
 }
 function teamProfiles(m){
  const a=m.multimarket||{};
  return '<div class="mx-teams">'+['home','away'].map(side=>{
   const ft=a.profiles?.[side]?.for_ft||{},def=a.profiles?.[side]?.against_ft||{},ht=a.profiles?.[side]?.for_ht||{};
   return '<section><h4>'+esc(m.fixture?.[side]||(side==='home'?'Casa':'Ospite'))+'</h4><dl>'+[
    ['Storico utilizzato',num(ft.sample,0)+' FT · '+num(ht.sample,0)+' PT'],['Attacco / difesa',num(ft.rate,2)+' / '+num(def.rate,2)],
    ['Gol medi / ultima',num(ft.mean,2)+' / '+num(ft.last,0)],['Media senza ultima',num(ft.without_last,2)],
    ['xG osservati',ft.xg_sample?num(ft.xg_mean,2)+' · '+ft.xg_sample+' gare':'Non disponibili'],
    ['xG primo tempo',ht.xg_sample?num(ht.xg_mean,2)+' · '+ht.xg_sample+' gare':'Non disponibili'],['Gare senza segnare',num(ft.drought,0)],['Gol attesi FT / PT',num(a.rates?.[side+'_ft'],2)+' / '+num(a.rates?.[side+'_ht'],2)]
    ].map(([k,v])=>'<div><dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd></div>').join('')+'</dl>'+
    (ft.under_conversion?'<p>Potenziale xG superiore ai gol realizzati.</p>':'')+(ft.peak_reduction>.15?'<p>Goleada recente ridimensionata.</p>':'')+'</section>';
  }).join('')+'</div>';
 }
 const MATRIX_SECTIONS={targets:'Obiettivi gol',ft:'Gol FT',ht:'Primo tempo',result:'1 · X · 2',asian:'Asiatici'};
 let dialog=null,modalFixtureId=null,modalSection='targets',bodyOverflow='';
 function releaseModal(instance){
  if(dialog!==instance)return;
  const oldId=modalFixtureId;dialog=null;modalFixtureId=null;instance.remove();document.body.style.overflow=bodyOverflow;
  [...(host?.querySelectorAll('[data-mx-open]')||[])].find(e=>e.dataset.mxOpen===oldId)?.focus();
 }
 function closeModal(){const instance=dialog;if(!instance)return;instance.close();releaseModal(instance);}
 function sectionRows(rows,section){return rows.filter(r=>section==='targets'?goalMarket(r)!==null||isCombo(r):section==='asian'?r.family==='asian':section==='result'?r.spec?.kind==='result':r.period===section&&['total','btts'].includes(r.spec?.kind));}
 function matrixDetail(m,rows,services){
  const tierOptions={archived:(services.phase||phase)(m)!=='upcoming',kickoff:m.fixture?.kickoff};
  const signals=new Set(marketSignals(rowsOf(m),tierOptions).map(r=>r.id));
  const platinum=new Set(platinumSignals(rowsOf(m),tierOptions).map(r=>r.id));
  return '<div class="mx-detail"><p class="mx-explanation">'+rows.length+' esiti in questa vista. Quote, OCI e aggio della stessa linea e periodo.</p>'+
   '<div class="mx-table" role="table" aria-label="Matrice delle opportunità"><div class="mx-table-head" role="row">'+['Mercato','Statistica','Prima → attuale','OCI','Aggio','Valore'].map(t=>'<div role="columnheader">'+t+'</div>').join('')+'</div>'+rows.map(r=>marketRow(r,services,{marketSignal:signals.has(r.id),platinum:platinum.has(r.id)})).join('')+'</div>'+
   '<details class="mx-method"><summary>Statistiche delle squadre</summary>'+teamProfiles(m)+'</details>'+
   '<details class="mx-method"><summary>Come leggere i valori</summary><p>Aggio: competitività dei prezzi, non misura di liquidità. OCI: movimento delle quote, non volume di denaro. La sensibilità prudente non è un intervallo di confidenza. Il valore asiatico include rimborsi e mezzi esiti.</p></details></div>';
 }
 function updateModal(resetScroll=false){
  if(!dialog)return;
  const m=(payload?.matches||[]).find(x=>String(x.fixture?.fixture_id)===modalFixtureId);
  if(!m){closeModal();return;}
  const focused=dialog.contains(document.activeElement)?document.activeElement?.dataset?.mxSection:null;
  const closeFocused=document.activeElement?.hasAttribute('data-mx-close');
  const scroll=resetScroll?0:dialog.querySelector('.mx-modal-scroll')?.scrollTop||0;
  dialog.innerHTML='<header class="mx-modal-header"><div><span>MATRICE · MERCATI UTILI</span><h2 id="mx-modal-title">'+esc(m.fixture?.match)+'</h2><small>'+esc(m.fixture?.country)+' · '+esc(m.fixture?.league)+'</small></div><button type="button" data-mx-close aria-label="Chiudi matrice" autofocus>×</button></header>'+
   '<nav class="mx-modal-tabs" aria-label="Mercati della matrice">'+Object.entries(MATRIX_SECTIONS).map(([k,l])=>'<button type="button" data-mx-section="'+k+'" aria-pressed="'+(modalSection===k)+'">'+esc(l)+'</button>').join('')+'</nav>'+
   '<div class="mx-modal-scroll">'+matrixDetail(m,sectionRows(rowsOf(m),modalSection),helpers)+'</div>';
  dialog.querySelector('.mx-modal-scroll').scrollTop=scroll;
  if(focused)dialog.querySelector('[data-mx-section="'+focused+'"]').focus();
  else if(closeFocused)dialog.querySelector('[data-mx-close]').focus();
 }
 function openModal(id){
  if(dialog){modalFixtureId=id;modalSection='targets';updateModal(true);return;}
  modalFixtureId=id;modalSection='targets';
  dialog=document.createElement('dialog');dialog.className='mx-modal';dialog.setAttribute('aria-labelledby','mx-modal-title');dialog.setAttribute('aria-modal','true');
  document.body.append(dialog);bodyOverflow=document.body.style.overflow;document.body.style.overflow='hidden';
  const instance=dialog;
  dialog.addEventListener('close',()=>releaseModal(instance),{once:true});
  dialog.addEventListener('cancel',e=>{e.preventDefault();closeModal();});
  dialog.addEventListener('click',e=>{
   if(e.target.closest?.('[data-mx-close]')){closeModal();return;}
   const tab=e.target.closest?.('[data-mx-section]');
   if(tab){modalSection=tab.dataset.mxSection;updateModal(true);return;}
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
  const tierOptions={archived:phase(m)!=='upcoming',monitoringClosed:(helpers.phase||phase)(m)!=='upcoming',kickoff:f.kickoff,lastProposal:a.matrix?.last_proposal};
  const elite=!noSnapshot&&platinumSignals(all,tierOptions)[0];
  return '<article class="mx-card'+(elite?' mx-card-platinum':'')+'" data-live-fixture="'+esc(fid)+'"><header><div><time>'+esc(f.time||'')+'</time><small>'+esc(f.country)+' · '+esc(f.league)+'</small>'+(elite?'<span class="mx-badge mx-platinum mx-tier-label"><strong>PLATINUM</strong></span>':'')+'<h3>'+esc(f.match)+'</h3></div><div class="mx-live"><span data-live-slot="badge">'+(helpers.liveBadge?.(m)||'')+'</span><span data-live-slot="state">'+esc(helpers.stateLabel?.(m)||'')+'</span><span data-live-slot="halftime">'+halfTimeText(helpers.firstHalf?.(m))+'</span></div></header>'+
   (noSnapshot?'<p class="mx-explanation">Partita presente nel palinsesto. Nessuna previsione salvata prima dell’inizio: gli esiti non vengono usati per ricostruirla a posteriori.</p>':'<div class="mx-match-layout">'+quoteBoard(all,helpers)+opportunityPanel(all,a.matrix?.goal_context,tierOptions)+'</div>')+
   '<div class="mx-match-summary"><span>'+all.length+' mercati</span><span>'+counts.convergence+' convergenze</span><span>'+counts.divergence+' divergenze</span><span>'+all.filter(r=>priced(r)).length+' quote utilizzabili · '+all.filter(r=>r.quote_status==='cached_stable').length+' da cache</span></div>'+
   (all.length?'<button class="mx-open" type="button" data-mx-open="'+esc(fid)+'" aria-haspopup="dialog">Apri matrice · mercati utili <span>↗</span></button>':'')+'<footer>Analisi '+esc(helpers.timestamp?.(a.captured_at)||a.captured_at||'non disponibile')+' · Regole '+esc(a.matrix?.opportunity_policy?.id||'non disponibili')+' · Modello statistico '+esc(a.model_id||'non disponibile')+(f.data_freshness&&f.data_freshness!=='fresh'?' · '+esc(f.data_freshness):'')+'</footer></article>';
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
  const count=key=>key==='all'?all.length:all.filter(m=>matchingRows(m,{...state,category:key,family:'all',period:'all'},Date.now(),services.phase||phase).length).length;
  const options=(items,chosen)=>Object.entries(items).map(([v,l])=>'<option value="'+v+'" '+(v===chosen?'selected':'')+'>'+esc(l)+'</option>').join('');
  h.innerHTML='<section class="mx-dashboard"><header class="mx-heading"><div><span>ARABSNIPER · GOLD</span><h2>Campionati scelti. Focus sui gol.</h2><p>Gold v6.1 · Platinum · Radar di Mercato · Combo PT+ST.</p></div><b>GOLD v6.1</b></header>'+
   '<div class="mx-counters"><span><b>'+all.length+'</b> partite in whitelist</span><span><b>'+rows.length+'</b> mercati calcolati</span><span><b>'+rows.filter(r=>priced(r)).length+'</b> quote utilizzabili · '+rows.filter(r=>r.quote_status==='cached_stable').length+' da cache</span><span>Proposte su 5 mercati gol · Combo PT+ST</span></div>'+(!p.gold_scope.ready?'<p class="mx-caption">Dati in aggiornamento alla whitelist: i campionati della versione precedente restano nascosti.</p>':'')+
   '<nav class="mx-categories" aria-label="Lettura del mercato">'+Object.entries(CATEGORIES).map(([k,l])=>'<button type="button" data-mx-category="'+k+'" aria-pressed="'+(state.category===k)+'">'+esc(l)+' <b>'+count(k)+'</b></button>').join('')+'</nav>'+
   '<div class="mx-controls"><label>Mercati<select data-mx-filter="family">'+options(FAMILIES,state.family)+'</select></label><label>Periodo<select data-mx-filter="period">'+options({all:'Tutti i periodi',ft:'Finale',ht:'Primo tempo'},state.period)+'</select></label>'+
   '<label>Stato<select data-mx-filter="phase">'+options({all:'Tutte le partite',upcoming:'Da iniziare',live:'In corso',finished:'Terminate',other:'Rinviate / altri stati'},state.phase)+'</select></label><label>Ordine<select data-mx-filter="sort">'+options({time:'Orario',value:'Valore teorico più stabile'},state.sort)+'</select></label>'+
   '<label class="mx-search">Cerca<input type="search" data-mx-filter="search" value="'+esc(state.search)+'" placeholder="Squadra o campionato"></label></div>'+
   '<p class="mx-caption">Platinum (argento): quota alta o EV elevato, sostenuti da movimenti concordi. Gold v6.1 conserva i propri criteri statistici. Radar di Mercato (blu): movimenti delle quote, anche con statistica debole o assente. Solo studio, escluso dalle giocate Gold. Quote mancanti o scadute: partite con almeno un mercato senza prezzo utilizzabile; la Combo solo statistica è esclusa dal conteggio. Quota equa = prezzo calcolato dal modello, non una quota offerta. OCI e aggio non sono volumi di denaro.</p>'+
   '<div class="mx-results" aria-live="polite"><p class="mx-results-count">'+matches.length+' di '+all.length+' partite · '+esc(CATEGORIES[state.category])+'</p>'+
   (matches.length?matches.map(m=>card(m,services)).join(''):'<div class="mx-empty">Nessuna partita con questi filtri. <button type="button" data-mx-reset>Mostra tutto il palinsesto</button></div>')+'</div>'+
   '<p class="mx-caption">Ogni Fast ricalcola le analisi prepartita. Gli aggiornamenti del fornitore possono essere meno frequenti; la data delle quote è sempre indicata. Le partite iniziate conservano la previsione prepartita, con punteggio live separato.</p></section>';
  h.onclick=e=>{
   const expand=e.target.closest?.('[data-mx-open]'),category=e.target.closest?.('[data-mx-category]');
   if(expand){openModal(expand.dataset.mxOpen);}
   else if(category){state.category=category.dataset.mxCategory;if(['combo','platinum'].includes(state.category)){state.family='all';state.period='all';}render(payload,host,helpers);}
   else if(e.target.closest?.('[data-mx-reset]')){Object.assign(state,{category:'all',family:'all',period:'all',phase:'all',sort:'time',search:''});render(payload,host,helpers);}
  };
  h.onchange=e=>{const key=e.target.dataset?.mxFilter;if(key&&key in state){state[key]=e.target.value;render(payload,host,helpers);}};
  updateModal();
 }
 const api={PLATINUM,PLATINUM_POLICY,platinumSignals,flowSignalValid,halfTimeText,MARKET_SIGNAL,marketSignals,comboText,comboTone,comboReasons,extraGoalText,missingQuote,v61Valid,minOdd,MODEL,POLICY,SCOPE,state,render,rowsOf,matchingRows,selectMatches,phase,marketRow,bestOpportunity,movementSignal,quoteCell,quoteBoard,opportunityPanel,timingContext,scopePayload,goalMarket,maxOdd,minProbability,firstHalfEvidence,coreMarket,sectionRows,esc};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 root.ArabMatrix=api;
})(typeof window!=='undefined'?window:globalThis);

