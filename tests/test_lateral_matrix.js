const assert=require('node:assert/strict');
const api=require('../web/lateral-matrix.js');
// A changed whitelist cannot be shipped under an already cached script URL.
const fs=require('node:fs'),{createHash}=require('node:crypto');
const releaseHtml=fs.readFileSync('web/gold-dashboard-live.html','utf8');
const matrixSrc=releaseHtml.match(/<script src="(\/lateral-matrix\.js[^\"]*)"><\/script>/)?.[1];
const matrixHash=createHash('sha256').update(fs.readFileSync('web/lateral-matrix.js')).digest('hex').slice(0,12);
assert.equal(matrixSrc,'/lateral-matrix.js?v='+matrixHash,'Update the Gold script URL to its content hash when the bundle changes');
const matrixCssSrc=releaseHtml.match(/href="(\/lateral-matrix\.css[^\"]*)"/)?.[1];
const matrixCssHash=createHash('sha256').update(fs.readFileSync('web/lateral-matrix.css')).digest('hex').slice(0,12);
assert.equal(matrixCssSrc,'/lateral-matrix.css?v='+matrixCssHash,'Conditional colors must not reuse a cached stylesheet URL');
const liveHash=createHash('sha256').update(fs.readFileSync('web/live-scores.js')).digest('hex').slice(0,12);
assert(releaseHtml.includes('/live-scores.js?v='+liveHash),'Half-time overlay changes need a new live bundle URL');
assert.match(fs.readFileSync('web/_headers','utf8'),/\/lateral-matrix\.js\n\s+Cache-Control: no-cache, must-revalidate/);
const rows=[{id:'a',family:'totals',period:'ft',spec:{kind:'total',period:'ft',side:'over',line:2.5},category:'mature',market_behavior:'convergence',ev_pct:12,ev_low_pct:5,quote_status:'recent',quality:'supported'},
 {id:'b',family:'asian',period:'ht',spec:{kind:'asian_total',period:'ht',side:'over',line:1.25},category:'contrarian',market_behavior:'divergence',ev_pct:30,ev_low_pct:20,quote_status:'recent',quality:'supported'},
 {id:'c',family:'btts',period:'ht',spec:{kind:'btts',period:'ht',side:'yes'},category:'no_quote',quote_status:'missing'}];
const matches=Array.from({length:160},(_,i)=>({gold_scope:{policy_id:api.SCOPE,allowed:true},fixture:{fixture_id:String(i+1),kickoff:'2026-10-26T20:00:00Z',match:'Casa '+i+' – Ospite',league:'Serie prova'},multimarket:{model_id:api.MODEL,matrix:{rows:structuredClone(rows)}}}));
const p={matches,gold_scope:{policy_id:api.SCOPE}},opts={...api.state};
assert.equal(api.selectMatches(p,opts,0).length,160);
assert.equal(api.selectMatches({matches:[{...matches[0],gold_scope:null}]},opts,0).length,0);
assert.equal(api.scopePayload({matches:[{...matches[0],gold_scope:{policy_id:api.SCOPE,allowed:false}}]}).matches.length,0);
assert.equal(api.matchingRows(matches[0],{...opts,category:'divergence'}).length,1);
assert.equal(api.matchingRows(matches[0],{...opts,family:'asian'}).length,1);
assert.equal(api.matchingRows(matches[0],{...opts,category:'no_quote'}).length,1);
assert.equal(api.selectMatches(p,{...opts,search:'Casa 159'},0).length,1);
for(const scope of ['gold-senior-whitelist-v1','gold-senior-whitelist-v2']) {
 const mixed={gold_scope:{policy_id:scope},matches:[{...matches[0],gold_scope:{policy_id:scope,allowed:true}}]};
 assert.equal(api.scopePayload(mixed).matches.length,1);
 assert.equal(api.scopePayload(mixed).gold_scope.ready,true);
}
assert.equal(api.scopePayload({matches:[{...matches[0],gold_scope:{policy_id:'unknown',allowed:true}}]}).matches.length,0);
const frozen=JSON.stringify(p);api.selectMatches(p,{...opts,sort:'value'},0);assert.equal(JSON.stringify(p),frozen);
assert(!api.esc('<img src=x onerror=alert(1)>').includes('<img'));
const html=api.marketRow({...rows[0],label:'<script>x</script>',statistical_pct:61,odd:2,first_odd:2.2,margin_pct:4,oci:50},{});
assert(html.includes('&lt;script&gt;'));assert(html.includes('Aggio'));assert(html.includes('OCI'));
const decision={policy_id:api.POLICY,eligible:true,rank:1,priority:0,min_probability_pct:48,min_ev_pct:0,min_ev_low_pct:2,structural_supported:true,market_checks_passed:true,signal:'convergence',label:'Convergenza di Trend',reasons:[]};
const priced={...rows[0],selected_margin_pct:3,oci:30,delta_pct:-1,odd:2,statistical_pct:60,id:'regular',spec:{kind:'total',period:'ft',side:'over',line:2.5},opportunity:decision};
const asian={...priced,id:'asian',family:'asian',spec:{kind:'handicap',period:'ft',side:'home',line:.25},quote_adapter_version:2,ev_pct:30,ev_low_pct:12,opportunity:{...decision,rank:2,priority:1,min_probability_pct:35}};
const fragile={...priced,id:'fragile',ev_pct:100,ev_low_pct:-20};
const prior={...priced,id:'prior',quality:'prior_only',ev_pct:200,ev_low_pct:200};
assert.equal(api.bestOpportunity([priced,asian,fragile,prior]).id,'regular');
assert.equal(api.bestOpportunity([{...asian,quote_status:'stale'}]),null);
for(const extra of [{odd:150,statistical_pct:1},{odd:3.51},{odd:1.39},{statistical_pct:29.99},{opportunity:null},{spec:{kind:'score'}},{spec:{kind:'dc',period:'ft',side:'x2'}}])assert.equal(api.bestOpportunity([{...priced,...extra}]),null);
const high={...priced,odd:5.5,statistical_pct:30,opportunity:{...decision,min_probability_pct:26,ht_history_supported:true},spec:{kind:'btts',period:'ht',side:'yes'}};
assert.equal(api.bestOpportunity([high]).id,'regular');
assert(api.marketRow(high).includes('Idoneo'));
assert(!api.opportunityPanel([high]).includes('3,50–5,50'));
assert(!api.opportunityPanel([high]).includes('Soglie statistiche e controlli di mercato applicati'));
// Frozen v5 keeps its original floors, expanded prices and HT evidence gate.
for(const [spec,floor,max] of [[{kind:'total',period:'ht',side:'over',line:1.5},45,3.5],[{kind:'btts',period:'ht',side:'yes'},40,5.5],[{kind:'total',period:'ft',side:'over',line:2.5},35,3.5],[{kind:'total',period:'ft',side:'over',line:3.5},35,5.5]]){
 const archived={...priced,spec,odd:max,statistical_pct:floor,opportunity:{...decision,policy_id:'gold-goals-v5',min_probability_pct:floor,ht_history_supported:true}};
 assert.equal(api.bestOpportunity([archived],{archived:true})?.id,'regular');
 assert.equal(api.bestOpportunity([{...archived,statistical_pct:floor-.01,opportunity:{...archived.opportunity,min_probability_pct:30}}]),null);
 if(spec.period==='ht')assert.equal(api.bestOpportunity([{...archived,opportunity:{...archived.opportunity,ht_history_supported:false}}]),null);
 assert(api.opportunityPanel([archived]).includes('Proposta archiviata'));
 assert(api.opportunityPanel([archived]).includes('gold-goals-v5'));
}
assert.equal(api.bestOpportunity([{...high,opportunity:{...decision,policy_id:'gold-goals-v3'}}]),null);
assert.equal(api.bestOpportunity([{...priced,odd:4.2,opportunity:{...decision,odds_max:5.5}}]),null);
assert(api.opportunityPanel([{...priced,odd:150}]).includes('Nessuna proposta supera i criteri'));
assert(api.opportunityPanel([priced]).includes('Convergenza di Trend'));
assert(api.movementSignal({...priced,delta_pct:-10}).includes('mx-net-red'));
assert(api.movementSignal({...priced,delta_pct:5}).includes('↑ +5,0%'));
assert(api.movementSignal({...priced,delta_pct:1}).includes('mx-net-green'));
assert(api.movementSignal({...priced,delta_pct:10,quote_status:'stale'}).includes('Storico assente'));
const cell=api.quoteCell({...priced,first_odd:2.2,delta_pct:-9.09,margin_pct:4,oci:55},'1 · Casa');
assert(cell.includes('Apertura'));assert(cell.includes('Attuale'));assert(cell.includes('2,20'));assert(cell.includes('OCI'));assert(cell.includes('Aggio'));
assert(api.opportunityPanel([priced,asian]).includes('MIGLIOR VALORE STIMATO'));
const oldHandicap={...asian,spec:{kind:'handicap',period:'ft',side:'home',line:.25},quote_adapter_version:1,odd:12,oci:99,margin_pct:4};
const legacyMatch={multimarket:{matrix:{rows:[oldHandicap,priced]}}};
const safe=api.rowsOf(legacyMatch);
assert.equal(safe[0].quote_status,'invalid_adapter');
assert.equal(safe[0].oci,null);assert.equal(safe[0].margin_pct,null);
assert.equal(api.bestOpportunity(safe).id,priced.id);
assert.equal(oldHandicap.odd,12);
assert.equal(api.rowsOf({multimarket:{matrix:{rows:[{...oldHandicap,quote_adapter_version:2}]}}})[0].odd,12);
assert.deepEqual(api.rowsOf({multimarket:{markets:[{market:'over25_ht'},{market:'over15_ht'},{market:'score'}]}}).map(r=>r.market),['over25_ht','over15_ht']);
console.log('PASS V5 UI: all 160 fixtures, macro behaviors, market/period filters, immutable inputs and escaped text');
const stale={...priced,odd:null,last_odd:2,quote_status:'stale',opportunity:{...decision,eligible:false,reasons:['price','stale','edge']}};
const remembered={market_id:priced.id,label:'Over 2,5 FT',odd:2,bookmaker:'Example',status:'awaiting_update'};
const pending=api.opportunityPanel([stale],null,{lastProposal:remembered});
assert(pending.includes('Ultima proposta pubblicata'));
assert(pending.includes('In attesa di aggiornamento'));
assert(pending.includes('Quote da aggiornare'));
assert(pending.includes('1 quota da aggiornare'));
assert(!pending.includes('quota fuori dal range'));
assert(!pending.includes('data-opportunity-id'));
assert.equal(api.bestOpportunity([stale]),null);
assert(api.opportunityPanel([stale],null,{lastProposal:{...remembered,status:'withdrawn'}}).includes('Proposta ritirata'));

// A statistical combo must be visible without pretending to have bookmaker odds.
const combo={id:'combo',family:'combo',period:'ft',spec:{kind:'halves_total',period:'ft',side:'over',line:.5,extra:.5},
 label:'Over 0,5 PT + Over 0,5 ST',quote_status:'missing',statistical_pct:60,fair_odd:100/60,
 combo:{policy_id:api.POLICY,active:true,reasons:[],lambda_ht:1.4,lambda_sh:1.8,over25_ft_oci:20,price_available:false}};
assert(api.opportunityPanel([priced,combo]).includes('★ Combo Consigliata'));
assert(api.comboText([combo]).includes('Quota book n.d.'));
assert(api.comboText([combo]).includes('<b>Equa 1,67</b>'));
assert(api.marketRow(combo).includes('EV non calcolabile senza quota reale'));
assert(api.marketRow(combo).includes('Riferimento Over 2,5 FT'));
assert.equal(api.bestOpportunity([combo]),null,'An active price-less combo is not a priced main proposal');
const complete={...matches[0],multimarket:{matrix:{rows:[priced,combo]}}};
assert.equal(api.matchingRows(complete,{...opts,category:'no_quote'}).length,0,'A model-only combo does not flag an otherwise fully priced match');
assert.equal(api.matchingRows({...complete,multimarket:{matrix:{rows:[priced,combo,stale]}}},{...opts,category:'no_quote'}).length,1);
assert.equal(api.missingQuote({...priced,quote_status:'cached_stable'}),false);
const blockedCombo={...combo,combo:{...combo.combo,active:false,reasons:['combo_goalless','combo_oci'],over25_ft_oci:-15},
 goal_evidence:{teams:{home:{ft:{goalless_matches:2}},away:{ft:{goalless_matches:0}}}}};
const blockedPanel=api.opportunityPanel([priced,blockedCombo]);
assert(blockedPanel.includes('Combo non attiva'));
assert(blockedPanel.includes('gare a zero FT casa/ospite 2/0 (max 1)'));
assert(blockedPanel.includes('OCI -15,0'));
assert(!blockedPanel.includes('★ Combo Consigliata'),'Probability above 56% cannot override failed structural/OCI conditions');
assert(api.marketRow(blockedCombo).includes('Sotto soglia'));
assert(!api.comboText([{...combo,combo:{...combo.combo,policy_id:'old'}}]).includes('★ Combo Consigliata'));
assert(api.comboText([]).includes('Dati assenti'));
const estimatedCombo={...combo,quote_status:'recent',odd:3.61,quote_source:'synthetic_combo',
 bookmaker:'Sample',selected_margin_pct:5.263,quote_provenance:{component_margins_pct:[5.263,5.263]}};
assert(api.comboText([estimatedCombo]).includes('Quota sintetica PT × ST'));
assert(!api.comboText([estimatedCombo]).includes('Quota book'));
assert(api.marketRow(estimatedCombo).includes('Massimo sui singoli PT/ST'));
assert(api.marketRow(estimatedCombo).includes('quota Combo non offerta dal book'));
const over25ht={...high,label:'Over 2,5 PT',period:'ht',spec:{kind:'total',period:'ht',side:'over',line:2.5},odd:5,
 opportunity:{...decision,min_probability_pct:18,ht_history_supported:true}};
assert(api.extraGoalText([over25ht,combo]).includes('Over 2,5 PT · Stima'));
assert(api.extraGoalText([over25ht,combo]).includes('Quota book 5,00'));
assert(api.quoteCell({...stale,last_odd:2.4},'Over 2,5 FT').includes('Ultima quota 2,40 · non valida per la proposta'));
assert(api.quoteCell({quote_status:'missing'},'GG PT').includes('Quota del mercato non ricevuta'));
assert(!api.quoteCell({quote_status:'missing'},'GG PT').includes('Ultima quota · da confermare'));
assert(api.extraGoalText([{...over25ht,quote_status:'missing',odd:null}]).includes('Non valutabile'));
assert(api.timingContext({pt_priority_active:true,preferred_pt_market:'over15_ht'}).includes('Precedenza tra idonei'));
assert(api.timingContext({pt_priority_active:false}).includes('Confronto dei cinque mercati gol'));
assert(releaseHtml.includes('<title>ArabSniper · Gold v6.1</title>'));
console.log('PASS Gold v6.1: visible combo decisions, real quote coverage and explicit evaluation order');

// Proximity is a visual cue only: every eligibility boundary stays strict.
const near={...combo,statistical_pct:54,fair_odd:100/54,
 combo:{...combo.combo,active:false,reasons:['combo_probability','combo_rates','combo_oci'],lambda_ht:1.1,lambda_sh:1.35,over25_ft_oci:-12},
 goal_evidence:{teams:{home:{ft:{goalless_matches:1}},away:{ft:{goalless_matches:0}}}}};
assert.equal(api.comboTone(combo),'active');
assert.equal(api.comboTone(near),'near');
assert.equal(api.comboTone(blockedCombo),'inactive');
// The Combo category follows the existing active decision, not Radar or quote availability.
const comboFilter={...opts,category:'combo'};
const comboCases=[[priced,combo],[combo],[priced,near],[priced,blockedCombo],[priced],
 [{...combo,combo:{...combo.combo,policy_id:'old'}}],[{...priced,combo:combo.combo}]]
 .map((rows,i)=>({...matches[i],multimarket:{matrix:{rows}}}));
const comboFrozen=JSON.stringify(comboCases);
assert.deepEqual(api.selectMatches({matches:comboCases},comboFilter,0).map(m=>m.fixture.fixture_id),['1','2']);
assert.deepEqual(api.matchingRows(comboCases[0],comboFilter,0),[combo]);
assert.equal(api.selectMatches({matches:comboCases},{...comboFilter,search:'Casa 1'},0).length,1);
assert.equal(api.selectMatches({matches:comboCases},{...comboFilter,phase:'finished'},0).length,0);
assert.equal(JSON.stringify(comboCases),comboFrozen);
assert.equal(api.bestOpportunity(api.rowsOf(comboCases[0])),priced);
const unchanged=JSON.stringify(near);
assert(api.comboText([near]).includes('mx-combo-near'));
assert(api.comboText([near]).includes('Combo vicina · non attiva'));
assert(!api.comboText([near]).includes('★ Combo Consigliata'));
assert.equal(api.bestOpportunity([near]),null);
assert.equal(JSON.stringify(near),unchanged);
for(const patch of [{statistical_pct:53.999},{combo:{...near.combo,lambda_ht:1.099}},{combo:{...near.combo,lambda_sh:1.349}},
 {combo:{...near.combo,over25_ft_oci:-12.001}},{combo:{...near.combo,over25_ft_oci:null}},{goal_evidence:{}},
 {goal_evidence:{teams:{home:{ft:{goalless_matches:2}},away:{ft:{goalless_matches:0}}}}}]){
 assert.equal(api.comboTone({...near,...patch}),'inactive');
}
assert(api.comboText([combo]).includes('<strong class="mx-combo-active"'));
assert(api.comboText([blockedCombo]).includes('mx-combo-inactive'));
assert(!api.quoteBoard([priced],{}).includes('Prima quota osservata dello stesso bookmaker'));
assert(!api.quoteBoard([priced],{}).includes('mx-quote-legend'));
console.log('PASS Gold cleanup: concise cards, green/yellow/red combo boundaries, unchanged eligibility');

// The blue radar depends only on price evidence; official Gold stays unchanged.
const signal={...priced,id:'monitor-ft',label:'Over 2,5 FT',statistical_pct:43,odd:2.5,first_odd:2.5/.98,
 fair_odd:100/43,delta_pct:-2,oci:30,selected_margin_pct:8,bookmaker:'OPERATORE SINTETICO',
 ev_pct:7.5,ev_low_pct:4,paired_books:3,agreeing_books:2,movement_confirmed:true,fair_direction_coherent:true,
 selected_movement:{delta_pct:-2,fair_delta_pp:1,persistence:.5,reversal:false},
 opportunity:{...decision,eligible:false,strict_eligible:false,rank:null,signal:'excluded',min_ev_low_pct:3.5,reasons:['probability']}};
const signalOptions={now:Date.parse('2026-10-28T10:00:00Z')};
const signalSnapshot=JSON.stringify(signal);
assert.deepEqual(api.marketSignals([signal],signalOptions),[signal]);
assert.equal(api.bestOpportunity([signal],signalOptions),null);
assert.deepEqual(api.marketSignals([signal,priced],signalOptions),[],'A Gold proposal takes precedence over blue monitoring');
assert.deepEqual(api.marketSignals([signal],{...signalOptions,archived:true}),[],'Never reclassify started/archived forecasts');
assert.deepEqual(api.marketSignals([signal],{...signalOptions,monitoringClosed:true}),[],'A live overlay can close monitoring without changing Gold');
const bluePanel=api.opportunityPanel([signal],null,signalOptions);
assert(bluePanel.includes('mx-best-market'));
assert(bluePanel.includes('Radar di Mercato · Statistica debole o assente · Solo studio'));
assert(bluePanel.includes('Persistenza del movimento'));
assert(!bluePanel.includes('Statistica / minimo Gold'));
assert(bluePanel.includes('Solo studio · esclusa dalle giocate Gold ufficiali.'));
assert(bluePanel.includes('0 proposte Gold'));
assert(!bluePanel.includes('data-opportunity-id'));
assert(!bluePanel.includes('Nessuna proposta supera i criteri'));
assert(!api.opportunityPanel([signal,priced],null,signalOptions).includes('data-market-signal-id'));
assert(api.marketRow(signal,{}, {marketSignal:true}).includes('Scartato · stima bassa'));
assert(api.marketRow(signal,{}, {marketSignal:true}).includes('Radar di Mercato · Solo studio'));
assert.equal(JSON.stringify(signal),signalSnapshot,'Display and classification cannot mutate the stored decision');
const excludedPatches=[
 {oci:29.999},{oci:-48},{oci:null},{oci:101},
 {delta_pct:-1.999},{delta_pct:3},{delta_pct:null},{first_odd:null},{first_odd:3},{bookmaker:''},{bookmaker:' '},{bookmaker:true},
 {selected_margin_pct:9.001},{selected_margin_pct:-.1},{selected_margin_pct:null},
 {odd:1},{odd:null},{odd:NaN},{odd:Infinity},
 {quote_status:'stale'},{quote_status:'missing'},{quote_status:'invalid_adapter'},
 {paired_books:1,agreeing_books:1},{paired_books:4,agreeing_books:2},{agreeing_books:4},{agreeing_books:2.5},
 {movement_confirmed:false},{fair_direction_coherent:false},{selected_movement:null},
 ...[{delta_pct:-1.999},{fair_delta_pp:0},{persistence:.499},{persistence:1.001},{persistence:null},{reversal:true}].map(move=>({selected_movement:{...signal.selected_movement,...move}})),
 {spec:{kind:'total',period:'ft',side:'under',line:2.5}},
 {spec:{kind:'halves_total',period:'ft',side:'over',line:.5,extra:.5}}
];
for(const patch of excludedPatches)assert.equal(api.marketSignals([{...signal,...patch}],signalOptions).length,0,JSON.stringify(patch));
const extendedMargin={...signal,selected_margin_pct:9,opportunity:{...signal.opportunity,market_checks_passed:false,min_ev_low_pct:2,reasons:['probability','margin']}};
assert.equal(api.marketSignals([extendedMargin],signalOptions).length,1,'9% margin is admitted only to study');
assert.equal(api.bestOpportunity([extendedMargin],signalOptions),null);
const modelOnlyPatches=[
 {statistical_pct:0},{statistical_pct:90},{statistical_pct:null},{statistical_pct:NaN},{statistical_pct:undefined},
 {ev_pct:-70,ev_low_pct:-90},{ev_pct:null,ev_low_pct:null,fair_odd:null},
 ...['limited','prior_only','prior_dominant',undefined].map(quality=>({quality})),
 {ht_evidence:null,goal_evidence:null},{opportunity:null},{opportunity:undefined},
 ...[{policy_id:'gold-goals-v6'},{structural_supported:false},{ht_history_supported:false},
  {market_checks_passed:false},{min_probability_pct:null},{min_ev_pct:null},{min_ev_low_pct:null},
  {reasons:[]},{reasons:['history','ht_history','structural_missing','ht_symmetry','ht_expected','ht_frequency','isolated_peak','probability','edge','sensitivity','coherence','suspicious_divergence','yield_floor','price']}]
  .map(d=>({opportunity:{...signal.opportunity,...d}}))
];
for(const patch of modelOnlyPatches){
 const candidate={...signal,...patch},before=JSON.stringify(candidate);
 assert.deepEqual(api.marketSignals([candidate],signalOptions),[candidate],'Statistical/value veto leaked into radar: '+JSON.stringify(patch));
 assert.equal(api.bestOpportunity([candidate],signalOptions),null);
 assert(api.opportunityPanel([candidate],null,signalOptions).includes('Radar di Mercato'));
 assert.equal(JSON.stringify(candidate),before);
}
for(const odd of [1.5,2.7,8])assert.equal(api.marketSignals([{...signal,odd,first_odd:odd/.98}],signalOptions).length,1,'Gold yield/odds bounds do not apply to price monitoring');
const noStatistics={...extendedMargin,quality:'prior_only',statistical_pct:null,ev_pct:null,ev_low_pct:null,fair_odd:null,ht_evidence:null,goal_evidence:null,opportunity:null};
assert.deepEqual(api.marketSignals([noStatistics],signalOptions),[noStatistics]);
assert(api.opportunityPanel([noStatistics],null,signalOptions).includes('Persistenza del movimento'));
assert.equal(api.bestOpportunity([noStatistics,priced],signalOptions),priced);
assert.deepEqual(api.marketSignals([noStatistics,priced],signalOptions),[]);
const cachedSignal={...signal,quote_status:'cached_stable',cache_source_at:'2026-10-28T00:00:00Z',cache_expires_at:'2026-10-28T12:00:00Z'};
assert.equal(api.marketSignals([cachedSignal],signalOptions).length,1);
assert.equal(api.marketSignals([cachedSignal],{now:Date.parse('2026-10-28T12:00:00.001Z')}).length,0);
assert.equal(api.marketSignals([cachedSignal],{now:Date.parse('2026-10-27T23:59:59Z')}).length,0);
assert.equal(api.marketSignals([{...cachedSignal,cache_expires_at:null}],signalOptions).length,0);
for(const [kind,period,line,p,odd,minEV,minLow] of [
 ['total','ht',1.5,37,3,0,3.5],['total','ht',2.5,15,7,0,1.5],
 ['total','ft',3.5,32,3.5,0,2.5],['btts','ht',null,23,5,6,.5]
]){
 const s={...signal,period,spec:{kind,period,line,side:kind==='btts'?'yes':'over'},statistical_pct:p,odd,first_odd:odd/.98,
  opportunity:{...signal.opportunity,min_probability_pct:p+3,min_ev_pct:minEV,min_ev_low_pct:minLow,ht_history_supported:true}};
 assert.equal(api.marketSignals([s],signalOptions).length,1,JSON.stringify(s.spec));
 assert.equal(api.bestOpportunity([s],signalOptions),null);
 if(period==='ht')assert.equal(api.marketSignals([{...s,opportunity:{...s.opportunity,ht_history_supported:false}}],signalOptions).length,1,'No PT history veto on the radar');
}
const equivalent={...signal,id:'monitor-asian',family:'asian',spec:{...signal.spec,kind:'asian_total'}};
assert.equal(api.marketSignals([signal,equivalent],signalOptions).length,1,'Equivalent half-lines do not double count');
assert.deepEqual(api.marketSignals([equivalent,signal],signalOptions),api.marketSignals([signal,equivalent],signalOptions),'Stable order independent of feed order');
const futureSignal={...matches[0],fixture:{...matches[0].fixture,kickoff:'2026-10-28T20:00:00Z',status_short:'NS'},multimarket:{matrix:{rows:[signal]}}};
const signalFilter={...opts,category:'market_signal'};
assert.equal(api.selectMatches({matches:[futureSignal]},signalFilter,signalOptions.now).length,1);
assert.equal(api.selectMatches({matches:[futureSignal]},signalFilter,signalOptions.now,()=> 'live').length,0,'Use the displayed live lifecycle even before the scheduled kickoff');
assert.equal(api.selectMatches({matches:[futureSignal]},signalFilter,Date.parse('2026-10-28T20:00:00Z')).length,0);
assert.equal(api.selectMatches({matches:[{...futureSignal,gold_scope:{policy_id:api.SCOPE,allowed:false}}]},signalFilter,signalOptions.now).length,0);
assert.equal(api.matchingRows(futureSignal,{...signalFilter,period:'ht'},signalOptions.now).length,0);
assert.equal(api.matchingRows({...futureSignal,multimarket:{matrix:{rows:[signal,priced]}}},signalFilter,signalOptions.now).length,0);
const escapedBlue=api.opportunityPanel([{...signal,label:'<img src=x onerror=alert(1)>',bookmaker:'<script>x</script>'}],null,signalOptions);
assert(escapedBlue.includes('&lt;img'));assert(!escapedBlue.includes('<img'));assert(!escapedBlue.includes('<script>'));
console.log('PASS market radar: absent/weak statistics and negative EV admitted only to study; confirmed same-book flow, cache TTL, official Gold precedence and immutable decisions');
assert.equal(api.halfTimeText({status:'pending',score:null}),'');
assert(api.halfTimeText({status:'in_progress',score:'0–0'}).includes('PT 0–0</b> · in corso'));
assert(api.halfTimeText({status:'complete',score:'1–0'}).includes('PT 1–0</b> · concluso'));
assert(api.halfTimeText({status:'complete',score:null}).includes('dato non disponibile'));
assert(api.halfTimeText({status:'in_progress',score:'1–0',stale:true}).includes('ultimo rilevato'));


// Shared boundary cases verify Python/client parity without changing Gold.
const platinumCases=JSON.parse(fs.readFileSync('tests/platinum_cases.json','utf8'));
const platinumNow=Date.parse(platinumCases.now),platinumOptions={now:platinumNow,kickoff:platinumCases.kickoff};
const platinumDecision={policy_id:api.PLATINUM_POLICY,eligible:true,rank:1,evaluated_at:platinumCases.now};
for(const test of platinumCases.cases){
 const r={...structuredClone(platinumCases.base),...test.patch,platinum:platinumDecision};
 assert.equal(api.platinumSignals([r],platinumOptions).length>0,test.eligible,test.name);
}
const platinum={...structuredClone(platinumCases.base),platinum:platinumDecision};
assert.equal(api.platinumSignals([platinumCases.base],platinumOptions).length,0,'Old snapshots cannot acquire a new tier');
assert.equal(api.platinumSignals([platinum],{...platinumOptions,now:platinumNow-1}).length,0);
assert.equal(api.platinumSignals([platinum],{...platinumOptions,kickoff:platinumCases.now}).length,0);
const cachedPlatinum={...platinum,quote_status:'cached_stable',cache_source_at:'2026-10-29T00:00:00Z',cache_expires_at:'2026-10-29T12:00:00Z'};
assert.equal(api.platinumSignals([cachedPlatinum],platinumOptions).length,1);
assert.equal(api.platinumSignals([cachedPlatinum],{...platinumOptions,now:Date.parse('2026-10-29T13:00:00Z')}).length,0);
assert.equal(api.platinumSignals([cachedPlatinum],{...platinumOptions,now:Date.parse('2026-10-30T13:00:00Z'),archived:true}).length,1,'Preserve the actual prematch tier after kickoff');
const tierMatch={...matches[0],fixture:{...matches[0].fixture,kickoff:platinumCases.kickoff},multimarket:{matrix:{rows:[platinum]}}};
assert.equal(api.matchingRows(tierMatch,{...opts,category:'platinum'},platinumNow).length,1);
assert.equal(api.matchingRows(tierMatch,{...opts,category:'market_signal'},platinumNow).length,0,'Silver is distinct from blue in category navigation');
assert.equal(api.marketSignals([platinum],platinumOptions).length,1,'The underlying Radar thresholds remain unchanged');
const belowFloor={...platinum,ev_pct:-15.0001};
const excludedMatch={...tierMatch,multimarket:{matrix:{rows:[belowFloor]}}};
assert.equal(api.matchingRows(excludedMatch,{...opts,category:'platinum'},platinumNow).length,0);
assert.equal(api.matchingRows(excludedMatch,{...opts,category:'market_signal'},platinumNow).length,1,'Excluded high odds fall back to unchanged Radar');
assert(!api.opportunityPanel([belowFloor],null,platinumOptions).includes('data-platinum-id'));
assert(api.opportunityPanel([belowFloor],null,platinumOptions).includes('data-market-signal-id'));
const fallbackPlatinum={...platinum,id:'over25',spec:{...platinum.spec,line:2.5},ev_pct:-15,platinum:{...platinumDecision,rank:2}};
assert.equal(api.platinumSignals([belowFloor,fallbackPlatinum],platinumOptions)[0].id,'over25');
const legacyPlatinum={...belowFloor,platinum:{...platinumDecision,policy_id:'platinum-flow-v1'}};
assert.equal(api.platinumSignals([legacyPlatinum],platinumOptions).length,0,'Future fixtures require the new policy');
assert.equal(api.platinumSignals([legacyPlatinum],{...platinumOptions,archived:true}).length,1,'Frozen v1 selections keep their original classification');
assert.equal(api.platinumSignals([legacyPlatinum],{...platinumOptions,monitoringClosed:true}).length,1);
assert.equal(api.platinumSignals([belowFloor],{...platinumOptions,archived:true}).length,0,'New policy never bypasses the floor');
const absentEv={...platinum};delete absentEv.ev_pct;delete absentEv.statistical_pct;
assert.equal(api.platinumSignals([absentEv],platinumOptions).length,1);
for(const ev of [NaN,Infinity,-Infinity])assert.equal(api.platinumSignals([{...platinum,ev_pct:ev}],platinumOptions).length,0);
const both=[platinum,priced],bothBefore=JSON.stringify(both),goldBefore=api.bestOpportunity(both,platinumOptions);
const silverPanel=api.opportunityPanel(both,null,platinumOptions);
assert(silverPanel.includes('data-platinum-id'));
assert(silverPanel.includes('<strong>PLATINUM</strong>'));
assert(silverPanel.includes('Top Pick'));
assert(silverPanel.includes('Giocata ufficiale Gold v6.1'));
assert(silverPanel.includes('data-opportunity-id'));
assert.equal(api.bestOpportunity(both,platinumOptions),goldBefore);
assert.equal(JSON.stringify(both),bothBefore);
assert(api.marketRow(platinum,{}, {platinum:true,marketSignal:true}).includes('<strong>PLATINUM</strong>'));
assert(!api.marketRow(platinum,{}, {platinum:true,marketSignal:true}).includes('Radar di Mercato · Solo studio'));
assert(!/undefined|NaN/.test(silverPanel));
console.log('PASS Platinum: shared flow/value boundaries, missing statistics, separate silver filter, frozen archive and unchanged Gold decisions/ranking');
// Waiting for quotes is visible in Radar without acquiring a confirmed signal.
const waitingRow={...signal,id:'pending-quote',odd:null,first_odd:null,oci:null,quote_status:'missing',opportunity:null};
const waitingOptions={...signalOptions,monitoring:{version:'quote-monitoring-v1',study_only:true,state:'waiting_quotes',row_ids:['pending-quote'],waiting_markets:[{market:'over35_ft'}]}};
assert.equal(api.marketSignals([waitingRow],waitingOptions).length,0);
assert.equal(api.monitoringRows([waitingRow],waitingOptions).length,1);
const waitingPanel=api.opportunityPanel([waitingRow],null,waitingOptions);
assert(waitingPanel.includes('data-market-monitoring="waiting_quotes"'));
assert(waitingPanel.includes('In attesa di quote'));
assert(!waitingPanel.includes('data-market-signal-id'));
assert(!waitingPanel.includes('data-platinum-id'));
assert.equal(api.monitoringRows([waitingRow],{...waitingOptions,archived:true}).length,0);
assert.equal(api.monitoringRows([waitingRow],{...waitingOptions,monitoringClosed:true}).length,0);
assert.equal(api.monitoringRows([waitingRow,priced],waitingOptions).length,0);
assert.equal(api.monitoringRows([waitingRow,platinum],{...waitingOptions,...platinumOptions}).length,0);
