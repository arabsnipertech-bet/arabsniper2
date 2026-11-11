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
const historicOci=api.marketRow({...rows[2],quote_status:'recent',oci:null,
 oci_unavailable_reason:'nessuna_maggioranza_multi_operatore',
 last_confirmed_oci:{value:-58.42,observed_at:'2026-11-10T11:32:00+01:00'}},
 {timestamp:()=> '11:32'},{v7:{status:'published',niches:{}}});
assert.match(historicOci,/OCI corrente non confermato · Consenso tra bookmaker insufficiente/);
assert.match(historicOci,/Ultimo OCI confermato <b>[^<]+<\/b> · 11:32/);
assert.match(historicOci,/Effetto molla · assente nello snapshot/);
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
assert.equal(api.bestOpportunity([{...priced,odd:1.69}]),null);
assert.equal(api.bestOpportunity([{...priced,odd:1.70}]).id,'regular');
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
assert(cell.includes('Prima osservata'));assert(cell.includes('Attuale'));assert(cell.includes('2,20'));assert(cell.includes('OCI'));assert(cell.includes('Aggio'));
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
assert(api.marketRow(combo).includes('Probabilità senza aggio dello stesso operatore'));
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
const platinumDecision={policy_id:api.PLATINUM_POLICY,rules_revision:'v7-prudence-v1',eligible:true,rank:1,evaluated_at:platinumCases.now};
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
const belowFloor={...platinum,ev_pct:-15};
const excludedMatch={...tierMatch,multimarket:{matrix:{rows:[belowFloor]}}};
assert.equal(api.matchingRows(excludedMatch,{...opts,category:'platinum'},platinumNow).length,0);
assert.equal(api.matchingRows(excludedMatch,{...opts,category:'market_signal'},platinumNow).length,1,'Excluded high odds fall back to unchanged Radar');
assert(!api.opportunityPanel([belowFloor],null,platinumOptions).includes('data-platinum-id'));
assert(api.opportunityPanel([belowFloor],null,platinumOptions).includes('data-market-signal-id'));
const fallbackPlatinum={...platinum,id:'over25',spec:{...platinum.spec,line:2.5},platinum:{...platinumDecision,rank:2}};
assert.equal(api.platinumSignals([belowFloor,fallbackPlatinum],platinumOptions)[0].id,'over25');
const legacyPlatinum={...belowFloor,platinum:{...platinumDecision,rules_revision:undefined,policy_id:'platinum-flow-v1'}};
assert.equal(api.platinumSignals([legacyPlatinum],platinumOptions).length,0,'Future fixtures require the new policy');
assert.equal(api.platinumSignals([legacyPlatinum],{...platinumOptions,archived:true}).length,1,'Frozen v1 selections keep their original classification');
assert.equal(api.platinumSignals([legacyPlatinum],{...platinumOptions,monitoringClosed:true}).length,1);
assert.equal(api.platinumSignals([{...legacyPlatinum,platinum:{...legacyPlatinum.platinum,policy_id:api.PLATINUM_POLICY}}],{...platinumOptions,archived:true}).length,1);
assert.equal(api.platinumSignals([belowFloor],{...platinumOptions,archived:true}).length,0,'New policy never bypasses the floor');
const absentEv={...platinum};delete absentEv.ev_pct;delete absentEv.statistical_pct;
assert.equal(api.platinumSignals([absentEv],platinumOptions).length,0);
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
const secondary={...platinum,oci:50,platinum:{...platinumDecision,secondary_to_gold:true}};
assert(api.opportunityPanel([secondary,priced],null,platinumOptions).includes('data-opportunity-id'));
assert(!api.opportunityPanel([secondary,priced],null,platinumOptions).includes('data-platinum-id'));
assert(api.opportunityPanel([secondary],null,platinumOptions).includes('data-platinum-id'));
console.log('PASS Platinum: shared flow/value boundaries, Gold precedence over high OCI, separate silver filter and frozen archive');
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

// One unavailable goal market must not hide the other quoted goals or label the entire fixture missing.
const monitoredGoals=[
 ['over15_ht','total','ht','over',1.5],['over25_ht','total','ht','over',2.5],
 ['over25_ft','total','ft','over',2.5],['over35_ft','total','ft','over',3.5],
 ['gg_ht','btts','ht','yes',null],['combo_ht_sh','halves_total','ft','over',.5]
].map(([id,kind,period,side,line])=>({...waitingRow,id,period,spec:{kind,period,side,line,...(kind==='halves_total'?{extra:.5}:{})},odd:2,quote_status:'recent',selected_margin_pct:6}));
const partialOptions={...waitingOptions,monitoring:{...waitingOptions.monitoring,row_ids:monitoredGoals.map(r=>r.id),waiting_markets:[{market:'over35_ft',reason:'incomplete_or_invalid_pair'}]}};
const partialPanel=api.opportunityPanel(monitoredGoals.map(r=>r.id==='over35_ft'?{...r,selected_margin_pct:10}:r),null,partialOptions);
assert(partialPanel.includes('5 dei 6 mercati gol'));
assert(partialPanel.includes('Over 3,5 FT · aggio oltre il 9%'));
assert(partialPanel.includes('data-market-monitoring="partial_quotes"'));
assert(!partialPanel.includes('In attesa di quote'));
assert(!/data-(market-signal|platinum|opportunity)-id/.test(partialPanel));

const firstObservation={...priced,odd:2,first_odd:2,delta_pct:0,first_observed_at:'2026-10-31T06:00:00Z',quote_observed_at:'2026-10-31T06:00:00Z'};
assert(api.movementSignal(firstObservation).includes('Prima rilevazione'));
assert(api.movementSignal({...firstObservation,quote_observed_at:'2026-10-31T07:00:00Z'}).includes('Invariata'));
const target=monitoredGoals.find(r=>r.id==='over35_ft');
const asianBoard=api.quoteBoard([{...target,odd:null,quote_status:'missing'},
 {...target,id:'asian35',spec:{...target.spec,kind:'asian_total'},bookmaker:'Book della linea 3,5',odd:2.88,first_odd:2.75}],{});
assert(asianBoard.includes('Book della linea 3,5')&&asianBoard.includes('2,88')&&asianBoard.includes('2,75'));

// The same 54 visible prices yield three distinct V5.1 states. Pressure on the
// away side is study-only and must expire even on a dashboard left open.
{
 const now=Date.parse('2026-10-31T08:00:00Z'),kickoff='2026-10-31T10:00:00Z';
 const wide=Array.from({length:54},(_,i)=>({id:'watch-'+i,spec:{kind:'result',period:'ft',side:'2'},
  quote_status:'recent',odd:1.9,ev_pct:3,quality:'supported',oci:34.2,agreeing_books:4,paired_books:6,
  speed_pp_per_hour:.8,provider_updated_at:new Date(now).toISOString(),quote_observed_at:new Date(now).toISOString()}));
 const monitor={version:'quote-monitoring-v1',study_only:true,state:'observing_flow',priced_markets:54,
  row_ids:[wide[0].id],priced_core:['over15_ht','over25_ft'],watch_id:wide[0].id};
 const panel=(state,extra={})=>api.opportunityPanel(wide,null,{now,kickoff,monitoring:{...monitor,radar_state:state},...extra});
 assert(panel('balanced').includes('Mercato in equilibrio · Monitoraggio attivo'));
 assert(panel('waiting_quotes').includes('<h4>In attesa di quote</h4>'));
 const forming=panel('radar_watch');
 assert(forming.includes('RADAR WATCH · FLUSSO IN FORMAZIONE'));
 assert(forming.includes('2 · Ospite')&&forming.includes('4/6 operatori')&&forming.includes('+34,2'));
 assert(!forming.includes('data-opportunity-id')&&!forming.includes('data-platinum-id'));
 assert(!panel('radar_watch',{now:now+3600001}).includes('FLUSSO IN FORMAZIONE'));
 assert(!panel('radar_watch',{archived:true}).includes('FLUSSO IN FORMAZIONE'));
}

// A saved positive decision on the former scope must not leak into any Gold
// or Radar view while a new five-day projection is being published.
{
 const stale=(country,league,id)=>({...matches[0],gold_scope:{policy_id:'gold-senior-whitelist-v2.1',allowed:true},
  fixture:{...matches[0].fixture,country,league,fixture_id:id}});
 const keep=stale('England','League Two','42'),remove=stale('Romania','Liga III - Serie 1','43');
 const persisted={matches:[remove,keep],gold_scope:{policy_id:'gold-senior-whitelist-v2.1'}};
 assert.deepEqual(api.scopePayload(persisted).matches.map(m=>m.fixture.fixture_id),['42']);
 assert.deepEqual(api.selectMatches(persisted,{...api.state,category:'all'},0).map(m=>m.fixture.fixture_id),['42']);
 for(const category of ['platinum','market_signal']) assert.equal(api.matchingRows(remove,{...api.state,category},0).length,0);
 assert.equal(api.scopePayload({matches:[stale('England','League One','44'),stale('Scotland','League One','45')]}).matches.length,1);
}

// A study card can expose model and quote evidence without feeding it back to eligibility.
{
 const kickoff='2026-10-31T13:00:00+01:00',now=Date.parse('2026-10-31T11:30:00+01:00');
 const at='2026-10-31T09:20:00+01:00',blank={kickoff,now};
 const study={...blank,home:'Casa <A>',profiles:{home:{for_ft:{xg_sample:3,xg_mean:1.2,shots_mean:0}}}};
 const r={...rows[0],statistical_pct:65,fair_odd:1.54,calibration_base:{statistical_pct:50,fair_odd:2},
  first_odd:2.4,odd:2.1,delta_pct:-12.5,oci:0,bookmaker:'Nuovo',paired_books:4,agreeing_books:2,
  opportunity:{...decision,eligible:false},platinum:{...platinumDecision,eligible:false}};
 const before=JSON.stringify(r),board=api.quoteBoard([r],{},study);
 for(const t of ['Analisi Statistica di Base','50,0%','Quota equa Poisson <b>2,00','xG medio FT 1,20','tiri in porta medi FT 0,0','&lt;A&gt;','OCI <b>0,0','-12,5%','2/4'])assert(board.includes(t),t);
 assert(!board.includes('65,0%'));
 assert(!api.quoteBoard([r],{}).includes('Analisi Statistica di Base'));
 assert.deepEqual(api.poissonEstimate({spec:r.spec,statistical_pct:25}),{probability:25,fair:4});
 assert.deepEqual(api.poissonEstimate({spec:r.spec,statistical_pct:null}),{probability:null,fair:null});
 assert.equal(api.poissonEstimate({spec:{kind:'asian_total',line:2.25},statistical_pct:50,fair_odd:1.72}).fair,1.72);
 assert.equal(api.poissonEstimate({spec:{kind:'handicap',line:.25},statistical_pct:50}).fair,null);
 const snapshot={first_odd:2.4,odd:2.04,oci:32,first_observed_at:'2026-10-31T08:00:00+01:00',quote_observed_at:at,
  provider_updated_at:at,bookmaker:'Archivio',paired_books:6,agreeing_books:4,speed_pp_per_hour:1.2,fair_delta_pp:1.4};
 const stale={...r,odd:null,oci:null,quote_status:'stale',cached_quote:snapshot};
 const stored=api.quoteCell(stale,'Over 2,5 FT',{},study);
 for(const t of ['Ultima osservata','Ultimo OCI osservato','+32,0','-15,0%','4/6','Archivio',at,'Flusso storico','1,2 pp/ora'])assert(stored.includes(t),t);
 const partial={...r,oci:null,study_flow:snapshot};
 const newer=api.quoteCell(partial,'Over 2,5 FT',{},study);
 for(const t of ['Attuale<b>2,10','Ultimo OCI osservato','Nuovo','Archivio','4/6',at])assert(newer.includes(t),t);
 assert(!newer.includes('Ultima osservata<b>2,04'));
 assert(api.marketRow(partial,{}, {study}).includes('Ultimo OCI osservato · Archivio'));
 assert(api.marketRow(stale,{}, {study}).includes('prima osservata 2,40'));
 assert.equal(api.studyObservation(stale,{...blank,now:Date.parse(at)-1}),null);
 assert.equal(api.studyObservation(stale,{...blank,kickoff:at}),null);
 const invalid={...stale,quote_status:'invalid_adapter'};
 assert.equal(api.studyObservation(invalid,study),null);
 assert(!api.quoteCell({...r,oci:null,odd:null,quote_status:'missing'},'studio',{},study).includes('Ultimo OCI osservato'));
 for(const row of [r,partial,stale]){
  assert.equal(api.bestOpportunity([row],blank),null);
  assert.equal(api.platinumSignals([row],blank).length,0);
 }
 assert.equal(JSON.stringify(r),before);
}

// Real fixture identity, controlled quotes shared with the Python settlement test.
{
 const {execFileSync}=require('node:child_process');
 const m=JSON.parse(execFileSync('python',['-c',
  "import sys,json;sys.path[:0]=['src','tests'];from test_selection_studies import perth_fixture; print(json.dumps(perth_fixture()))"],{encoding:'utf8'}));
 const monitor=m.multimarket.matrix.quote_monitoring;
 const frozen=JSON.stringify(monitor);
 const html=api.opportunityPanel(m.multimarket.matrix.rows,null,{now:Date.parse(monitor.as_of),kickoff:m.fixture.kickoff,monitoring:monitor});
 assert.equal(JSON.stringify(monitor),frozen);
 for(const label of ['RADAR BLU · MONITORAGGIO','SOLO STUDIO',
  '<li>1 FT · Quota 2,40 (valore e flusso)</li>','<li>Over 1,5 PT · Quota 2,50 (pressione book, statistica debole)</li>',
  '<li>1X + Over 2,5 FT · Quota 3,20 (combo correlata)</li>'])assert(html.includes(label),label);
 assert.equal((html.match(/<li>/g)||[]).length,5);
 assert(html.includes('<li>Over 2,5 FT'));
 assert(!/data-opportunity-id|data-platinum-id|Fuori Gold|mercato escluso|Poisson|OCI|EV [%P]|%|<br>/i.test(html));
 assert.equal((html.match(/data-best-study/g)||[]).length,0);
 // Even an old feed with a stale backend badge cannot star a remote outsider.
 const outsider={...monitor.studio_ideas[0],label:'Venezia 2',market:'1x2_2',odd:9.50,poisson_pct:31.999,best_study:true};
 const panel=i=>api.opportunityPanel(m.multimarket.matrix.rows,null,{now:Date.parse(monitor.as_of),kickoff:m.fixture.kickoff,monitoring:{...monitor,studio_ideas:[i]}});
 assert(!panel(outsider).includes('Venezia 2'));
 assert(!panel(outsider).includes('data-best-study'));
 assert(!panel({...outsider,poisson_pct:32}).includes('data-best-study'));
 assert(!panel({...outsider,market:'1x2_1',odd:2,poisson_pct:40}).includes('data-best-study'));
 assert(panel({...outsider,market:'1x2_1',odd:2,poisson_pct:60,quality:'supported',quote_executable:true,quote_status:'recent',ev_low_pct:0,star_gate:{eligible:true}}).includes('data-best-study'));

}

{
 const {execFileSync}=require('node:child_process');
 for(const name of ['santander','elfsborg']){
  const m=JSON.parse(execFileSync('python',['-c',"import sys,json;sys.path[:0]=['src','tests'];from test_selection_studies import distortion_fixture;print(json.dumps(distortion_fixture('"+name+"')))"],{encoding:'utf8'}));
  const monitor=m.multimarket.matrix.quote_monitoring, frozen=JSON.stringify(monitor);
  const html=api.opportunityPanel(m.multimarket.matrix.rows,null,{now:Date.parse(monitor.as_of),kickoff:m.fixture.kickoff,monitoring:monitor});
  assert.equal(JSON.stringify(monitor),frozen);
  assert(!html.includes('Quota non disponibile (combo correlata)'));
  assert(!html.includes('a 7,50')&&!html.includes('a 12,00')&&!html.includes('a 5,20'));
  assert(html.includes('Over 1,5 PT')||html.includes('Over 3,5 FT')||html.includes('GG FT'));
 }
}

// Even a stale real_flow=true flag cannot label a flat/rising quote as flow.
{
 const idea={type:'studio_1x2_value_flow',market:'1x2_1',label:'1 FT',odd:2.4,
  poisson_pct:45,market_fair_pct:42,fair_delta_pp:3,real_flow:true};
 const options={now:Date.parse('2026-11-01T07:00:00Z'),kickoff:'2026-11-01T09:00:00Z'};
 for(const delta of [0,6.1,-.5]){
  const html=api.opportunityPanel([],null,{...options,monitoring:{studio_ideas:[{...idea,delta_pct:delta}]}});
  assert.equal(html.includes('valore e flusso'),delta<0);
 }
 const html=api.opportunityPanel([],null,{...options,monitoring:{studio_ideas:[{...idea,market_fair_pct:null}]}});
 assert(!html.includes('Quota 2,40'));
}

{
 const a={id:'pt',label:'Over 2,5 PT',spec:{kind:'total',period:'ht',side:'over',line:2.5},odd:2,quote_status:'recent',statistical_pct:40,oci:0,selected_margin_pct:5};
 const b={...a,id:'ft',label:'Over 2,5 FT',spec:{...a.spec,period:'ft'},statistical_pct:55};
 const idea={spec:b.spec,label:b.label,market:'over25_ft',odd:2,poisson_pct:55,best_study:true,quality:'supported',quote_executable:true,quote_status:'recent',ev_low_pct:1};
 const m={fixture:{fixture_id:909,kickoff:'2099-01-01T20:00:00Z'},multimarket:{matrix:{rows:[a,b],quote_monitoring:{studio_ideas:[idea]}}}};
 const frozen=JSON.stringify(m);
 assert.equal(api.studyDefaultRow(m),b,'Select the existing starred market, including its period');
 const html=api.cockpit(m,{},true);
 assert(html.includes('Miglior spunto · Over 2,5 FT'));
 assert(html.includes('MIGLIOR SPUNTO · SOLO STUDIO'));
 assert(!html.includes('mx-instrument-empty'),'Zero OCI is available data');
 assert.equal(api.cockpitTier(m).name,'STUDIO');
 assert.equal(JSON.stringify(m),frozen,'Display default cannot mutate feed or ranking');
 for(const change of [{suppressed:true},{best_study:false},{poisson_pct:31},{spec:{...b.spec,line:3.5}}]){
  const copy=structuredClone(m);Object.assign(copy.multimarket.matrix.quote_monitoring.studio_ideas[0],change);
  assert.equal(api.studyDefaultRow(copy),null,'No guessed alternative when the best study is absent or excluded');
 }
 const missing=structuredClone(m);missing.multimarket.matrix.rows[1].oci=null;
 assert(api.cockpit(missing,{},true).includes('mx-instrument-empty'),'Missing OCI stays missing');
 const absent=structuredClone(m);absent.multimarket.status='no_prematch_snapshot';assert.equal(api.studyDefaultRow(absent),null);
 console.log('PASS Study cockpit: existing best study selected, exact market, zero/missing data, no invented fallback or payload changes');
}

{
 const now=Date.now(),iso=n=>new Date(n).toISOString();
 const row=(id,p,low)=>({id,label:id,spec:{kind:'total',period:'ft',side:'over',line:id==='a'?2.5:3.5},quality:'supported',quote_executable:true,quote_status:'recent',odd:2,statistical_pct:p,low_pct:low,provider_updated_at:iso(now-60000),quote_observed_at:iso(now-60000),ev_pct:-10,ev_low_pct:-20});
 const a=row('a',60,40),b=row('b',55,45);
 const m={fixture:{fixture_id:910,kickoff:iso(now+7200000),status_short:'NS'},multimarket:{matrix:{rows:[a,b],quote_monitoring:{studio_ideas:[]}}}};
 const before=JSON.stringify(m);
 assert.equal(api.studyDefaultRow(m,now),b,'Prudent probability selects the study display, even without a star');
 assert(api.compactProposal(m).includes('Miglior proposta Studio'));assert(api.compactProposal(m).includes('<strong>b</strong>'));
 const html=api.cockpit(m,{},true);assert(html.includes('STUDIO STATISTICO · VALORE NON CONFERMATO'));assert(html.includes('Stima disponibile per lo studio'));assert(!html.includes('⭐'));
 assert.equal(JSON.stringify(m),before,'Display fallback cannot mutate ranking, certificates or forecasts');
 for(const change of [{quality:'limited'},{quote_executable:false},{quote_status:'stale'},{odd:null},{statistical_pct:null},{provider_updated_at:iso(now-7200000)},{provider_updated_at:iso(now+60000)}]){
  const copy=structuredClone(m);copy.multimarket.matrix.rows=copy.multimarket.matrix.rows.map(r=>({...r,...change}));assert.equal(api.studyDefaultRow(copy,now),null);
 }
 const history=structuredClone(m);history._gold_history={verified_prematch:true};assert.equal(api.studyDefaultRow(history,now),null);
 const live=structuredClone(m);live.fixture.kickoff=iso(now-1);assert.equal(api.studyDefaultRow(live,now),null);
 const absent=structuredClone(m);absent.multimarket.status='no_prematch_snapshot';assert.equal(api.studyDefaultRow(absent,now),null);
 console.log('PASS Supported study default: prudent ranking, fresh quotes, no certification, immutable history');
}
// Overview exposes only 1X2 prices; other markets and research stay in the dialog.
const compactRows=[['1',2.50,2.20],['x',3.10,3.10],['2',4,4.40]].map(([side,first_odd,odd])=>({
 period:'ft',spec:{kind:'result',period:'ft',side},odd,first_odd,delta_pct:-5,quote_status:'recent'
}));
const compact=api.compactQuotes(compactRows);
assert.equal((compact.match(/<section aria-label/g)||[]).length,3);
assert.match(compact,/2,50 → <strong class="mx-compact-price">2,20/);
assert.match(compact,/data-movement="down"[^>]*>↓ -12,0%/);
assert.match(compact,/data-movement="flat"[^>]*>→ 0,0%/);
assert.match(compact,/data-movement="up"[^>]*>↑ \+10,0%/);
assert(!compact.includes('Poisson'));
const compactCard=api.card({fixture:{fixture_id:'compact',match:'A - B'},multimarket:{matrix:{rows:compactRows}}},{});
assert.match(compactCard,/<\/header><div class="mx-compact-quotes"/);
assert(compactCard.indexOf('mx-compact-quotes')<compactCard.indexOf('mx-compact-proposal'));
assert(compactCard.includes('Apri analisi'));assert(!compactCard.includes('mx-cockpit'));assert(!compactCard.includes('mx-study-base'));
const quoteOnlyCard=api.card({fixture:{fixture_id:'no-forecast',match:'A - B'},multimarket:{status:'no_prematch_snapshot',matrix:{rows:compactRows}}},{});
assert(quoteOnlyCard.includes('mx-compact-quotes'),'Real 1X2 prices remain visible without an eligible forecast');
assert(quoteOnlyCard.includes('non apertura ufficiale'));
const unconfirmed=api.compactQuotes([{...compactRows[0],quote_status:'missing',last_odd:2.2}]);
assert(unconfirmed.includes('Da confermare'));assert(!unconfirmed.includes('↓'));
assert.equal(api.compactQuotes([{...compactRows[0],odd:null,first_odd:null}]),'');
const quoteNow=Date.now(),observed=new Date(quoteNow-3600000).toISOString();
const oneObservation={...compactRows[1],first_observed_at:observed,quote_observed_at:observed,quote_status:'cached_stable'};
assert.match(api.compactQuotes([oneObservation]),/title="Prima rilevazione">—<\/b><small>Cache/);
const past={...compactRows[0],quote_status:'stale',last_odd:2.2,bookmaker:'Book A',quote_observed_at:observed,provider_updated_at:observed};
const history=api.compactQuotes([past],{kickoff:new Date(quoteNow-1800000).toISOString(),closed:true});
assert(history.includes('Prima osservata → Ultima prematch'));
assert.match(history,/↓ -12,0%<\/b><small>Ultima/);
console.log('PASS compact 1X2: placement, paired prices, three directions, cache and unconfirmed history');

{
// Real matrix-shaped readings, including cached_quote which omits movement_hours.
const oneBook={oci:null,paired_books:1,agreeing_books:1,bookmaker:'Book A',quote_status:'cached_stable',odd:2,
 selected_movement:{fair_delta_pp:2,persistence:1,reversal:false,observed_from:'2026-11-08T08:00:00Z',observed_to:'2026-11-08T10:00:00Z'},
 movement_hours:2,speed_pp_per_hour:1};
const frozen=JSON.stringify(oneBook);
assert(Math.abs(api.displayOci(oneBook).value-(20+25+10/1.5+15+1.25))<1e-9);
const revisedOci={...oneBook,oci_method:'oci-as-intermediate-persistence-v1'};
assert(Math.abs(api.displayOci(revisedOci).value-api.displayOci(oneBook).value+15)<1e-9);
assert.equal(api.displayOci({...revisedOci,selected_movement:{...oneBook.selected_movement,persistence:.5}}).value,
 api.displayOci(oneBook).value);
const cached=structuredClone(oneBook);delete cached.movement_hours;
assert.equal(api.displayOci(cached).value,api.displayOci(oneBook).value);
const negative=structuredClone(cached);negative.selected_movement.fair_delta_pp=-2;
assert.equal(api.displayOci(negative).value,-api.displayOci(cached).value);
const flat=structuredClone(cached);flat.selected_movement.fair_delta_pp=0;flat.speed_pp_per_hour=0;
assert.equal(api.displayOci(flat).value,0);
assert.equal(api.displayOci({...oneBook,oci:72,paired_books:3}).value,72);
assert.equal(api.displayOci({...oneBook,paired_books:2}).value,null);
assert.equal(api.displayOci({...oneBook,selected_movement:null}).value,null);
assert.equal(api.displayOci({...oneBook,quote_source:'synthetic_combo'}).value,null);
const invalid=structuredClone(oneBook);invalid.selected_movement.observed_to=invalid.selected_movement.observed_from;
assert.equal(api.displayOci(invalid).value,null);
assert(api.quoteCell(oneBook,'Test').includes('Singolo bookmaker'));
assert(api.marketRow({...oneBook,spec:{kind:'total'},label:'Test'}).includes('Singolo bookmaker'));
assert.equal(JSON.stringify(oneBook),frozen);
console.log('PASS matrix single-book OCI: cached history, zero, sign, invalid history, multi-book parity, table, quote panel and immutability');

}


const v7Time='2026-11-09T18:00:00+01:00';
const v7Match={fixture:{fixture_id:'7',kickoff:v7Time},multimarket:{matrix:{rows:[],v7:{
 version:'arabsniper-v7.0',status:'shadow',fixture_id:'7',kickoff:v7Time,captured_at:'2026-11-09T16:00:00Z',
 rates:{home_ht:.72,home_ft:1.7,away_ht:.51,away_ft:1.3},means:{},
 signals:{lineare:{market_id:'over',reasons:['probabilita_e_prezzo']}},
 markets:{over:{label:'Over 2,5 FT <test>',odd:2.15,calibrated:.54,raw:.5,fair:null}}
}}}};
assert.match(api.v7Panel(v7Match), /Over 2,5 FT &lt;test&gt;/);
assert.match(api.v7Panel(v7Match), /54,0%/);
assert.match(api.v7Panel(v7Match), /Fair mercato <b>—<\/b>/);
assert.match(api.v7Panel(v7Match,{},true), /Archivio V7/);
v7Match.multimarket.matrix.v7.captured_at=v7Time;
assert.equal(api.v7Panel(v7Match),'','An observation at kickoff cannot appear as prematch');

// Every active V7 card reserves three slots, with trusted saved evidence only.
const triadNow=Date.now(), triadAt=new Date(triadNow-60000).toISOString();
const triadStart=new Date(triadNow+3600000).toISOString(), triadModel='arabsniper-v7.0';
const triadRow={id:'st-goals',family:'totals',period:'sh',
 spec:{kind:'total',period:'sh',side:'over',line:1.5},probability_model:triadModel,odd:2.10,
 quote_executable:true,quote_status:'recent',quote_observed_at:triadAt,provider_updated_at:triadAt};
const triadSignal={market_id:'st-goals',source_market_id:triadRow.id,odd:2.10,probability:.54,ev:.134,
 tier:'GOLD',badge:'● GOLD',horizon:'ST',horizon_label:'[2° TEMPO]',
 niche_id:'pt_st_symmetry_11_22',niche_label:'Simmetria 1°+2°T'};
const triadMatch={fixture:{fixture_id:'701',kickoff:triadStart},multimarket:{matrix:{
 publication_model:triadModel,rows:[triadRow],v7:{version:triadModel,status:'published',
  fixture_id:'701',kickoff:triadStart,captured_at:triadAt,tier:'GOLD',
  triad:{most_probable:triadSignal,recommended_alternative:null,high_odds_shot:null},
  markets:{'st-goals':{source_market_id:triadRow.id,label:'Over 1,5 ST',odd:2.10,calibrated:.54}}}
}}};
const triadHtml=api.card(triadMatch,{});
assert.equal((triadHtml.match(/data-v7-slot=/g)||[]).length,3);
for(const label of ['● GOLD','Over 1,5 ST','[2° TEMPO]','54,0%','EV +13,4%','Simmetria 1°+2°T'])
 assert(triadHtml.includes(label),label);
assert.equal((triadHtml.match(/Nessun mercato idoneo/g)||[]).length,2);
triadRow.quote_observed_at=triadStart;
assert.match(api.card(triadMatch,{}),/Quota da aggiornare/);
assert(!api.card(triadMatch,{}).includes('Over 1,5 ST'));

const scope=require('../web/competition-scope.js');
assert(scope.allows(scope.id,'World','UEFA Nations League - League C'));
assert(scope.allows(scope.id,'World','World Cup - Qualification CONMEBOL'));
assert(!scope.allows(scope.id,'World','Friendlies'));
assert(!scope.allows(scope.id,'World','UEFA Nations League Women'));

const study=structuredClone(triadMatch),studyMatrix=study.multimarket.matrix;
const oldPrice=new Date(triadNow-36*3600000).toISOString();
Object.assign(studyMatrix.rows[0],{quote_observed_at:oldPrice,provider_updated_at:oldPrice,
 quote_status:'cached_stable',prematch_stable:true,cache_expires_at:triadStart,first_odd:2.10});
Object.assign(studyMatrix.v7.triad.most_probable,{tier:'BLU',badge:'◆ BLU',study_only:true,quote_state:'stable'});
studyMatrix.v7.markets['st-goals'].model_fair_odd=1.85;
const estimate={id:'pt-goals',family:'totals',period:'ht',spec:{kind:'total',period:'ht',side:'over',line:1.5},
 probability_model:triadModel,odd:null,quote_status:'missing'};
studyMatrix.rows.push(estimate);
studyMatrix.v7.markets[estimate.id]={source_market_id:estimate.id,label:'Over 1,5 PT',odd:null,calibrated:.40,model_fair_odd:2.50};
studyMatrix.v7.triad.recommended_alternative={market_id:estimate.id,source_market_id:estimate.id,
 tier:'BLU',badge:'◆ BLU',study_only:true,odd:null,quote_state:'model_estimate',probability:.40,
 model_fair_odd:2.50,horizon_label:'[1° TEMPO]'};
const studyHtml=api.card(study,{});
for(const label of ['Stabile da apertura','Quota equa 1,85','Quota equa 2,50','Stima V7 · in attesa di quote bookmaker'])
 assert(studyHtml.includes(label),label);
assert.equal(api.publishedView(study,triadNow).tier,'blu');
studyMatrix.rows[0].quote_status='recent';
studyMatrix.rows[0].prematch_stable=false;
studyMatrix.v7.triad.most_probable.quote_state='moving';
assert.equal(api.publishedView(study,triadNow).triad.most_probable.signal.quote_state,'stable');
studyMatrix.v7.triad.recommended_alternative.tier='GOLD';
assert.equal(api.publishedView(study,triadNow).triad.recommended_alternative,null);

const unquoted=structuredClone(triadMatch), unquotedMatrix=unquoted.multimarket.matrix;
unquotedMatrix.v7.triad={most_probable:null,recommended_alternative:null,high_odds_shot:null};
const emptyCard=api.card(unquoted,{});
assert.match(emptyCard,/Nessuna proposta prematch disponibile/);
assert.doesNotMatch(emptyCard,/Nessun mercato idoneo/);

const boardHost={};
const activeFixture={...matches[0],fixture:{...matches[0].fixture,fixture_id:'801',status_short:'NS',match:'In calendario'}};
const postponedFixture={...activeFixture,fixture:{...activeFixture.fixture,fixture_id:'802',status_short:'PST',
 kickoff:new Date(triadNow-86400000).toISOString(),match:'Orfana rinviata'},
 multimarket:{status:'no_prematch_snapshot',matrix:{rows:[]}}};
api.render({matches:[postponedFixture,activeFixture],gold_scope:{policy_id:scope.id}},boardHost);
assert(boardHost.innerHTML.indexOf('In calendario')<boardHost.innerHTML.indexOf('Rinviate, annullate o sospese'));
assert.match(boardHost.innerHTML,/<details class="mx-reading-help mx-inactive"><summary>[^<]+<\/summary>[\s\S]*Orfana rinviata/);
