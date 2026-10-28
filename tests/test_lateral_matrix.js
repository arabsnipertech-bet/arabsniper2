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

