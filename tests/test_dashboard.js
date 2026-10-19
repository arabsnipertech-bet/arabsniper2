// Exercise the two selection views against the production rendering function.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../web/gold-dashboard-live.html'), 'utf8');
for (const [, script] of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) {
  new vm.Script(script);
}
let section;
const future = hours => new Date(Date.now() + hours * 3600000).toISOString();
const row = (match, hours, value = false) => ({match, kickoff: future(hours), time: '15:00',
  country: 'Italy', league: 'Test', probability_pct: 52, odd: 1.8, expected_value_pct: -6.4,
  gate: {eligible: value}, probability_assessment: {eligible: true, value_eligible: value,
    components: {book_fair_pct: 50, poisson_pct: 52, empirical_pct: 50}}});
const payload = {updated_at: future(-1), probability_rankings: {over15_ht: [
  row('Later research match', 3), row('Earlier <script> match', 2), row('Started match', -1)]},
  market_rankings: {over15_ht: [row('Value match', 4, true)], gg_ht: [], over25_ft: []}};
const context = {Date, Number, Math, currentPayload: payload,
  document: {getElementById: () => ({remove() {}}),
    createElement: () => (section = {}), querySelector: () => ({before() {}})},
  num: x => Number(x) || 0, formatPct: x => x == null ? 'n/d' : x + '%',
  formatOdd: x => String(x), formatTimestamp: x => x,
  escapeHtml: x => String(x ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
  RELATION_LABELS: {}};
vm.createContext(context);
vm.runInContext(html.slice(html.indexOf("    let activeRankingMarket ="),
                           html.indexOf('    function setRelationFilter(')), context);
vm.runInContext('renderMarketRankings(currentPayload)', context);
assert(section.innerHTML.includes('Book senza margine'));
assert(section.innerHTML.includes('Fuori dal filtro valore della quota'));
assert(section.innerHTML.indexOf('Earlier &lt;script&gt; match') < section.innerHTML.indexOf('Later research match'));
assert(!section.innerHTML.includes('Started match'));
assert(!section.innerHTML.includes('Value match'));
vm.runInContext("selectRankingView('value')", context);
assert(section.innerHTML.includes('Value match'));
assert(!section.innerHTML.includes('Earlier &lt;script&gt; match'));
vm.runInContext("selectRankingMarket('gg_ht')", context);
assert(section.innerHTML.includes('Nessuna selezione disponibile'));
console.log('Dashboard: syntax, separate views, chronology, started-match exclusion and escaping passed.');

// The new view must preserve chronology, missing prices and per-fixture comparisons.
const host={};
const market=(key='over35_ft',odd=3)=>({market:key,selection:key==='over15_ht_over05_sh'?'yes':'over',label:key==='over35_ft'?'Over 3,5 finale':'PT + ST',probability_pct:37,
  statistical_pct:40,book_probability_pct:32,model_range_pct:[32,40],odd,quote:odd?{bookmaker:'Book <script>',provider_updated_at:future(-1)}:null,
  reasons:odd?['valore_non_concorde_tra_modelli']:['quota_specifica_assente'],research_candidate:false,
  conservative_ev_pct:-4,expected_value_pct:11,movement:{paired_books:0}});
const fixture=(id,hours)=>({fixture:{fixture_id:id,match:'Match '+id+' <script>',time:'16:00',kickoff:future(hours),country:'Italy',league:'Test'},
  multimarket:{status:'experimental',markets:[market(),market('over15_ht_over05_sh',null)],rates:{combined:[.6,.6,.8,.8]}}});
const researchPayload={updated_at:future(-1),multimarket_method:{model_id:'coherent-markets-v1'},matches:[fixture('later',3),fixture('earlier',2),fixture('started',-1)]};
researchPayload.matches[1].multimarket.markets.push({...market('over15_ht',2.2),
  selection:'under',label:'Under 1,5 PT',research_candidate:true});
const researchContext={...context,currentPayload:researchPayload,Map,
 document:{getElementById:()=>host}};
vm.createContext(researchContext);
vm.runInContext(html.slice(html.indexOf('    const RESEARCH_MARKET_OPTIONS ='),html.indexOf('    let activeRankingMarket =')),researchContext);
vm.runInContext('renderMatchResearch(currentPayload)',researchContext);
assert(host.innerHTML.indexOf('Match earlier &lt;script&gt;')<host.innerHTML.indexOf('Match later &lt;script&gt;'));
assert(!host.innerHTML.includes('Match started'));
assert(host.innerHTML.includes('IN VERIFICA'));
assert(host.innerHTML.includes('Under 1,5 PT'));
assert(host.innerHTML.includes('1 con candidati sperimentali'));
vm.runInContext("researchFilter('market','combo')",researchContext);
assert(host.innerHTML.includes('Quota specifica assente'));
assert(!host.innerHTML.includes('onclick="addResearchComparison'));
vm.runInContext("addResearchComparison('earlier','over35_ft','over');addResearchComparison('earlier','over35_ft','over');addResearchComparison('later','over35_ft','over')",researchContext);
assert(host.innerHTML.includes('2/3 partite distinte'));
assert(host.innerHTML.includes('assumendo indipendenza'));
vm.runInContext("researchFilter('price','2to5')",researchContext);
assert(host.innerHTML.includes('Nessuna partita con questi criteri'));
console.log('Multimarket: chronological cards, experimental status, absent quotes, unique fixtures and filters passed.');


vm.runInContext("researchFilter('price','all');researchFilter('market','over35_ft::over')",researchContext);
assert.equal(vm.runInContext('researchMarkets(currentPayload.matches[0]).length',researchContext),1);
assert.equal(vm.runInContext('researchMarkets(currentPayload.matches[0])[0].market',researchContext),'over35_ft');
vm.runInContext("researchFilter('market','all');researchSelectFixture('earlier','over15_ht::under')",researchContext);
assert.equal(vm.runInContext("researchFocus.get('earlier')",researchContext),'over15_ht::under');
assert(host.innerHTML.includes('Valuta una giocata per questa partita'));
assert(host.innerHTML.includes('over05_ht_over05_sh::yes'));
assert(host.innerHTML.includes('over15_ht_over15_sh::yes'));
assert(host.innerHTML.includes('over15_ft::over'));
assert(host.innerHTML.includes('gg_ht::no'));
vm.runInContext("researchFilter('market','ht')",researchContext);
assert(host.innerHTML.includes('<details open>'));
console.log('Exact market filters and per-fixture selection passed.');

// Suggestions compare only the same exact market/side, not raw cross-market probability.
const suggestionMarket=(key,side,p,odd=2.5)=>({...market(key,odd),market:key,selection:side,
  label:key+' '+side,probability_pct:p,model_range_pct:[p-2,p+2],research_candidate:true,
  conservative_ev_pct:5,book_probability_pct:p-3});
const suggestionFixtures=Array.from({length:5},(_,i)=>({fixture:{fixture_id:'s'+i,kickoff:future(2),match:'Suggestion '+i},
  multimarket:{model_id:'coherent-markets-v2',markets:[
    suggestionMarket('gg_ht','yes',i===0?35:20+i,4),
    suggestionMarket('over05_ht','over',i===0?70:80+i),
    suggestionMarket('over25_ft','over',i===0?60:40+i),
    suggestionMarket('over35_ft','over',i===0?40:30+i,3.5),
    suggestionMarket('gg_ht','no',i===0?65:80-i,3)
  ]}}));
researchContext.suggestionFixtures=suggestionFixtures;
let suggested=vm.runInContext('researchSuggestions(suggestionFixtures[0],suggestionFixtures)',researchContext);
assert.equal(suggested[0].pick.row.market,'gg_ht');
assert.equal(suggested[0].pick.row.probability_pct,35);
assert.equal(suggested[1].pick.row.market,'over25_ft');
assert.equal(suggested[2].pick.row.market,'over35_ft');
assert.equal(new Set(suggested.map(x=>x.pick.row.market)).size,3);
vm.runInContext("researchFilter('market','over35_ft::over');researchFilter('price','2to5')",researchContext);
assert.equal(vm.runInContext('researchSuggestions(suggestionFixtures[0],suggestionFixtures)[0].pick.row.market',researchContext),'gg_ht');
assert(vm.runInContext('researchSuggestions(suggestionFixtures[0],suggestionFixtures.slice(0,4)).every(x=>!x.pick)',researchContext));
const stale=JSON.parse(JSON.stringify(suggestionFixtures));
stale[0].multimarket.markets.forEach(r=>r.quote.provider_updated_at=future(-7));
researchContext.stale=stale;
assert(vm.runInContext('researchSuggestions(stale[0],stale).every(x=>!x.pick)',researchContext));
const noEV=JSON.parse(JSON.stringify(suggestionFixtures));
noEV[0].multimarket.markets.forEach(r=>r.conservative_ev_pct=-1);
researchContext.noEV=noEV;
assert(vm.runInContext('researchSuggestions(noEV[0],noEV).every(x=>!x.pick)',researchContext));
const unpriced=JSON.parse(JSON.stringify(suggestionFixtures));
unpriced[0].multimarket.markets.forEach(r=>r.quote=null);
researchContext.unpriced=unpriced;
assert(vm.runInContext('researchSuggestions(unpriced[0],unpriced).every(x=>!x.pick)',researchContext));
console.log('Three proposals: exact-market ranking, unchanged probability, distinct markets, stable filters, missing/stale evidence passed.');
