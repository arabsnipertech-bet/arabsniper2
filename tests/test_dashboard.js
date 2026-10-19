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
const researchContext={...context,currentPayload:researchPayload,Map,
 document:{getElementById:()=>host}};
vm.createContext(researchContext);
vm.runInContext(html.slice(html.indexOf('    let researchMarket ='),html.indexOf('    let activeRankingMarket =')),researchContext);
vm.runInContext('renderMatchResearch(currentPayload)',researchContext);
assert(host.innerHTML.indexOf('Match earlier &lt;script&gt;')<host.innerHTML.indexOf('Match later &lt;script&gt;'));
assert(!host.innerHTML.includes('Match started'));
assert(host.innerHTML.includes('IN VERIFICA'));
vm.runInContext("researchFilter('market','combo')",researchContext);
assert(host.innerHTML.includes('Quota specifica assente'));
assert(!host.innerHTML.includes('onclick="addResearchComparison'));
vm.runInContext("addResearchComparison('earlier','over35_ft','over');addResearchComparison('earlier','over35_ft','over');addResearchComparison('later','over35_ft','over')",researchContext);
assert(host.innerHTML.includes('2/3 partite distinte'));
assert(host.innerHTML.includes('assumendo indipendenza'));
vm.runInContext("researchFilter('price','2to5')",researchContext);
assert(host.innerHTML.includes('Nessuna partita con questi criteri'));
console.log('Multimarket: chronological cards, experimental status, absent quotes, unique fixtures and filters passed.');
