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


// One proposal: quality before agreement; independent of other fixtures.
const single=(key,stat,book,p,ev,candidate=true)=>({...market(key,2),market:key,selection:'over',label:key,
 statistical_pct:stat,book_probability_pct:book,probability_pct:p,bookmakers_count:3,
 conservative_ev_pct:ev,research_candidate:candidate,reasons:[]});
researchContext.singleMatch={fixture:{fixture_id:'single'},multimarket:{markets:[
 single('over15_ht',55,52,54,5),single('over25_ft',90,50,70,20,false),single('over35_ft',45,43,44,1)]}};
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[]).length',researchContext),1);
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[])[0].pick.row.market',researchContext),'over15_ht');
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[singleMatch])[0].pick.row.probability_pct',researchContext),54);
let result=vm.runInContext('researchSuggestionsHtml(singleMatch,[])',researchContext);
assert(result.includes('GG PT sì — entrambe segnano'));
assert(result.includes('Dati non disponibili per questo mercato'));
assert(result.includes('Fonti divergenti'));
assert(!result.includes('tre migliori'));
assert(result.includes('Statistica:'));
vm.runInContext("singleMatch.multimarket.markets[0].quote.provider_updated_at='2000-01-01T00:00:00Z'",researchContext);
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[])[0].pick.row.market',researchContext),'over35_ft');
assert(vm.runInContext('researchExplanation(singleMatch.multimarket.markets[0])',researchContext).includes('scaduta'));
vm.runInContext('singleMatch.multimarket.markets.forEach(r=>{r.quote=null;r.book_probability_pct=null;r.research_candidate=false;})',researchContext);
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[]).length',researchContext),1);
assert(vm.runInContext('researchSuggestionsHtml(singleMatch,[])',researchContext).includes('preferenza esplorativa'));
vm.runInContext('singleMatch.multimarket.markets=[]',researchContext);
assert.equal(vm.runInContext('researchSuggestions(singleMatch,[]).length',researchContext),0);
console.log('Single proposal: quality priority, four fixed markets, divergence, stale/missing data and stable probabilities passed.');


// V3 consumes the backend's single primary, independently of selected market.
researchContext.v3={matches:[{fixture:{fixture_id:'100',kickoff:future(2),match:'Squadra casa – Squadra ospite'},multimarket:{
  model_id:'context-goals-v3',status:'experimental',profiles:{},weights:{},
  best_candidate:{...market('over25_ft'),source:'analisi statistica',pros:['Produzione osservata'],cons:[],missing:['PT incompleti']},
  markets:[{...market('over25_ft'),source:'analisi statistica',pros:['Produzione osservata'],cons:[],missing:['PT incompleti']},
           {...market('over15_ht'),source:'analisi statistica',pros:['Produzione PT'],cons:[],missing:[]}],ranked_hypotheses:[]}}]};
researchContext.currentPayload=researchContext.v3;
vm.runInContext("researchFilter('market','all');researchSearch='';renderMatchResearch(currentPayload)",researchContext);
assert(host.innerHTML.includes('La partita, spiegata.'));
assert.equal((host.innerHTML.match(/PROPOSTA PRINCIPALE/g)||[]).length,1);
assert(host.innerHTML.includes('A favore'));
assert(host.innerHTML.includes('Da considerare'));
assert(host.innerHTML.includes('Dati mancanti'));
assert(!host.innerHTML.includes('Probabilità combinata'));
vm.runInContext("researchSelectFixture('100','over15_ht::over')",researchContext);
assert(host.innerHTML.includes('value="over15_ht::over" selected'));
assert(host.innerHTML.includes('Pesi e calcolo, dato per dato'));
console.log('V3 dossier: backend primary, independent sources, evidence, missing data and market selection passed.');

// Opening must belong to the displayed book; single-book movements get no signal.
researchContext.oddsFixture={fixture:{home:'Home',away:'Away'},multimarket:{markets:[{
 market:'1x2_ft',selection:'2',odd:1.6,book_probability_pct:60,quote:{bookmaker:'A',provider_updated_at:future(-1)},
 movement:{confirmed:true,color:'red',arrow:'⇅',amplitude_pct:10,agreeing_books:2,paired_books:3,
 operators:[{bookmaker:'B',initial:9,current:8},{bookmaker:'A',initial:1.9,current:1.6}]}}]}};
let prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);
assert(prices.includes('1.90'));assert(prices.includes('1.60'));assert(!prices.includes('9.00'));
assert(prices.includes('cx-red'));assert(prices.includes('⇅'));assert(prices.includes('Over 1,5 PT'));
vm.runInContext('oddsFixture.multimarket.markets[0].movement.confirmed=false',researchContext);
prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);assert(!prices.includes('class="cx-red"'));
vm.runInContext('oddsFixture.multimarket.markets[0].quote.bookmaker="C"',researchContext);
prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);assert(!prices.includes('1.90'));
console.log('Quote overview: same-book opening, missing history and multi-book arrows passed.');
