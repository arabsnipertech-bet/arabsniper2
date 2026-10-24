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
assert(host.innerHTML.includes('cx-list-row'));
assert(!host.innerHTML.includes('Quadro statistico'));
vm.runInContext("contextDetailId='100';renderMatchResearch(currentPayload)",researchContext);
assert.equal((host.innerHTML.match(/ANALISI OVER 2,5 FT/g)||[]).length,1);
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
 movement:{confirmed:true,color:'red',direction:'down',has_direction_reversal:true,arrow:'↓',amplitude_pct:10,agreeing_books:2,paired_books:3,
 operators:[{bookmaker:'B',initial:9,current:8},{bookmaker:'A',initial:1.9,current:1.6}]}}]}};
let prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);
assert(prices.includes('1.90'));assert(prices.includes('1.60'));assert(!prices.includes('9.00'));
assert(prices.includes('cx-red'));assert(prices.includes('↓ discesa'));assert(prices.includes('-15.79%'));assert(prices.includes('inversione di movimento'));assert(prices.includes('Over 2,5 FT'));assert(prices.includes('Over 1,5 PT'));
vm.runInContext('oddsFixture.multimarket.markets[0].movement.confirmed=false',researchContext);
prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);assert(prices.includes('cx-green cx-unconfirmed'));assert(!prices.includes('cx-signal cx-red'));
vm.runInContext('oddsFixture.multimarket.markets[0].quote.bookmaker="C"',researchContext);
prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);assert(!prices.includes('1.90'));
vm.runInContext('oddsFixture.multimarket.markets[0].quote.bookmaker="A";oddsFixture.multimarket.markets[0].odd=1.9',researchContext);
prices=vm.runInContext('contextOdds(oddsFixture)',researchContext);assert(prices.includes('→ invariata 0.00%'));
console.log('Quote overview: same-book opening, missing history and multi-book arrows passed.');

researchContext.liveFixture={fixture:{fixture_id:'live',kickoff:future(-1),status_short:'HT',live:{status:'HT',elapsed:45,goals:{home:1,away:1},halftime:{home:1,away:1},observed_at:future(-0.1)}},multimarket:researchContext.v3.matches[0].multimarket};
assert.equal(vm.runInContext('researchMatches({matches:[liveFixture]},true).length',researchContext),1);
assert.equal(vm.runInContext('contextState(liveFixture)',researchContext),'live');
assert.equal(vm.runInContext("contextOutcome(liveFixture,'gg_ht')",researchContext),'Verificato: riuscito');
assert.equal(vm.runInContext("contextOutcome(liveFixture,'over25_ht')",researchContext),'Verificato: non riuscito');
assert.equal(vm.runInContext("contextOutcome(liveFixture,'over25_ft')",researchContext),'Esito da verificare');
vm.runInContext("liveFixture.fixture.live.halftime={}",researchContext);
assert.equal(vm.runInContext("contextOutcome(liveFixture,'gg_ht')",researchContext),'Risultato non disponibile');
vm.runInContext("liveFixture.fixture.live.status='AET';liveFixture.fixture.live.fulltime={home:1,away:1};liveFixture.fixture.live.goals={home:3,away:2}",researchContext);
assert.equal(vm.runInContext("contextOutcome(liveFixture,'over25_ft')",researchContext),'Verificato: non riuscito');
assert.equal(vm.runInContext('contextState(liveFixture)',researchContext),'finished');
assert(vm.runInContext('contextLivePanel(liveFixture)',researchContext).includes('non sono probabilità live'));
vm.runInContext("currentPayload={matches:[liveFixture]};contextDetailId=null;contextStateFilter='all';renderMatchResearch(currentPayload)",researchContext);
assert(host.innerHTML.includes('cx-list-row'));
vm.runInContext("contextStateFilter='upcoming';renderMatchResearch(currentPayload)",researchContext);
assert(!host.innerHTML.includes('cx-list-row'));
console.log('Lifecycle: retained fixtures, status filters, halftime settlement, missing scores and regulation-time scores passed.');

// Market comparison ranks one source and never presents started games as prematch picks.
researchContext.comparison=[.4,.7,.55].map((p,i)=>({fixture:{fixture_id:String(i),kickoff:future(i===2?-1:2)},multimarket:{markets:[{market:'over25_ft',selection:'over',statistical_pct:p*100,book_probability_pct:(1-p)*100}]}}));
vm.runInContext("contextCompareMarket='over25_ft'",researchContext);
assert.equal(vm.runInContext('contextComparisonMatches(comparison).map(m=>m.fixture.fixture_id).join() ',researchContext),'0,1');
assert.equal(vm.runInContext('contextComparisonMatches(comparison).map(m=>m.fixture.fixture_id).join()',researchContext),'0,1');
assert.equal(researchContext.comparison[0].fixture.fixture_id,'0');
researchContext.window={ArabLive:{view:()=>({active:true}),fixture:m=>m.fixture}};
vm.runInContext("liveFixture.fixture.live={status:'HT',goals:{home:0,away:0}};liveFixture.fixture.result=null;liveFixture.fixture.status_short='HT'",researchContext);
assert(vm.runInContext("contextGoalProgress(liveFixture,{market:'over25_ft'})",researchContext).includes('Servono ancora 3 gol'));
vm.runInContext('liveFixture.fixture.live.goals.home=2',researchContext);
assert(vm.runInContext("contextGoalProgress(liveFixture,{market:'over25_ft'})",researchContext).includes('Serve ancora 1 gol'));
console.log('Market comparison: chronological ranking, started exclusion, immutable order and live goal distance passed.');

// Visual intensity must never promote a large, unconfirmed movement.
for(const [amplitude,count,confirmed,expected] of [[2.99,3,true,'green'],[3,3,true,'yellow'],[8,3,true,'red'],[30,1,false,'green']]){
 researchContext.visualMovement={amplitude_pct:amplitude,paired_books:4,agreeing_books:count,confirmed,direction:'up'};
 const badge=vm.runInContext('contextMovementBadge(visualMovement)',researchContext);
 assert(badge.includes('cx-signal cx-'+expected));
 assert(badge.includes(confirmed?'✓':'Non confermato'));
}
assert(vm.runInContext('contextMovementBadge({})',researchContext).includes('cx-unknown'));

// Gold marker uses the same unrounded inputs and boundaries as the audit.
const badgeContext={Number,Math,contextPct:x=>x+'%'};
vm.createContext(badgeContext);
vm.runInContext(html.slice(html.indexOf('    function contextAgreementBadge('),html.indexOf('    function contextPrice(')),badgeContext);
const badge=(p,b,model='context-goals-v3.4')=>badgeContext.contextAgreementBadge({multimarket:{model_id:model,markets:[{market:'over25_ft',selection:'over',probabilities:{statistics:p},book_probability_pct:b}]}});
assert(badge(.60,50.1).includes('cx-agreement-ring'));
assert(badge(.65,55).includes('GOLD · OVER 2,5 FT'));
assert.equal(badge(.6501,55),'');
assert.equal(badge(.5,55),'');
assert.equal(badge(.6,50),'');
assert.equal(badge(null,55),'');
assert.equal(badge(.6,NaN),'');
assert.equal(badge(.6,55,'context-goals-v3.3'),'');
assert(html.includes('${contextAgreementBadge(match)}'));
assert(html.includes('${contextSignalClass(match)}">${contextAgreementBadge(match)}'));
assert(!html.includes('contextCoverage(match)+contextAgreementBadge(match)'));
console.log('Gold marker: audit thresholds, missing data, model scope and both placements passed.');

// Public feed is Over 2.5 regardless of the frozen original primary. Only an
// actual frozen Over 3.5 proposal qualifies for the recommended view.
const feedRows=[{market:'over25_ft',selection:'over',label:'Over 2,5 FT',statistical_pct:61,book_probability_pct:55},
 {market:'over35_ft',selection:'over',label:'Over 3,5 FT',odd:2.2},
 {market:'over15_ht',selection:'over',label:'Over 1,5 PT',odd:2}];
researchContext.feedFixture={fixture:{fixture_id:'feed',kickoff:future(1),match:'Feed test'},multimarket:{model_id:'context-goals-v3.4',markets:feedRows,best_candidate:feedRows[2]}};
const original=JSON.stringify(researchContext.feedFixture);
assert.equal(vm.runInContext('contextOver25(feedFixture.multimarket).market',researchContext),'over25_ft');
assert.equal(vm.runInContext('contextOver35(feedFixture.multimarket)',researchContext),null);
vm.runInContext("contextCompareMarket='';contextFeed='over25_ft';researchSearch='';",researchContext);
let feed=vm.runInContext('contextList([feedFixture])',researchContext);
assert(feed.includes('Over 2,5 FT'));assert(feed.includes('Over 1,5 PT'));assert(feed.includes('OVER 2,5 FT · ANALISI PREMATCH'));
assert.equal(JSON.stringify(researchContext.feedFixture),original);
vm.runInContext("contextFeed='over35_ft'",researchContext);
assert.equal(vm.runInContext('contextFeedMatches([feedFixture]).length',researchContext),0);
researchContext.feedFixture.multimarket.best_candidate=feedRows[1];
assert.equal(vm.runInContext('contextFeedMatches([feedFixture]).length',researchContext),1);
assert(vm.runInContext('contextList([feedFixture])',researchContext).includes('Over 3,5 FT'));
researchContext.feedFixture.multimarket.proposals_by_min_odd={'1.5':feedRows[2]};
assert.equal(vm.runInContext('contextFeedMatches([feedFixture]).length',researchContext),0);
console.log('Over 2.5 feed: frozen data preserved, HT hidden, missing prices retained, exact Over 3.5 recommendation eligibility passed.');

// HT observation feed and joint table never alter the frozen forecast or Gold.
const frozenBeforeObservation=JSON.stringify(researchContext.feedFixture);
vm.runInContext("contextFeed='over15_ht';contextCompareMarket=''",researchContext);
assert.equal(vm.runInContext('contextFeedMatches([feedFixture]).length',researchContext),1);
const htFeed=vm.runInContext('contextList([feedFixture])',researchContext);
assert(htFeed.includes('OVER 1,5 PT · IN OSSERVAZIONE'));
assert(htFeed.includes('Over 1,5 PT'));
assert(!htFeed.includes('CONSIGLIATA DAL MODELLO · PREMATCH'));
const joint=vm.runInContext('contextJointMovements(feedFixture)',researchContext);
assert(joint.includes('1 · Casa')&&joint.includes('Over 1,5 PT')&&joint.includes('Over 2,5 FT'));
assert(joint.includes('Storico multi-book assente')||joint.includes('Dati assenti'));
assert(joint.includes('Nessun bonus'));
assert.equal(JSON.stringify(researchContext.feedFixture),frozenBeforeObservation);
assert.equal(vm.runInContext('contextFeedMatches([{multimarket:{markets:[]}}]).length',researchContext),0);
console.log('HT observation and joint movements: missing markets, absent history, immutable forecasts and non-recommendation label passed.');

// Both Over movements are visible in the main feed without opening a dossier.
vm.runInContext("contextFeed='over25_ft'",researchContext);
const overview=vm.runInContext('contextOdds(feedFixture,true)',researchContext);
for(const label of ['1 · Casa','X · Pareggio','2 · Ospite','Over 1,5 PT','Over 2,5 FT','Stat.','Book','Apertura*','Attuale'])assert(overview.includes(label));
assert(!overview.includes('<details class="cx-detail">'));
assert(overview.includes('cx-overview-cell'));
assert(!overview.includes('Over 3,5 FT'));
console.log('Main overview: five markets, estimates, missing-history states and distinct Gold meaning passed.');

// Joint badge requires existing Gold AND the exact observed 1-up / Over-down pattern.
const strongMove=(direction,amplitude=3,agree=3,paired=4)=>({confirmed:true,direction,amplitude_pct:amplitude,agreeing_books:agree,paired_books:paired});
const crossed={multimarket:{model_id:'context-goals-v3.4',markets:[
 {market:'over25_ft',selection:'over',probabilities:{statistics:.61},book_probability_pct:57,movement:strongMove('down')},
 {market:'over15_ht',selection:'over',movement:strongMove('down',8)},
 {market:'1x2_ft',selection:'1',movement:strongMove('up')}]}};
const beforeCross=JSON.stringify(crossed);
let crossHtml=badgeContext.contextAgreementBadge(crossed);
assert.equal((crossHtml.match(/class="cx-crossing"/g)||[]).length,1);
assert(crossHtml.includes('1 ↑ + Over 2,5 FT ↓'));
assert(crossHtml.includes('1 ↑ + Over 1,5 PT ↓'));
assert(!crossHtml.includes('<details'));
assert(!crossHtml.includes('non dimostra maggiore affidabilità'));
assert.equal(JSON.stringify(crossed),beforeCross);
for(const move of [strongMove('down'),strongMove('up',2.99),strongMove('up',10,2,4),{...strongMove('up'),confirmed:false},{...strongMove('up'),amplitude_pct:null}]){
 crossed.multimarket.markets[2].movement=move;
 crossHtml=badgeContext.contextAgreementBadge(crossed);
 assert(crossHtml.includes('cx-agreement-ring'));assert(!crossHtml.includes('class="cx-crossing"'));
}
crossed.multimarket.markets[2].movement=strongMove('up');
crossed.multimarket.markets[0].movement=strongMove('up');
assert(badgeContext.contextAgreementBadge(crossed).includes('1 ↑ + Over 2,5 FT ↑'));
crossed.multimarket.markets[2].movement=strongMove('down');
crossed.multimarket.markets.push({market:'1x2_ft',selection:'2',movement:strongMove('down',8)});
crossed.multimarket.markets[0].movement=strongMove('down');
assert(badgeContext.contextAgreementBadge(crossed).includes('2 ↓ + Over 2,5 FT ↓'));
assert(!badgeContext.contextAgreementBadge(crossed).includes('2 ↓ + Over 1,5 PT'));
crossed.multimarket.markets[3].movement=strongMove('down',2.99);
assert(!badgeContext.contextAgreementBadge(crossed).includes('class="cx-crossing"'));
crossed.multimarket.markets[0].probabilities.statistics=.5;
assert.equal(badgeContext.contextAgreementBadge(crossed),'');
console.log('Gold plus quote crossing: exact directions, consensus, thresholds, market labels and immutable probabilities passed.');

// Favorite-red PT signal uses initial fair medians, complete data and a unique favorite.
crossed.multimarket.markets[0].probabilities.statistics=.61;
const side1=crossed.multimarket.markets[2],side2=crossed.multimarket.markets[3];
side1.movement={...strongMove('down',8),operators:[{fair_initial_pct:60},{fair_initial_pct:64}]};
side2.movement={...strongMove('up'),operators:[{fair_initial_pct:18}]};
const draw={market:'1x2_ft',selection:'x',movement:{operators:[{fair_initial_pct:20}]}};
crossed.multimarket.markets.push(draw);
assert(badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓ + Over 1,5 PT ↓'));
side1.movement.amplitude_pct=7.99;
assert(!badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓'));
side1.movement.amplitude_pct=8;
draw.movement.operators=[];
assert(!badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓'));
draw.movement.operators=[{fair_initial_pct:62}];
assert(!badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓'));
draw.movement.operators=[{fair_initial_pct:20}];
crossed.multimarket.markets[1].movement=strongMove('up');
assert(!badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓'));
console.log('Initial favorite red PT: medians, missing data, ties, intensity and Over direction passed.');

// Recovering a common window must not redefine the initially favored outcome.
crossed.multimarket.markets[1].movement=strongMove('down');
side1.movement.operators=[{fair_initial_pct:15,first_observed_fair_pct:62}];
side2.movement.operators=[{fair_initial_pct:65,first_observed_fair_pct:18}];
assert(badgeContext.contextAgreementBadge(crossed).includes('Favorita iniziale 1 (rosso) ↓ + Over 1,5 PT ↓'));
console.log('Common window preserves the original favorite passed.');

// Observe the original requested HOME win band; never promote forecasts.
for(const [odd,expected] of [[1.579,false],[1.58,true],[1.65,true],[1.75,true],[1.751,false],[null,false]]){
 researchContext.bandMatch={multimarket:{markets:[{market:'1x2_ft',selection:'1',odd,book_probability_pct:60},{market:'1x2_ft',selection:'x',book_probability_pct:23},{market:'1x2_ft',selection:'2',book_probability_pct:17}]}};
 assert.equal(vm.runInContext('contextHomeBand(bandMatch)',researchContext),expected);
}
researchContext.bandMatch={multimarket:{markets:[{market:'1x2_ft',selection:'2',odd:1.65}]}};
assert.equal(vm.runInContext('contextHomeBand(bandMatch)',researchContext),false);
researchContext.bandMatch={multimarket:{markets:[{market:'1x2_ft',selection:'1',odd:1.65,book_probability_pct:60},{market:'1x2_ft',selection:'x',book_probability_pct:23},{market:'1x2_ft',selection:'2',book_probability_pct:17}]}};
const bandBefore=JSON.stringify(researchContext.bandMatch);
assert(vm.runInContext('contextHomeBandBadge(bandMatch)',researchContext).includes('in osservazione'));
assert.equal(JSON.stringify(researchContext.bandMatch),bandBefore);
vm.runInContext("contextHomeBandOnly=true;contextFeed='over25_ft'",researchContext);
assert.equal(vm.runInContext('contextFeedMatches([bandMatch,{multimarket:{markets:[]}}]).length',researchContext),1);
vm.runInContext('contextHomeBandOnly=false',researchContext);
assert(vm.runInContext('contextFeedMatches([{multimarket:{markets:[]}}]).length',researchContext)===1);
researchContext.priceMatch={multimarket:{markets:[{market:'1x2_ft',selection:'1',odd:1.65,quote:{bookmaker:'Same book'},movement:{operators:[{bookmaker:'Other book',initial:9},{bookmaker:'Same book',initial:1.8,first_observed_odd:1.95}]}}]}};
const dualPrice=vm.runInContext('contextOdds(priceMatch,true)',researchContext);
assert(dualPrice.includes('1.95')&&dualPrice.includes('1.65'));
console.log('Gold frames, compact dual odds and observational home-odds band passed.');

// Signal filters are mutually exclusive; combinations with the home band are intersections.
researchContext.filterMatches=[
 {multimarket:{model_id:'context-goals-v3.4',markets:[{market:'over25_ft',selection:'over',probabilities:{statistics:.6},book_probability_pct:55}]}},
 {multimarket:{model_id:'context-goals-v3.4',markets:[{market:'over25_ft',selection:'over',probabilities:{statistics:.6},book_probability_pct:55,movement:strongMove('down')},{market:'1x2_ft',selection:'1',movement:strongMove('up')}]}},
 {multimarket:{markets:[]}}
];
vm.runInContext("contextGoldFilter='gold'",researchContext);
assert.equal(vm.runInContext('contextFeedMatches(filterMatches).length',researchContext),1);
assert.equal(vm.runInContext('contextFeedMatches(filterMatches)[0]===filterMatches[0]',researchContext),true);
vm.runInContext("contextGoldFilter='dual'",researchContext);
assert.equal(vm.runInContext('contextFeedMatches(filterMatches)[0]===filterMatches[1]',researchContext),true);
vm.runInContext('contextHomeBandOnly=true',researchContext);
assert.equal(vm.runInContext('contextFeedMatches(filterMatches).length',researchContext),0);
vm.runInContext("contextHomeBandOnly=false;contextGoldFilter='all'",researchContext);
assert.equal(vm.runInContext('contextFeedMatches(filterMatches).length',researchContext),3);
researchContext.bandMatch.multimarket.markets[2].book_probability_pct=65;
assert.equal(vm.runInContext('contextHomeBand(bandMatch)',researchContext),false);
researchContext.bandMatch.multimarket.markets[2].book_probability_pct=60;
assert.equal(vm.runInContext('contextHomeBand(bandMatch)',researchContext),false);
researchContext.bandMatch.multimarket.markets.pop();
assert.equal(vm.runInContext('contextHomeBand(bandMatch)',researchContext),false);
console.log('Exclusive Gold filters, filter intersections, confirmed home favorite and missing data passed.');
