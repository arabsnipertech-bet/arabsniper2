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
