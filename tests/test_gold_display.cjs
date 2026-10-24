const fs=require('fs'),vm=require('vm'),assert=require('assert/strict');
const html=fs.readFileSync(__dirname+'/../web/gold-dashboard-live.html','utf8');
const script=html.match(/<script>([\s\S]*?)<\/script>/)[1].replace('    initializeAccess();','');
const ctx={console,document:{addEventListener(){}},window:{addEventListener(){}},Map,Set,URL,URLSearchParams,Intl,Date};
vm.createContext(ctx);vm.runInContext(script,ctx);
function run(code){return vm.runInContext(code,ctx);}
function match(id,p,b,odd,cross=false){
 const mov={confirmed:true,paired_books:3,agreeing_books:2,amplitude_pct:4,direction:'down',operators:[]};
 const rows=[{market:'over25_ft',selection:'over',label:'Over 2,5 FT',probabilities:{statistics:p},statistical_pct:p*100,book_probability_pct:b,odd,source:'Modello statistico',quote:odd?{bookmaker:'Book test',provider_updated_at:new Date().toISOString()}:null,movement:cross?mov:{},price_assessment:{reference_probability_pct:55,break_even_probability_pct:odd?100/odd:null}},
 {market:'over15_ht',selection:'over',label:'Over 1,5 PT',odd:odd?2.2:null,statistical_pct:42,book_probability_pct:odd?40:null}];
 if(cross)rows.push({market:'1x2_ft',selection:'1',odd:2.3,movement:{...mov,direction:'up'}});
 return {fixture:{fixture_id:id,match:'Squadra '+id+' – Ospite',country:'Anteprima',league:'Dati dimostrativi',time:'15:00',kickoff:'2026-10-25T15:00:00+01:00'},multimarket:{model_id:'context-goals-v3.4',status:'experimental',markets:rows}};
}
ctx.sample=[match('Gold',.6,55,1.8),match('GoldAzzurro',.6,55,1.8,true),match('NonGold',.45,52,1.9),match('SenzaQuote',.45,null,null)];
assert.equal(run('contextVisibleMatch(sample[3])'),false);
assert.equal(run('contextVisibleMatch(sample[2])'),true);
assert.equal(run("contextSignal(sample[0]).gold"),true);
assert.equal(run("contextSignal(sample[1]).crosses.length"),1);
run("contextSignalFilter='dual'");assert.equal(run('contextVisibleMatch(sample[0])'),false);assert.equal(run('contextVisibleMatch(sample[1])'),true);
run("contextSignalFilter='nongold'");assert.equal(run('contextVisibleMatch(sample[2])'),true);run("contextQuoteFilter='all'");assert.equal(run('contextVisibleMatch(sample[3])'),false);
run("contextSignalFilter='nongold_watch'");assert.equal(run('contextVisibleMatch(sample[2])'),true);assert.equal(run('contextVisibleMatch(sample[0])'),false);assert.equal(run('contextVisibleMatch(sample[3])'),false);
run("contextFeed='over15_ht'");assert.equal(run('contextVisibleMatch(sample[2])'),false);run("contextFeed='over25_ft'");
run("contextSignalFilter='all';contextQuoteFilter='missing'");assert.equal(run('contextVisibleMatch(sample[3])'),true);assert.equal(run('contextVisibleMatch(sample[2])'),false);
assert(!run('contextOdds(sample[3],true)').includes('cx-signal'));
assert(!run('contextPrice(sample[3].multimarket.markets[0])').includes('pareggio'));
run("contextQuoteFilter='priced'");const cards=run('contextList(sample)');
assert(!cards.includes('Squadra SenzaQuote'));
assert(cards.indexOf('GOLD ·')<cards.indexOf('MOVIMENTI PREMATCH'));
assert(cards.includes('cx-gold-blue'));
ctx.host={innerHTML:''};ctx.payload={matches:ctx.sample,updated_at:new Date().toISOString()};run('renderContextDashboard(payload,host)');
assert(ctx.host.innerHTML.includes('3 partite con quota · 1 in attesa'));
console.log('PASS: quote required by default; unknown separate; Gold and dual signals; top badge; missing quotes compact; dashboard counts; JavaScript syntax.');
