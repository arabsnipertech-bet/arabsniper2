const fs=require('fs'),vm=require('vm'),assert=require('node:assert/strict');
const code=fs.readFileSync('web/homepage.js','utf8');
async function run(payload,fail=false){
 const nodes=Object.fromEntries(['daily-picks','preview-date'].map(id=>[id,{textContent:'',innerHTML:''}]));
 const calls=[];let pending;
 class Clock extends Date{constructor(...a){super(...(a.length?a:['2026-10-22T12:00:00Z']));}static now(){return +new Clock();}}
 const ctx={ArabCompetitionScope:require('../web/competition-scope.js'),Date:Clock,Intl,Set,Number,String,AbortSignal,handleUrlMessages(){},setInterval(){},document:{getElementById:id=>nodes[id],addEventListener(){}},fetch:async(url)=>{calls.push(url);return {ok:!fail,json:async()=>payload}}};
 vm.runInNewContext(code,ctx);await new Promise(r=>setImmediate(r));return {nodes,calls};
}
(async()=>{
 const data={date:'2026-10-22', selections:[1,2,3].map(id=>({fixture_id:String(id),match:'Home - Away',league:'League',kickoff:'2026-10-22T20:00:00+02:00',captured_at:'2026-10-22T10:00:00+02:00',odd:1.8,label:'Over 2,5 finale',statistical_pct:65,book_probability_pct:60}))};
 let r=await run(data);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,3);assert.equal(r.calls.length,1);assert.match(r.calls[0],/arab_quote_homepage.json/);
 r=await run({...data,date:'2026-10-21'});assert.match(r.nodes['daily-picks'].innerHTML,/Nessuna selezione/);
 const started=structuredClone(data);started.selections[0].kickoff='2026-10-22T10:00:00Z';
 r=await run(started);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,2);
 const evil=structuredClone(data);evil.selections[0].match='<img src=x onerror=alert(1)>';
 r=await run(evil);assert(!r.nodes['daily-picks'].innerHTML.includes('<img'));assert(r.nodes['daily-picks'].innerHTML.includes('&lt;img'));
 r=await run(data,true);assert.match(r.nodes['daily-picks'].innerHTML,/temporaneamente/);
 const v5=structuredClone(data);
 for(const row of v5.selections)Object.assign(row,{model_id:'lateral-matrix-v5.0',market_confirmation:'contrarian',competition_policy:'gold-senior-whitelist-v1',opportunity:{policy_id:'gold-goals-v2',eligible:true,min_probability_pct:35}});
 for(const scope of ['gold-senior-whitelist-v1','gold-senior-whitelist-v2','unknown']) {
  const updated=structuredClone(v5);for(const row of updated.selections)row.competition_policy=scope;
  r=await run(updated);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,scope==='unknown'?0:3);
 }
 v5.selections[1].odd=150;v5.selections[2].statistical_pct=34;
 r=await run(v5);assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,1);assert.match(r.nodes['daily-picks'].innerHTML,/Contrarian/);
 for(const [spec,max] of [[{kind:'total',period:'ft',side:'over',line:2.5},3.5],[{kind:'total',period:'ht',side:'over',line:1.5},3.5],[{kind:'total',period:'ft',side:'over',line:3.5},5.5],[{kind:'btts',period:'ht',side:'yes'},5.5],[{kind:'asian_total',period:'ft',side:'over',line:3.5},5.5]]){
  const selection={...v5.selections[0],spec,odd:max,statistical_pct:35,opportunity:{policy_id:'gold-goals-v4',eligible:true,min_probability_pct:35,odds_min:1.4,odds_max:max}};
  for(const [extra,ok] of [[{},true],[{odd:max+.0001},false],[{odd:1.399},false],[{statistical_pct:34.999},false],[{opportunity:{...selection.opportunity,odds_max:150}},false]]){
   r=await run({...v5,selections:[{...selection,...extra}]});assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,ok?1:0);
  }
 }
 for(const [kind,line,floor] of [['total',1.5,45],['asian_total',1.5,45],['btts',null,40]]){
  const spec={kind,period:'ht',side:kind==='btts'?'yes':'over',line};
  const max=kind==='btts'?5.5:3.5;
  const selection={...v5.selections[0],spec,odd:max,statistical_pct:floor,opportunity:{policy_id:'gold-goals-v5',eligible:true,min_probability_pct:floor,odds_max:max,ht_history_supported:true}};
  for(const [extra,ok] of [[{},true],[{statistical_pct:floor-.001},false],[{statistical_pct:35,opportunity:{...selection.opportunity,min_probability_pct:35}},false],[{opportunity:{...selection.opportunity,ht_history_supported:false}},false],[{opportunity:{...selection.opportunity,min_probability_pct:null}},false]]){
   r=await run({...v5,selections:[{...selection,...extra}]});assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,ok?1:0);
  }
 }
 const html=fs.readFileSync('web/index.html','utf8');assert(!html.includes('localStorage.clear'));assert(!html.includes('logout=true'));assert(!html.includes('details_day1'));
 for(const [kind,period,line,floor,max] of [['total','ht',1.5,35,3.5],['asian_total','ht',1.5,35,3.5],['btts','ht',null,30,5.5],['total','ft',2.5,30,3.5],['total','ft',3.5,30,5.5],['asian_total','ft',3.5,30,5.5]]){
  const spec={kind,period,side:kind==='btts'?'yes':'over',line};
  const selection={...v5.selections[0],spec,odd:max,statistical_pct:floor,opportunity:{policy_id:'gold-goals-v6',eligible:true,min_probability_pct:floor,odds_max:max,ht_history_supported:true}};
  for(const [extra,ok] of [[{},true],[{statistical_pct:floor-.001},false],[{statistical_pct:floor-1,opportunity:{...selection.opportunity,min_probability_pct:floor-1}},false],[{odd:max+.001},false],[{odd:1.399},false],[{competition_policy:'unknown'},false],[{opportunity:{...selection.opportunity,ht_history_supported:false}},period==='ft'],[{opportunity:{...selection.opportunity,min_probability_pct:null}},false]]){
   r=await run({...v5,selections:[{...selection,...extra}]});assert.equal((r.nodes['daily-picks'].innerHTML.match(/class="pick"/g)||[]).length,ok?1:0,JSON.stringify({spec,extra}));
  }
 }
 const releaseHash=require('node:crypto').createHash('sha256').update(code).digest('hex').slice(0,12);
 assert(html.includes('src="homepage.js?v='+releaseHash+'"'));
 const currentMarkets=[
  ['total','ht','over',1.5,'over15_ht',2.1,3.5,40],
  ['total','ht','over',2.5,'over25_ht',4.5,7.5,18],
  ['total','ft','over',2.5,'over25_ft',1.85,2.6,48],
  ['asian_total','ft','over',3.5,'over35_ft',2.1,4.5,35],
  ['btts','ht','yes',undefined,'gg_ht',3.5,5.5,26]
 ];
 for(const [kind,period,side,line,market,lo,hi,floor] of currentMarkets){
  const spec={kind,period,side,line};
  const opportunity={policy_id:'gold-goals-v6.1',eligible:true,goal_market:market,odds_min:lo,odds_max:hi,
   min_probability_pct:floor,ht_history_supported:true,structural_supported:true,market_checks_passed:true};
  const row={...data.selections[0],model_id:'lateral-matrix-v5.0',competition_policy:'gold-senior-whitelist-v2.1',
   spec,odd:lo,statistical_pct:floor,opportunity,quote_executable:true};
  for(const [changes,visible] of [[{},true],[{odd:hi},true],[{odd:hi+.01},false],[{odd:lo-.01},false],
   [{competition_policy:'unknown'},false],[{quote_executable:false},false],[{statistical_pct:floor-.01},false],
   [{spec:{...spec,extra:'home'}},false],[{opportunity:{...opportunity,eligible:false}},false],
   [{opportunity:{...opportunity,market_checks_passed:false}},false],[{opportunity:{...opportunity,structural_supported:false}},false],
   [{opportunity:{...opportunity,ht_history_supported:false}},period==='ft'],[{opportunity:{...opportunity,odds_max:200}},false]]){
   r=await run({...data,selections:[{...row,...changes}]});
   assert.equal(r.nodes['daily-picks'].innerHTML.includes('class="pick"'),visible,JSON.stringify({market,changes}));
  }
 }
 const currentScope=require('../web/competition-scope.js').id;
 const pick={...data.selections[0],model_id:'lateral-matrix-v5.0',country:'England',league:'League Two',
  spec:{kind:'total',period:'ft',side:'over',line:2.5},odd:2,statistical_pct:60,competition_policy:currentScope,
  opportunity:{policy_id:'gold-goals-v6.1',eligible:true,goal_market:'over25_ft',odds_min:1.85,odds_max:2.6,
   min_probability_pct:48,structural_supported:true,market_checks_passed:true},quote_executable:true};
 for(const [country,league,scope,visible] of [['England','League Two',currentScope,true],['Romania','Liga III - Serie 1',currentScope,false],
  ['Austria','Regionalliga - West','gold-senior-whitelist-v2.1',false],[null,'II Liga - East','gold-senior-whitelist-v2.1',false],
  ['Poland','I Liga',currentScope,true]]){
  r=await run({...data,selections:[{...pick,country,league,competition_policy:scope}]});
  assert.equal(r.nodes['daily-picks'].innerHTML.includes('class="pick"'),visible);
 }
 for(const plan of ['daily','weekly','monthly']) assert(html.includes("buy('"+plan+"', this)"));
 console.log('PASS: public feed only, three cards, stale day, escaped HTML, unavailable feed, Gold entry links and plan IDs');
})();

