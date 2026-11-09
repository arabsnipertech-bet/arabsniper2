// End-to-end UI on synthetic backend output. No account or provider calls.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict'),cp=require('node:child_process');
async function verifyCachedRelease(browser,payload){
 const http=require('node:http'),requests=[];
 const html=fs.readFileSync('web/gold-dashboard-live.html','utf8');
 const script=html.match(/<script src="(\/lateral-matrix\.js[^\"]*)"><\/script>/)[1];
 const assets=new Set(['vendor/fflate-0.8.2.js','competition-scope.js','statistics-first.js','statistics-first.css','lateral-matrix.js','lateral-matrix.css','live-scores.js']);
 const oldScript='/lateral-matrix.js?v=5.10';
 const server=http.createServer((req,res)=>{
  requests.push(req.url);
  const url=new URL(req.url,'http://127.0.0.1');
  res.setHeader('Cache-Control','no-store');
  if(url.pathname==='/prime-cached-v6'){
   res.setHeader('Content-Type','text/html; charset=utf-8');
   return res.end('<!doctype html><script src="'+oldScript+'"></script>');
  }
  if(req.url===oldScript){
   res.setHeader('Cache-Control','public, max-age=31536000');
   res.setHeader('Content-Type','text/javascript');
   return res.end('window.ArabMatrix={POLICY:"gold-goals-v6",SCOPE:"gold-senior-whitelist-v2"};');
  }
  if(url.pathname==='/gold-dashboard-live'){
   res.setHeader('Content-Type','text/html; charset=utf-8');return res.end(html);
  }
  if(url.pathname==='/api/auth/session'){
   res.setHeader('Content-Type','application/json');return res.end('{"active":false}');
  }
  if(url.pathname==='/api/auth/request-code'){
   res.setHeader('Content-Type','application/json');return res.end(JSON.stringify({challenge:'a'.repeat(64)}));
  }
  if(url.pathname==='/api/auth/verify-code'){
   let body='';req.on('data',part=>body+=part);req.on('end',()=>{const valid=JSON.parse(body).code==='123456';res.statusCode=valid?200:400;res.setHeader('Content-Type','application/json');res.end(JSON.stringify(valid?{active:true}:{error:'Codice errato, scaduto o già utilizzato.'}));});return;
  }
  const file=url.pathname.slice(1);
  if(assets.has(file)){
   res.setHeader('Content-Type',file.endsWith('.css')?'text/css':'text/javascript');
   if(file==='lateral-matrix.js')res.setHeader('Cache-Control','no-cache, must-revalidate');
   return res.end(fs.readFileSync('web/'+file));
  }
  res.statusCode=404;res.end();
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const origin='http://127.0.0.1:'+server.address().port,page=await browser.newPage();
 try{
  // Real HTTP cache, with no route interception or direct script injection.
  await page.goto(origin+'/prime-cached-v6');
  await page.goto(origin+'/prime-cached-v6?second=1');
  assert.equal(requests.filter(url=>url===oldScript).length,1,'Previous bundle is genuinely cached');
  assert.equal(await page.evaluate(()=>ArabMatrix.POLICY),'gold-goals-v6');
  await page.goto(origin+'/gold-dashboard-live');
  assert.equal(await page.evaluate(()=>ArabMatrix.POLICY),'gold-goals-v6.1','Production HTML must replace the cached client');
  assert.equal(await page.evaluate(()=>ArabMatrix.SCOPE),payload.gold_scope.policy_id);
  assert.equal(requests.filter(url=>url===script).length,1,'Current content-versioned bundle requested');
  // Synthetic email flow on this local server: no real email or entitlement changes.
  await page.evaluate(()=>{window.showApplication=()=>{window.loginVerified=true;};});
  await page.fill('#loginEmail','reader@example.com');
  await page.click('#loginButton');
  await page.waitForFunction(()=>!document.getElementById('loginCode').hidden);
  assert((await page.locator('#loginStatus').textContent()).includes('reader@example.com'));
  assert.equal(await page.locator('#loginError').textContent(),'');
  assert(await page.locator('#resendCode').isDisabled());
  await page.waitForFunction(()=>getComputedStyle(document.getElementById('loginButton')).backgroundColor==='rgb(232, 199, 131)');
  assert.equal(await page.locator('#loginButton').evaluate(el=>getComputedStyle(el).backgroundColor),'rgb(232, 199, 131)','Login action keeps its gold contrast despite shared dashboard styles');
  fs.mkdirSync('test-artifacts',{recursive:true});
  await page.setViewportSize({width:390,height:844});
  await page.screenshot({path:'test-artifacts/gold-login-code-mobile.png',fullPage:true});
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Login fits mobile width');
  await page.fill('#loginCode','000000');await page.click('#loginButton');
  await page.waitForFunction(()=>document.getElementById('loginError').textContent.includes('Codice errato'));
  await page.fill('#loginCode','123456');await page.click('#loginButton');
  await page.waitForFunction(()=>window.loginVerified===true);
  await page.setViewportSize({width:1280,height:900});
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  assert.equal(await page.locator('.mx-card').count(),payload.matches.length,'Current whitelist fixtures remain visible');
  assert((await page.locator('.mx-counters').textContent()).includes('Proposte su 5 mercati gol · Combo PT+ST'));
  assert(!(await page.locator('.mx-dashboard').textContent()).includes('Dati in aggiornamento alla whitelist'));
  fs.mkdirSync('test-artifacts',{recursive:true});
  await page.screenshot({path:'test-artifacts/matrix-cache-rollout.png'});
  const legacy=await page.evaluate(()=>buildMatchCard({
   fixture:{fixture_id:999,home:'Casa',away:'Ospite'},model:{probabilities_poisson:{over15_ht:{over:.5}}},
   statistics:{xg_layer:{home:{available:true,sample_all:3,xg_for_ft:1.5}},shots_layer:{home:{shots_on_target_for_sample:3,shots_on_target_for:0}}},
   display:{markets:{over15_ht:{model_pick:'over',selection_order:['over'],recommendation_gate:{eligible:false},
    selections:{over:{open_odd:2.5,current_odd:2,oci_as:0,delta_odd:-.5,model_probability_pct:60,marker:{shape:'circle'}}}}}}
  },0));
  for(const text of ['In osservazione','Poisson prematch<b>50.0%','Quota equa Poisson<b>2.00','xG medio FT 1.50','tiri in porta medi FT 0.0','OCI-AS<b>0'])assert(legacy.includes(text),text);
  console.log('PASS Gold release: cached v6 bundle replaced by versioned v6.1, current whitelist fixtures visible');
 }finally{await page.close();await new Promise(resolve=>server.close(resolve));}
}
async function verifyCalendarRollover(browser,payload){
 const http=require('node:http');
 let dates=['2026-10-28','2026-10-29','2026-10-30','2026-10-31','2026-11-01'];
 const assets=new Set(['vendor/fflate-0.8.2.js','competition-scope.js','statistics-first.js','statistics-first.css','lateral-matrix.js','lateral-matrix.css','live-scores.js']);
 const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  res.setHeader('Cache-Control','no-store');
  if(url.pathname==='/gold-dashboard-live'){
   res.setHeader('Content-Type','text/html; charset=utf-8');return res.end(fs.readFileSync('web/gold-dashboard-live.html'));
  }
  if(url.pathname==='/api/auth/session'){
   res.setHeader('Content-Type','application/json');return res.end('{"active":true}');
  }
  if(url.pathname.startsWith('/api/gold-data/')){
   const slot=url.pathname.match(/arab_quote_day([1-5])\.json$/);
   res.setHeader('Content-Type','application/json');
   if(!slot)return res.end('{}');
   const date=dates[Number(slot[1])-1],match=structuredClone(payload.matches[0]);
   match.fixture={...match.fixture,fixture_id:date,match:'Partita di prova '+date,
    date,kickoff:date+'T18:00:00Z',status_short:'NS',time:'19:00'};
   const archived=structuredClone(match);
   archived.fixture.status_short='FT';archived.fixture.result={status:'FT',home_ft:2,away_ft:1,home_ht:1,away_ht:0};
   archived._gold_history={version:'gold-history-v1',verified_prematch:true,name:'GOLD',tone:'gold',category:'gold',proposal_id:archived.multimarket.matrix.rows[0].id,captured_at:date+'T10:00:00Z',platinum_ids:[],radar_ids:[]};
   const current_day_archive={...payload,date,history_view:true,history_version:'gold-history-v1',matches:[archived]};
   return res.end(JSON.stringify({...payload,date,updated_at:'2026-10-28T23:22:00Z',matches:[match],current_day_archive}));
  }
  const file=url.pathname.slice(1);
  if(assets.has(file)){
   res.setHeader('Content-Type',file.endsWith('.css')?'text/css':'text/javascript');return res.end(fs.readFileSync('web/'+file));
  }
  res.statusCode=404;res.end();
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const context=await browser.newContext({timezoneId:'America/Los_Angeles'});
 const page=await context.newPage();
 try{
  await page.clock.install({time:new Date('2026-10-29T05:30:00Z')});
  await page.goto('http://127.0.0.1:'+server.address().port+'/gold-dashboard-live');
  await page.waitForFunction(()=>document.getElementById('selectedDay').textContent==='Oggi · 29/10'&&document.querySelectorAll('.mx-card').length===1);
  assert((await page.locator('#fileSource').textContent()).includes('day2.json'));
  assert((await page.locator('.mx-card').textContent()).includes('Partita di prova 2026-10-29'));
  assert(!(await page.locator('#matchResearch').textContent()).includes('2026-10-28'));
  assert((await page.locator('#liveStatus').textContent()).includes('in attesa della nuova scansione'));
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',360,800]]){
   await page.setViewportSize({width,height});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
   await page.screenshot({path:`test-artifacts/calendar-${name}.png`,fullPage:true});
  }
  await page.locator('.day-btn[data-day="0"]').click();
  await page.waitForFunction(()=>document.getElementById('selectedDay').textContent==='Ieri · 28/10'&&document.querySelectorAll('.mx-card').length===1);
  await page.locator('[data-mx-open]').click();
  assert((await page.locator('dialog').textContent()).includes('Proposta prematch conservata'));
  await page.locator('[data-mx-close]').click();
  assert((await page.locator('.mx-card').textContent()).includes('Partita di prova 2026-10-28'));
  assert((await page.locator('.mx-card').textContent()).includes('FT (90 minuti): 2'));
  await page.screenshot({path:'test-artifacts/gold-yesterday-mobile.png',fullPage:true});
  await page.locator('.day-btn[data-day="2"]').click();
  await page.waitForFunction(()=>document.getElementById('fileSource').textContent.includes('day3.json'));
  assert((await page.locator('.mx-card').textContent()).includes('2026-10-30'));
  await page.locator('.day-btn[data-day="5"]').click();
  await page.waitForFunction(()=>document.getElementById('liveStatus').textContent==='Giornata in aggiornamento');
  assert.equal(await page.locator('.mx-card').count(),0);
  assert((await page.locator('#matchResearch').textContent()).includes('02/11 non ancora pubblicati'));
  // New Night publication: the same calendar selection now resolves to day1.
  dates=['2026-10-29','2026-10-30','2026-10-31','2026-11-01','2026-11-02'];
  await page.locator('.day-btn[data-day="1"]').click();
  await page.waitForFunction(()=>document.getElementById('liveStatus').textContent==='Dati caricati · scansione separata');
  assert((await page.locator('#fileSource').textContent()).includes('day1.json'));
  // An already-open page rolls forward in Rome, independently of browser timezone.
  await page.clock.setFixedTime(new Date('2026-10-29T23:01:00Z'));
  await page.clock.runFor(60000);
  await page.waitForFunction(()=>document.getElementById('selectedDay').textContent==='Oggi · 30/10'&&document.getElementById('fileSource').textContent.includes('day2.json'));
  assert((await page.locator('.day-btn.active').textContent()).includes('30/10'));
  assert((await page.locator('.mx-card').textContent()).includes('2026-10-30'));
  console.log('PASS Gold calendar: real HTTP stale/current feeds, desktop/mobile, missing date and open-page Rome midnight');
 }finally{await context.close();await new Promise(resolve=>server.close(resolve));}
}
(async()=>{
 const raw=cp.execFileSync('python',['-c',`
import sys,json
sys.path[:0]=['src','tests']
from test_statistics_first import match,NOW
from test_lateral_matrix import payload
from statistics_first import statistical_analysis
from lateral_matrix import build_matrix
from matrix_quotes import attach
from copy import deepcopy
from test_gold_scope_goals import example
from opportunity_policy import annotate
from platinum_policy import annotate as annotate_platinum
from test_opportunity_policy import row
from lateral_matrix import specification as S, label
from gold_competitions import SCOPE_ID
a=build_matrix(statistical_analysis(match(),NOW),NOW)
variants=[]
for first,last in [(2.5,2.1),(2.1,2.6)]:
 b=deepcopy(a);state={}
 attach(b,payload('2026-10-26T06:00:00Z',first,1.8),state,match()['fixture'],'2026-10-26T06:00:00Z')
 attach(b,payload('2026-10-26T07:00:00Z',(first+last)/2,1.8),state,match()['fixture'],'2026-10-26T07:00:00Z')
 attach(b,payload(NOW,last,1.8),state,match()['fixture'],NOW)
 variants.append(b)
contexts=[]
for args in [('home','1',None),('away','2',None),('away','1',None),('home','2',None),('home','1',.4)]:
 b=deepcopy(a);context=example(*args)
 b['inputs']=context['inputs']
 for source in context['matrix']['rows']:
  target=next(r for r in b['matrix']['rows'] if r['spec']==source['spec'])
  target.update({k:v for k,v in source.items() if k not in ('id','label')})
 annotate(b['matrix'],NOW,b)
 contexts.append(b)
caps=[]
for spec,price in [(S('btts','ht','yes'),5.5),(S('total','ft','over',3.5),4.5),
                   (S('asian_total','ft','over',3.5),4.5),(S('total','ht','over',1.5),3.5),
                   (S('total','ft','over',2.5),2.6),(S('total','ht','over',1.5),3.5001),
                   (S('total','ft','over',3.5),4.5001)]:
 b=deepcopy(a)
 b['matrix']['rows']=[row(spec=spec,period=spec['period'],label=label(spec),odd=price,statistical_pct=60)]
 annotate(b['matrix'],NOW)
 caps.append(b)
combo=deepcopy(next(r for r in a['matrix']['rows'] if r['spec']['kind']=='halves_total'))
reference=deepcopy(next(r for r in a['matrix']['rows'] if r['spec']==S('total','ft','over',2.5)))
reference['oci']=20
caps[0]['matrix']['rows'].extend([reference,combo])
annotate(caps[0]['matrix'],NOW,caps[0])
monitor=[]
watch=row(id='watch-ft',label='Over 2,5 FT',statistical_pct=43,odd=2.5,first_odd=2.5/.98,
          ev_pct=7.5,ev_low_pct=4,oci=30,delta_pct=-2,bookmaker='OPERATORE SINTETICO',
          selected_movement={'delta_pct':-2,'fair_delta_pp':1,'persistence':.5,'reversal':False})
pt=row(id='watch-ht',spec=S('total','ht','over',1.5),period='ht',label='Over 1,5 PT',
       statistical_pct=37,odd=3,first_odd=3/.92,ev_pct=11,ev_low_pct=4,oci=45,bookmaker='OPERATORE SINTETICO')
for rows in [[watch],[pt],[dict(watch,statistical_pct=10,ev_pct=-75,ev_low_pct=-90)],
             [dict(watch,oci=-45)],[dict(watch,selected_margin_pct=8.1)],
             [dict(watch,quality='prior_only',statistical_pct=None,ev_pct=None,ev_low_pct=None,fair_odd=None,ht_evidence=None,goal_evidence=None)],[watch,row(id='official',statistical_pct=60)],
             [dict(watch,quote_status='stale')],[dict(watch,selected_movement=None)]]:
 b=deepcopy(a);b['matrix']['rows']=deepcopy(rows)
 annotate(b['matrix'],NOW)
 monitor.append(b)
assert monitor[0]['matrix']['rows'][0]['opportunity']['reasons']==['probability']
assert monitor[1]['matrix']['rows'][0]['opportunity']['reasons']==['probability']
assert sum(r['opportunity']['eligible'] for b in monitor for r in b['matrix']['rows'])==1
elite=[]
strong=dict(watch,odd=2.5,first_odd=2.5/.96,delta_pct=-4,statistical_pct=None,ev_pct=None,
            ev_low_pct=None,quality='prior_only',ht_evidence=None,goal_evidence=None,
            selected_movement={'delta_pct':-4,'fair_delta_pp':1,'persistence':.75,'reversal':False})
excellent=dict(strong,id='elite-ht',spec=S('total','ht','over',1.5),period='ht',label='Over 1,5 PT',
               odd=1.9,first_odd=1.9/.96,ev_pct=10)
both=row(id='both',odd=2.5,first_odd=2.5/.92,bookmaker='BOOK SINTETICO')
for r in [strong,excellent,both,row(id='gold-only',oci=0),dict(strong,delta_pct=-2,first_odd=2.5/.98,
          selected_movement={'delta_pct':-2,'fair_delta_pp':1,'persistence':.75,'reversal':False}),dict(strong,oci=0),
          dict(strong,ev_pct=-15.0001,statistical_pct=33.99996)]:
 b=deepcopy(a);b['matrix']['rows']=[deepcopy(r)]
 annotate(b['matrix'],NOW,b)
 annotate_platinum(b['matrix'],match()['fixture'],NOW)
 elite.append(b)
assert sum(bool(b['matrix']['platinum_policy']['best_market_id']) for b in elite)==3
print(json.dumps({'variants':variants,'contexts':contexts,'caps':caps,'monitor':monitor,'elite':elite,'scope':SCOPE_ID}))
`],{encoding:'utf8',maxBuffer:10*1024*1024});
 const {variants,contexts,caps,monitor,elite,scope}=JSON.parse(raw),now=Date.now();
 Object.assign(caps[4].matrix.rows[0],{quote_status:'cached_stable',cache_source_at:new Date(now-2*3600000).toISOString(),cache_expires_at:new Date(now+10*3600000).toISOString()});
 const payload={gold_scope:{policy_id:scope},updated_at:new Date(now).toISOString(),multimarket_method:{model_id:'lateral-matrix-v5.0'},matches:Array.from({length:9},(_,i)=>({gold_scope:{policy_id:scope,allowed:true},fixture:{fixture_id:String(i+1),home:'Squadra casa',away:'Squadra ospite',
  kickoff:new Date(now+7200000+i*60000).toISOString(),status_short:'NS',time:'20:45',country:'Italia',league:'DATI SINTETICI DI VERIFICA',match:i===8?'Nome squadra molto lungo per verificare la visualizzazione da telefono – Avversaria':'Squadra '+(i+1)+' – Avversaria'},multimarket:structuredClone(variants[i%2])}))};
 const researchMatch=payload.matches[0];
 const researchMarket=researchMatch.multimarket.matrix.rows.find(row=>row.spec.kind==='total'&&row.spec.period==='ft'&&row.spec.line===2.5&&row.spec.side==='over');
 researchMatch.multimarket.matrix.v7={
  version:'arabsniper-v7.0',status:'shadow',fixture_id:researchMatch.fixture.fixture_id,
  kickoff:researchMatch.fixture.kickoff,captured_at:new Date(now).toISOString(),calibration:'baseline',
  rates:{home_ht:.65,away_ht:.44,home_ft:1.72,away_ft:1.29},means:{},market_projection:{},
  signals:{lineare:{market_id:researchMarket.id,reasons:['probabilita_e_prezzo']}},
  markets:{[researchMarket.id]:{label:'Over 2,5 FT',odd:2.05,raw:.48,calibrated:.52,fair:.49,
   quote:{first_odd:2.23,quote_status:'recent'},features:{delta_pct:-8,oci:24,margin_pct:4}}}
 };
 // UI-only price fixtures exercise all three result cells and movement colors.
 for(const m of payload.matches)for(const row of m.multimarket.matrix.rows){
  if(row.spec.kind==='result'&&row.period==='ft')Object.assign(row,{quote_status:'recent',first_odd:2.5,odd:2.2,delta_pct:-12,bookmaker:'OPERATORE SINTETICO',oci:55,paired_books:3,agreeing_books:3,margin_pct:4,margin_delta_pp:-1,margin_band:'low',ev_pct:-1,ev_low_pct:-2});
 }
 const nearCombo=payload.matches[3].multimarket.matrix.rows.find(r=>r.spec.kind==='halves_total');
 Object.assign(nearCombo,{statistical_pct:55,fair_odd:100/55});
 Object.assign(nearCombo.combo,{active:false,reasons:['combo_probability','combo_rates'],lambda_ht:1.12,lambda_sh:1.8,over25_ft_oci:0});
 const browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH});
 try{
  await verifyCachedRelease(browser,payload);
  await verifyCalendarRollover(browser,payload);
  const page=await browser.newPage();await page.route('**/*',r=>r.abort());
  await page.setContent(fs.readFileSync('web/gold-dashboard-live.html','utf8'));
  await page.addStyleTag({content:fs.readFileSync('web/lateral-matrix.css','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/competition-scope.js','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/lateral-matrix.js','utf8')});
  await page.addScriptTag({content:fs.readFileSync('web/live-scores.js','utf8')});
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  fs.mkdirSync('test-artifacts',{recursive:true});

  const original=await page.evaluate(()=>JSON.stringify(currentPayload));
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844],['small-mobile',360,800]]){
   await page.setViewportSize({width,height});
   assert.equal(await page.locator('.mx-card').count(),9);
   assert.equal(await page.locator('.mx-compact-quotes>section').count(),27);
   assert.equal(await page.locator('.mx-card .mx-cockpit,.mx-card .mx-study-base,.mx-card .mx-best').count(),0);
   assert.equal(await page.locator('.mx-compact-quotes .mx-net-red').count(),27);
   assert((await page.locator('.mx-compact-quotes').first().textContent()).includes('Apertura'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
   await page.locator('.mx-card').first().scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert(await page.locator('dialog.mx-modal').isVisible());
   assert.equal(await page.locator('dialog .mx-cockpit .mx-goal-panel').count(),2);
   const v7=page.locator('dialog .mx-v7-panel');
   assert(await v7.isVisible(),'V7 section shown with compatible feed');
   assert((await v7.innerText()).includes('52,0%'));
   await v7.locator('summary').click();
   assert(await v7.getByText('Poisson originale').isVisible());
   assert(await v7.evaluate(e=>e.scrollWidth<=e.clientWidth),'V7 detail overflow '+name);
   assert.equal(await page.locator('dialog .mx-1x2 .mx-quote-cell').count(),3);
   assert.equal(await page.locator('dialog [data-mx-instrument] option').count(),61);
   assert(!(await page.locator('dialog').innerText()).includes('EV'));
   assert.equal(await page.locator('.mx-modal-scroll > .mx-best').count(),1,'Official analysis moved inside');
   for(const [section,count] of [['ft',8],['ht',8],['result',3],['asian',40],['targets',6]]){
    await page.locator('[data-mx-section="'+section+'"]').click();
    assert.equal(await page.locator('.mx-market-row').count(),count);
   }
   await page.keyboard.press('Shift+Tab');
   assert(await page.evaluate(()=>!!document.activeElement.closest('dialog')));
   assert(await page.locator('dialog').evaluate(e=>e.scrollWidth<=e.clientWidth),'Modal overflow '+name);
   await page.screenshot({path:`test-artifacts/matrix-details-${name}.png`});
   await page.keyboard.press('Escape');
   assert.equal(await page.locator('dialog').count(),0);
   assert(await page.locator('[data-mx-open]').first().evaluate(e=>e===document.activeElement));
   await page.locator('[data-mx-open]').first().click();
   await page.locator('[data-mx-section="ht"]').click();
   await page.evaluate(()=>renderMatchResearch(currentPayload));
   assert.equal(await page.locator('[data-mx-section="ht"]').getAttribute('aria-pressed'),'true');
   await page.locator('[data-mx-close]').click();
   await page.locator('[data-mx-category="combo"]').click();
   assert.equal(await page.locator('.mx-card').count(),5);
   await page.locator('[data-mx-category="all"]').click();
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),original);
  }
  await page.evaluate(()=>{currentPayload.matches[0].fixture.kickoff=new Date(Date.now()-3600000).toISOString();currentPayload.matches[0].fixture.status_short='HT';currentPayload.matches[0].fixture.live={status:'HT',elapsed:45,goals:{home:1,away:1},halftime:{home:1,away:1},observed_at:new Date().toISOString()};renderMatchResearch(currentPayload)});
  assert.equal(await page.locator('[data-live-fixture="1"] [data-live-slot="halftime"]').textContent(),'PT 1–1 · concluso');
  const frozen=await page.evaluate(()=>{window.savedMatrixCard=document.querySelector('.mx-card');return JSON.stringify(currentPayload)});
  await page.evaluate(()=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'1':{fixture_id:'1',status:'2H',elapsed:62,goals:{home:2,away:1},halftime:{home:1,away:1},observed_at:new Date(Date.now()+1000).toISOString()}}});window.dispatchEvent(new Event('arabsniper-live-update'))});
  assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozen);
  assert(await page.evaluate(()=>savedMatrixCard===document.querySelector('.mx-card')));
  assert((await page.locator('.mx-card').first().textContent()).includes('2–1'));
  assert.equal(await page.locator('[data-live-fixture="1"] [data-live-slot="halftime"]').textContent(),'PT 1–1 · concluso','The second-half score never replaces PT');
  for(const [status,goals,halftime,offset,expected] of [
   ['1H',{home:1,away:0},{home:0,away:0},2000,'PT 1–0 · in corso'],
   ['HT',{home:0,away:0},{home:0,away:0},3000,'PT 0–0 · concluso'],
   ['2H',{home:2,away:0},null,4000,'PT 0–0 · concluso']
  ]){
   await page.evaluate(({status,goals,halftime,offset})=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'2':{fixture_id:'2',status,elapsed:status==='1H'?25:status==='HT'?45:60,goals,halftime,observed_at:new Date(Date.now()+offset).toISOString()}}});window.dispatchEvent(new Event('arabsniper-live-update'))},{status,goals,halftime,offset});
   assert.equal(await page.locator('[data-live-fixture="2"] [data-live-slot="halftime"]').textContent(),expected);
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozen);
   assert(await page.evaluate(()=>savedMatrixCard===document.querySelector('.mx-card')));
  }
  await page.locator('[data-live-fixture="2"] header').scrollIntoViewIfNeeded();
  await page.screenshot({path:'test-artifacts/matrix-halftime-live-small-mobile.png'});
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Live overflow');

  // Categories and all research remain accessible inside the compact card.
  for(const [label,variantsToUse] of [['timing',contexts],['caps',caps],['radar',monitor],['platinum',elite]]){
   const sample={...payload,matches:variantsToUse.map((a,i)=>({...payload.matches[i%payload.matches.length],fixture:{...payload.matches[0].fixture,fixture_id:String(100+i),kickoff:new Date(Date.now()+7200000).toISOString()},multimarket:a}))};
   await page.evaluate(p=>{Object.assign(ArabMatrix.state,{category:'all',family:'all',period:'all',phase:'all'});currentPayload=p;renderMatchResearch(p)},sample);
   const snapshot=await page.evaluate(()=>JSON.stringify(currentPayload));
   assert.equal(await page.locator('.mx-card').count(),variantsToUse.length);
   for(let i=0;i<variantsToUse.length;i++){
    await page.locator('[data-mx-open]').nth(i).click();
    assert.equal(await page.locator('dialog .mx-cockpit').count(),1);
    assert(await page.locator('dialog .mx-best').count()>=1,label+' research remains available');
    assert(!/NaN|undefined/.test(await page.locator('dialog').innerText()));
    const select=page.locator('dialog [data-mx-instrument]');
    const choices=await select.locator('option').evaluateAll(opts=>opts.map(o=>o.value).filter(Boolean));
    if(choices.length){await select.selectOption(choices[0]);await page.evaluate(()=>renderMatchResearch(currentPayload));assert.equal(await select.inputValue(),choices[0]);}
    await page.locator('[data-mx-close]').click();
   }
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),snapshot,label+' data immutable');
  }
  // Reproduce the current Gold modal (not the legacy context renderer).
  const singlePayload=structuredClone(payload);singlePayload.matches=singlePayload.matches.slice(0,1);
  const singleRow=singlePayload.matches[0].multimarket.matrix.rows.find(r=>r.spec.kind==='total'&&r.spec.side==='over');
  Object.assign(singleRow,{oci:null,paired_books:1,agreeing_books:1,movement_confirmed:false,
   quote_status:'cached_stable',odd:2,first_odd:2.4,movement_hours:2,
   selected_movement:{fair_delta_pp:2,persistence:1,reversal:false,observed_from:'2026-11-08T08:00:00Z',observed_to:'2026-11-08T10:00:00Z'}});
  for(const width of [1280,390,360]){
   await page.setViewportSize({width,height:900});
   await page.evaluate(p=>{Object.assign(ArabMatrix.state,{category:'all',family:'all',period:'all',phase:'all',search:''});currentPayload=p;renderMatchResearch(p)},singlePayload);
   const before=await page.evaluate(()=>JSON.stringify(currentPayload));
   await page.locator('[data-mx-open]').first().click();
   await page.locator('dialog [data-mx-instrument]').selectOption(singleRow.id);
   const gauge=page.locator('dialog .mx-instrument').first();
   assert((await gauge.innerText()).includes('Singolo bookmaker'));
   assert(!(await gauge.locator('strong').innerText()).includes('—'));
   assert(await page.evaluate(()=>{const d=document.querySelector('dialog');return d.scrollWidth<=d.clientWidth+1}));
   await page.screenshot({path:`test-artifacts/oci-single-${width}.png`});
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),before);
   await page.locator('[data-mx-close]').click();
  }
  const activePayload=structuredClone(singlePayload),activeMatch=activePayload.matches[0];
  activeMatch.fixture.fixture_id='751';
  const matrix=activeMatch.multimarket.matrix,v7=matrix.v7,model=v7.version;
  matrix.publication_model=model;
  Object.assign(v7,{status:'published',fixture_id:'751',tier:'radar',signals:{},markets:{}});
  const picks=[
   ['primary',r=>r.spec.kind==='btts'&&r.period==='ft'&&r.spec.side==='yes','GG FT',1.90,.58],
   ['consigliata',r=>r.spec.kind==='halves_total','Over 0,5 PT + Over 0,5 ST',2.40,.49],
   ['colpaccio',r=>r.spec.kind==='total'&&r.period==='ht'&&r.spec.line===3.5&&r.spec.side==='over','Over 3,5 PT',10.,.12]
  ];
  for(const [role,match,label,odd,prob] of picks){
   const source=matrix.rows.find(match);
   Object.assign(source,{probability_model:model,odd,first_odd:odd,statistical_pct:prob*100,
    quote_executable:true,quote_status:'recent',provider_updated_at:v7.captured_at,quote_observed_at:v7.captured_at});
   v7.signals[role]={market_id:source.id,source_market_id:source.id,odd};
   v7.markets[source.id]={label,odd,source_market_id:source.id,raw:prob,calibrated:prob,fair:prob,
    quote:{first_odd:odd,bookmaker:'Test'},features:{oci:12,margin_pct:5}};
  }
  for(const width of [1280,390,360]){
   await page.setViewportSize({width,height:900});
   await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},activePayload);
   const card=page.locator('.mx-card');
   assert.equal(await card.locator('.mx-category').innerText(),'RADAR BLU');
   assert.equal(await card.locator('.mx-compact-proposal strong').innerText(),'GG FT');
   assert((await card.innerText()).includes('58,0%'));
   assert(!/Consigliata|Colpaccio|Studio V7/.test(await card.innerText()));
   await card.locator('[data-mx-open]').click();
   const modal=page.locator('dialog');
   const headline=modal.locator('[data-v7-role="primary"]');
   assert((await headline.innerText()).includes('GG FT'));
   assert((await headline.innerText()).includes('58,0%'));
   assert.deepEqual(await modal.locator('[data-v7-role]').count(),3);
   assert(!(await modal.locator('.mx-cockpit').isVisible()));
   assert(await modal.evaluate(el=>el.scrollWidth<=el.clientWidth),'V7 sheet fits '+width);
   await modal.locator('[data-v7-details] summary').click();
   assert(await modal.getByText('Poisson originale').isVisible());
   await modal.locator('[data-v7-advanced] > summary').click();
   await modal.locator('[data-mx-section="ht"]').click();
   assert(await modal.locator('.mx-cockpit').isVisible());
   assert.equal(await modal.locator('[data-mx-section="ht"]').getAttribute('aria-pressed'),'true');
   await page.screenshot({path:`test-artifacts/matrix-v7-selection-${width}.png`});
   await page.locator('[data-mx-close]').click();
  }
  console.log('PASS compact Gold browser: 1X2 only, full detail, categories, selector, live overlays, calendar/login/cache, desktop/390/360');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});
