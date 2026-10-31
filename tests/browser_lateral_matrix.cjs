// End-to-end UI on synthetic backend output. No account or provider calls.
const {chromium}=require('playwright');
const fs=require('node:fs'),assert=require('node:assert/strict'),cp=require('node:child_process');
async function verifyCachedRelease(browser,payload){
 const http=require('node:http'),requests=[];
 const html=fs.readFileSync('web/gold-dashboard-live.html','utf8');
 const script=html.match(/<script src="(\/lateral-matrix\.js[^\"]*)"><\/script>/)[1];
 const assets=new Set(['competition-scope.js','statistics-first.js','statistics-first.css','lateral-matrix.js','lateral-matrix.css','live-scores.js']);
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
  await page.evaluate(p=>{document.getElementById('app').hidden=false;document.getElementById('loginOverlay').classList.remove('show');currentPayload=p;renderMatchResearch(p)},payload);
  assert.equal(await page.locator('.mx-card').count(),payload.matches.length,'Current whitelist fixtures remain visible');
  assert((await page.locator('.mx-counters').textContent()).includes('Proposte su 5 mercati gol · Combo PT+ST'));
  assert(!(await page.locator('.mx-dashboard').textContent()).includes('Dati in aggiornamento alla whitelist'));
  fs.mkdirSync('test-artifacts',{recursive:true});
  await page.screenshot({path:'test-artifacts/matrix-cache-rollout.png'});
  console.log('PASS Gold release: cached v6 bundle replaced by versioned v6.1, current whitelist fixtures visible');
 }finally{await page.close();await new Promise(resolve=>server.close(resolve));}
}
async function verifyCalendarRollover(browser,payload){
 const http=require('node:http');
 let dates=['2026-10-28','2026-10-29','2026-10-30','2026-10-31','2026-11-01'];
 const assets=new Set(['competition-scope.js','statistics-first.js','statistics-first.css','lateral-matrix.js','lateral-matrix.css','live-scores.js']);
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
   return res.end(JSON.stringify({...payload,date,updated_at:'2026-10-28T23:22:00Z',matches:[match]}));
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
  await page.waitForFunction(()=>document.getElementById('selectedDay').textContent==='Giorno 1 · 29/10'&&document.querySelectorAll('.mx-card').length===1);
  assert((await page.locator('#fileSource').textContent()).includes('day2.json'));
  assert((await page.locator('.mx-card').textContent()).includes('Partita di prova 2026-10-29'));
  assert(!(await page.locator('#matchResearch').textContent()).includes('2026-10-28'));
  assert((await page.locator('#liveStatus').textContent()).includes('in attesa della nuova scansione'));
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',360,800]]){
   await page.setViewportSize({width,height});
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
   await page.screenshot({path:`test-artifacts/calendar-${name}.png`,fullPage:true});
  }
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
  await page.waitForFunction(()=>document.getElementById('selectedDay').textContent==='Giorno 1 · 30/10'&&document.getElementById('fileSource').textContent.includes('day2.json'));
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
 // UI-only price fixtures exercise all three result cells and movement colors.
 for(const m of payload.matches)for(const row of m.multimarket.matrix.rows){
  if(row.spec.kind==='result'&&row.period==='ft')Object.assign(row,{quote_status:'recent',first_odd:2.5,odd:2.2,delta_pct:-12,bookmaker:'OPERATORE SINTETICO',oci:55,paired_books:3,agreeing_books:3,margin_pct:4,margin_delta_pp:-1,margin_band:'low',ev_pct:-1,ev_low_pct:-2});
 }
 const nearCombo=payload.matches[3].multimarket.matrix.rows.find(r=>r.spec.kind==='halves_total');
 Object.assign(nearCombo,{statistical_pct:55,fair_odd:100/55});
 Object.assign(nearCombo.combo,{active:false,reasons:['combo_probability','combo_rates'],lambda_ht:1.12,lambda_sh:1.8,over25_ft_oci:0});
 const browser=await chromium.launch({headless:true});
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
  for(const [name,width,height] of [['desktop',1280,1000],['mobile',390,844],['small-mobile',360,800]]){
   await page.setViewportSize({width,height});
   assert.equal(await page.locator('.mx-card').count(),9);
   assert.equal(await page.locator('.mx-heading > b').textContent(),'GOLD v6.1');
   assert((await page.locator('.mx-card footer').first().textContent()).includes('Regole gold-goals-v6.1 · Modello statistico lateral-matrix-v5.0'));
   assert.equal(await page.locator('.mx-1x2 .mx-quote-cell').count(),27);
   assert.equal(await page.locator('.mx-best').count(),9);
   const summaries=await page.locator('.mx-best').allTextContents();
   assert(summaries.every(t=>t.includes('Over 0,5 PT + Over 0,5 ST')&&t.includes('Quota book n.d.')));
   assert.equal(summaries.filter(t=>t.includes('★ Combo Consigliata')).length,5);
   assert.equal(summaries.filter(t=>t.includes('Combo non attiva')&&t.includes('OCI -92,5')).length,3);
   assert.equal(summaries.filter(t=>t.includes('Combo vicina · non attiva')).length,1);
   assert(summaries.every(t=>t.includes('Over 2,5 PT · Stima')));
   assert(summaries.every(t=>!t.includes('1,85–2,60')&&!t.includes('Soglie statistiche e controlli')));
   assert.equal(await page.locator('.mx-quote-legend').count(),0);
   const tones=await page.locator('.mx-best [class^="mx-combo-"]').evaluateAll(nodes=>nodes.map(n=>({tone:n.className,color:getComputedStyle(n).color,bold:+getComputedStyle(n).fontWeight})));
   assert.equal(tones.filter(t=>t.tone==='mx-combo-active'&&t.color==='rgb(129, 225, 180)'&&t.bold>=700).length,5);
   assert.equal(tones.filter(t=>t.tone==='mx-combo-near'&&t.color==='rgb(255, 224, 128)').length,1);
   assert.equal(tones.filter(t=>t.tone==='mx-combo-inactive'&&t.color==='rgb(255, 157, 144)').length,3);
   assert(!summaries.some(t=>t.includes('Confronto dei quattro mercati')));
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5);
   const proposals=await page.locator('.mx-best[data-opportunity-id]').evaluateAll(nodes=>nodes.map(n=>({odd:+n.dataset.opportunityOdd,max:+n.dataset.opportunityMaxOdd,p:+n.dataset.opportunityProbability,text:n.textContent})));
   assert(proposals.every(r=>r.odd>=1.85&&r.odd<=r.max&&[2.6,3.5,4.5,5.5,7.5].includes(r.max)&&r.p>=18));
   assert(proposals.some(r=>r.text.includes('Convergenza di Trend')));
   assert(!proposals.some(r=>r.text.includes('Contrarian sano')));
   assert.equal(await page.locator('.mx-1x2 .mx-net-red').count(),27);
   assert((await page.locator('.mx-1x2').first().textContent()).includes('Apertura'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'List overflow '+name);
   await page.screenshot({path:`test-artifacts/matrix-${name}.png`});
   await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-proposal-${name}.png`});
   await page.locator('.mx-best').nth(3).scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-combo-near-${name}.png`});
   await page.locator('.mx-best').nth(1).scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-combo-inactive-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert(await page.locator('dialog.mx-modal').isVisible(),'Matrix must be a native modal');
   assert.equal(await page.locator('.mx-market-row').count(),6,'Five goal targets and the half-time combo open by default');
   const comboRow=page.locator('.mx-market-row').filter({hasText:'Over 0,5 PT + Over 0,5 ST'});
   assert((await comboRow.textContent()).includes('Combo Attiva'));
   assert((await comboRow.textContent()).includes('EV non calcolabile senza quota reale'));
   assert.equal(await page.locator('.mx-card .mx-detail').count(),0,'No inline expansion');
   await page.keyboard.press('Shift+Tab');
   assert(await page.evaluate(()=>!!document.activeElement.closest('dialog.mx-modal')),'Native modal focus trap');
   for(const [section,count] of [['ft',8],['ht',6],['result',3],['asian',40],['targets',6]]){
    await page.locator('[data-mx-section="'+section+'"]').click();
    assert.equal(await page.locator('.mx-market-row').count(),count);
    assert.equal(await page.locator('.mx-modal-scroll').evaluate(e=>e.scrollTop),0,'Changing section starts at its first row');
    if(section==='asian')await page.locator('.mx-modal-scroll').evaluate(e=>{e.scrollTop=e.scrollHeight});
   }
   const text=await page.locator('dialog.mx-modal').textContent();
   assert(!/Risultato esatto|Over 5,5|Under 5,5|Multigol/.test(text),'Dead markets must be absent');
   assert(await page.evaluate(()=>{const e=document.querySelector('dialog.mx-modal');return e.scrollWidth<=e.clientWidth}),'Modal horizontal overflow');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Matrix overflow '+name);
   await page.locator('[data-mx-close]').focus();
   await page.screenshot({path:`test-artifacts/matrix-details-${name}.png`});
   await page.keyboard.press('Escape');
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0);
   assert(await page.locator('[data-mx-open]').first().evaluate(e=>e===document.activeElement),'Focus restored after Escape');
   await page.locator('[data-mx-open]').first().click();
   await page.locator('[data-mx-section="ht"]').click();
   await page.evaluate(()=>renderMatchResearch(currentPayload));
   assert(await page.locator('dialog.mx-modal').isVisible(),'Fast render preserves modal');
   assert.equal(await page.locator('[data-mx-section="ht"]').getAttribute('aria-pressed'),'true');
   assert.equal(await page.locator('.mx-market-row').count(),6);
   await page.locator('[data-mx-close]').click();
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0);
   await page.locator('[data-mx-open]').first().click();
   await page.mouse.click(1,1);
   await page.locator('dialog.mx-modal').waitFor({state:'detached',timeout:5000});
   assert.equal(await page.locator('dialog.mx-modal').count(),0,'Backdrop closes modal');
   assert(await page.evaluate(()=>document.body.style.overflow!=='hidden'),'Body scrolling restored');
   const beforeComboFilter=await page.evaluate(()=>JSON.stringify(currentPayload));
   await page.locator('[data-mx-filter="family"]').selectOption('asian');
   await page.locator('[data-mx-filter="period"]').selectOption('ht');
   await page.getByRole('button',{name:'Combo 5',exact:true}).click();
   assert.equal(await page.locator('[data-mx-category="combo"]').getAttribute('aria-pressed'),'true');
   assert.equal(await page.locator('[data-mx-filter="family"]').inputValue(),'all');
   assert.equal(await page.locator('[data-mx-filter="period"]').inputValue(),'all');
   assert.equal(await page.locator('.mx-card').count(),5);
   assert.equal(await page.locator('.mx-best .mx-combo-active').count(),5);
   assert.equal(await page.locator('.mx-best .mx-combo-near,.mx-best .mx-combo-inactive').count(),0);
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5,'Combo filtering cannot replace official proposals');
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),beforeComboFilter);
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Combo filter overflow '+name);
   await page.locator('[data-mx-category="combo"]').scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-combo-filter-${name}.png`});
   await page.locator('[data-mx-category="all"]').click();
   assert.equal(await page.locator('.mx-card').count(),9);
  }
  await page.locator('[data-mx-category="divergence"]').click();
  assert.equal(await page.locator('.mx-card').count(),4);
  await page.locator('[data-mx-category="all"]').click();
  await page.locator('[data-mx-filter="family"]').selectOption('asian');
  await page.locator('[data-mx-filter="period"]').selectOption('ht');
  assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5,'Matrix filters must not replace the match proposal');
  await page.locator('[data-mx-open]').first().click();
  await page.locator('[data-mx-section="ht"]').click();
  assert.equal(await page.locator('.mx-market-row').count(),6);
  assert((await page.locator('.mx-market-row').first().textContent()).includes('PT'));
  await page.locator('[data-mx-close]').click();
  await page.locator('[data-mx-filter="family"]').selectOption('all');
  await page.locator('[data-mx-filter="period"]').selectOption('all');
  await page.locator('[data-mx-category="no_quote"]').click();
  assert.equal(await page.locator('.mx-card').count(),9);
  await page.locator('[data-mx-category="all"]').click();
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
  const timed={...payload,matches:contexts.map((a,i)=>({...payload.matches[i],multimarket:a}))};
  await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},timed);
  const timing=await page.locator('.mx-timing:not(.mx-first-half)').allTextContents();
  assert(timing[0].includes('Ordine di valutazione: Over 1,5 PT')&&timing[0].includes('Favorita in casa'));
  assert(timing[1].includes('Ordine di valutazione: Over 1,5 PT')&&timing[1].includes('Favorita in trasferta'));
  assert(timing[2].includes('Ordine di valutazione: Goal PT')&&timing[3].includes('Ordine di valutazione: Goal PT'));
  assert(timing[0].includes('Precedenza tra idonei'));
  assert(timing[4].includes('Quote bilanciate')&&timing[4].includes('0,40'));
  assert((await page.locator('.mx-best h4').allTextContents())[2].includes('Goal'));
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Drought priority overflow');
  await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
  await page.screenshot({path:'test-artifacts/matrix-drought-priority-mobile.png'});
  const capped={...payload,matches:caps.map((a,i)=>({...payload.matches[i],multimarket:a}))};
  await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},capped);
  for(const [name,width] of [['desktop',1280],['mobile',390],['small-mobile',360]]){
   await page.setViewportSize({width,height:844});
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),5);
   assert.equal(await page.locator('.mx-best[data-opportunity-odd="5.5"]').count(),1);
   assert.equal(await page.locator('.mx-best-empty').count(),2);
   assert((await page.locator('.mx-best').nth(4).textContent()).includes('Quota stabile da cache'));
   assert((await page.locator('.mx-best').first().textContent()).includes('minimo 26'));
   assert(!(await page.locator('.mx-best').first().textContent()).includes('2,10–3,50'));
   assert.equal(await page.locator('.mx-first-half').count(),2);
   assert((await page.locator('.mx-best').first().textContent()).includes('★ Combo Consigliata'));
   assert((await page.locator('.mx-first-half').first().textContent()).includes('xG PT non disponibili'));
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Market caps overflow '+name);
   await page.locator('.mx-best').first().scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-market-caps-${name}.png`});
   await page.locator('[data-mx-open]').first().click();
   assert((await page.locator('.mx-market-row').first().textContent()).includes('Idoneo'));
   assert(await page.evaluate(()=>{const e=document.querySelector('dialog.mx-modal');return e.scrollWidth<=e.clientWidth}),'Market caps modal overflow');
   await page.locator('[data-mx-close]').click();
  }
  const monitored={...payload,matches:monitor.map((a,i)=>({...payload.matches[i],fixture:{...payload.matches[i].fixture,fixture_id:String(101+i)},multimarket:a}))};
  await page.evaluate(p=>{currentPayload=p;renderMatchResearch(p)},monitored);
  const monitorBefore=await page.evaluate(()=>JSON.stringify(currentPayload));
  for(const [name,width] of [['desktop',1280],['mobile',390],['small-mobile',360]]){
   await page.setViewportSize({width,height:844});
   assert.equal(await page.locator('.mx-best[data-market-signal-id]').count(),5);
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),1);
   assert.equal(await page.locator('.mx-best-empty').count(),3);
   const blue=page.locator('.mx-best-market').first();
   assert((await blue.textContent()).includes('Radar di Mercato · Statistica debole o assente · Solo studio'));
   assert((await blue.textContent()).includes('Solo studio · esclusa dalle giocate Gold ufficiali.'));
   assert.equal(await blue.evaluate(e=>getComputedStyle(e).backgroundColor),'rgb(16, 47, 80)');
   assert.equal(await blue.locator('h4').evaluate(e=>getComputedStyle(e).color),'rgb(158, 216, 255)');
   assert.equal(await blue.getAttribute('data-opportunity-id'),null);
   const missingStats=page.locator('[data-live-fixture="106"] .mx-best-market');
   assert((await missingStats.textContent()).includes('Radar di Mercato · Statistica debole o assente · Solo studio'));
   assert((await missingStats.textContent()).includes('Persistenza del movimento'));
   assert(!(await missingStats.textContent()).includes('minimo Gold'));
   assert(!/NaN|undefined/.test(await missingStats.textContent()));
   assert.equal(await page.locator('[data-live-fixture="103"] .mx-best-market').count(),1,'Negative model EV cannot veto price monitoring');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Blue signals overflow '+name);
   await blue.scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-market-signal-${name}.png`});
   await page.locator('[data-mx-category="market_signal"]').click();
   assert.equal(await page.locator('.mx-card').count(),5);
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),0);
   assert.equal(await page.locator('[data-mx-category="market_signal"] b').textContent(),'5');
   await page.locator('[data-mx-filter="period"]').selectOption('ht');
   assert.equal(await page.locator('.mx-card').count(),1);
   assert((await page.locator('.mx-best h4').textContent()).includes('Over 1,5 PT'));
   await page.locator('[data-mx-filter="period"]').selectOption('all');
   await page.locator('[data-mx-open]').first().click();
   const signalRow=page.locator('.mx-market-row').first();
   assert((await signalRow.textContent()).includes('Radar di Mercato · Solo studio'));
   assert((await signalRow.textContent()).includes('Scartato · stima bassa'));
   assert(!/Idoneo/.test(await signalRow.textContent()));
   assert(await page.evaluate(()=>{const e=document.querySelector('dialog.mx-modal');return e.scrollWidth<=e.clientWidth}),'Blue modal overflow '+name);
   await page.screenshot({path:`test-artifacts/matrix-market-signal-detail-${name}.png`});
   await page.locator('[data-mx-close]').click();
   await page.locator('[data-mx-category="all"]').click();
   assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),1);
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),monitorBefore,'Monitoring leaves backend decisions and ranks unchanged');
  }
  await page.evaluate(()=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'101':{fixture_id:'101',status:'1H',elapsed:1,goals:{home:0,away:0},observed_at:new Date(Date.now()+1000).toISOString()}}});renderMatchResearch(currentPayload)});
  assert.equal(await page.locator('.mx-best[data-market-signal-id]').count(),4,'A live overlay closes monitoring even before the scheduled kickoff');
  assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),monitorBefore,'The live lifecycle never rewrites prematch decisions');
  assert.equal(await page.locator('.mx-best[data-opportunity-id]').count(),1);
  await page.locator('[data-mx-open]').first().click();
  assert.equal(await page.locator('dialog .mx-market-signal').count(),0,'The matrix also respects the live lifecycle');
  await page.locator('[data-mx-close]').click();
  await page.locator('[data-mx-category="market_signal"]').click();
  assert.equal(await page.locator('.mx-card').count(),4);
  console.log('PASS market radar: absent statistics and negative EV, separate category/counts, official Gold unchanged, blue desktop/mobile cards and matrix, no live reclassification');
  const tiered={...payload,matches:elite.map((a,i)=>({...payload.matches[i],fixture:{...payload.matches[i].fixture,fixture_id:String(501+i),match:'Verifica Platinum '+(i+1)},multimarket:structuredClone(a)}))};
  for(const m of tiered.matches){
   m.multimarket.matrix.platinum_policy.evaluated_at=new Date(now).toISOString();
   for(const r of m.multimarket.matrix.rows)r.platinum.evaluated_at=new Date(now).toISOString();
  }
  await page.evaluate(p=>{Object.assign(ArabMatrix.state,{category:'all',period:'all',family:'all',phase:'all',search:''});currentPayload=p;renderMatchResearch(p)},tiered);
  const frozenTiers=await page.evaluate(()=>JSON.stringify(currentPayload));
  for(const [name,width] of [['desktop',1280],['mobile',390],['small-mobile',360]]){
   await page.setViewportSize({width,height:844});
   assert.equal(await page.locator('.mx-card-platinum').count(),3);
   assert.equal(await page.locator('.mx-match-layout > .mx-best-platinum').count(),3);
   assert.equal(await page.locator('.mx-match-layout > .mx-best-stable').count(),1,'Official Gold remains available');
   assert.equal(await page.locator('.mx-best-market').count(),2);
   const silver=page.locator('[data-live-fixture="501"]');
   assert.equal(await silver.locator('header .mx-tier-label').textContent(),'PLATINUM');
   assert.equal(await silver.locator('header .mx-tier-label').evaluate(e=>getComputedStyle(e).color),'rgb(237, 243, 250)');
   assert(+await silver.locator('header .mx-tier-label strong').evaluate(e=>getComputedStyle(e).fontWeight)>=700);
   assert.equal(await silver.locator('.mx-best-platinum').getAttribute('data-opportunity-id'),null);
   assert(!(await silver.locator('.mx-best-platinum').textContent()).includes('Nessuna proposta supera'));
   assert(!/NaN|undefined/.test(await silver.textContent()));
   await page.locator('[data-mx-category="platinum"]').click();
   assert.equal(await page.locator('.mx-card').count(),3);
   assert.equal(await page.locator('[data-mx-category="platinum"] b').textContent(),'3');
   assert.equal(await page.locator('[data-mx-category="market_signal"] b').textContent(),'2');
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'Platinum overflow '+name);
   await page.locator('[data-mx-category="platinum"]').scrollIntoViewIfNeeded();
   await page.screenshot({path:`test-artifacts/matrix-platinum-${name}.png`});
   await page.locator('[data-mx-filter="period"]').selectOption('ht');
   assert.equal(await page.locator('.mx-card').count(),1);
   assert((await page.locator('.mx-best-platinum h4').textContent()).includes('Over 1,5 PT'));
   await page.locator('[data-mx-filter="period"]').selectOption('all');
   await page.locator('[data-mx-open]').first().click();
   assert.equal(await page.locator('dialog .mx-market-row .mx-platinum').count(),1);
   assert.equal(await page.locator('dialog .mx-market-row .mx-market-signal').count(),0);
   assert((await page.locator('dialog .mx-market-row').textContent()).includes('Gold: Scartato'));
   await page.locator('[data-mx-close]').click();
   const official=page.locator('[data-live-fixture="503"] .mx-platinum-gold');
   await official.locator('summary').click();
   assert.equal(await official.locator('[data-opportunity-id]').count(),1);
   await official.locator('summary').click();
   await page.locator('[data-mx-category="market_signal"]').click();
   assert.equal(await page.locator('.mx-card').count(),2);
   const rejected=page.locator('[data-live-fixture="507"]');
   assert.equal(await rejected.count(),1);
   assert.equal(await rejected.locator('.mx-best-platinum').count(),0);
   assert((await rejected.textContent()).includes('Radar di Mercato'));
   await page.locator('[data-mx-category="all"]').click();
   assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozenTiers);
  }
  await page.evaluate(()=>{ArabLive.accept({schema_version:1,enabled:true,fixtures:{'501':{fixture_id:'501',status:'1H',elapsed:1,goals:{home:0,away:0},observed_at:new Date(Date.now()+1000).toISOString()}}});renderMatchResearch(currentPayload)});
  await page.locator('[data-mx-category="platinum"]').click();
  assert.equal(await page.locator('.mx-card').count(),3,'A stored prematch Platinum stays visible live');
  await page.locator('[data-mx-filter="phase"]').selectOption('upcoming');
  assert.equal(await page.locator('.mx-card').count(),2);
  assert.equal(await page.evaluate(()=>JSON.stringify(currentPayload)),frozenTiers);
  console.log('PASS Platinum browser: backend decisions, silver title/cards/filter, no-statistics and EV routes, separate Gold, immutable live, desktop/390/360');
  console.log('PASS Gold v6.1 browser: concise cards, green/yellow/red combos, versions, quote coverage, unchanged desktop/390/360 grid, modal and immutable live overlays');
 }finally{await browser.close()}
})().catch(e=>{console.error(e);process.exitCode=1});

